# Proxmox lifecycle state (fork design)

> This document describes the personal fork design for dnsweaver v3.3.2. It
> is not an upstream design and is **not end-to-end verified**.

## Scope

`DNSWEAVER_PROXMOX_LIFECYCLE_MODE=stateful` enables the lifecycle adapter. The
default `legacy` mode keeps the v3.3.2 behavior and data path. The stateful
path still uses the PVE cluster resource API, the normal periodic reconciler,
the existing Proxmox source, and the configured DNS provider.

The state file is JSON and is replaced atomically. Its default path is
`proxmox-lifecycle.json`; production deployments should set
`DNSWEAVER_PROXMOX_LIFECYCLE_STATE_FILE` to a durable path. The in-process
mutex serializes state reads and writes. This slice does not provide a
cross-process lock, so one dnsweaver process must own a state file.

## Identity and generations

The persisted instance key is:

```
normalized PVE endpoint identity + resource type + VMID
```

The node and the display name are attributes, not identity. A node migration
therefore keeps the same key and last-confirmed address. A name change for the
same key starts a new generation and clears the old address before a new
address is confirmed. Confirmed deletion leaves a generation tombstone so a
later VMID reuse cannot inherit the old address.

The original PVE `name` is retained in `workload.Workload.Name`.

## States

The state file records a base state for each observed instance:

- `running+address`: a successful resolution produced at least one qualified
  address; it becomes the new last-confirmed address.
- `stopped`: PVE reported a non-running resource; the last-confirmed address
  and DNS name are retained.
- `qga-unavailable` / `qga-timeout`: QGA was unavailable or timed out; the
  last-confirmed address and DNS name are retained.
- `successful-empty`: the resolution completed successfully but no address
  passed the existing family/interface filters; the last-confirmed address is
  retained.
- `inventory-unknown` / `absent-pending`: the resource was not present in the
  current inventory, but the observation is not yet deletion evidence. The
  record is retained and orphan removal is held.
- `confirmed-absent/deleted`: after the configured consecutive complete
  inventory observations, the instance is removed from the state snapshot and
  its old DNS desired state may be retired by the reconciler.
- `name-collision`: more than one lifecycle identity maps to the same
  normalized Proxmox DNS name. The underlying state is retained separately.

QGA absence is still returned by the client as `*ErrAgentNotRunning`; only the
stateful adapter asks the resolver to preserve that transient error. Legacy
callers continue to receive the historical empty-address result.

## Inventory and deletion confirmation

The stateful adapter requires a successful JSON array response from
`cluster/resources?type=vm`. A PVE API error, a null/missing data payload, an
IP-resolution error other than QGA absence/timeout, or a failed state write
returns an error and suppresses reconciler removals.

Deletion also requires `DNSWEAVER_PROXMOX_LIFECYCLE_SCOPE_CONFIRMED=true`, an
operator assertion that the API token can see the complete VM scope. The
default policy requires two consecutive complete absent observations; tune it
with `DNSWEAVER_PROXMOX_LIFECYCLE_DELETE_OBSERVATIONS`. Until confirmation,
the adapter emits the retained workload (when it has an address) and reports
that orphan cleanup is unsafe. An initially empty inventory is not sufficient
because the state file must first record a non-empty complete inventory.

The reconciler consumes the optional lister snapshot status and suppresses
member removals while the Proxmox snapshot is incomplete or lifecycle state is
uncertain. This is a conservative cycle-wide removal gate in this first slice;
it does not alter source/provider behavior in legacy mode.

## Name collision freeze

When two distinct stateful identities produce the same normalized DNS name,
the adapter reports the name as blocked and the reconciler skips all sets at
that name. Existing records remain untouched; no automatic create, update,
delete, or automatic numeric suffix is attempted. The name is unblocked only
after the inventory contains a single remaining identity.

## Address selection

The existing `IP_VERSION`, interface allow-list, and interface tag preference
remain the source of qualified addresses. Stateful mode additionally enables
strict selection when a preference or allow-list is configured, so an
unqualified interface cannot silently replace the last-confirmed address.
When no qualified address exists, the stateful adapter retains the previous
address instead of treating the workload as absent.

## Reconciler connection points

1. `internal/proxmox.WorkloadListerAdapter` persists identity/state and emits
   a workload carrying the retained address.
2. `pkg/workload.SnapshotStatusProvider` exposes completeness, removal safety,
   and collision names without making the reconciler Proxmox-specific.
3. `internal/reconciler` folds those optional signals into its existing
   desired-set removal gate and freezes blocked normalized names.
4. `sources/proxmox` and all provider implementations continue to receive the
   ordinary desired hostname/record flow.
