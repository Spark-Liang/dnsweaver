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
`DNSWEAVER_PROXMOX_LIFECYCLE_STATE_FILE` to a durable path. The temporary file
is mode `0600`, newly created parent directories are mode `0700`, and the
temporary file and containing directory are synced before the update is
considered complete. The in-process mutex serializes state reads and writes.
Corrupt or unsupported-schema state is an error and blocks orphan cleanup; it
is not treated as an empty state file.
This slice does not provide a cross-process lock, so a deployment must ensure
that exactly one dnsweaver process owns a state file. Stateful mode is not
production-ready when that deployment gate cannot be enforced.

## Identity and generations

The persisted instance key is:

```
actual PVE cluster name + resource type + VMID
```

The node and the display name are attributes, not identity. A node migration
therefore keeps the same key and last-confirmed address. A name change for the
same key starts a new generation and clears the old address before a new
address is confirmed. Confirmed deletion leaves a generation tombstone so a
later VMID reuse cannot inherit the old address.

The generation is only a local counter; it is not an instance fingerprint
reported or verifiable by PVE. If a VMID is reused with the same name before a
complete inventory has observed the prior instance absent, this design cannot
distinguish the new VM from the old one. The PVE cluster name is also not a
globally unique identity. Keep each state file dedicated to one PVE cluster and
never reuse it across clusters. `LIFECYCLE_SCOPE_CONFIRMED` remains an operator
assertion and does not prove inventory visibility or permissions.

The original PVE `name` is retained in `workload.Workload.Name`. Stateful DNS
names use the strict form:

```
<PVE name>.<actual PVE cluster name>.<VM_ZONE>
```

The cluster name is read from the `type=cluster` entry of `/cluster/status`;
the API endpoint host is never used as a silent substitute. `VM_ZONE` is an
explicit deployment setting (`DNSWEAVER_PROXMOX_VM_ZONE` or
`proxmox.vm_zone`) because it is the DNS placement zone and is not a PVE
cluster property. Stateful mode ignores `HOSTNAME_TAG_PREFIX` hostname
overrides so a tag cannot replace the PVE name. Legacy mode keeps the existing
tag and domain behavior.

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
operator assertion that the API token can see the complete VM scope. This flag
does not inspect PVE permissions, pagination, or scope automatically; it is a
deployment declaration that must be verified by the operator. The default
policy requires two consecutive complete absent observations; tune it with
`DNSWEAVER_PROXMOX_LIFECYCLE_DELETE_OBSERVATIONS`. Until confirmation,
the adapter emits the retained workload (when it has an address) and reports
that orphan cleanup is unsafe. An initially empty inventory is not sufficient
because the state file must first record a non-empty complete inventory.

The reconciler consumes the optional lister snapshot status and suppresses
member removals while the Proxmox snapshot is incomplete or lifecycle state is
uncertain. A restarted reconciler must call its existing `RecoverOwnership`
startup hook so provider ownership markers repopulate the in-memory orphan
state; the lifecycle JSON persists the deletion observation and tombstone
independently. This is a conservative cycle-wide removal gate in this first
slice; it does not alter source/provider behavior in legacy mode.

## Name collision freeze

When two distinct identities among the current managed inventory and retained,
unconfirmed lifecycle records produce the same normalized DNS name, the adapter
reports the name as blocked and the reconciler skips all sets at that name.
Resources outside the node or tag filters remain visible to the complete
inventory safety gate, but do not freeze names that this adapter is not
managing. Existing records remain untouched; no automatic create, update,
delete, or automatic numeric suffix is attempted. A retained `absent-pending`
identity freezes the name even when it has no qualified address and therefore
produces no DNS claim. The name is unblocked only after the other identity is
confirmed deleted or is outside the configured management filters.

## Unowned provider records

With ownership tracking enabled and effective adoption disabled, a stateful
Proxmox claim also freezes its normalized DNS name when the provider snapshot
contains any member without matching ownership evidence. For providers without
ownership TXT support, an exact member from this process's previous desired set
is also evidence of local ownership; that evidence is in-memory and is not
restored after restart. The freeze covers all record types at that name,
including A, AAAA, ordinary TXT, type replacement, TTL changes, ownership
writes, and pending recovery or repair for any member that does not match
ownership evidence. A stateful pending member-ownership repair keeps its old DNS
name in preflight after the PVE name changes, and may retry only when its locally
recorded member still matches the observed target, TTL, and provider options. A
legacy hostname-level ownership marker can identify one old-style member, but
does not prove that additional siblings were created by dnsweaver; it never
authorized MX or NS records. Uncontested names continue to reconcile even when
lifecycle orphan cleanup is not enabled by the PVE scope declaration.

Provider ownership migration is backend-scoped rather than name-scoped. While
a stateful name is frozen, migration is held for that backend to prevent a
migration write from touching the name; ordinary reconciliation for other
names remains available. The migration resumes after the name is no longer
blocked.

## Address selection

The existing `IP_VERSION`, interface allow-list, and interface tag preference
remain the source of qualified addresses. In stateful mode, a matching
interface tag is exclusive even when an allow-list is also configured; if that
interface has no qualified address, the adapter does not fall back to the
allow-list. Strict selection prevents an unqualified interface from silently
replacing the last-confirmed address.
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
