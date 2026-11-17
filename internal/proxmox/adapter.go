package proxmox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/maxfield-allison/dnsweaver/pkg/workload"
)

const (
	resourceTypeLXC  = "lxc"
	resourceTypeQEMU = "qemu"
	tagLabelValue    = "true"

	stateRunning = "running"
)

// Metadata/log field keys shared between logging and the toWorkload metadata map.
const (
	metaKeyNode   = "node"
	metaKeyVMID   = "vmid"
	metaKeyTags   = "tags"
	metaKeyStatus = "status"
	metaKeyPool   = "pool"
	metaKeyIP     = "ip"
	metaKeyIP6    = "ip6"
)

// IPVersion selects which address families the Proxmox source resolves.
type IPVersion string

const (
	// IPVersionIPv4 resolves IPv4 only and emits A records. This is the
	// default and preserves historical behavior.
	IPVersionIPv4 IPVersion = "ipv4"

	// IPVersionIPv6 resolves IPv6 only and emits AAAA records.
	IPVersionIPv6 IPVersion = "ipv6"

	// IPVersionDual resolves both families, emitting an A record and an AAAA
	// record for guests that have both.
	IPVersionDual IPVersion = "dual"
)

// ParseIPVersion parses a string into an IPVersion. Empty input maps to the
// default (IPVersionIPv4). Returns an error for unrecognized values.
func ParseIPVersion(s string) (IPVersion, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", string(IPVersionIPv4), "4", "v4":
		return IPVersionIPv4, nil
	case string(IPVersionIPv6), "6", "v6":
		return IPVersionIPv6, nil
	case string(IPVersionDual), "both", "dualstack", "dual-stack":
		return IPVersionDual, nil
	default:
		return "", fmt.Errorf("invalid proxmox ip version %q (must be one of: ipv4, ipv6, dual)", s)
	}
}

// wantsV4 reports whether A records should be produced.
func (v IPVersion) wantsV4() bool {
	return v != IPVersionIPv6
}

// wantsV6 reports whether AAAA records should be produced.
func (v IPVersion) wantsV6() bool {
	return v == IPVersionIPv6 || v == IPVersionDual
}

// AdapterConfig holds filtering options for the WorkloadListerAdapter.
type AdapterConfig struct {
	// NodeFilter restricts listing to a specific Proxmox node name.
	// Empty string means all nodes.
	NodeFilter string

	// TagFilter restricts listing to resources that have at least one tag
	// with the given prefix (e.g., "dnsweaver"). Matching is case-sensitive.
	// Empty string means all resources regardless of tags.
	TagFilter string

	// StateFilter restricts listing to resources in the given state.
	// Typical values: "running", "stopped". Defaults to "running" if empty.
	StateFilter string

	// InterfacePreference selects a specific guest interface name to use for IP resolution.
	// Empty string means no preference and the adapter will use the allow-list below.
	InterfacePreference string

	// AllowedInterfaces is a list of interface names that are allowed for IP resolution.
	// If provided, the resolver will only consider those interfaces in order.
	AllowedInterfaces []string

	// IPVersion selects which address families to resolve.
	// Defaults to IPVersionIPv4 if empty.
	IPVersion IPVersion

	// LifecycleMode enables the persisted Proxmox lifecycle adapter when set to
	// "stateful". Empty and "legacy" preserve v3.3.2 behavior.
	LifecycleMode string

	// LifecycleStateFile is the JSON state path used by stateful mode.
	LifecycleStateFile string

	// LifecycleScopeConfirmed is an explicit operator assertion that the PVE
	// account can see the complete VM inventory. Without it, absence never
	// becomes deletion evidence.
	LifecycleScopeConfirmed bool

	// LifecycleDeleteObservations is the number of complete consecutive absent
	// snapshots required before an instance is confirmed deleted.
	LifecycleDeleteObservations int

	// DomainSuffix and HostnameTagPrefix mirror the legacy Proxmox source.
	// Stateful mode uses VMZone and the actual cluster name from /cluster/status
	// for strict DNS naming; hostname tag overrides are ignored there.
	DomainSuffix      string
	HostnameTagPrefix string
	VMZone            string // Final DNS zone for stateful names.
}

// WorkloadListerAdapter wraps a Proxmox Client to implement the workload.Lister interface.
// It fetches VMs and LXC containers from the Proxmox cluster, applies configured
// filters, resolves IP addresses, and converts each resource to a platform-agnostic
// workload.Workload.
type WorkloadListerAdapter struct {
	client *Client
	cfg    AdapterConfig
	logger *slog.Logger
	store  *lifecycleStore

	snapshotMu sync.RWMutex
	snapshot   workload.SnapshotStatus
}

// NewWorkloadListerAdapter creates a new adapter that wraps a Proxmox Client.
func NewWorkloadListerAdapter(c *Client, cfg AdapterConfig, logger *slog.Logger) *WorkloadListerAdapter {
	ipVersion := cfg.IPVersion
	if ipVersion == "" {
		ipVersion = IPVersionIPv4
	}
	lifecycleMode := strings.ToLower(strings.TrimSpace(cfg.LifecycleMode))
	if lifecycleMode == "" {
		lifecycleMode = LifecycleModeLegacy
	}
	deleteObservations := cfg.LifecycleDeleteObservations
	if deleteObservations < 1 {
		deleteObservations = DefaultLifecycleDeleteObservations
	}
	stateFile := cfg.LifecycleStateFile
	if stateFile == "" {
		stateFile = DefaultLifecycleStateFile
	}
	stateFilter := cfg.StateFilter
	if lifecycleMode == LifecycleModeStateful {
		// A lifecycle inventory must retain stopped resources. The legacy
		// running-only default is intentionally bypassed only in opt-in mode.
		stateFilter = ""
	} else if strings.TrimSpace(stateFilter) == "" {
		stateFilter = stateRunning
	}
	snapshot := workload.SnapshotStatus{Complete: true, AllowOrphanCleanup: true}
	var store *lifecycleStore
	if lifecycleMode == LifecycleModeStateful && c != nil {
		store = newLifecycleStore(stateFile)
	}
	return &WorkloadListerAdapter{
		client: c,
		cfg: AdapterConfig{
			NodeFilter:                  cfg.NodeFilter,
			TagFilter:                   cfg.TagFilter,
			StateFilter:                 stateFilter,
			InterfacePreference:         cfg.InterfacePreference,
			AllowedInterfaces:           cfg.AllowedInterfaces,
			IPVersion:                   ipVersion,
			LifecycleMode:               lifecycleMode,
			LifecycleStateFile:          stateFile,
			LifecycleScopeConfirmed:     cfg.LifecycleScopeConfirmed,
			LifecycleDeleteObservations: deleteObservations,
			DomainSuffix:                cfg.DomainSuffix,
			HostnameTagPrefix:           cfg.HostnameTagPrefix,
			VMZone:                      strings.Trim(strings.TrimSpace(cfg.VMZone), "."),
		},
		logger:   logger,
		store:    store,
		snapshot: snapshot,
	}
}

// ListWorkloads returns Proxmox VMs and LXC containers as platform-agnostic workloads.
// Applies node, tag, and state filters. Resolves IP addresses via LXC config parsing
// or the qemu-guest-agent.
func (a *WorkloadListerAdapter) ListWorkloads(ctx context.Context) ([]workload.Workload, error) {
	if a.cfg.LifecycleMode == LifecycleModeStateful {
		return a.listStateful(ctx)
	}
	return a.listLegacy(ctx)
}

func (a *WorkloadListerAdapter) listLegacy(ctx context.Context) ([]workload.Workload, error) {
	resources, err := a.client.ListClusterResources(ctx)
	if err != nil {
		a.setSnapshot(workload.SnapshotStatus{Complete: false, AllowOrphanCleanup: false})
		return nil, fmt.Errorf("listing proxmox cluster resources: %w", err)
	}

	var result []workload.Workload

	for _, r := range resources {
		if !a.matchesFilters(r) {
			continue
		}

		interfacePreference := parseInterfacePreferenceFromTags(r.Tags, a.cfg.InterfacePreference)
		ips, err := ResolveIPs(ctx, a.client, r, a.logger, ResolveOptions{
			InterfacePreference: interfacePreference,
			AllowedInterfaces:   a.cfg.AllowedInterfaces,
			WantV6:              a.cfg.IPVersion.wantsV6(),
		})
		if err != nil {
			// A successful partial list would turn an unavailable guest into an
			// orphan. Let reconciliation preserve the last complete snapshot.
			return nil, fmt.Errorf("resolving IP for proxmox resource %s/%d: %w", r.Node, r.VMID, err)
		}

		w := toWorkload(r, ips, a.cfg.IPVersion)
		result = append(result, w)
	}

	a.setSnapshot(workload.SnapshotStatus{Complete: true, AllowOrphanCleanup: true})
	return result, nil
}

// SnapshotStatus implements workload.SnapshotStatusProvider. The value is
// updated only after a successful inventory cycle, so an API error cannot be
// mistaken for a safe empty inventory.
func (a *WorkloadListerAdapter) SnapshotStatus() workload.SnapshotStatus {
	a.snapshotMu.RLock()
	defer a.snapshotMu.RUnlock()
	return a.snapshot
}

func (a *WorkloadListerAdapter) setSnapshot(status workload.SnapshotStatus) {
	a.snapshotMu.Lock()
	a.snapshot = status
	a.snapshotMu.Unlock()
}

func (a *WorkloadListerAdapter) listStateful(ctx context.Context) ([]workload.Workload, error) {
	if a.store == nil {
		a.setSnapshot(workload.SnapshotStatus{Complete: false, AllowOrphanCleanup: false})
		return nil, fmt.Errorf("proxmox lifecycle state store is not initialized")
	}

	snapshot, err := a.client.ListClusterResourceSnapshot(ctx)
	if err != nil {
		a.setSnapshot(workload.SnapshotStatus{Complete: false, AllowOrphanCleanup: false})
		return nil, fmt.Errorf("listing proxmox cluster resources: %w", err)
	}
	if !snapshot.Complete || !snapshot.ScopeClear {
		a.setSnapshot(workload.SnapshotStatus{Complete: false, AllowOrphanCleanup: false})
		return nil, fmt.Errorf("proxmox cluster resource inventory is incomplete")
	}
	clusterName, err := a.client.GetClusterName(ctx)
	if err != nil {
		a.setSnapshot(workload.SnapshotStatus{Complete: false, AllowOrphanCleanup: false})
		return nil, fmt.Errorf("getting proxmox cluster name: %w", err)
	}
	clusterName = strings.Trim(strings.TrimSpace(clusterName), ".")
	if clusterName == "" {
		a.setSnapshot(workload.SnapshotStatus{Complete: false, AllowOrphanCleanup: false})
		return nil, fmt.Errorf("getting proxmox cluster name: empty cluster name")
	}
	if strings.TrimSpace(a.cfg.VMZone) == "" {
		a.setSnapshot(workload.SnapshotStatus{Complete: false, AllowOrphanCleanup: false})
		return nil, fmt.Errorf("proxmox stateful lifecycle requires a VM zone")
	}

	state, err := a.store.snapshot()
	if err != nil {
		a.setSnapshot(workload.SnapshotStatus{Complete: false, AllowOrphanCleanup: false})
		return nil, err
	}
	next := cloneLifecycleFile(state)
	if len(snapshot.Resources) > 0 {
		next.ObservedNonEmpty = true
	}

	resources := make([]ClusterResource, 0, len(snapshot.Resources))
	inventoryObserved := make(map[string]struct{}, len(snapshot.Resources))
	filteredKeys := make(map[string]struct{})
	for _, resource := range snapshot.Resources {
		key := lifecycleIdentity(clusterName, resource)
		if _, duplicate := inventoryObserved[key]; duplicate {
			a.setSnapshot(workload.SnapshotStatus{Complete: false, AllowOrphanCleanup: false})
			return nil, fmt.Errorf("duplicate Proxmox lifecycle identity %s", key)
		}
		inventoryObserved[key] = struct{}{}
		if a.matchesFilters(resource) {
			resources = append(resources, resource)
		} else {
			filteredKeys[key] = struct{}{}
		}
	}
	sort.Slice(resources, func(i, j int) bool {
		if resources[i].Type != resources[j].Type {
			return resources[i].Type < resources[j].Type
		}
		if resources[i].VMID != resources[j].VMID {
			return resources[i].VMID < resources[j].VMID
		}
		return resources[i].Node < resources[j].Node
	})

	now := time.Now().UTC()
	workloads := make([]workload.Workload, 0, len(resources)+len(state.Instances))
	for _, resource := range resources {
		key := lifecycleIdentity(clusterName, resource)

		record, exists := next.Instances[key]
		if !exists {
			record = lifecycleRecord{
				Key:        key,
				VMID:       resource.VMID,
				Type:       resource.Type,
				Generation: next.Tombstones[key] + 1,
			}
			if record.Generation == 0 {
				record.Generation = 1
			}
		} else if record.Name != "" && record.Name != resource.Name {
			// A name change on a reused VMID is treated as a new generation. A
			// legitimate rename may briefly re-learn its address, which is safer
			// than publishing an address belonging to the previous instance.
			record.Generation++
			record.LastIPs = ResolvedIPs{}
			record.LastConfirmedAt = time.Time{}
		}

		record.Key = key
		record.VMID = resource.VMID
		record.Type = resource.Type
		record.Name = resource.Name
		record.Node = resource.Node
		record.ClusterName = clusterName
		record.LastSeenAt = now
		record.MissingObservations = 0
		record.Collision = false

		effectiveIPs, baseStatus, err := a.observeResource(ctx, resource, record)
		if err != nil {
			a.setSnapshot(workload.SnapshotStatus{Complete: false, AllowOrphanCleanup: false})
			return nil, fmt.Errorf("resolving IP for proxmox resource %s/%d: %w", resource.Node, resource.VMID, err)
		}
		if baseStatus == LifecycleStateRunningAddress && !effectiveIPs.IsEmpty() {
			record.LastIPs = effectiveIPs
			record.LastConfirmedAt = now
		}
		record.Status = baseStatus
		next.Instances[key] = record
		workloads = append(workloads, a.lifecycleWorkload(resource, record, effectiveIPs, baseStatus))
	}

	uncertain := false
	for key, record := range next.Instances {
		if _, seen := inventoryObserved[key]; seen {
			if _, filtered := filteredKeys[key]; filtered {
				record.Status = LifecycleStateInventoryUnknown
				record.LastSeenAt = now
				record.MissingObservations = 0
				record.Collision = false
				next.Instances[key] = record
				uncertain = true
			}
			if record.Status != LifecycleStateRunningAddress {
				uncertain = true
			}
			continue
		}

		if a.cfg.LifecycleScopeConfirmed && snapshot.ScopeClear {
			record.MissingObservations++
			record.Status = LifecycleStateAbsentPending
			if record.MissingObservations >= a.cfg.LifecycleDeleteObservations {
				generation := record.Generation
				if generation == 0 {
					generation = 1
				}
				if next.Tombstones[key] < generation {
					next.Tombstones[key] = generation
				}
				delete(next.Instances, key)
				continue
			}
		} else {
			record.Status = LifecycleStateInventoryUnknown
		}
		uncertain = true
		next.Instances[key] = record
		workloads = append(workloads, a.lifecycleWorkload(
			ClusterResource{VMID: record.VMID, Name: record.Name, Node: record.Node, Type: record.Type, Status: "stopped"},
			record, record.LastIPs, record.Status,
		))
	}

	// Only resources inside the configured management filters are eligible to
	// freeze a managed DNS name. Out-of-scope nodes/tags remain part of the
	// complete inventory for deletion safety but cannot block this adapter's
	// desired state.
	for key, record := range next.Instances {
		record.Collision = false
		next.Instances[key] = record
	}
	collisionKeys, collisionNames := collisionIdentities(next.Instances, filteredKeys, a, clusterName)
	for key := range collisionKeys {
		if record, ok := next.Instances[key]; ok {
			record.Collision = true
			next.Instances[key] = record
		}
	}
	for i := range workloads {
		if key := workloads[i].ID; key != "" {
			if record, ok := next.Instances[key]; ok && record.Collision {
				setLifecycleMetadata(&workloads[i], record, LifecycleStateNameCollision)
			}
		}
	}
	for _, record := range next.Instances {
		if record.Collision {
			uncertain = true
		}
	}

	if err := a.store.commit(next); err != nil {
		a.setSnapshot(workload.SnapshotStatus{Complete: false, AllowOrphanCleanup: false})
		return nil, err
	}
	a.setSnapshot(workload.SnapshotStatus{
		Complete:           true,
		AllowOrphanCleanup: a.cfg.LifecycleScopeConfirmed && next.ObservedNonEmpty && !uncertain,
		BlockedHostnames:   sortedNames(collisionNames),
	})
	return workloads, nil
}

func (a *WorkloadListerAdapter) observeResource(ctx context.Context, resource ClusterResource, record lifecycleRecord) (ResolvedIPs, string, error) {
	if resource.Status != stateRunning {
		return record.LastIPs, LifecycleStateStopped, nil
	}
	ips, err := ResolveIPs(ctx, a.client, resource, a.logger, ResolveOptions{
		InterfacePreference:      parseInterfacePreferenceFromTags(resource.Tags, a.cfg.InterfacePreference),
		AllowedInterfaces:        a.cfg.AllowedInterfaces,
		WantV6:                   a.cfg.IPVersion.wantsV6(),
		StrictInterfaceSelection: true,
		PreserveTransientErrors:  true,
	})
	if err != nil {
		var agentErr *ErrAgentNotRunning
		if errors.As(err, &agentErr) {
			return record.LastIPs, LifecycleStateQGAUnavailable, nil
		}
		if isTimeoutError(err) {
			return record.LastIPs, LifecycleStateQGATimeout, nil
		}
		return ResolvedIPs{}, "", err
	}
	if !a.cfg.IPVersion.wantsV4() {
		ips.V4 = ""
	}
	if !a.cfg.IPVersion.wantsV6() {
		ips.V6 = ""
	}
	if ips.IsEmpty() {
		return record.LastIPs, LifecycleStateSuccessfulEmpty, nil
	}
	record.LastIPs = ips
	return ips, LifecycleStateRunningAddress, nil
}

func isTimeoutError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var timeout net.Error
	return errors.As(err, &timeout) && timeout.Timeout()
}

func (a *WorkloadListerAdapter) lifecycleWorkload(resource ClusterResource, record lifecycleRecord, ips ResolvedIPs, status string) workload.Workload {
	w := toWorkload(resource, ips, a.cfg.IPVersion)
	w.ID = record.Key
	setLifecycleMetadata(&w, record, status)
	return w
}

func setLifecycleMetadata(w *workload.Workload, record lifecycleRecord, status string) {
	if w.Metadata == nil {
		w.Metadata = make(map[string]string)
	}
	w.Metadata["proxmox.lifecycle.mode"] = LifecycleModeStateful
	w.Metadata["proxmox.lifecycle.identity"] = record.Key
	w.Metadata["proxmox.lifecycle.generation"] = fmt.Sprintf("%d", record.Generation)
	w.Metadata["proxmox.lifecycle.state"] = status
	w.Metadata["proxmox.lifecycle.base_state"] = record.Status
	if record.ClusterName != "" {
		w.Metadata["proxmox.lifecycle.cluster_name"] = record.ClusterName
	}
	if record.Collision {
		w.Metadata["proxmox.lifecycle.collision"] = "true"
	}
}

func collisionIdentities(records map[string]lifecycleRecord, filteredKeys map[string]struct{}, adapter *WorkloadListerAdapter, clusterName string) (map[string]struct{}, map[string]struct{}) {
	byName := make(map[string]map[string]struct{})
	for key, record := range records {
		if _, filtered := filteredKeys[key]; filtered {
			continue
		}
		cluster := record.ClusterName
		if cluster == "" {
			cluster = clusterName
		}
		name := adapter.lifecycleName(ClusterResource{Name: record.Name}, cluster)
		if name == "" {
			continue
		}
		if byName[name] == nil {
			byName[name] = make(map[string]struct{})
		}
		byName[name][key] = struct{}{}
	}
	collisions := make(map[string]struct{})
	collisionNames := make(map[string]struct{})
	for name, keys := range byName {
		if len(keys) < 2 {
			continue
		}
		collisionNames[name] = struct{}{}
		for key := range keys {
			collisions[key] = struct{}{}
		}
	}
	return collisions, collisionNames
}

func sortedNames(names map[string]struct{}) []string {
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

func (a *WorkloadListerAdapter) lifecycleName(resource ClusterResource, clusterName string) string {
	if a.cfg.LifecycleMode == LifecycleModeStateful {
		return strictLifecycleName(resource.Name, clusterName, a.cfg.VMZone)
	}
	if a.cfg.HostnameTagPrefix != "" {
		prefix := a.cfg.HostnameTagPrefix + "+"
		for _, tag := range strings.Split(resource.Tags, ";") {
			tag = strings.TrimSpace(tag)
			if strings.HasPrefix(tag, prefix) {
				name := strings.TrimSpace(strings.TrimPrefix(tag, prefix))
				if name != "" {
					if !strings.Contains(name, ".") && a.cfg.DomainSuffix != "" {
						name += "." + a.cfg.DomainSuffix
					}
					return strings.ToLower(strings.TrimSuffix(name, "."))
				}
			}
		}
	}
	name := strings.TrimSpace(resource.Name)
	if !strings.Contains(name, ".") && a.cfg.DomainSuffix != "" {
		name += "." + a.cfg.DomainSuffix
	}
	return strings.ToLower(strings.TrimSuffix(name, "."))
}

func strictLifecycleName(resourceName, clusterName, vmZone string) string {
	resourceName = strings.TrimSuffix(strings.TrimSpace(resourceName), ".")
	clusterName = strings.Trim(strings.TrimSpace(clusterName), ".")
	vmZone = strings.Trim(strings.TrimSpace(vmZone), ".")
	if resourceName == "" || clusterName == "" || vmZone == "" {
		return ""
	}
	return strings.ToLower(resourceName + "." + clusterName + "." + vmZone)
}

// Platform returns PlatformProxmox, identifying this adapter as a Proxmox source.
func (a *WorkloadListerAdapter) Platform() workload.Platform {
	return workload.PlatformProxmox
}

// matchesFilters returns true if the given resource passes all configured filters.
func (a *WorkloadListerAdapter) matchesFilters(r ClusterResource) bool {
	if a.cfg.StateFilter != "" && r.Status != a.cfg.StateFilter {
		return false
	}
	if a.cfg.NodeFilter != "" && r.Node != a.cfg.NodeFilter {
		return false
	}
	if a.cfg.TagFilter != "" && !hasTagWithPrefix(r.Tags, a.cfg.TagFilter) {
		return false
	}
	return true
}

// toWorkload converts a ClusterResource and its resolved addresses into a
// platform-agnostic Workload. Only the families selected by ipVersion are
// recorded, so an IPv4-only deployment never sees an ip6 key.
func toWorkload(r ClusterResource, ips ResolvedIPs, ipVersion IPVersion) workload.Workload {
	kind := workload.KindVM
	if r.Type == resourceTypeLXC {
		kind = workload.KindLXC
	}

	meta := map[string]string{
		metaKeyNode:   r.Node,
		metaKeyVMID:   fmt.Sprintf("%d", r.VMID),
		metaKeyTags:   r.Tags,
		metaKeyStatus: r.Status,
	}
	if r.Pool != "" {
		meta[metaKeyPool] = r.Pool
	}
	if ipVersion.wantsV4() && ips.V4 != "" {
		meta[metaKeyIP] = ips.V4
	}
	if ipVersion.wantsV6() && ips.V6 != "" {
		meta[metaKeyIP6] = ips.V6
	}

	// Parse PVE tags into workload labels so sources can act on them.
	// Proxmox tags are semicolon-delimited. We expose them as
	// "proxmox.tag/<tagvalue>" = "true" labels.
	labels := parseTags(r.Tags)

	// Expose the PVE resource pool as a label so provider label selectors can
	// route by pool — the closest thing PVE has to a tenant boundary.
	if r.Pool != "" {
		labels["proxmox.pool/"+r.Pool] = tagLabelValue
	}

	return workload.Workload{
		ID:       fmt.Sprintf("%s/%d", r.Node, r.VMID),
		Name:     r.Name,
		Labels:   labels,
		Platform: workload.PlatformProxmox,
		Kind:     kind,
		Metadata: meta,
	}
}

// hasTagWithPrefix returns true if the semicolon-delimited tags string contains
// at least one tag that starts with the given prefix.
func hasTagWithPrefix(tags, prefix string) bool {
	if tags == "" {
		return false
	}
	for _, tag := range strings.Split(tags, ";") {
		tag = strings.TrimSpace(tag)
		if strings.HasPrefix(tag, prefix) {
			return true
		}
	}
	return false
}

// parseTags converts a semicolon-delimited PVE tags string into a labels map.
// Each tag becomes a "proxmox.tag/<value>" = "true" entry.
func parseTags(tags string) map[string]string {
	labels := make(map[string]string)
	if tags == "" {
		return labels
	}
	for _, tag := range strings.Split(tags, ";") {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		labels["proxmox.tag/"+tag] = tagLabelValue
	}
	return labels
}
