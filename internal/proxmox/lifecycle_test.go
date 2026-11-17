package proxmox

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

type lifecycleTestServer struct {
	resources            []ClusterResource
	interfaces           []AgentNetworkInterface
	agentError           bool
	agentEmpty           bool
	agentDelay           time.Duration
	nullInventory        bool
	clusterStatusMissing bool
	clusterStatusError   bool
	clusterName          string
}

func (s *lifecycleTestServer) handler(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/api2/json/cluster/resources":
		if s.nullInventory {
			_ = json.NewEncoder(w).Encode(map[string]any{"data": nil})
			return
		}
		resources := s.resources
		if resources == nil {
			resources = []ClusterResource{}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": resources})
	case r.URL.Path == "/api2/json/cluster/status":
		if s.clusterStatusError {
			http.Error(w, "cluster status unavailable", http.StatusServiceUnavailable)
			return
		}
		if s.clusterStatusMissing {
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []ClusterStatusEntry{{Type: "node", Name: "pve-00"}}})
			return
		}
		name := s.clusterName
		if name == "" {
			name = "test-cluster"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []ClusterStatusEntry{{Type: "cluster", Name: name}}})
	case strings.HasSuffix(r.URL.Path, "/agent/network-get-interfaces"):
		if s.agentDelay > 0 {
			select {
			case <-time.After(s.agentDelay):
			case <-r.Context().Done():
				return
			}
		}
		if s.agentError {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"errors":{"command":"QEMU guest agent is not running"}}`)
			return
		}
		if s.agentEmpty {
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"result": []AgentNetworkInterface{}}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"result": s.interfaces}})
	default:
		http.NotFound(w, r)
	}
}

func statefulQEMUFixture() *lifecycleTestServer {
	return &lifecycleTestServer{
		resources:  []ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: resourceTypeQEMU, Status: stateRunning}},
		interfaces: []AgentNetworkInterface{{Name: "eth0", IPAddresses: []AgentIPAddress{{IPAddress: "10.20.0.10"}}}},
	}
}

func newStatefulTestAdapter(t *testing.T, fixture *lifecycleTestServer, stateFile string) (*WorkloadListerAdapter, func()) {
	return newStatefulTestAdapterWithZone(t, fixture, stateFile, "example.com")
}

func newStatefulTestAdapterWithZone(
	t *testing.T,
	fixture *lifecycleTestServer,
	stateFile string,
	vmZone string,
) (*WorkloadListerAdapter, func()) {
	t.Helper()
	server, client := newTestServer(t, fixture.handler)
	adapter := NewWorkloadListerAdapter(client, AdapterConfig{
		LifecycleMode:               LifecycleModeStateful,
		LifecycleStateFile:          stateFile,
		LifecycleScopeConfirmed:     true,
		LifecycleDeleteObservations: 2,
		DomainSuffix:                "example.com",
		VMZone:                      vmZone,
	}, testLogger())
	return adapter, server.Close
}

func TestStatefulLifecycleRetainsTransientAddressAndRestoresAfterRestart(t *testing.T) {
	fixture := statefulQEMUFixture()
	stateFile := filepath.Join(t.TempDir(), "lifecycle.json")
	adapter, closeServer := newStatefulTestAdapter(t, fixture, stateFile)
	t.Cleanup(closeServer)

	workloads, err := adapter.ListWorkloads(context.Background())
	if err != nil {
		t.Fatalf("initial ListWorkloads: %v", err)
	}
	if got := workloads[0].Metadata[metaKeyIP]; got != "10.20.0.10" {
		t.Fatalf("initial address = %q, want 10.20.0.10", got)
	}
	if got := workloads[0].Metadata["proxmox.lifecycle.cluster_name"]; got != "test-cluster" {
		t.Fatalf("cluster name = %q, want test-cluster", got)
	}
	if workloads[0].ID != "test-cluster|qemu|100" {
		t.Fatalf("identity = %q, want cluster/type/vmid identity", workloads[0].ID)
	}

	fixture.agentError = true
	workloads, err = adapter.ListWorkloads(context.Background())
	if err != nil {
		t.Fatalf("QGA unavailable ListWorkloads: %v", err)
	}
	if got := workloads[0].Metadata[metaKeyIP]; got != "10.20.0.10" {
		t.Fatalf("retained address = %q, want 10.20.0.10", got)
	}
	if got := workloads[0].Metadata["proxmox.lifecycle.state"]; got != LifecycleStateQGAUnavailable {
		t.Fatalf("state = %q, want %q", got, LifecycleStateQGAUnavailable)
	}
	if adapter.SnapshotStatus().AllowOrphanCleanup {
		t.Fatal("QGA outage must hold orphan cleanup")
	}

	// A new adapter represents a process restart. The persisted address remains
	// available even though the current QGA observation is still unavailable.
	second, closeSecond := newStatefulTestAdapter(t, fixture, stateFile)
	t.Cleanup(closeSecond)
	workloads, err = second.ListWorkloads(context.Background())
	if err != nil {
		t.Fatalf("restart ListWorkloads: %v", err)
	}
	if got := workloads[0].Metadata[metaKeyIP]; got != "10.20.0.10" {
		t.Fatalf("restart retained address = %q, want 10.20.0.10", got)
	}
}

func TestStatefulLifecycleTimeoutRetainsAddress(t *testing.T) {
	fixture := statefulQEMUFixture()
	stateFile := filepath.Join(t.TempDir(), "lifecycle.json")
	adapter, closeServer := newStatefulTestAdapter(t, fixture, stateFile)
	t.Cleanup(closeServer)
	if _, err := adapter.ListWorkloads(context.Background()); err != nil {
		t.Fatalf("initial ListWorkloads: %v", err)
	}

	fixture.agentDelay = 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	workloads, err := adapter.ListWorkloads(ctx)
	if err != nil {
		t.Fatalf("timeout ListWorkloads: %v", err)
	}
	if len(workloads) != 1 || workloads[0].Metadata[metaKeyIP] != "10.20.0.10" {
		t.Fatalf("timeout did not retain address: %+v", workloads)
	}
	if got := workloads[0].Metadata["proxmox.lifecycle.state"]; got != LifecycleStateQGATimeout {
		t.Fatalf("timeout state = %q, want %q", got, LifecycleStateQGATimeout)
	}
}

func TestStatefulLifecycleStoppedAndSuccessfulEmptyRetainAddress(t *testing.T) {
	fixture := statefulQEMUFixture()
	stateFile := filepath.Join(t.TempDir(), "lifecycle.json")
	adapter, closeServer := newStatefulTestAdapter(t, fixture, stateFile)
	t.Cleanup(closeServer)
	if _, err := adapter.ListWorkloads(context.Background()); err != nil {
		t.Fatalf("initial ListWorkloads: %v", err)
	}

	fixture.resources[0].Status = "stopped"
	workloads, err := adapter.ListWorkloads(context.Background())
	if err != nil {
		t.Fatalf("stopped ListWorkloads: %v", err)
	}
	if got := workloads[0].Metadata[metaKeyIP]; got != "10.20.0.10" {
		t.Fatalf("stopped address = %q, want retained address", got)
	}
	if got := workloads[0].Metadata["proxmox.lifecycle.state"]; got != LifecycleStateStopped {
		t.Fatalf("stopped state = %q, want %q", got, LifecycleStateStopped)
	}

	fixture.resources[0].Status = stateRunning
	fixture.agentEmpty = true
	workloads, err = adapter.ListWorkloads(context.Background())
	if err != nil {
		t.Fatalf("successful empty ListWorkloads: %v", err)
	}
	if got := workloads[0].Metadata[metaKeyIP]; got != "10.20.0.10" {
		t.Fatalf("successful empty address = %q, want retained address", got)
	}
	if got := workloads[0].Metadata["proxmox.lifecycle.state"]; got != LifecycleStateSuccessfulEmpty {
		t.Fatalf("successful empty state = %q, want %q", got, LifecycleStateSuccessfulEmpty)
	}
}

func TestStatefulLifecycleConfirmedDeleteAndVMIDReuseDoNotInheritAddress(t *testing.T) {
	fixture := statefulQEMUFixture()
	stateFile := filepath.Join(t.TempDir(), "lifecycle.json")
	adapter, closeServer := newStatefulTestAdapter(t, fixture, stateFile)
	t.Cleanup(closeServer)
	if _, err := adapter.ListWorkloads(context.Background()); err != nil {
		t.Fatalf("initial ListWorkloads: %v", err)
	}

	fixture.resources = nil
	workloads, err := adapter.ListWorkloads(context.Background())
	if err != nil {
		t.Fatalf("first absent ListWorkloads: %v", err)
	}
	if len(workloads) != 1 || workloads[0].Metadata[metaKeyIP] != "10.20.0.10" {
		t.Fatalf("first absent snapshot did not retain workload: %+v", workloads)
	}
	workloads, err = adapter.ListWorkloads(context.Background())
	if err != nil {
		t.Fatalf("confirmed absent ListWorkloads: %v", err)
	}
	if len(workloads) != 0 || !adapter.SnapshotStatus().AllowOrphanCleanup {
		t.Fatalf("confirmed deletion = workloads=%+v status=%+v", workloads, adapter.SnapshotStatus())
	}

	fixture.resources = []ClusterResource{{VMID: 100, Name: "new-web", Node: "pve-01", Type: resourceTypeQEMU, Status: stateRunning}}
	fixture.agentError = true
	workloads, err = adapter.ListWorkloads(context.Background())
	if err != nil {
		t.Fatalf("reused VMID ListWorkloads: %v", err)
	}
	if len(workloads) != 1 {
		t.Fatalf("reused VMID workload count = %d, want 1", len(workloads))
	}
	if _, ok := workloads[0].Metadata[metaKeyIP]; ok {
		t.Fatalf("reused VMID inherited old address: %+v", workloads[0].Metadata)
	}
	if workloads[0].Name != "new-web" || workloads[0].ID != "test-cluster|qemu|100" {
		t.Fatalf("reused VMID identity/name = %q/%q", workloads[0].ID, workloads[0].Name)
	}
}

func TestStatefulLifecycleNodeMigrationKeepsIdentity(t *testing.T) {
	fixture := statefulQEMUFixture()
	stateFile := filepath.Join(t.TempDir(), "lifecycle.json")
	adapter, closeServer := newStatefulTestAdapter(t, fixture, stateFile)
	t.Cleanup(closeServer)
	first, err := adapter.ListWorkloads(context.Background())
	if err != nil {
		t.Fatalf("initial ListWorkloads: %v", err)
	}

	fixture.resources[0].Node = "pve-01"
	fixture.agentError = true
	second, err := adapter.ListWorkloads(context.Background())
	if err != nil {
		t.Fatalf("migrated ListWorkloads: %v", err)
	}
	if first[0].ID != second[0].ID {
		t.Fatalf("node migration changed identity: %q -> %q", first[0].ID, second[0].ID)
	}
	if second[0].Metadata[metaKeyIP] != "10.20.0.10" {
		t.Fatalf("node migration lost retained address: %+v", second[0].Metadata)
	}
}

func TestStatefulLifecycleNameCollisionBlocksHostname(t *testing.T) {
	fixture := &lifecycleTestServer{
		resources: []ClusterResource{
			{VMID: 100, Name: "web", Node: "pve-00", Type: resourceTypeQEMU, Status: stateRunning},
			{VMID: 101, Name: "WEB", Node: "pve-01", Type: resourceTypeQEMU, Status: stateRunning},
		},
		interfaces: []AgentNetworkInterface{{Name: "eth0", IPAddresses: []AgentIPAddress{{IPAddress: "10.20.0.10"}}}},
	}
	adapter, closeServer := newStatefulTestAdapter(t, fixture, filepath.Join(t.TempDir(), "lifecycle.json"))
	t.Cleanup(closeServer)

	workloads, err := adapter.ListWorkloads(context.Background())
	if err != nil {
		t.Fatalf("collision ListWorkloads: %v", err)
	}
	if len(workloads) != 2 {
		t.Fatalf("collision workloads = %d, want 2", len(workloads))
	}
	status := adapter.SnapshotStatus()
	if status.AllowOrphanCleanup || len(status.BlockedHostnames) != 1 || status.BlockedHostnames[0] != "web.test-cluster.example.com" {
		t.Fatalf("collision snapshot status = %+v", status)
	}
	for _, workload := range workloads {
		if got := workload.Metadata["proxmox.lifecycle.state"]; got != LifecycleStateNameCollision {
			t.Fatalf("collision workload state = %q, want %q", got, LifecycleStateNameCollision)
		}
	}
}

func TestStatefulLifecycleCollisionUnblocksWhenIdentityIsFiltered(t *testing.T) {
	fixture := &lifecycleTestServer{
		resources: []ClusterResource{
			{VMID: 100, Name: "web", Node: "pve-00", Type: resourceTypeQEMU, Status: stateRunning},
			{VMID: 101, Name: "WEB", Node: "pve-01", Type: resourceTypeQEMU, Status: stateRunning},
		},
		interfaces: []AgentNetworkInterface{{Name: "eth0", IPAddresses: []AgentIPAddress{{IPAddress: "10.20.0.10"}}}},
	}
	server, client := newTestServer(t, fixture.handler)
	t.Cleanup(server.Close)
	adapter := NewWorkloadListerAdapter(client, AdapterConfig{
		LifecycleMode:               LifecycleModeStateful,
		LifecycleStateFile:          filepath.Join(t.TempDir(), "lifecycle.json"),
		LifecycleScopeConfirmed:     true,
		LifecycleDeleteObservations: 2,
		VMZone:                      "example.com",
	}, testLogger())

	workloads, err := adapter.ListWorkloads(context.Background())
	if err != nil {
		t.Fatalf("initial collision ListWorkloads: %v", err)
	}
	if len(workloads) != 2 || len(adapter.SnapshotStatus().BlockedHostnames) != 1 {
		t.Fatalf("initial collision workloads/status = %+v / %+v, want both identities frozen", workloads, adapter.SnapshotStatus())
	}

	adapter.cfg.NodeFilter = "pve-00"
	workloads, err = adapter.ListWorkloads(context.Background())
	if err != nil {
		t.Fatalf("filtered collision ListWorkloads: %v", err)
	}
	if len(workloads) != 1 || workloads[0].Name != "web" {
		t.Fatalf("filtered collision workloads = %+v, want only managed web", workloads)
	}
	status := adapter.SnapshotStatus()
	if len(status.BlockedHostnames) != 0 {
		t.Fatalf("filtered retained collision froze managed hostname: %+v", status.BlockedHostnames)
	}
	if got := workloads[0].Metadata["proxmox.lifecycle.state"]; got == LifecycleStateNameCollision {
		t.Fatalf("managed workload was marked as a collision: %q", got)
	}
}

func TestStatefulLifecycleStrictInterfaceSelectionDoesNotFallback(t *testing.T) {
	fixture := &lifecycleTestServer{
		resources: []ClusterResource{{VMID: 200, Name: "multi", Node: "pve-00", Type: resourceTypeLXC, Status: stateRunning}},
	}
	// The LXC endpoints are added by a small wrapper below so the test keeps
	// using the same real resolver and DNS liveness gate.
	fixtureHandler := func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api2/json/cluster/resources":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": fixture.resources})
		case "/api2/json/cluster/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []ClusterStatusEntry{{Type: "cluster", Name: "test-cluster"}}})
		case "/api2/json/nodes/pve-00/lxc/200/config":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{
				"net0": "name=eth0,ip=dhcp",
				"net1": "name=eth1,ip=10.30.0.7/24",
			}})
		case "/api2/json/nodes/pve-00/lxc/200/interfaces":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []LXCInterface{{Name: "eth1", IPAddresses: []AgentIPAddress{{IPAddress: "10.30.0.7"}}}}})
		default:
			http.NotFound(w, r)
		}
	}
	server, client := newTestServer(t, fixtureHandler)
	t.Cleanup(server.Close)
	adapter := NewWorkloadListerAdapter(client, AdapterConfig{
		LifecycleMode:           LifecycleModeStateful,
		LifecycleStateFile:      filepath.Join(t.TempDir(), "lifecycle.json"),
		LifecycleScopeConfirmed: true,
		AllowedInterfaces:       []string{"eth9"},
		VMZone:                  "example.com",
	}, testLogger())
	workloads, err := adapter.ListWorkloads(context.Background())
	if err != nil {
		t.Fatalf("strict interface ListWorkloads: %v", err)
	}
	if len(workloads) != 1 {
		t.Fatalf("workload count = %d, want 1", len(workloads))
	}
	if _, ok := workloads[0].Metadata[metaKeyIP]; ok {
		t.Fatalf("strict allow-list silently fell back to eth1: %+v", workloads[0].Metadata)
	}
}

func TestStatefulLifecycleInterfaceTagDoesNotFallbackToAllowList(t *testing.T) {
	fixture := &lifecycleTestServer{
		resources: []ClusterResource{{
			VMID: 200, Name: "multi", Node: "pve-00", Type: resourceTypeQEMU,
			Status: stateRunning, Tags: "dnsweaver+eth9",
		}},
		interfaces: []AgentNetworkInterface{{Name: "eth0", IPAddresses: []AgentIPAddress{{IPAddress: "10.20.0.10"}}}},
	}
	adapter, closeServer := newStatefulTestAdapter(t, fixture, filepath.Join(t.TempDir(), "lifecycle.json"))
	t.Cleanup(closeServer)
	adapter.cfg.InterfacePreference = "dnsweaver"
	adapter.cfg.AllowedInterfaces = []string{"eth0"}

	workloads, err := adapter.ListWorkloads(context.Background())
	if err != nil {
		t.Fatalf("tagged interface ListWorkloads: %v", err)
	}
	if len(workloads) != 1 {
		t.Fatalf("workload count = %d, want 1", len(workloads))
	}
	if _, exists := workloads[0].Metadata[metaKeyIP]; exists {
		t.Fatalf("tagged interface without an address fell back to allow-listed eth0: %+v", workloads[0].Metadata)
	}
	if got := workloads[0].Metadata["proxmox.lifecycle.state"]; got != LifecycleStateSuccessfulEmpty {
		t.Fatalf("lifecycle state = %q, want successful empty qualified address", got)
	}
}

func TestStatefulLifecycleIPv6OnlyRetainsAddressWhenOnlyIPv4IsObserved(t *testing.T) {
	fixture := &lifecycleTestServer{
		resources:  []ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: resourceTypeQEMU, Status: stateRunning}},
		interfaces: []AgentNetworkInterface{{Name: "eth0", IPAddresses: []AgentIPAddress{{IPAddress: "fd00::10"}}}},
	}
	server, client := newTestServer(t, fixture.handler)
	t.Cleanup(server.Close)
	adapter := NewWorkloadListerAdapter(client, AdapterConfig{
		LifecycleMode:               LifecycleModeStateful,
		LifecycleStateFile:          filepath.Join(t.TempDir(), "lifecycle.json"),
		LifecycleScopeConfirmed:     true,
		LifecycleDeleteObservations: 2,
		IPVersion:                   IPVersionIPv6,
		VMZone:                      "example.com",
	}, testLogger())

	workloads, err := adapter.ListWorkloads(context.Background())
	if err != nil {
		t.Fatalf("initial IPv6 ListWorkloads: %v", err)
	}
	if len(workloads) != 1 || workloads[0].Metadata[metaKeyIP6] != "fd00::10" {
		t.Fatalf("initial IPv6 workload = %+v, want fd00::10", workloads)
	}

	fixture.interfaces = []AgentNetworkInterface{{Name: "eth0", IPAddresses: []AgentIPAddress{{IPAddress: "10.20.0.10"}}}}
	workloads, err = adapter.ListWorkloads(context.Background())
	if err != nil {
		t.Fatalf("IPv4-only observation in IPv6 mode: %v", err)
	}
	if len(workloads) != 1 || workloads[0].Metadata[metaKeyIP6] != "fd00::10" {
		t.Fatalf("IPv4-only observation dropped last confirmed IPv6: %+v", workloads)
	}
	if _, exists := workloads[0].Metadata[metaKeyIP]; exists {
		t.Fatalf("IPv6-only workload exposed IPv4 address: %+v", workloads[0].Metadata)
	}
	if got := workloads[0].Metadata["proxmox.lifecycle.state"]; got != LifecycleStateSuccessfulEmpty {
		t.Fatalf("lifecycle state = %q, want successful empty qualified address", got)
	}
}

func TestStatefulLifecycleNullInventoryIsNotDeletionEvidence(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "lifecycle.json")
	fixture := statefulQEMUFixture()
	server, client := newTestServer(t, fixture.handler)
	t.Cleanup(server.Close)
	adapter := NewWorkloadListerAdapter(client, AdapterConfig{LifecycleMode: LifecycleModeStateful, LifecycleStateFile: stateFile, LifecycleScopeConfirmed: true, VMZone: "example.com"}, testLogger())
	if _, err := adapter.ListWorkloads(context.Background()); err != nil {
		t.Fatalf("initial ListWorkloads: %v", err)
	}

	fixture.nullInventory = true
	if _, err := adapter.ListWorkloads(context.Background()); err == nil {
		t.Fatal("null inventory was accepted as deletion evidence")
	}
	if adapter.SnapshotStatus().AllowOrphanCleanup {
		t.Fatal("null inventory enabled orphan cleanup")
	}
}

func TestStatefulLifecycleDoesNotUseEndpointHostAsClusterName(t *testing.T) {
	fixture := statefulQEMUFixture()
	fixture.clusterStatusMissing = true
	stateFile := filepath.Join(t.TempDir(), "lifecycle.json")
	server, client := newTestServer(t, fixture.handler)
	t.Cleanup(server.Close)
	adapter := NewWorkloadListerAdapter(client, AdapterConfig{
		LifecycleMode:           LifecycleModeStateful,
		LifecycleStateFile:      stateFile,
		LifecycleScopeConfirmed: true,
		VMZone:                  "example.com",
	}, testLogger())
	if _, err := adapter.ListWorkloads(context.Background()); err == nil {
		t.Fatal("stateful adapter accepted cluster status without an actual cluster name")
	}
	if adapter.SnapshotStatus().AllowOrphanCleanup {
		t.Fatal("missing actual cluster name enabled orphan cleanup")
	}
}

func TestStatefulLifecycleClusterStatusFailureBlocksCleanup(t *testing.T) {
	fixture := statefulQEMUFixture()
	stateFile := filepath.Join(t.TempDir(), "lifecycle.json")
	server, client := newTestServer(t, fixture.handler)
	t.Cleanup(server.Close)
	adapter := NewWorkloadListerAdapter(client, AdapterConfig{
		LifecycleMode:               LifecycleModeStateful,
		LifecycleStateFile:          stateFile,
		LifecycleScopeConfirmed:     true,
		LifecycleDeleteObservations: 2,
		VMZone:                      "example.com",
	}, testLogger())
	if _, err := adapter.ListWorkloads(context.Background()); err != nil {
		t.Fatalf("initial ListWorkloads: %v", err)
	}

	fixture.resources = nil
	fixture.clusterStatusError = true
	if _, err := adapter.ListWorkloads(context.Background()); err == nil {
		t.Fatal("cluster status API failure was treated as a successful lifecycle snapshot")
	}
	status := adapter.SnapshotStatus()
	if status.Complete || status.AllowOrphanCleanup {
		t.Fatalf("cluster status failure left cleanup enabled: %+v", status)
	}
}

func TestProxmoxLegacyDefaultsToRunningAndStatefulRetainsStopped(t *testing.T) {
	fixture := &lifecycleTestServer{
		resources: []ClusterResource{
			{VMID: 100, Name: "running", Node: "pve-00", Type: resourceTypeQEMU, Status: stateRunning},
			{VMID: 101, Name: "stopped", Node: "pve-00", Type: resourceTypeQEMU, Status: "stopped"},
		},
		interfaces: []AgentNetworkInterface{{Name: "eth0", IPAddresses: []AgentIPAddress{{IPAddress: "10.20.0.10"}}}},
	}
	server, client := newTestServer(t, fixture.handler)
	t.Cleanup(server.Close)

	legacy := NewWorkloadListerAdapter(client, AdapterConfig{}, testLogger())
	legacyWorkloads, err := legacy.ListWorkloads(context.Background())
	if err != nil {
		t.Fatalf("legacy ListWorkloads: %v", err)
	}
	if len(legacyWorkloads) != 1 || legacyWorkloads[0].Name != "running" {
		t.Fatalf("legacy workloads = %+v, want only the running resource", legacyWorkloads)
	}

	stateful := NewWorkloadListerAdapter(client, AdapterConfig{
		LifecycleMode:           LifecycleModeStateful,
		LifecycleStateFile:      filepath.Join(t.TempDir(), "state.json"),
		LifecycleScopeConfirmed: true,
		StateFilter:             stateRunning,
		VMZone:                  "example.com",
	}, testLogger())
	statefulWorkloads, err := stateful.ListWorkloads(context.Background())
	if err != nil {
		t.Fatalf("stateful ListWorkloads: %v", err)
	}
	if len(statefulWorkloads) != 2 {
		t.Fatalf("stateful workloads = %+v, want running and stopped resources", statefulWorkloads)
	}
	for _, current := range statefulWorkloads {
		if current.Name == "stopped" && current.Metadata["proxmox.lifecycle.state"] != LifecycleStateStopped {
			t.Fatalf("stopped lifecycle state = %q, want %q", current.Metadata["proxmox.lifecycle.state"], LifecycleStateStopped)
		}
	}
}

func TestLifecyclePostRenameSyncFailureDoesNotResurrectDeletedAddress(t *testing.T) {
	fixture := statefulQEMUFixture()
	stateFile := filepath.Join(t.TempDir(), "lifecycle.json")
	adapter, closeServer := newStatefulTestAdapter(t, fixture, stateFile)
	t.Cleanup(closeServer)
	ctx := context.Background()
	if workloads, err := adapter.ListWorkloads(ctx); err != nil || len(workloads) != 1 || workloads[0].Metadata[metaKeyIP] != "10.20.0.10" {
		t.Fatalf("initial ListWorkloads = %+v, err=%v", workloads, err)
	}

	fixture.resources = nil
	if _, err := adapter.ListWorkloads(ctx); err != nil {
		t.Fatalf("first absent observation: %v", err)
	}
	originalSync := adapter.store.fileOps.syncDirectory
	adapter.store.fileOps.syncDirectory = func(string) error { return errors.New("injected directory sync failure") }
	if _, err := adapter.ListWorkloads(ctx); err == nil {
		t.Fatal("directory sync failure after rename was reported as a successful observation")
	}
	adapter.store.fileOps.syncDirectory = originalSync

	diskState, err := newLifecycleStore(stateFile).snapshot()
	if err != nil {
		t.Fatalf("reload renamed state file: %v", err)
	}
	key := "test-cluster|qemu|100"
	if _, exists := diskState.Instances[key]; exists || diskState.Tombstones[key] == 0 {
		t.Fatalf("rename did not persist the confirmed deletion: instances=%+v tombstones=%+v", diskState.Instances, diskState.Tombstones)
	}

	fixture.resources = []ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: resourceTypeQEMU, Status: stateRunning}}
	fixture.agentError = true
	workloads, err := adapter.ListWorkloads(ctx)
	if err != nil {
		t.Fatalf("reappearance after failed directory sync: %v", err)
	}
	if len(workloads) != 1 {
		t.Fatalf("reappeared workloads = %+v, want one new generation", workloads)
	}
	if got := workloads[0].Metadata[metaKeyIP]; got != "" {
		t.Fatalf("reappeared VMID inherited deleted address %q after post-rename failure", got)
	}
}

func TestLifecycleStateFileCreatedWithPrivateMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "state.json")
	store := newLifecycleStore(path)
	if err := store.commit(lifecycleFile{Schema: lifecycleSchemaVersion}); err != nil {
		t.Fatalf("commit state: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat state: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("state mode = %o, want 600", info.Mode().Perm())
	}
	dirInfo, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatalf("stat state directory: %v", err)
	}
	if dirInfo.Mode().Perm() != 0o700 {
		t.Fatalf("state directory mode = %o, want 700", dirInfo.Mode().Perm())
	}
}

func TestLifecycleV1NameLessTombstoneDoesNotInventRetiredName(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "lifecycle.json")
	legacyState := `{"schema":1,"instances":{},"tombstones":{"test-cluster|qemu|100":1},"observed_non_empty":true}`
	if err := os.WriteFile(stateFile, []byte(legacyState), 0o600); err != nil {
		t.Fatalf("write legacy tombstone-only state: %v", err)
	}
	fixture := statefulQEMUFixture()
	fixture.resources = nil
	adapter, closeServer := newStatefulTestAdapter(t, fixture, stateFile)
	t.Cleanup(closeServer)

	workloads, err := adapter.ListWorkloads(context.Background())
	if err != nil {
		t.Fatalf("ListWorkloads from legacy tombstone-only state: %v", err)
	}
	if len(workloads) != 0 {
		t.Fatalf("tombstone-only state generated workloads: %+v", workloads)
	}
	if names := adapter.SnapshotStatus().RetiredHostnames; len(names) != 0 {
		t.Fatalf("adapter invented a retired name absent from schema 1: %+v", names)
	}

	persisted, err := newLifecycleStore(stateFile).snapshot()
	if err != nil {
		t.Fatalf("reload upgraded state file: %v", err)
	}
	if persisted.Schema != lifecycleSchemaVersion || len(persisted.RetiredNames) != 0 || persisted.Tombstones["test-cluster|qemu|100"] != 1 {
		t.Fatalf("tombstone-only migration fabricated identity data: %+v", persisted)
	}
}

func TestStatefulRetiredHostnamePersistsAcrossVMZoneChange(t *testing.T) {
	fixture := statefulQEMUFixture()
	stateFile := filepath.Join(t.TempDir(), "lifecycle.json")
	initial, closeInitial := newStatefulTestAdapter(t, fixture, stateFile)
	t.Cleanup(closeInitial)
	if _, err := initial.ListWorkloads(context.Background()); err != nil {
		t.Fatalf("initial ListWorkloads: %v", err)
	}
	const previousHostname = "web.test-cluster.example.com"
	if names := initial.SnapshotStatus().RetiredHostnames; !slices.Contains(names, previousHostname) {
		t.Fatalf("initial lifecycle snapshot did not retain %q: %+v", previousHostname, names)
	}
	persisted, err := newLifecycleStore(stateFile).snapshot()
	if err != nil {
		t.Fatalf("read initial lifecycle state: %v", err)
	}
	persistedHostname := false
	for _, retired := range persisted.RetiredNames {
		if retired.Hostname == previousHostname {
			persistedHostname = true
			break
		}
	}
	if !persistedHostname {
		t.Fatalf("lifecycle file did not persist the concrete DNS hostname %q: %+v", previousHostname, persisted.RetiredNames)
	}

	fixture.resources = nil
	reconfigured, closeReconfigured := newStatefulTestAdapterWithZone(t, fixture, stateFile, "new.example.net")
	t.Cleanup(closeReconfigured)
	if _, err := reconfigured.ListWorkloads(context.Background()); err != nil {
		t.Fatalf("ListWorkloads after VM_ZONE change: %v", err)
	}
	names := reconfigured.SnapshotStatus().RetiredHostnames
	if !slices.Contains(names, previousHostname) {
		t.Fatalf("retired hostname changed when VM_ZONE changed: got %v, want retained %q", names, previousHostname)
	}
	if slices.Contains(names, "web.test-cluster.new.example.net") {
		t.Fatalf("retired name was reconstructed using the new VM_ZONE: %+v", names)
	}
}

func TestLifecycleStateStoreRejectsCorruptFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.json")
	if err := os.WriteFile(path, []byte("{not-json"), 0o600); err != nil {
		t.Fatalf("write corrupt state: %v", err)
	}
	if _, err := newLifecycleStore(path).snapshot(); err == nil {
		t.Fatal("corrupt lifecycle state was accepted")
	}
}

func TestLifecycleStateStoreSerializesConcurrentCommits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent.json")
	store := newLifecycleStore(path)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := store.commit(lifecycleFile{Schema: lifecycleSchemaVersion}); err != nil {
				t.Errorf("concurrent state commit: %v", err)
			}
		}()
	}
	wg.Wait()
	if _, err := store.snapshot(); err != nil {
		t.Fatalf("read state after concurrent commits: %v", err)
	}
}
