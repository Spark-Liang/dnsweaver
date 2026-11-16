package proxmox

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type lifecycleTestServer struct {
	resources []ClusterResource
	interfaces []AgentNetworkInterface
	agentError bool
	agentEmpty bool
	agentDelay time.Duration
	nullInventory bool
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
		resources: []ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: resourceTypeQEMU, Status: stateRunning}},
		interfaces: []AgentNetworkInterface{{Name: "eth0", IPAddresses: []AgentIPAddress{{IPAddress: "10.20.0.10"}}}},
	}
}

func newStatefulTestAdapter(t *testing.T, fixture *lifecycleTestServer, stateFile string) (*WorkloadListerAdapter, func()) {
	t.Helper()
	server, client := newTestServer(t, fixture.handler)
	adapter := NewWorkloadListerAdapter(client, AdapterConfig{
		LifecycleMode:              LifecycleModeStateful,
		LifecycleStateFile:         stateFile,
		LifecycleScopeConfirmed:    true,
		LifecycleDeleteObservations: 2,
		DomainSuffix:               "example.com",
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
	if workloads[0].ID != adapter.client.ClusterIdentity()+"|qemu|100" {
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
	if workloads[0].Name != "new-web" || workloads[0].ID != adapter.client.ClusterIdentity()+"|qemu|100" {
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
	if status.AllowOrphanCleanup || len(status.BlockedHostnames) != 1 || status.BlockedHostnames[0] != "web.example.com" {
		t.Fatalf("collision snapshot status = %+v", status)
	}
	for _, workload := range workloads {
		if got := workload.Metadata["proxmox.lifecycle.state"]; got != LifecycleStateNameCollision {
			t.Fatalf("collision workload state = %q, want %q", got, LifecycleStateNameCollision)
		}
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

func TestStatefulLifecycleNullInventoryIsNotDeletionEvidence(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "lifecycle.json")
	fixture := statefulQEMUFixture()
	server, client := newTestServer(t, fixture.handler)
	t.Cleanup(server.Close)
	adapter := NewWorkloadListerAdapter(client, AdapterConfig{LifecycleMode: LifecycleModeStateful, LifecycleStateFile: stateFile, LifecycleScopeConfirmed: true}, testLogger())
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
}
