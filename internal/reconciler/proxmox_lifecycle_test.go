package reconciler

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	proxmoxclient "github.com/maxfield-allison/dnsweaver/internal/proxmox"
	"github.com/maxfield-allison/dnsweaver/pkg/provider"
	"github.com/maxfield-allison/dnsweaver/pkg/source"
	"github.com/maxfield-allison/dnsweaver/pkg/workload"
	proxmoxsource "github.com/maxfield-allison/dnsweaver/sources/proxmox"
)

type snapshotWorkloadLister struct {
	workloads []workload.Workload
	status    workload.SnapshotStatus
}

func (l *snapshotWorkloadLister) ListWorkloads(context.Context) ([]workload.Workload, error) {
	return append([]workload.Workload(nil), l.workloads...), nil
}

func (l *snapshotWorkloadLister) Platform() workload.Platform {
	return workload.PlatformProxmox
}

func (l *snapshotWorkloadLister) SnapshotStatus() workload.SnapshotStatus {
	return l.status
}

func newProxmoxDNSLifecycleReconciler(t *testing.T, lister *snapshotWorkloadLister) (*Reconciler, *testMockProvider) {
	t.Helper()
	backend := newTestMockProvider("dns")
	providers := testProviderRegistry(quietLogger(), backend)
	if err := providers.CreateInstance(provider.ProviderInstanceConfig{
		Name:       "dns",
		TypeName:   "mock",
		RecordType: provider.RecordTypeA,
		Target:     "192.0.2.1",
		TTL:        300,
		Domains:    []string{"*.example.com"},
	}); err != nil {
		t.Fatal(err)
	}
	sources := source.NewRegistry(quietLogger())
	if err := sources.Register(proxmoxsource.New(
		proxmoxsource.WithDomain("example.com"),
		proxmoxsource.WithVMZone("example.com"),
		proxmoxsource.WithLogger(quietLogger()),
	)); err != nil {
		t.Fatal(err)
	}
	return New([]workload.Lister{lister}, sources, providers,
		WithConfig(DefaultConfig()), WithLogger(quietLogger())), backend
}

func statefulProxmoxWorkload(id, name, node, vmid, ip string) workload.Workload {
	return workload.Workload{
		ID:       id,
		Name:     name,
		Platform: workload.PlatformProxmox,
		Kind:     workload.KindVM,
		Metadata: map[string]string{
			"node":                           node,
			"vmid":                           vmid,
			"ip":                             ip,
			"proxmox.lifecycle.mode":         "stateful",
			"proxmox.lifecycle.identity":     id,
			"proxmox.lifecycle.cluster_name": "test-cluster",
		},
	}
}

func TestProxmoxLifecycleSnapshotGateRetainsDNSUntilConfirmedDelete(t *testing.T) {
	lister := &snapshotWorkloadLister{
		workloads: []workload.Workload{statefulProxmoxWorkload("cluster|qemu|100", "web", "pve-00", "100", "10.20.0.10")},
		status:    workload.SnapshotStatus{Complete: true, AllowOrphanCleanup: true},
	}
	r, backend := newProxmoxDNSLifecycleReconciler(t, lister)
	ctx := context.Background()

	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	created := backend.GetCreatedDNSRecords()
	if len(created) != 1 || created[0].Target != "10.20.0.10" {
		t.Fatalf("initial DNS desired state = %+v, want one A for 10.20.0.10", created)
	}

	lister.workloads = nil
	lister.status = workload.SnapshotStatus{Complete: true, AllowOrphanCleanup: false}
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("uncertain empty reconcile: %v", err)
	}
	if records, err := backend.List(ctx); err != nil || len(records) == 0 {
		t.Fatalf("uncertain inventory removed DNS: records=%+v err=%v", records, err)
	}

	lister.status = workload.SnapshotStatus{Complete: true, AllowOrphanCleanup: true}
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("confirmed delete reconcile: %v", err)
	}
	if records, err := backend.List(ctx); err != nil || len(records) != 0 {
		t.Fatalf("confirmed inventory did not remove DNS and ownership: records=%+v err=%v", records, err)
	}
}

func TestProxmoxLifecycleCollisionFreezesDNSName(t *testing.T) {
	first := statefulProxmoxWorkload("cluster|qemu|100", "WEB", "pve-00", "100", "10.20.0.10")
	second := statefulProxmoxWorkload("cluster|qemu|101", "web", "pve-01", "101", "10.20.0.11")
	lister := &snapshotWorkloadLister{
		workloads: []workload.Workload{first},
		status:    workload.SnapshotStatus{Complete: true, AllowOrphanCleanup: true},
	}
	r, backend := newProxmoxDNSLifecycleReconciler(t, lister)
	ctx := context.Background()

	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("initial collision fixture reconcile: %v", err)
	}

	lister.workloads = []workload.Workload{first, second}
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("collision reconcile: %v", err)
	}
	if records := dnsAnswerRecords(ctx, t, backend); len(records) != 1 || records[0].Target != "10.20.0.10" {
		t.Fatalf("collision changed existing DNS records: %+v", records)
	}

	lister.workloads = []workload.Workload{first}
	lister.status.BlockedHostnames = nil
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("unfrozen reconcile: %v", err)
	}
	if records := backend.GetCreatedDNSRecords(); len(records) != 1 || records[0].Target != "10.20.0.10" {
		t.Fatalf("unfrozen name did not create one desired record: %+v", records)
	}
}

func TestProxmoxLifecycleNodeMigrationUsesOneDesiredMember(t *testing.T) {
	identity := "cluster|qemu|100"
	lister := &snapshotWorkloadLister{
		workloads: []workload.Workload{statefulProxmoxWorkload(identity, "web", "pve-00", "100", "10.20.0.10")},
		status:    workload.SnapshotStatus{Complete: true, AllowOrphanCleanup: true},
	}
	r, backend := newProxmoxDNSLifecycleReconciler(t, lister)
	ctx := context.Background()
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}

	lister.workloads = []workload.Workload{statefulProxmoxWorkload(identity, "web", "pve-01", "100", "10.20.0.11")}
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("migration reconcile: %v", err)
	}
	records, err := backend.List(ctx)
	if err != nil {
		t.Fatalf("list after migration: %v", err)
	}
	var answers []provider.Record
	for _, record := range records {
		if record.Type == provider.RecordTypeA {
			answers = append(answers, record)
		}
	}
	if len(answers) != 1 || answers[0].Target != "10.20.0.11" {
		t.Fatalf("node migration produced wrong DNS members: %+v", answers)
	}
}

type proxmoxDNSFixture struct {
	mu                       sync.Mutex
	resources                []proxmoxclient.ClusterResource
	interfaces               []proxmoxclient.AgentNetworkInterface
	interfacesByVMID         map[string][]proxmoxclient.AgentNetworkInterface
	agentError               bool
	agentErrorMessagesByVMID map[string]string
	agentEmpty               bool
	agentEmptyVMIDs          map[string]bool
	inventoryError           bool
	lxcConfig                map[string]string
	lxcInterfaces            []proxmoxclient.LXCInterface
}

func (f *proxmoxDNSFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch {
	case r.URL.Path == "/api2/json/cluster/resources":
		if f.inventoryError {
			http.Error(w, "temporary inventory failure", http.StatusServiceUnavailable)
			return
		}
		resources := f.resources
		if resources == nil {
			resources = []proxmoxclient.ClusterResource{}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": resources})
	case r.URL.Path == "/api2/json/cluster/status":
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []proxmoxclient.ClusterStatusEntry{{Type: "cluster", Name: "test-cluster"}}})
	case strings.HasSuffix(r.URL.Path, "/agent/network-get-interfaces"):
		segments := strings.Split(r.URL.Path, "/")
		vmid := ""
		for i, segment := range segments {
			if segment == "qemu" && i+1 < len(segments) {
				vmid = segments[i+1]
				break
			}
		}
		if message, unavailable := f.agentErrorMessagesByVMID[vmid]; unavailable {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{"data": nil, "message": message})
			return
		}
		if f.agentError {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"errors":{"command":"QEMU guest agent is not running"}}`)
			return
		}
		if f.agentEmpty || f.agentEmptyVMIDs[vmid] {
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"result": []proxmoxclient.AgentNetworkInterface{}}})
			return
		}
		interfaces := f.interfaces
		if byVMID, ok := f.interfacesByVMID[vmid]; ok {
			interfaces = byVMID
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"result": interfaces}})
	case strings.HasSuffix(r.URL.Path, "/lxc/200/config"):
		_ = json.NewEncoder(w).Encode(map[string]any{"data": f.lxcConfig})
	case strings.HasSuffix(r.URL.Path, "/lxc/200/interfaces"):
		_ = json.NewEncoder(w).Encode(map[string]any{"data": f.lxcInterfaces})
	default:
		http.NotFound(w, r)
	}
}

func newStatefulProxmoxDNSReconciler(t *testing.T, client *proxmoxclient.Client, stateFile string, extra func(*proxmoxclient.AdapterConfig)) (*Reconciler, *testMockProvider, *proxmoxclient.WorkloadListerAdapter) {
	t.Helper()
	backend := newTestMockProvider("dns")
	return newStatefulProxmoxDNSReconcilerWithBackend(t, client, stateFile, backend, extra)
}

func newStatefulProxmoxDNSReconcilerWithBackend(t *testing.T, client *proxmoxclient.Client, stateFile string, backend *testMockProvider, extra func(*proxmoxclient.AdapterConfig)) (*Reconciler, *testMockProvider, *proxmoxclient.WorkloadListerAdapter) {
	t.Helper()
	providers := testProviderRegistry(quietLogger(), backend)
	if err := providers.CreateInstance(provider.ProviderInstanceConfig{
		Name:       "dns",
		TypeName:   "mock",
		RecordType: provider.RecordTypeA,
		Target:     "192.0.2.1",
		TTL:        300,
		Domains:    []string{"*.example.com"},
	}); err != nil {
		t.Fatal(err)
	}
	sources := source.NewRegistry(quietLogger())
	if err := sources.Register(proxmoxsource.New(
		proxmoxsource.WithDomain("example.com"),
		proxmoxsource.WithVMZone("example.com"),
		proxmoxsource.WithLogger(quietLogger()),
	)); err != nil {
		t.Fatal(err)
	}
	adapterConfig := proxmoxclient.AdapterConfig{
		LifecycleMode:               proxmoxclient.LifecycleModeStateful,
		LifecycleStateFile:          stateFile,
		LifecycleScopeConfirmed:     true,
		LifecycleDeleteObservations: 2,
		DomainSuffix:                "example.com",
		VMZone:                      "example.com",
	}
	if extra != nil {
		extra(&adapterConfig)
	}
	adapter := proxmoxclient.NewWorkloadListerAdapter(client, adapterConfig, quietLogger())
	r := New([]workload.Lister{adapter}, sources, providers,
		WithConfig(DefaultConfig()), WithLogger(quietLogger()))
	return r, backend, adapter
}

func newProxmoxFixtureClient(t *testing.T, fixture *proxmoxDNSFixture) (*httptest.Server, *proxmoxclient.Client) {
	t.Helper()
	server := httptest.NewServer(fixture)
	t.Cleanup(server.Close)
	client, err := proxmoxclient.NewClient(proxmoxclient.ClientConfig{
		BaseURL:     server.URL,
		TokenID:     "fixture@pve!dns",
		TokenSecret: "fixture",
		VerifyTLS:   false,
		Logger:      quietLogger(),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return server, client
}

func dnsAnswerRecords(ctx context.Context, t *testing.T, backend provider.Provider) []provider.Record {
	t.Helper()
	records, err := backend.List(ctx)
	if err != nil {
		t.Fatalf("list DNS records: %v", err)
	}
	answers := make([]provider.Record, 0, len(records))
	for _, record := range records {
		if record.Type == provider.RecordTypeA || record.Type == provider.RecordTypeAAAA {
			answers = append(answers, record)
		}
	}
	return answers
}

func TestProxmoxStatefulLifecycleEndToEndRetainsAndCleansDNSSafely(t *testing.T) {
	fixture := &proxmoxDNSFixture{
		resources: []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"}},
		interfaces: []proxmoxclient.AgentNetworkInterface{{
			Name:        "eth0",
			IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.10"}},
		}},
	}
	_, client := newProxmoxFixtureClient(t, fixture)
	stateFile := filepath.Join(t.TempDir(), "proxmox-lifecycle.json")
	r, backend, _ := newStatefulProxmoxDNSReconciler(t, client, stateFile, nil)
	ctx := context.Background()

	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	answers := dnsAnswerRecords(ctx, t, backend)
	if len(answers) != 1 || answers[0].Target != "10.20.0.10" || answers[0].Hostname != "web.test-cluster.example.com" {
		t.Fatalf("initial DNS answers = %+v, want web.test-cluster.example.com -> 10.20.0.10", answers)
	}

	fixture.agentError = true
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("QGA unavailable reconcile: %v", err)
	}
	if answers = dnsAnswerRecords(ctx, t, backend); len(answers) != 1 || answers[0].Target != "10.20.0.10" {
		t.Fatalf("QGA outage changed DNS answers = %+v", answers)
	}

	fixture.agentError = false
	fixture.inventoryError = true
	if _, err := r.Reconcile(ctx); err == nil {
		t.Fatal("inventory API failure was treated as a successful reconciliation")
	}
	if answers = dnsAnswerRecords(ctx, t, backend); len(answers) != 1 || answers[0].Target != "10.20.0.10" {
		t.Fatalf("inventory API failure changed DNS answers = %+v", answers)
	}

	fixture.inventoryError = false
	fixture.agentEmpty = true
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("successful empty reconcile: %v", err)
	}
	if answers = dnsAnswerRecords(ctx, t, backend); len(answers) != 1 || answers[0].Target != "10.20.0.10" {
		t.Fatalf("successful empty changed DNS answers = %+v", answers)
	}

	fixture.resources[0].Status = "stopped"
	fixture.agentEmpty = false
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("stopped reconcile: %v", err)
	}
	if answers = dnsAnswerRecords(ctx, t, backend); len(answers) != 1 || answers[0].Target != "10.20.0.10" {
		t.Fatalf("stopped state changed DNS answers = %+v", answers)
	}

	// Recreate both the adapter and reconciler to verify that the retained
	// address is read from the durable lifecycle state after a restart.
	fixture.resources[0].Status = "running"
	fixture.agentError = true
	r, backend, _ = newStatefulProxmoxDNSReconcilerWithBackend(t, client, stateFile, backend, nil)
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("restart reconcile: %v", err)
	}
	if answers = dnsAnswerRecords(ctx, t, backend); len(answers) != 1 || answers[0].Target != "10.20.0.10" {
		t.Fatalf("restart lost DNS answers = %+v", answers)
	}

	fixture.resources = nil
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("first confirmed absence observation: %v", err)
	}
	if answers = dnsAnswerRecords(ctx, t, backend); len(answers) != 1 {
		t.Fatalf("first absence removed DNS too early: %+v", answers)
	}
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("second confirmed absence observation: %v", err)
	}
	if answers = dnsAnswerRecords(ctx, t, backend); len(answers) != 0 {
		t.Fatalf("confirmed absence left DNS answers: %+v", answers)
	}

	// VMID reuse is a new generation. QGA is unavailable, so the new instance
	// must not publish the old address and the deleted answer must stay absent.
	fixture.resources = []proxmoxclient.ClusterResource{{VMID: 100, Name: "new-web", Node: "pve-01", Type: "qemu", Status: "running"}}
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("VMID reuse reconcile: %v", err)
	}
	if answers = dnsAnswerRecords(ctx, t, backend); len(answers) != 0 {
		t.Fatalf("VMID reuse inherited old DNS answer: %+v", answers)
	}
}

func TestProxmoxStatefulQGAUnavailablePreservesVMAndUpdatesPeer(t *testing.T) {
	const unavailableMessage = "QEMU guest agent is not running\n"
	fixture := &proxmoxDNSFixture{
		resources: []proxmoxclient.ClusterResource{
			{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"},
			{VMID: 101, Name: "api", Node: "pve-01", Type: "qemu", Status: "running"},
		},
		interfacesByVMID: map[string][]proxmoxclient.AgentNetworkInterface{
			"100": {{Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.100"}}}},
			"101": {{Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.101"}}}},
		},
	}
	_, client := newProxmoxFixtureClient(t, fixture)
	stateFile := filepath.Join(t.TempDir(), "proxmox-lifecycle.json")
	r, backend, _ := newStatefulProxmoxDNSReconciler(t, client, stateFile, nil)
	ctx := context.Background()

	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	assertAnswers := func(want map[string]string) {
		t.Helper()
		answers := dnsAnswerRecords(ctx, t, backend)
		got := make(map[string]string, len(answers))
		for _, answer := range answers {
			got[answer.Hostname] = answer.Target
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("DNS answers = %v, want %v", got, want)
		}
	}
	assertAnswers(map[string]string{
		"web.test-cluster.example.com": "10.20.0.100",
		"api.test-cluster.example.com": "10.20.0.101",
	})

	fixture.mu.Lock()
	fixture.agentErrorMessagesByVMID = map[string]string{"100": unavailableMessage}
	fixture.interfacesByVMID["101"] = []proxmoxclient.AgentNetworkInterface{{
		Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.102"}},
	}}
	fixture.mu.Unlock()

	if _, err := r.Reconcile(ctx); err != nil {
		t.Errorf("reconcile with VM100 QGA unavailable and VM101 address change: %v", err)
	}
	assertAnswers(map[string]string{
		"web.test-cluster.example.com": "10.20.0.100",
		"api.test-cluster.example.com": "10.20.0.102",
	})

	stateBytes, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("read persisted lifecycle state: %v", err)
	}
	var persisted struct {
		Instances map[string]struct {
			VMID   int    `json:"vmid"`
			Status string `json:"status"`
			LastIP struct {
				V4 string `json:"V4"`
			} `json:"last_confirmed_ips"`
		} `json:"instances"`
	}
	if err := json.Unmarshal(stateBytes, &persisted); err != nil {
		t.Fatalf("decode persisted lifecycle state: %v", err)
	}
	for vmid, want := range map[int]struct {
		status string
		ip     string
	}{
		100: {status: proxmoxclient.LifecycleStateQGAUnavailable, ip: "10.20.0.100"},
		101: {status: proxmoxclient.LifecycleStateRunningAddress, ip: "10.20.0.102"},
	} {
		var found bool
		for _, instance := range persisted.Instances {
			if instance.VMID != vmid {
				continue
			}
			found = true
			if instance.Status != want.status || instance.LastIP.V4 != want.ip {
				t.Errorf("persisted VM%d status/IP = %q/%q, want %q/%q",
					vmid, instance.Status, instance.LastIP.V4, want.status, want.ip)
			}
			break
		}
		if !found {
			t.Errorf("persisted lifecycle state has no VM%d record: %+v", vmid, persisted.Instances)
		}
	}
}

func TestProxmoxStatefulLifecycleRestartCleansConfirmedDeleteFromStateAndOwnership(t *testing.T) {
	fixture := &proxmoxDNSFixture{
		resources: []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"}},
		interfaces: []proxmoxclient.AgentNetworkInterface{{
			Name:        "eth0",
			IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.10"}},
		}},
	}
	_, client := newProxmoxFixtureClient(t, fixture)
	stateFile := filepath.Join(t.TempDir(), "proxmox-lifecycle.json")
	r, backend, _ := newStatefulProxmoxDNSReconciler(t, client, stateFile, nil)
	ctx := context.Background()
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}

	fixture.resources = nil
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("first absent observation: %v", err)
	}
	if answers := dnsAnswerRecords(ctx, t, backend); len(answers) != 1 || answers[0].Hostname != "web.test-cluster.example.com" || answers[0].Target != "10.20.0.10" {
		t.Fatalf("first absent observation changed retained DNS: %+v", answers)
	}

	// The new reconciler has no previousDesired or knownHostnames. Recovering
	// provider ownership and reloading the lifecycle file must still allow the
	// second confirmed absence to remove the old member.
	restarted, backend, _ := newStatefulProxmoxDNSReconcilerWithBackend(t, client, stateFile, backend, nil)
	if err := restarted.RecoverOwnership(ctx); err != nil {
		t.Fatalf("recover ownership after restart: %v", err)
	}
	if _, err := restarted.Reconcile(ctx); err != nil {
		t.Fatalf("confirmed absence after restart: %v", err)
	}
	if answers := dnsAnswerRecords(ctx, t, backend); len(answers) != 0 {
		t.Fatalf("confirmed deletion after restart left DNS: %+v", answers)
	}
}

func TestProxmoxStatefulLifecycleAbsentPendingCollisionRemainsFrozen(t *testing.T) {
	fixture := &proxmoxDNSFixture{
		resources: []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"}},
		interfaces: []proxmoxclient.AgentNetworkInterface{{
			Name:        "eth0",
			IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.10"}},
		}},
	}
	_, client := newProxmoxFixtureClient(t, fixture)
	stateFile := filepath.Join(t.TempDir(), "proxmox-lifecycle.json")
	r, backend, _ := newStatefulProxmoxDNSReconciler(t, client, stateFile, nil)
	ctx := context.Background()
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}

	fixture.resources = []proxmoxclient.ClusterResource{
		{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"},
		{VMID: 101, Name: "WEB", Node: "pve-01", Type: "qemu", Status: "running"},
	}
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("collision reconcile: %v", err)
	}
	if answers := dnsAnswerRecords(ctx, t, backend); len(answers) != 1 || answers[0].Target != "10.20.0.10" {
		t.Fatalf("collision changed existing DNS: %+v", answers)
	}

	// One colliding identity disappears. It is absent-pending and is emitted
	// with its retained address, so claims must keep the normalized name frozen.
	fixture.resources = []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"}}
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("absent-pending collision reconcile: %v", err)
	}
	if answers := dnsAnswerRecords(ctx, t, backend); len(answers) != 1 || answers[0].Target != "10.20.0.10" {
		t.Fatalf("absent-pending collision changed DNS: %+v", answers)
	}

	// The second complete absence observation confirms deletion; the surviving
	// instance can then continue to own the name.
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("collision deletion confirmation: %v", err)
	}
	if answers := dnsAnswerRecords(ctx, t, backend); len(answers) != 1 || answers[0].Target != "10.20.0.10" {
		t.Fatalf("unfrozen surviving instance has wrong DNS: %+v", answers)
	}
}

func TestProxmoxStatefulNoAddressAbsentPendingCollisionStaysFrozenAfterRestart(t *testing.T) {
	fixture := &proxmoxDNSFixture{
		resources: []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"}},
		interfaces: []proxmoxclient.AgentNetworkInterface{{
			Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.10"}},
		}},
	}
	_, client := newProxmoxFixtureClient(t, fixture)
	stateFile := filepath.Join(t.TempDir(), "proxmox-lifecycle.json")
	r, backend, _ := newStatefulProxmoxDNSReconciler(t, client, stateFile, nil)
	ctx := context.Background()
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	assertAnswers := func(want string) {
		t.Helper()
		answers := dnsAnswerRecords(ctx, t, backend)
		if len(answers) != 1 || answers[0].Hostname != "web.test-cluster.example.com" || answers[0].Target != want {
			t.Fatalf("DNS answers = %+v, want one member for %s", answers, want)
		}
	}
	assertAnswers("10.20.0.10")

	fixture.mu.Lock()
	fixture.resources = append(fixture.resources, proxmoxclient.ClusterResource{
		VMID: 101, Name: "WEB", Node: "pve-01", Type: "qemu", Status: "running",
	})
	fixture.agentEmptyVMIDs = map[string]bool{"101": true}
	fixture.interfaces = []proxmoxclient.AgentNetworkInterface{{
		Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.11"}},
	}}
	fixture.mu.Unlock()
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("no-address collision reconcile: %v", err)
	}
	assertAnswers("10.20.0.10")

	// The new adapter and reconciler must recover both the retained instance
	// state and DNS ownership before the no-address collision member disappears.
	r, backend, _ = newStatefulProxmoxDNSReconcilerWithBackend(t, client, stateFile, backend, nil)
	if err := r.RecoverOwnership(ctx); err != nil {
		t.Fatalf("recover ownership after restart: %v", err)
	}
	createdBefore := len(backend.GetCreated())
	deletedBefore := len(backend.GetDeleted())
	fixture.mu.Lock()
	fixture.resources = []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"}}
	fixture.interfaces = []proxmoxclient.AgentNetworkInterface{{
		Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.12"}},
	}}
	fixture.mu.Unlock()
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("first absent-pending observation after restart: %v", err)
	}
	assertAnswers("10.20.0.10")
	if len(backend.GetCreated()) != createdBefore || len(backend.GetDeleted()) != deletedBefore {
		t.Fatalf("absent-pending name freeze wrote DNS: created=%+v deleted=%+v", backend.GetCreated(), backend.GetDeleted())
	}

	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("confirmed absent observation: %v", err)
	}
	assertAnswers("10.20.0.12")
}

func TestProxmoxStatefulNewIdentityWaitsForAddresslessAbsentPendingIdentity(t *testing.T) {
	fixture := &proxmoxDNSFixture{
		resources:       []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"}},
		agentEmptyVMIDs: map[string]bool{"100": true},
	}
	_, client := newProxmoxFixtureClient(t, fixture)
	stateFile := filepath.Join(t.TempDir(), "proxmox-lifecycle.json")
	extra := func(cfg *proxmoxclient.AdapterConfig) {
		cfg.LifecycleDeleteObservations = 3
	}
	r, backend, _ := newStatefulProxmoxDNSReconciler(t, client, stateFile, extra)
	ctx := context.Background()
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("initial addressless reconcile: %v", err)
	}
	if got := dnsAnswerRecords(ctx, t, backend); len(got) != 0 {
		t.Fatalf("addressless initial identity created DNS: %+v", got)
	}

	fixture.mu.Lock()
	fixture.resources = []proxmoxclient.ClusterResource{{VMID: 101, Name: "WEB", Node: "pve-01", Type: "qemu", Status: "running"}}
	fixture.agentEmptyVMIDs = nil
	fixture.interfaces = []proxmoxclient.AgentNetworkInterface{{
		Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.12"}},
	}}
	fixture.mu.Unlock()
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("new identity with first absent-pending observation: %v", err)
	}
	if got := dnsAnswerRecords(ctx, t, backend); len(got) != 0 {
		t.Fatalf("new identity wrote DNS before the retained addressless identity was confirmed absent: %+v", got)
	}

	// The lifecycle file must carry the unseen name through a process restart,
	// even though the old identity never had an address or ownership marker.
	r, backend, _ = newStatefulProxmoxDNSReconcilerWithBackend(t, client, stateFile, backend, extra)
	if err := r.RecoverOwnership(ctx); err != nil {
		t.Fatalf("recover ownership after restart: %v", err)
	}
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("second absent-pending observation after restart: %v", err)
	}
	if got := dnsAnswerRecords(ctx, t, backend); len(got) != 0 {
		t.Fatalf("new identity wrote DNS before post-restart absence confirmation: %+v", got)
	}

	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("confirmed absent observation: %v", err)
	}
	answers := dnsAnswerRecords(ctx, t, backend)
	if len(answers) != 1 || answers[0].Target != "10.20.0.12" || answers[0].Hostname != "web.test-cluster.example.com" {
		t.Fatalf("new identity DNS after confirmed deletion = %+v, want WEB address only after confirmation", answers)
	}
}

func TestProxmoxStatefulFilteredReappearanceResetsAbsenceCount(t *testing.T) {
	fixture := &proxmoxDNSFixture{
		resources: []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"}},
		interfaces: []proxmoxclient.AgentNetworkInterface{{
			Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.10"}},
		}},
	}
	_, client := newProxmoxFixtureClient(t, fixture)
	stateFile := filepath.Join(t.TempDir(), "proxmox-lifecycle.json")
	r, backend, _ := newStatefulProxmoxDNSReconciler(t, client, stateFile, func(cfg *proxmoxclient.AdapterConfig) {
		cfg.NodeFilter = "pve-00"
	})
	ctx := context.Background()
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}

	fixture.mu.Lock()
	fixture.resources = nil
	fixture.mu.Unlock()
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("first absence: %v", err)
	}

	// The full inventory sees the same identity again, but the configured node
	// filter keeps it outside desired state. This still breaks the absence run.
	fixture.mu.Lock()
	fixture.resources = []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-01", Type: "qemu", Status: "running"}}
	fixture.mu.Unlock()
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("filtered reappearance: %v", err)
	}

	fixture.mu.Lock()
	fixture.resources = nil
	fixture.mu.Unlock()
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("absence after filtered reappearance: %v", err)
	}
	answers := dnsAnswerRecords(ctx, t, backend)
	if len(answers) != 1 || answers[0].Target != "10.20.0.10" {
		t.Fatalf("non-consecutive absence observations removed DNS: %+v", answers)
	}

	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("second consecutive absence after reappearance: %v", err)
	}
	if answers = dnsAnswerRecords(ctx, t, backend); len(answers) != 0 {
		t.Fatalf("confirmed consecutive absence left DNS: %+v", answers)
	}
}

func TestProxmoxStatefulCollisionHoldsPendingReplacementAndOwnershipRepair(t *testing.T) {
	const host = "web.test-cluster.example.com"
	first := statefulProxmoxWorkload("cluster|qemu|100", "web", "pve-00", "100", "10.20.0.10")
	second := statefulProxmoxWorkload("cluster|qemu|101", "WEB", "pve-01", "101", "10.20.0.11")
	lister := &snapshotWorkloadLister{
		workloads: []workload.Workload{first, second},
		status:    workload.SnapshotStatus{Complete: true, AllowOrphanCleanup: true},
	}
	backend := newTestMockProvider("dns")
	providers := testProviderRegistry(quietLogger(), backend)
	providers.SetInstanceID("test-instance")
	if err := providers.CreateInstance(provider.ProviderInstanceConfig{
		Name: "dns", TypeName: "mock", RecordType: provider.RecordTypeA,
		Target: "192.0.2.1", TTL: 300, Domains: []string{"*.example.com"},
	}); err != nil {
		t.Fatal(err)
	}
	instance, ok := providers.Get("dns")
	if !ok {
		t.Fatal("DNS provider instance is missing")
	}
	adopt := true // isolate the lifecycle collision gate from unowned-record policy
	instance.AdoptExisting = &adopt
	compact, err := provider.NewCompactOwnershipProvider(backend, "test-instance", "example.com", quietLogger(), nil)
	if err != nil {
		t.Fatalf("NewCompactOwnershipProvider: %v", err)
	}
	instance.Provider = compact
	sources := source.NewRegistry(quietLogger())
	if err := sources.Register(proxmoxsource.New(
		proxmoxsource.WithDomain("example.com"),
		proxmoxsource.WithVMZone("example.com"),
		proxmoxsource.WithLogger(quietLogger()),
	)); err != nil {
		t.Fatal(err)
	}
	r := New([]workload.Lister{lister}, sources, providers, WithConfig(DefaultConfig()), WithLogger(quietLogger()))
	repair := provider.Record{Hostname: host, Type: provider.RecordTypeA, Target: "10.20.0.11", TTL: 300}
	backend.AddRecord(repair)
	r.rememberMemberOwnershipRepair(instance, repair)
	if len(r.pendingMemberOwnership) != 1 {
		t.Fatal("fixture did not create pending member ownership repair")
	}
	old := provider.Record{Hostname: host, Type: provider.RecordTypeA, Target: "10.20.0.10", TTL: 300}
	key := replacementKey{Identity: instance.Identity, Hostname: source.NormalizeHostname(host)}
	r.pendingReplacements = map[replacementKey]replacementRecovery{
		key: {Instance: instance, Removed: []provider.Record{old}},
	}

	createdBefore := len(backend.GetCreated())
	deletedBefore := len(backend.GetDeleted())
	recordsBefore, err := backend.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result, err := r.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("collision reconcile with pending recovery: %v", err)
	}
	recordsAfter, err := backend.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(backend.GetCreated()) != createdBefore || len(backend.GetDeleted()) != deletedBefore || !reflect.DeepEqual(recordsAfter, recordsBefore) {
		t.Fatalf("frozen name recovery wrote DNS or ownership data: actions=%+v before=%+v after=%+v created=%+v deleted=%+v",
			result.Actions, recordsBefore, recordsAfter, backend.GetCreated(), backend.GetDeleted())
	}
	if len(r.pendingReplacements) != 1 || len(r.pendingMemberOwnership) != 1 {
		t.Fatalf("collision discarded held recovery intent: replacements=%d ownership=%d", len(r.pendingReplacements), len(r.pendingMemberOwnership))
	}
}

func TestProxmoxStatefulNoTXTProviderUsesPreviousDesiredEvidence(t *testing.T) {
	fixture := &proxmoxDNSFixture{
		resources: []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"}},
		interfaces: []proxmoxclient.AgentNetworkInterface{{
			Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.10"}},
		}},
	}
	_, client := newProxmoxFixtureClient(t, fixture)
	backend := newTestMockProvider("dns")
	backend.capabilities = &provider.Capabilities{
		SupportsNativeUpdate: true,
		SupportedRecordTypes: []provider.RecordType{provider.RecordTypeA},
	}
	r, backend, _ := newStatefulProxmoxDNSReconcilerWithBackend(
		t, client, filepath.Join(t.TempDir(), "proxmox-lifecycle.json"), backend,
		func(cfg *proxmoxclient.AdapterConfig) { cfg.LifecycleScopeConfirmed = false },
	)

	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("initial no-TXT reconcile: %v", err)
	}
	answers := dnsAnswerRecords(context.Background(), t, backend)
	if len(answers) != 1 || answers[0].Target != "10.20.0.10" {
		t.Fatalf("initial no-TXT answers = %+v, want 10.20.0.10", answers)
	}

	fixture.mu.Lock()
	fixture.interfaces = []proxmoxclient.AgentNetworkInterface{{
		Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.11"}},
	}}
	fixture.mu.Unlock()
	result, err := r.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("updated no-TXT reconcile: %v", err)
	}
	if result.HasErrors() {
		t.Fatalf("no-TXT provider was frozen after its own write: actions=%+v", result.Actions)
	}
	answers = dnsAnswerRecords(context.Background(), t, backend)
	if len(answers) != 1 || answers[0].Target != "10.20.0.11" {
		t.Fatalf("updated no-TXT answers = %+v, want 10.20.0.11", answers)
	}
}

func TestProxmoxStatefulLegacyOwnershipDoesNotAuthorizeManualMXOrNSThroughReconcile(t *testing.T) {
	for _, recordType := range []provider.RecordType{provider.RecordTypeMX, provider.RecordTypeNS} {
		t.Run(string(recordType), func(t *testing.T) {
			const host = "web.test-cluster.example.com"
			backend := newTestMockProvider("dns")
			backend.AddRecord(provider.Record{Hostname: host, Type: recordType, Target: "manual.example.net", TTL: 60})
			backend.AddRecord(provider.OwnershipRecord(host, 300, "test-instance", nil))
			recordsBefore, err := backend.List(context.Background())
			if err != nil {
				t.Fatalf("list seeded records: %v", err)
			}

			fixture := &proxmoxDNSFixture{
				resources:  []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"}},
				interfaces: []proxmoxclient.AgentNetworkInterface{{Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.202"}}}},
			}
			_, client := newProxmoxFixtureClient(t, fixture)
			r, backend, _ := newStatefulProxmoxDNSReconcilerWithBackend(
				t, client, filepath.Join(t.TempDir(), "proxmox-lifecycle.json"), backend,
				func(cfg *proxmoxclient.AdapterConfig) { cfg.LifecycleScopeConfirmed = false },
			)
			result, err := r.Reconcile(context.Background())
			if err != nil {
				t.Fatalf("reconcile name with legacy ownership: %v", err)
			}
			recordsAfter, err := backend.List(context.Background())
			if err != nil {
				t.Fatalf("list reconciled records: %v", err)
			}
			if !reflect.DeepEqual(recordsAfter, recordsBefore) || len(backend.GetCreated()) != 0 || len(backend.GetDeleted()) != 0 {
				t.Fatalf("legacy marker authorized a write beside manual %s: actions=%+v before=%+v after=%+v created=%+v deleted=%+v",
					recordType, result.Actions, recordsBefore, recordsAfter, backend.GetCreated(), backend.GetDeleted())
			}
		})
	}
}

func TestProxmoxStatefulLifecycleMultiNICReconcilesSecondInterface(t *testing.T) {
	fixture := &proxmoxDNSFixture{
		resources: []proxmoxclient.ClusterResource{{VMID: 200, Name: "multi", Node: "pve-00", Type: "lxc", Status: "running"}},
		lxcConfig: map[string]string{
			"net0": "name=eth0,ip=dhcp",
			"net1": "name=eth1,ip=10.30.0.7/24",
		},
	}
	_, client := newProxmoxFixtureClient(t, fixture)
	r, backend, _ := newStatefulProxmoxDNSReconciler(t, client, filepath.Join(t.TempDir(), "proxmox-lifecycle.json"), func(cfg *proxmoxclient.AdapterConfig) {
		cfg.AllowedInterfaces = []string{"eth"}
	})

	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("multi-NIC reconcile: %v", err)
	}
	answers := dnsAnswerRecords(context.Background(), t, backend)
	if len(answers) != 1 || answers[0].Target != "10.30.0.7" {
		t.Fatalf("multi-NIC DNS answers = %+v, want second interface address", answers)
	}
}
