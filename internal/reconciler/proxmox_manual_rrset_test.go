package reconciler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	proxmoxclient "github.com/maxfield-allison/dnsweaver/internal/proxmox"
	"github.com/maxfield-allison/dnsweaver/pkg/provider"
	"github.com/maxfield-allison/dnsweaver/pkg/source"
	"github.com/maxfield-allison/dnsweaver/pkg/workload"
	"github.com/maxfield-allison/dnsweaver/providers/technitium"
	proxmoxsource "github.com/maxfield-allison/dnsweaver/sources/proxmox"
)

type proxmoxManualRRsetRecord struct {
	Name   string
	Type   string
	Target string
	TTL    int
}

type proxmoxManualRRsetWrite struct {
	Path   string
	Name   string
	Type   string
	Target string
	TTL    int
}

// proxmoxManualRRsetAPI models the observed Technitium behavior that adding a
// same-name A member applies the requested TTL to the whole A RRset. The HTTP
// requests themselves are made by the real Technitium provider.
type proxmoxManualRRsetAPI struct {
	mu                  sync.Mutex
	records             []proxmoxManualRRsetRecord
	writes              []proxmoxManualRRsetWrite
	failOwnershipWrites int
}

func (f *proxmoxManualRRsetAPI) handler(t *testing.T) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("ParseForm: %v", err)
			http.Error(w, "bad form", http.StatusBadRequest)
			return
		}

		f.mu.Lock()
		defer f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		params := r.Form
		name, recordType := params.Get("domain"), params.Get("type")
		target := params.Get("ipAddress")
		if recordType == "TXT" {
			target = params.Get("text")
		}
		ttl, _ := strconv.Atoi(params.Get("ttl"))

		switch r.URL.Path {
		case "/api/zones/records/get":
			apiRecords := make([]map[string]any, 0, len(f.records))
			for _, record := range f.records {
				rdata := map[string]any{}
				switch record.Type {
				case "A", "AAAA":
					rdata["ipAddress"] = record.Target
				case "TXT":
					rdata["text"] = record.Target
				}
				apiRecords = append(apiRecords, map[string]any{
					"name": record.Name, "type": record.Type, "ttl": record.TTL,
					"disabled": false, "rData": rdata,
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": "ok",
				"response": map[string]any{
					"zone":    map[string]any{"name": "example.com", "type": "Primary", "disabled": false},
					"records": apiRecords,
				},
			})
		case "/api/zones/records/add":
			f.writes = append(f.writes, proxmoxManualRRsetWrite{
				Path: r.URL.Path, Name: name, Type: recordType, Target: target, TTL: ttl,
			})
			if recordType == "TXT" && provider.IsOwnershipRecord(name) && f.failOwnershipWrites > 0 {
				f.failOwnershipWrites--
				http.Error(w, "injected ownership marker failure", http.StatusServiceUnavailable)
				return
			}
			if recordType == "A" {
				for i := range f.records {
					if f.records[i].Name == name && f.records[i].Type == recordType {
						f.records[i].TTL = ttl
					}
				}
			}
			f.records = append(f.records, proxmoxManualRRsetRecord{Name: name, Type: recordType, Target: target, TTL: ttl})
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		case "/api/zones/records/delete":
			f.writes = append(f.writes, proxmoxManualRRsetWrite{
				Path: r.URL.Path, Name: name, Type: recordType, Target: target,
			})
			f.records = slices.DeleteFunc(f.records, func(record proxmoxManualRRsetRecord) bool {
				return record.Name == name && record.Type == recordType && record.Target == target
			})
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		case "/api/zones/records/update":
			f.writes = append(f.writes, proxmoxManualRRsetWrite{
				Path: r.URL.Path, Name: name, Type: recordType, Target: target, TTL: ttl,
			})
			newTarget := params.Get("newIpAddress")
			if newTarget == "" {
				newTarget = params.Get("newText")
			}
			for i := range f.records {
				if f.records[i].Name == name && f.records[i].Type == recordType && f.records[i].Target == target {
					if newTarget != "" {
						f.records[i].Target = newTarget
					}
					if ttl > 0 {
						f.records[i].TTL = ttl
					}
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		default:
			t.Errorf("unexpected Technitium API path %q", r.URL.Path)
			http.NotFound(w, r)
		}
	})
}

func (f *proxmoxManualRRsetAPI) snapshot() ([]proxmoxManualRRsetRecord, []proxmoxManualRRsetWrite) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]proxmoxManualRRsetRecord(nil), f.records...), append([]proxmoxManualRRsetWrite(nil), f.writes...)
}

func newStatefulProxmoxTechnitiumReconciler(
	t *testing.T,
	client *proxmoxclient.Client,
	stateFile string,
	api *proxmoxManualRRsetAPI,
) (*Reconciler, *proxmoxclient.WorkloadListerAdapter) {
	t.Helper()

	dnsServer := httptest.NewServer(api.handler(t))
	t.Cleanup(dnsServer.Close)
	logger := quietLogger()
	dnsProvider, err := technitium.New("dns", &technitium.Config{
		URL: dnsServer.URL, Token: "fixture-token", Zone: "example.com", TTL: 300,
	}, technitium.WithProviderLogger(logger))
	if err != nil {
		t.Fatalf("technitium.New: %v", err)
	}

	providers := provider.NewRegistry(logger)
	providers.SetInstanceID("test-instance")
	providers.SetOwnershipStorage("legacy")
	providers.RegisterFactory("technitium-test", func(provider.FactoryConfig) (provider.Provider, error) {
		return dnsProvider, nil
	})
	adoptExisting := false
	if err := providers.CreateInstance(provider.ProviderInstanceConfig{
		Name: "dns", TypeName: "technitium-test", RecordType: provider.RecordTypeA,
		Target: "192.0.2.1", TTL: 300, Mode: provider.ModeManaged,
		Domains: []string{"*.example.com"}, AdoptExisting: &adoptExisting,
	}); err != nil {
		t.Fatalf("CreateInstance: %v", err)
	}

	sources := source.NewRegistry(logger)
	if err := sources.Register(proxmoxsource.New(
		proxmoxsource.WithDomain("example.com"),
		proxmoxsource.WithVMZone("example.com"),
		proxmoxsource.WithLogger(logger),
	)); err != nil {
		t.Fatalf("register Proxmox source: %v", err)
	}
	adapter := proxmoxclient.NewWorkloadListerAdapter(client, proxmoxclient.AdapterConfig{
		LifecycleMode:               proxmoxclient.LifecycleModeStateful,
		LifecycleStateFile:          stateFile,
		LifecycleScopeConfirmed:     false,
		LifecycleDeleteObservations: 2,
		VMZone:                      "example.com",
	}, logger)
	config := DefaultConfig()
	config.InstanceID = "test-instance"
	config.OwnershipTracking = true
	config.AdoptExisting = false
	return New([]workload.Lister{adapter}, sources, providers, WithConfig(config), WithLogger(logger)), adapter
}

func TestProxmoxStatefulUnownedManualAFreezesEntireRRset(t *testing.T) {
	const manualHost = "web.test-cluster.example.com"
	const controlHost = "api.test-cluster.example.com"
	manual := proxmoxManualRRsetRecord{Name: manualHost, Type: "A", Target: "10.20.0.250", TTL: 60}
	api := &proxmoxManualRRsetAPI{records: []proxmoxManualRRsetRecord{manual}}
	pve := &proxmoxDNSFixture{
		resources: []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"}},
		interfaces: []proxmoxclient.AgentNetworkInterface{{
			Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.202"}},
		}},
	}
	_, client := newProxmoxFixtureClient(t, pve)
	r, adapter := newStatefulProxmoxTechnitiumReconciler(t, client, filepath.Join(t.TempDir(), "proxmox-lifecycle.json"), api)
	ctx := context.Background()

	result, err := r.Reconcile(ctx)
	if err != nil {
		t.Fatalf("manual RRset reconcile: %v", err)
	}
	if result.HostnamesDiscovered != 1 || result.DesiredMembers != 1 {
		t.Fatalf("stateful Proxmox claim was not reconciled: hostnames=%d desired_members=%d actions=%+v",
			result.HostnamesDiscovered, result.DesiredMembers, result.Actions)
	}
	if adapter.SnapshotStatus().AllowOrphanCleanup {
		t.Fatal("scope=false unexpectedly allowed orphan cleanup")
	}
	gotRecords, gotWrites := api.snapshot()
	if !slices.Equal(gotRecords, []proxmoxManualRRsetRecord{manual}) {
		t.Fatalf("unowned manual RRset changed: records=%+v writes=%+v, want only the original A member", gotRecords, gotWrites)
	}
	if len(gotWrites) != 0 {
		t.Fatalf("unowned manual RRset caused provider writes: %+v", gotWrites)
	}

	// A separate, uncontested stateful name proves scope=false is only a
	// removal-safety gate and must not disable all DNS creation.
	pve.mu.Lock()
	pve.resources = append(pve.resources, proxmoxclient.ClusterResource{VMID: 101, Name: "api", Node: "pve-00", Type: "qemu", Status: "running"})
	pve.interfaces = []proxmoxclient.AgentNetworkInterface{{
		Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.203"}},
	}}
	pve.mu.Unlock()
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("uncontested control reconcile: %v", err)
	}
	if adapter.SnapshotStatus().AllowOrphanCleanup {
		t.Fatal("scope=false unexpectedly allowed orphan cleanup after control reconcile")
	}

	gotRecords, gotWrites = api.snapshot()
	var manualMembers []proxmoxManualRRsetRecord
	var controlA, controlOwnership bool
	for _, record := range gotRecords {
		if strings.EqualFold(record.Name, manualHost) && record.Type == "A" {
			manualMembers = append(manualMembers, record)
		}
		if strings.EqualFold(record.Name, controlHost) && record.Type == "A" && record.Target == "10.20.0.203" && record.TTL == 300 {
			controlA = true
		}
		if record.Type == "TXT" && provider.IsOwnershipRecord(record.Name) && strings.EqualFold(provider.ExtractHostnameFromOwnership(record.Name), controlHost) {
			controlOwnership = true
		}
	}
	if !slices.Equal(manualMembers, []proxmoxManualRRsetRecord{manual}) {
		t.Fatalf("control create changed the manual RRset: records=%+v writes=%+v", gotRecords, gotWrites)
	}
	if !controlA || !controlOwnership {
		t.Fatalf("scope=false blocked an uncontested stateful create: A=%t ownership=%t records=%+v", controlA, controlOwnership, gotRecords)
	}
	for _, write := range gotWrites {
		if strings.EqualFold(write.Name, manualHost) || (provider.IsOwnershipRecord(write.Name) && strings.EqualFold(provider.ExtractHostnameFromOwnership(write.Name), manualHost)) {
			t.Fatalf("provider write touched the conflicted manual name: %+v", write)
		}
	}
}

func TestProxmoxStatefulPendingOwnershipRepairCanRecover(t *testing.T) {
	const host = "web.test-cluster.example.com"
	api := &proxmoxManualRRsetAPI{failOwnershipWrites: 1}
	pve := &proxmoxDNSFixture{
		resources: []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"}},
		interfaces: []proxmoxclient.AgentNetworkInterface{{
			Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.202"}},
		}},
	}
	_, client := newProxmoxFixtureClient(t, pve)
	r, _ := newStatefulProxmoxTechnitiumReconciler(t, client, filepath.Join(t.TempDir(), "lifecycle.json"), api)
	ctx := context.Background()
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("first reconcile with injected ownership failure: %v", err)
	}
	if len(r.pendingMemberOwnership) != 1 {
		t.Fatalf("pending ownership repairs after injected failure = %d, want 1", len(r.pendingMemberOwnership))
	}
	firstRecords, _ := api.snapshot()
	if len(firstRecords) != 1 || firstRecords[0].Type != "A" || firstRecords[0].Target != "10.20.0.202" {
		t.Fatalf("first reconcile records = %+v, want only the locally created A", firstRecords)
	}

	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("ownership repair retry: %v", err)
	}
	if len(r.pendingMemberOwnership) != 0 {
		t.Fatalf("pending ownership repairs after retry = %d, want 0", len(r.pendingMemberOwnership))
	}
	records, writes := api.snapshot()
	var addressCount, ownershipCount int
	for _, record := range records {
		if record.Name == host && record.Type == "A" && record.Target == "10.20.0.202" {
			addressCount++
		}
		if record.Type == "TXT" && provider.IsOwnershipRecord(record.Name) && strings.EqualFold(provider.ExtractHostnameFromOwnership(record.Name), host) {
			ownershipCount++
		}
	}
	if addressCount != 1 || ownershipCount != 1 {
		t.Fatalf("records after ownership repair = %+v, want one A and one ownership marker", records)
	}
	var addressWrites int
	for _, write := range writes {
		if write.Name == host && write.Type == "A" {
			addressWrites++
		}
	}
	if addressWrites != 1 {
		t.Fatalf("stateful name was frozen or duplicated during ownership repair; writes=%+v", writes)
	}
}

func TestProxmoxStatefulUnownedUnsupportedRecordTypeFreezesName(t *testing.T) {
	const host = "web.test-cluster.example.com"
	for _, recordType := range []string{"MX", "NS", "CAA"} {
		t.Run(recordType, func(t *testing.T) {
			manual := proxmoxManualRRsetRecord{
				Name: host, Type: recordType, Target: "operator-owned-data", TTL: 60,
			}
			api := &proxmoxManualRRsetAPI{records: []proxmoxManualRRsetRecord{manual}}
			pve := &proxmoxDNSFixture{
				resources: []proxmoxclient.ClusterResource{{
					VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running",
				}},
				interfaces: []proxmoxclient.AgentNetworkInterface{{
					Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.202"}},
				}},
			}
			_, client := newProxmoxFixtureClient(t, pve)
			r, adapter := newStatefulProxmoxTechnitiumReconciler(
				t, client, filepath.Join(t.TempDir(), "proxmox-lifecycle.json"), api,
			)

			result, err := r.Reconcile(context.Background())
			if err != nil {
				t.Fatalf("stateful reconcile with manual %s record: %v", recordType, err)
			}
			if result.HostnamesDiscovered != 1 || result.DesiredMembers != 1 {
				t.Fatalf("stateful claim was not reconciled: hostnames=%d desired_members=%d actions=%+v",
					result.HostnamesDiscovered, result.DesiredMembers, result.Actions)
			}
			if adapter.SnapshotStatus().AllowOrphanCleanup {
				t.Fatal("scope=false unexpectedly allowed orphan cleanup")
			}
			gotRecords, gotWrites := api.snapshot()
			if !slices.Equal(gotRecords, []proxmoxManualRRsetRecord{manual}) {
				t.Fatalf("manual %s record or same-name DNS data changed: records=%+v writes=%+v",
					recordType, gotRecords, gotWrites)
			}
			if len(gotWrites) != 0 {
				t.Fatalf("manual %s record caused provider writes: %+v", recordType, gotWrites)
			}
		})
	}
}
