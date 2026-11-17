package reconciler

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	proxmoxclient "github.com/maxfield-allison/dnsweaver/internal/proxmox"
	"github.com/maxfield-allison/dnsweaver/pkg/provider"
)

type proxmoxManualSiblingScenario struct {
	api        *proxmoxManualRRsetAPI
	fixture    *proxmoxDNSFixture
	client     *proxmoxclient.Client
	dnsServer  *httptest.Server
	stateFile  string
	reconciler *Reconciler
	expected   []proxmoxManualRRsetRecord
}

func newProxmoxManualSiblingScenario(t *testing.T) *proxmoxManualSiblingScenario {
	t.Helper()
	const host = "web.test-cluster.example.com"
	api := &proxmoxManualRRsetAPI{}
	fixture := &proxmoxDNSFixture{
		resources: []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"}},
		interfaces: []proxmoxclient.AgentNetworkInterface{{
			Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.202"}},
		}},
	}
	_, client := newProxmoxFixtureClient(t, fixture)
	dnsServer := httptest.NewServer(api.handler(t))
	t.Cleanup(dnsServer.Close)
	stateFile := filepath.Join(t.TempDir(), "proxmox-lifecycle.json")
	r, _ := newStatefulProxmoxTechnitiumReconcilerWithProvider(
		t, client, stateFile, newProxmoxManualRRsetTechnitiumProvider(t, dnsServer.URL), true,
	)
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("initial stateful reconcile: %v", err)
	}
	initial, _ := api.snapshot()
	var hasManagedAddress, hasOwnership bool
	for _, record := range initial {
		hasManagedAddress = hasManagedAddress || (record.Name == host && record.Type == "A" && record.Target == "10.20.0.202")
		hasOwnership = hasOwnership || (record.Type == "TXT" && provider.IsOwnershipRecord(record.Name) &&
			strings.EqualFold(provider.ExtractHostnameFromOwnership(record.Name), host))
	}
	if !hasManagedAddress || !hasOwnership {
		t.Fatalf("initial reconcile did not create an owned A member: records=%+v", initial)
	}

	// Technitium applies an A RRset TTL to every member when a manual sibling
	// is added. Model the resulting visible RRset exactly before testing removal.
	api.addManualA(host, "10.20.0.250", 60)
	api.clearWrites()
	expected, _ := api.snapshot()
	return &proxmoxManualSiblingScenario{
		api: api, fixture: fixture, client: client, dnsServer: dnsServer,
		stateFile: stateFile, reconciler: r, expected: expected,
	}
}

func (s *proxmoxManualSiblingScenario) confirmAbsentOnce(t *testing.T, r *Reconciler) {
	t.Helper()
	s.fixture.mu.Lock()
	s.fixture.resources = nil
	s.fixture.mu.Unlock()
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("confirmed absence reconcile: %v", err)
	}
}

func (s *proxmoxManualSiblingScenario) assertUnchanged(t *testing.T, when string) {
	t.Helper()
	got, writes := s.api.snapshot()
	if !slices.Equal(got, s.expected) {
		t.Errorf("%s changed the same-name RRset or TTL: records=%+v want=%+v writes=%+v", when, got, s.expected, writes)
	}
	if len(writes) != 0 {
		t.Errorf("%s made provider writes despite the manual sibling: %+v", when, writes)
	}
}

func assertDefaultHTTPSAddress(t *testing.T, api *proxmoxManualRRsetAPI, expectedIP string) {
	t.Helper()
	const host = "web.test-cluster.example.com"
	records, _ := api.snapshot()
	var addresses, companions int
	for _, record := range records {
		if record.Name == host && record.Type == "A" {
			addresses++
			if record.Target != expectedIP {
				t.Errorf("managed A address = %s, want %s; records=%+v", record.Target, expectedIP, records)
			}
		}
		if record.Name == host && record.Type == "HTTPS" && record.HTTPSPriority == 1 &&
			record.HTTPSTargetName == "." && record.HTTPSParams == "alpn|h2" && record.TTL == 300 {
			companions++
		}
	}
	if addresses != 1 {
		t.Errorf("managed A record count = %d, want exactly one for %s; records=%+v", addresses, expectedIP, records)
	}
	if companions != 1 {
		t.Errorf("default HTTPS companion count = %d, want one; records=%+v", companions, records)
	}
}

func setProxmoxFixtureAddress(fixture *proxmoxDNSFixture, address string) {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	fixture.interfaces = []proxmoxclient.AgentNetworkInterface{{
		Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: address}},
	}}
}

func newStatefulProxmoxHTTPSDefaultScenario(t *testing.T) (
	*proxmoxManualRRsetAPI,
	*proxmoxDNSFixture,
	*proxmoxclient.Client,
	func() provider.Provider,
) {
	t.Helper()
	api := &proxmoxManualRRsetAPI{}
	fixture := &proxmoxDNSFixture{
		resources: []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"}},
		interfaces: []proxmoxclient.AgentNetworkInterface{{
			Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.202"}},
		}},
	}
	_, client := newProxmoxFixtureClient(t, fixture)
	dnsServer := httptest.NewServer(api.handler(t))
	t.Cleanup(dnsServer.Close)
	return api, fixture, client, newDefaultHTTPSProxmoxManualRRsetProviderFactory(t, dnsServer.URL)
}

func assertStatefulManualRRsetUnchanged(t *testing.T, initial []proxmoxManualRRsetRecord) {
	t.Helper()
	api := &proxmoxManualRRsetAPI{records: append([]proxmoxManualRRsetRecord(nil), initial...)}
	pve := &proxmoxDNSFixture{
		resources: []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"}},
		interfaces: []proxmoxclient.AgentNetworkInterface{{
			Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.202"}},
		}},
	}
	_, client := newProxmoxFixtureClient(t, pve)
	r, _ := newStatefulProxmoxTechnitiumReconciler(t, client, filepath.Join(t.TempDir(), "lifecycle.json"), api)
	result, err := r.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("stateful manual RRset reconcile: %v", err)
	}
	if result.HostnamesDiscovered != 1 || result.DesiredMembers != 1 {
		t.Fatalf("stateful claim was not exercised: hostnames=%d desired=%d actions=%+v",
			result.HostnamesDiscovered, result.DesiredMembers, result.Actions)
	}
	got, writes := api.snapshot()
	if !slices.Equal(got, initial) {
		t.Fatalf("manual DNS name changed: records=%+v writes=%+v; want %+v", got, writes, initial)
	}
	if len(writes) != 0 {
		t.Fatalf("manual DNS name received provider writes: %+v", writes)
	}
}

func TestProxmoxStatefulUnownedManualAAAAFreezesAWrite(t *testing.T) {
	manual := proxmoxManualRRsetRecord{
		Name: "web.test-cluster.example.com", Type: "AAAA", Target: "2001:db8::250", TTL: 60,
	}
	assertStatefulManualRRsetUnchanged(t, []proxmoxManualRRsetRecord{manual})
}

func TestProxmoxStatefulUnownedManualTXTFreezesName(t *testing.T) {
	manual := proxmoxManualRRsetRecord{
		Name: "web.test-cluster.example.com", Type: "TXT", Target: "operator-owned=keep", TTL: 60,
	}
	assertStatefulManualRRsetUnchanged(t, []proxmoxManualRRsetRecord{manual})
}

func TestProxmoxStatefulMixedOwnedAndUnownedMembersFreezeName(t *testing.T) {
	const host = "web.test-cluster.example.com"
	manual := proxmoxManualRRsetRecord{Name: host, Type: "A", Target: "10.20.0.250", TTL: 60}
	owned := provider.Record{Hostname: host, Type: provider.RecordTypeA, Target: "10.20.0.251", TTL: 300}
	marker := provider.MemberOwnershipRecord(host, 300, "test-instance", owned, nil)
	initial := []proxmoxManualRRsetRecord{
		manual,
		{Name: owned.Hostname, Type: string(owned.Type), Target: owned.Target, TTL: owned.TTL},
		{Name: marker.Hostname, Type: string(marker.Type), Target: marker.Target, TTL: marker.TTL},
	}
	assertStatefulManualRRsetUnchanged(t, initial)
}

func TestProxmoxStatefulUnownedManualNameBlocksPendingOwnershipRepair(t *testing.T) {
	const host = "web.test-cluster.example.com"
	manual := proxmoxManualRRsetRecord{Name: host, Type: "A", Target: "10.20.0.250", TTL: 60}
	api := &proxmoxManualRRsetAPI{records: []proxmoxManualRRsetRecord{manual}}
	pve := &proxmoxDNSFixture{
		resources: []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"}},
		interfaces: []proxmoxclient.AgentNetworkInterface{{
			Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.202"}},
		}},
	}
	_, client := newProxmoxFixtureClient(t, pve)
	r, _ := newStatefulProxmoxTechnitiumReconciler(t, client, filepath.Join(t.TempDir(), "lifecycle.json"), api)
	instance, ok := r.providers.Get("dns")
	if !ok {
		t.Fatal("DNS provider instance is missing")
	}
	if _, supported := instance.Provider.(provider.SharedOwnership); !supported {
		t.Fatal("fixture provider does not support ownership repair")
	}
	r.rememberMemberOwnershipRepair(instance, provider.Record{
		Hostname: host, Type: provider.RecordTypeA, Target: "10.20.0.202", TTL: 300,
	})
	if len(r.pendingMemberOwnership) != 1 {
		t.Fatal("failed to seed pending member ownership repair")
	}

	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("stateful reconcile with pending repair: %v", err)
	}
	got, writes := api.snapshot()
	if !slices.Equal(got, []proxmoxManualRRsetRecord{manual}) || len(writes) != 0 {
		t.Fatalf("pending repair changed unowned manual name: records=%+v writes=%+v", got, writes)
	}
	for _, write := range writes {
		if strings.EqualFold(write.Name, host) ||
			(provider.IsOwnershipRecord(write.Name) && strings.EqualFold(provider.ExtractHostnameFromOwnership(write.Name), host)) {
			t.Fatalf("pending repair wrote at frozen name: %+v", write)
		}
	}
}

func TestProxmoxStatefulPendingOwnershipRepairFreezesRenamedOldName(t *testing.T) {
	const oldHost = "web.test-cluster.example.com"
	const currentHost = "api.test-cluster.example.com"
	oldAddress := provider.Record{Hostname: oldHost, Type: provider.RecordTypeA, Target: "10.20.0.202", TTL: 300}
	manual := proxmoxManualRRsetRecord{Name: oldHost, Type: "TXT", Target: "operator-owned=keep", TTL: 60}
	api := &proxmoxManualRRsetAPI{records: []proxmoxManualRRsetRecord{
		{Name: oldHost, Type: "A", Target: oldAddress.Target, TTL: oldAddress.TTL},
		manual,
	}}
	fixture := &proxmoxDNSFixture{
		resources: []proxmoxclient.ClusterResource{{VMID: 101, Name: "api", Node: "pve-00", Type: "qemu", Status: "running"}},
		interfaces: []proxmoxclient.AgentNetworkInterface{{
			Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.203"}},
		}},
	}
	_, client := newProxmoxFixtureClient(t, fixture)
	stateFile := filepath.Join(t.TempDir(), "proxmox-lifecycle.json")
	r, _ := newStatefulProxmoxTechnitiumReconciler(t, client, stateFile, api)
	instance, ok := r.providers.Get("dns")
	if !ok {
		t.Fatal("stateful DNS provider missing")
	}
	r.rememberMemberOwnershipRepair(instance, oldAddress, true)

	ctx := context.Background()
	result, err := r.Reconcile(ctx)
	if err != nil {
		t.Fatalf("reconcile renamed VM: %v", err)
	}
	if result.HostnamesDiscovered != 1 || result.DesiredMembers != 1 {
		t.Fatalf("current stateful claim was not reconciled: hostnames=%d desired=%d", result.HostnamesDiscovered, result.DesiredMembers)
	}
	if len(r.pendingMemberOwnership) != 1 {
		t.Fatalf("pending old-name repair was discarded: %d", len(r.pendingMemberOwnership))
	}

	records, writes := api.snapshot()
	var oldRecords []proxmoxManualRRsetRecord
	for _, record := range records {
		if record.Name == oldHost {
			oldRecords = append(oldRecords, record)
		}
	}
	if !slices.Equal(oldRecords, []proxmoxManualRRsetRecord{
		{Name: oldHost, Type: "A", Target: oldAddress.Target, TTL: oldAddress.TTL},
		manual,
	}) {
		t.Fatalf("renamed VM changed old manual name: records=%+v writes=%+v", oldRecords, writes)
	}
	for _, write := range writes {
		if write.Name == oldHost ||
			(provider.IsOwnershipRecord(write.Name) && strings.EqualFold(provider.ExtractHostnameFromOwnership(write.Name), oldHost)) {
			t.Fatalf("pending old-name repair wrote to frozen name: %+v", write)
		}
	}
	if !slices.ContainsFunc(records, func(record proxmoxManualRRsetRecord) bool {
		return record.Name == currentHost && record.Type == "A" && record.Target == "10.20.0.203"
	}) {
		t.Fatalf("uncontested renamed stateful name was not created: %+v", records)
	}
}

func TestProxmoxStatefulPendingReplacementFreezesManualNameWithoutCurrentClaim(t *testing.T) {
	const host = "web.test-cluster.example.com"
	manual := proxmoxManualRRsetRecord{Name: host, Type: "A", Target: "10.20.0.250", TTL: 60}
	api := &proxmoxManualRRsetAPI{records: []proxmoxManualRRsetRecord{manual}}
	pve := &proxmoxDNSFixture{}
	_, client := newProxmoxFixtureClient(t, pve)
	r, _ := newStatefulProxmoxTechnitiumReconciler(t, client, filepath.Join(t.TempDir(), "lifecycle.json"), api)
	instance, ok := r.providers.Get("dns")
	if !ok {
		t.Fatal("DNS provider instance is missing")
	}
	key := replacementKey{Identity: instance.Identity, Hostname: host}
	r.pendingReplacements = map[replacementKey]replacementRecovery{
		key: {
			Instance:        instance,
			Removed:         []provider.Record{{Hostname: host, Type: provider.RecordTypeA, Target: "10.20.0.202", TTL: 300}},
			StatefulProxmox: true,
		},
	}

	result, err := r.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("stateful reconcile with pending replacement: %v", err)
	}
	got, writes := api.snapshot()
	if !slices.Equal(got, []proxmoxManualRRsetRecord{manual}) || len(writes) != 0 {
		t.Fatalf("pending replacement changed unowned manual name: records=%+v writes=%+v actions=%+v", got, writes, result.Actions)
	}
	if _, pending := r.pendingReplacements[key]; !pending {
		t.Fatalf("held stateful replacement recovery was dropped: actions=%+v", result.Actions)
	}
}

func TestProxmoxStatefulDefaultHTTPSCompanionAllowsAddressUpdatesAfterRestart(t *testing.T) {
	api, fixture, client, newProvider := newStatefulProxmoxHTTPSDefaultScenario(t)
	stateFile := filepath.Join(t.TempDir(), "proxmox-lifecycle.json")
	r, _ := newStatefulProxmoxTechnitiumReconcilerWithProvider(t, client, stateFile, newProvider(), false)
	ctx := context.Background()

	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("initial reconcile with default HTTPS setting: %v", err)
	}
	assertDefaultHTTPSAddress(t, api, "10.20.0.202")

	setProxmoxFixtureAddress(fixture, "10.20.0.203")
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("first address update: %v", err)
	}
	assertDefaultHTTPSAddress(t, api, "10.20.0.203")

	// Recreate both stateful adapter/reconciler and the Technitium provider.
	restarted, _ := newStatefulProxmoxTechnitiumReconcilerWithProvider(
		t, client, stateFile, newProvider(), false,
	)
	if err := restarted.RecoverOwnership(ctx); err != nil {
		t.Fatalf("recover ownership after restart: %v", err)
	}
	setProxmoxFixtureAddress(fixture, "10.20.0.204")
	if _, err := restarted.Reconcile(ctx); err != nil {
		t.Fatalf("address update after adapter/reconciler restart: %v", err)
	}
	assertDefaultHTTPSAddress(t, api, "10.20.0.204")
}

func TestProxmoxStatefulDefaultHTTPSManualRecordFreezesName(t *testing.T) {
	const host = "web.test-cluster.example.com"
	manual := proxmoxManualRRsetRecord{
		Name: host, Type: "HTTPS", Target: "manual.example.net", TTL: 60,
		HTTPSPriority: 0, HTTPSTargetName: "manual.example.net", HTTPSParams: "alpn|h3",
	}
	api := &proxmoxManualRRsetAPI{records: []proxmoxManualRRsetRecord{manual}}
	fixture := &proxmoxDNSFixture{
		resources: []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"}},
		interfaces: []proxmoxclient.AgentNetworkInterface{{
			Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.202"}},
		}},
	}
	_, client := newProxmoxFixtureClient(t, fixture)
	dnsServer := httptest.NewServer(api.handler(t))
	t.Cleanup(dnsServer.Close)
	newProvider := newDefaultHTTPSProxmoxManualRRsetProviderFactory(t, dnsServer.URL)
	r, _ := newStatefulProxmoxTechnitiumReconcilerWithProvider(
		t, client, filepath.Join(t.TempDir(), "proxmox-lifecycle.json"), newProvider(), false,
	)
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("stateful reconcile with manual HTTPS: %v", err)
	}
	got, writes := api.snapshot()
	if !slices.Equal(got, []proxmoxManualRRsetRecord{manual}) {
		t.Errorf("manual HTTPS RRset changed: records=%+v writes=%+v", got, writes)
	}
	if len(writes) != 0 {
		t.Errorf("manual HTTPS did not freeze all writes at the name: %+v", writes)
	}
}

func TestProxmoxStatefulManualHTTPSMatchingCompanionShapeStillFreezesName(t *testing.T) {
	const host = "web.test-cluster.example.com"
	manual := proxmoxManualRRsetRecord{
		Name: host, Type: "HTTPS", Target: ".", TTL: 300,
		HTTPSPriority: 1, HTTPSTargetName: ".", HTTPSParams: "alpn|h2",
	}
	api := &proxmoxManualRRsetAPI{records: []proxmoxManualRRsetRecord{manual}}
	fixture := &proxmoxDNSFixture{
		resources: []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"}},
		interfaces: []proxmoxclient.AgentNetworkInterface{{
			Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.202"}},
		}},
	}
	_, client := newProxmoxFixtureClient(t, fixture)
	dnsServer := httptest.NewServer(api.handler(t))
	t.Cleanup(dnsServer.Close)
	newProvider := newDefaultHTTPSProxmoxManualRRsetProviderFactory(t, dnsServer.URL)
	r, _ := newStatefulProxmoxTechnitiumReconcilerWithProvider(
		t, client, filepath.Join(t.TempDir(), "proxmox-lifecycle.json"), newProvider(), false,
	)
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("stateful reconcile with shape-matching manual HTTPS: %v", err)
	}
	got, writes := api.snapshot()
	if !slices.Equal(got, []proxmoxManualRRsetRecord{manual}) {
		t.Errorf("shape-matching manual HTTPS was treated as an automatic companion: records=%+v writes=%+v", got, writes)
	}
	if len(writes) != 0 {
		t.Errorf("shape-matching manual HTTPS did not freeze all writes at the name: %+v", writes)
	}
}

func TestProxmoxStatefulConfirmedDeleteFreezesManualSiblingAfterAbsence(t *testing.T) {
	scenario := newProxmoxManualSiblingScenario(t)
	scenario.confirmAbsentOnce(t, scenario.reconciler)
	scenario.assertUnchanged(t, "first absent-pending observation")
	scenario.confirmAbsentOnce(t, scenario.reconciler)
	scenario.assertUnchanged(t, "confirmed deletion in the existing reconciler")
}

func TestProxmoxStatefulRestartConfirmedDeleteFreezesManualSibling(t *testing.T) {
	scenario := newProxmoxManualSiblingScenario(t)
	scenario.confirmAbsentOnce(t, scenario.reconciler)
	scenario.assertUnchanged(t, "first absent-pending observation before restart")

	restarted, _ := newStatefulProxmoxTechnitiumReconcilerWithProvider(
		t, scenario.client, scenario.stateFile,
		newProxmoxManualRRsetTechnitiumProvider(t, scenario.dnsServer.URL), true,
	)
	if err := restarted.RecoverOwnership(context.Background()); err != nil {
		t.Fatalf("recover ownership after restart: %v", err)
	}
	scenario.confirmAbsentOnce(t, restarted)
	scenario.assertUnchanged(t, "confirmed deletion after ownership recovery and restart")
}

func TestProxmoxStatefulRestartAfterConfirmedDeleteFreezesManualSibling(t *testing.T) {
	scenario := newProxmoxManualSiblingScenario(t)
	scenario.confirmAbsentOnce(t, scenario.reconciler)
	scenario.confirmAbsentOnce(t, scenario.reconciler)
	scenario.assertUnchanged(t, "confirmed deletion before restart")

	state, err := os.ReadFile(scenario.stateFile)
	if err != nil {
		t.Fatalf("read lifecycle state after confirmed absence: %v", err)
	}
	var persisted struct {
		Tombstones map[string]uint64 `json:"tombstones"`
	}
	if err := json.Unmarshal(state, &persisted); err != nil {
		t.Fatalf("decode lifecycle state after confirmed absence: %v", err)
	}
	if len(persisted.Tombstones) != 1 {
		t.Fatalf("confirmed absence did not persist a tombstone: %s", state)
	}
	for key, generation := range persisted.Tombstones {
		if generation == 0 {
			t.Fatalf("confirmed absence persisted zero generation for %q: %s", key, state)
		}
	}

	restarted, _ := newStatefulProxmoxTechnitiumReconcilerWithProvider(
		t, scenario.client, scenario.stateFile,
		newProxmoxManualRRsetTechnitiumProvider(t, scenario.dnsServer.URL), true,
	)
	if err := restarted.RecoverOwnership(context.Background()); err != nil {
		t.Fatalf("recover ownership after confirmed absence and restart: %v", err)
	}
	if _, err := restarted.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile after confirmed absence and restart: %v", err)
	}
	scenario.assertUnchanged(t, "ownership recovery after confirmed-absence restart")
}

func TestProxmoxStatefulConfirmedDeleteStillCleansUncontestedName(t *testing.T) {
	api := &proxmoxManualRRsetAPI{}
	fixture := &proxmoxDNSFixture{
		resources: []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"}},
		interfaces: []proxmoxclient.AgentNetworkInterface{{
			Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.202"}},
		}},
	}
	_, client := newProxmoxFixtureClient(t, fixture)
	dnsServer := httptest.NewServer(api.handler(t))
	t.Cleanup(dnsServer.Close)
	r, _ := newStatefulProxmoxTechnitiumReconcilerWithProvider(
		t, client, filepath.Join(t.TempDir(), "proxmox-lifecycle.json"),
		newProxmoxManualRRsetTechnitiumProvider(t, dnsServer.URL), true,
	)
	ctx := context.Background()
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	api.clearWrites()
	fixture.mu.Lock()
	fixture.resources = nil
	fixture.mu.Unlock()
	for observation := 1; observation <= 2; observation++ {
		if _, err := r.Reconcile(ctx); err != nil {
			t.Fatalf("confirmed absence observation %d: %v", observation, err)
		}
	}
	records, writes := api.snapshot()
	if len(records) != 0 {
		t.Errorf("uncontested confirmed deletion left DNS records: %+v", records)
	}
	if len(writes) == 0 {
		t.Errorf("uncontested confirmed deletion issued no cleanup writes")
	}
}

func TestProxmoxStatefulLegacyOwnershipDoesNotAuthorizeManualCAA(t *testing.T) {
	const host = "web.test-cluster.example.com"
	legacy := provider.OwnershipRecord(host, 300, "test-instance", nil)
	caa := proxmoxManualRRsetRecord{Name: host, Type: "CAA", Target: `0 issue "letsencrypt.org"`, TTL: 60}
	api := &proxmoxManualRRsetAPI{records: []proxmoxManualRRsetRecord{
		caa,
		{Name: legacy.Hostname, Type: string(legacy.Type), Target: legacy.Target, TTL: legacy.TTL},
	}}
	fixture := &proxmoxDNSFixture{
		resources: []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"}},
		interfaces: []proxmoxclient.AgentNetworkInterface{{
			Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: "10.20.0.202"}},
		}},
	}
	_, client := newProxmoxFixtureClient(t, fixture)
	dnsServer := httptest.NewServer(api.handler(t))
	t.Cleanup(dnsServer.Close)
	r, _ := newStatefulProxmoxTechnitiumReconcilerWithProvider(
		t, client, filepath.Join(t.TempDir(), "proxmox-lifecycle.json"),
		newProxmoxManualRRsetTechnitiumProvider(t, dnsServer.URL), false,
	)
	instance, ok := r.providers.Get("dns")
	if !ok {
		t.Fatal("DNS provider instance is missing")
	}
	lister, ok := unmanagedRecordLister(instance.Provider)
	if !ok {
		t.Fatal("Technitium fixture does not expose its optional unmanaged-record snapshot")
	}
	unmanaged, err := lister.ListUnmanagedRecords(context.Background())
	if err != nil {
		t.Fatalf("list unmanaged records: %v", err)
	}
	if !slices.ContainsFunc(unmanaged, func(record provider.Record) bool {
		return record.Hostname == host && record.Type == provider.RecordType("CAA")
	}) {
		t.Fatalf("real Technitium optional snapshot did not expose the CAA record: %+v", unmanaged)
	}
	before, _ := api.snapshot()
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile name with legacy ownership and manual CAA: %v", err)
	}
	after, writes := api.snapshot()
	if !slices.Equal(after, before) {
		t.Errorf("manual CAA name or legacy marker changed: before=%+v after=%+v writes=%+v", before, after, writes)
	}
	if len(writes) != 0 {
		t.Errorf("legacy marker authorized writes at a name containing manual CAA: %+v", writes)
	}
}

func TestProxmoxStatefulLegacyOwnershipStillMigratesA(t *testing.T) {
	const host = "web.test-cluster.example.com"
	owned := provider.Record{Hostname: host, Type: provider.RecordTypeA, Target: "10.20.0.202", TTL: 300}
	legacy := provider.OwnershipRecord(host, 300, "test-instance", nil)
	api := &proxmoxManualRRsetAPI{records: []proxmoxManualRRsetRecord{
		{Name: host, Type: "A", Target: owned.Target, TTL: owned.TTL},
		{Name: legacy.Hostname, Type: string(legacy.Type), Target: legacy.Target, TTL: legacy.TTL},
	}}
	fixture := &proxmoxDNSFixture{
		resources: []proxmoxclient.ClusterResource{{VMID: 100, Name: "web", Node: "pve-00", Type: "qemu", Status: "running"}},
		interfaces: []proxmoxclient.AgentNetworkInterface{{
			Name: "eth0", IPAddresses: []proxmoxclient.AgentIPAddress{{IPAddress: owned.Target}},
		}},
	}
	_, client := newProxmoxFixtureClient(t, fixture)
	dnsServer := httptest.NewServer(api.handler(t))
	t.Cleanup(dnsServer.Close)
	r, _ := newStatefulProxmoxTechnitiumReconcilerWithProvider(
		t, client, filepath.Join(t.TempDir(), "proxmox-lifecycle.json"),
		newProxmoxManualRRsetTechnitiumProvider(t, dnsServer.URL), false,
	)
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile legacy-owned A member: %v", err)
	}
	records, writes := api.snapshot()
	var addressCount, legacyCount, memberOwnershipCount int
	for _, record := range records {
		if record.Name == host && record.Type == "A" && record.Target == owned.Target {
			addressCount++
		}
		if record.Name == legacy.Hostname && record.Type == "TXT" && record.Target == legacy.Target {
			legacyCount++
		}
		if record.Name == legacy.Hostname && record.Type == "TXT" && provider.MatchesMemberOwnership(record.Target, "test-instance", owned) {
			memberOwnershipCount++
		}
	}
	if addressCount != 1 || legacyCount != 0 || memberOwnershipCount != 1 {
		t.Errorf("valid legacy A ownership did not migrate cleanly: records=%+v writes=%+v", records, writes)
	}
}
