package reconciler

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	proxmoxclient "github.com/maxfield-allison/dnsweaver/internal/proxmox"
	"github.com/maxfield-allison/dnsweaver/pkg/provider"
)

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
