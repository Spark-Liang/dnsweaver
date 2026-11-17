package reconciler

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"

	proxmoxclient "github.com/maxfield-allison/dnsweaver/internal/proxmox"
	"github.com/maxfield-allison/dnsweaver/pkg/provider"
)

const (
	reviewFollowupHost          = "web.test-cluster.example.com"
	reviewFollowupIdentity      = "test-cluster|qemu|100"
	relatedHTTPSOwnershipKey    = "dnsweaver.related.technitium-https"
	statefulProxmoxOwnershipKey = "dnsweaver.stateful-proxmox"
)

func TestProxmoxStatefulRelatedHTTPSManualReplacementFreezesInProcess(t *testing.T) {
	api, fixture, client, newProvider := newStatefulProxmoxHTTPSDefaultScenario(t)
	stateFile := filepath.Join(t.TempDir(), "proxmox-lifecycle.json")
	r, _ := newStatefulProxmoxTechnitiumReconcilerWithProvider(t, client, stateFile, newProvider(), false)
	ctx := context.Background()

	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("initial reconcile with default HTTPS setting: %v", err)
	}
	assertDefaultHTTPSAddress(t, api, "10.20.0.202")
	assertRelatedHTTPSOwnershipMarker(t, api, reviewFollowupHost)

	manual := manualReplacementHTTPSRecord()
	if !replaceHTTPSRecord(api, manual) {
		t.Fatal("initial automatic HTTPS companion was not present")
	}
	want, _ := api.snapshot()
	api.clearWrites()
	setProxmoxFixtureAddress(fixture, "10.20.0.203")
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("reconcile after manual HTTPS replacement: %v", err)
	}
	assertManualReplacementUnchanged(t, api, want)
}

func TestProxmoxStatefulRelatedHTTPSManualReplacementFreezesAfterRestart(t *testing.T) {
	api, fixture, client, newProvider := newStatefulProxmoxHTTPSDefaultScenario(t)
	stateFile := filepath.Join(t.TempDir(), "proxmox-lifecycle.json")
	r, _ := newStatefulProxmoxTechnitiumReconcilerWithProvider(t, client, stateFile, newProvider(), false)
	ctx := context.Background()

	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("initial reconcile with default HTTPS setting: %v", err)
	}
	assertDefaultHTTPSAddress(t, api, "10.20.0.202")
	assertRelatedHTTPSOwnershipMarker(t, api, reviewFollowupHost)

	manual := manualReplacementHTTPSRecord()
	if !replaceHTTPSRecord(api, manual) {
		t.Fatal("initial automatic HTTPS companion was not present")
	}
	want, _ := api.snapshot()

	restarted, _ := newStatefulProxmoxTechnitiumReconcilerWithProvider(
		t, client, stateFile, newProvider(), false,
	)
	if err := restarted.RecoverOwnership(ctx); err != nil {
		t.Fatalf("recover ownership after restart: %v", err)
	}
	api.clearWrites()
	setProxmoxFixtureAddress(fixture, "10.20.0.203")
	if _, err := restarted.Reconcile(ctx); err != nil {
		t.Fatalf("reconcile after restart and manual HTTPS replacement: %v", err)
	}
	assertManualReplacementUnchanged(t, api, want)
}

func TestProxmoxStatefulAA93OwnershipAfterConfirmedAbsenceFreezesManualSibling(t *testing.T) {
	api, _, stateFile, r, adapter := newAA93OwnershipScenario(t, true)
	ctx := context.Background()
	observeFirstAA93Absence(t, adapter, ctx)
	if err := r.RecoverOwnership(ctx); err != nil {
		t.Fatalf("recover aa93 ownership before second absence: %v", err)
	}
	if len(r.previousDesiredSnapshot()) != 0 {
		t.Fatal("fixture unexpectedly populated previousDesired before the confirmed-absence cycle")
	}
	want, _ := api.snapshot()
	api.clearWrites()

	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("reconcile on second complete absence: %v", err)
	}
	if !adapter.SnapshotStatus().AllowOrphanCleanup {
		t.Error("fixture did not reach confirmed absence with cleanup scope enabled")
	}
	assertAA93LifecycleTombstone(t, stateFile)
	assertManualReplacementUnchanged(t, api, want)
}

func TestProxmoxStatefulAA93OwnershipAfterTombstoneRestartFreezesManualSibling(t *testing.T) {
	api, client, stateFile, _, adapter := newAA93OwnershipScenario(t, true)
	ctx := context.Background()
	observeFirstAA93Absence(t, adapter, ctx)
	if workloads, err := adapter.ListWorkloads(ctx); err != nil {
		t.Fatalf("observe second complete absence: %v", err)
	} else if len(workloads) != 0 {
		t.Fatalf("confirmed absence retained current workloads: %+v", workloads)
	}
	assertAA93LifecycleTombstone(t, stateFile)

	restarted, _ := newAA93OwnershipReconciler(t, client, stateFile, api)
	if err := restarted.RecoverOwnership(ctx); err != nil {
		t.Fatalf("recover aa93 ownership after tombstone restart: %v", err)
	}
	want, _ := api.snapshot()
	api.clearWrites()
	if _, err := restarted.Reconcile(ctx); err != nil {
		t.Fatalf("reconcile after tombstone restart: %v", err)
	}
	assertManualReplacementUnchanged(t, api, want)
}

func TestProxmoxStatefulAA93OwnershipAfterConfirmedAbsenceStillCleansUncontestedName(t *testing.T) {
	api, _, _, r, adapter := newAA93OwnershipScenario(t, false)
	ctx := context.Background()
	observeFirstAA93Absence(t, adapter, ctx)
	if err := r.RecoverOwnership(ctx); err != nil {
		t.Fatalf("recover aa93 ownership before second absence: %v", err)
	}
	api.clearWrites()

	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("reconcile uncontested name on second complete absence: %v", err)
	}
	if records, writes := api.snapshot(); len(records) != 0 || len(writes) == 0 {
		t.Fatalf("uncontested aa93 ownership was not cleaned: records=%+v writes=%+v", records, writes)
	}
}

func TestProxmoxStatefulDefaultHTTPSOwnershipRepairRecoversAndUpdatesAddress(t *testing.T) {
	api, fixture, client, newProvider := newStatefulProxmoxHTTPSDefaultScenario(t)
	api.failOwnershipWrites = 1
	stateFile := filepath.Join(t.TempDir(), "proxmox-lifecycle.json")
	r, _ := newStatefulProxmoxTechnitiumReconcilerWithProvider(t, client, stateFile, newProvider(), true)
	ctx := context.Background()

	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("first reconcile with injected ownership TXT 500: %v", err)
	}
	if len(r.pendingMemberOwnership) != 1 {
		t.Fatalf("pending ownership repairs after injected failure = %d, want one", len(r.pendingMemberOwnership))
	}
	assertDefaultHTTPSAddress(t, api, "10.20.0.202")
	assertRelatedHTTPSOwnershipInPendingRepair(t, r)
	if records, _ := api.snapshot(); countMemberOwnershipRecords(records, reviewFollowupHost) != 0 {
		t.Fatalf("injected first ownership write unexpectedly persisted: %+v", records)
	}

	api.clearWrites()
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("ownership repair retry after backend recovery: %v", err)
	}
	if len(r.pendingMemberOwnership) != 0 {
		t.Errorf("pending ownership repairs after retry = %d, want zero", len(r.pendingMemberOwnership))
	}
	if records, writes := api.snapshot(); countMemberOwnershipRecords(records, reviewFollowupHost) != 1 || len(writes) == 0 {
		t.Errorf("ownership repair did not publish one durable marker: records=%+v writes=%+v", records, writes)
	}

	api.clearWrites()
	setProxmoxFixtureAddress(fixture, "10.20.0.203")
	if _, err := r.Reconcile(ctx); err != nil {
		t.Fatalf("address update after ownership repair: %v", err)
	}
	assertDefaultHTTPSAddress(t, api, "10.20.0.203")
	if records, _ := api.snapshot(); countMemberOwnershipRecords(records, reviewFollowupHost) != 1 {
		t.Errorf("address update lost the repaired ownership marker: %+v", records)
	}
}

func TestProxmoxStatefulPendingHTTPSRepairDoesNotTrustManualReplacement(t *testing.T) {
	api, _, client, newProvider := newStatefulProxmoxHTTPSDefaultScenario(t)
	api.failOwnershipWrites = 1
	r, _ := newStatefulProxmoxTechnitiumReconcilerWithProvider(
		t, client, filepath.Join(t.TempDir(), "proxmox-lifecycle.json"), newProvider(), true,
	)
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("first reconcile with injected ownership TXT 500: %v", err)
	}
	if len(r.pendingMemberOwnership) != 1 {
		t.Fatalf("pending ownership repairs = %d, want one", len(r.pendingMemberOwnership))
	}
	if !replaceHTTPSRecord(api, manualReplacementHTTPSRecord()) {
		t.Fatal("created automatic HTTPS companion was not present")
	}
	want, _ := api.snapshot()
	api.clearWrites()
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile with pending repair and manual HTTPS: %v", err)
	}
	assertManualReplacementUnchanged(t, api, want)
	if len(r.pendingMemberOwnership) != 1 {
		t.Errorf("manual HTTPS conflict discarded pending repair: %d", len(r.pendingMemberOwnership))
	}
}

func TestProxmoxStatefulPendingHTTPSRepairDoesNotTrustManualSibling(t *testing.T) {
	api, _, client, newProvider := newStatefulProxmoxHTTPSDefaultScenario(t)
	r, _ := newStatefulProxmoxTechnitiumReconcilerWithProvider(
		t, client, filepath.Join(t.TempDir(), "proxmox-lifecycle.json"), newProvider(), true,
	)
	api.failOwnershipWrites = 1
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("first reconcile with injected ownership TXT 500: %v", err)
	}
	if len(r.pendingMemberOwnership) != 1 {
		t.Fatalf("pending ownership repairs = %d, want one", len(r.pendingMemberOwnership))
	}
	api.addManualA(reviewFollowupHost, "10.20.0.250", 60)
	want, _ := api.snapshot()
	api.clearWrites()
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile with pending repair and manual A sibling: %v", err)
	}
	assertManualReplacementUnchanged(t, api, want)
	if len(r.pendingMemberOwnership) != 1 {
		t.Errorf("manual sibling conflict discarded pending repair: %d", len(r.pendingMemberOwnership))
	}
}

func newAA93OwnershipScenario(
	t *testing.T,
	withManualSibling bool,
) (*proxmoxManualRRsetAPI, *proxmoxclient.Client, string, *Reconciler, *proxmoxclient.WorkloadListerAdapter) {
	t.Helper()
	stateFile := filepath.Join(t.TempDir(), "proxmox-lifecycle.json")
	writeAA93LifecycleState(t, stateFile)
	member := provider.Record{
		Hostname: reviewFollowupHost,
		Type:     provider.RecordTypeA,
		Target:   "10.20.0.202",
		TTL:      300,
	}
	oldMarker := provider.MemberOwnershipRecord(reviewFollowupHost, 300, "test-instance", member, nil)
	owned, instanceID, parsed, metadata := provider.ParseMemberOwnershipValue(oldMarker.Target)
	if !owned || instanceID != "test-instance" || parsed == nil || !provider.SameRecordMember(*parsed, member) {
		t.Fatalf("aa93-format ownership marker failed to parse: owned=%t instance=%q member=%+v", owned, instanceID, parsed)
	}
	if _, exists := metadata[statefulProxmoxOwnershipKey]; exists {
		t.Fatalf("aa93 marker unexpectedly has new stateful metadata: %+v", metadata)
	}
	if _, exists := metadata[relatedHTTPSOwnershipKey]; exists {
		t.Fatalf("aa93 marker unexpectedly has new related-record metadata: %+v", metadata)
	}
	api := &proxmoxManualRRsetAPI{records: []proxmoxManualRRsetRecord{
		{Name: reviewFollowupHost, Type: "A", Target: member.Target, TTL: member.TTL},
		{Name: oldMarker.Hostname, Type: "TXT", Target: oldMarker.Target, TTL: oldMarker.TTL},
	}}
	if withManualSibling {
		api.addManualA(reviewFollowupHost, "10.20.0.250", 60)
	}
	fixture := &proxmoxDNSFixture{}
	_, client := newProxmoxFixtureClient(t, fixture)
	r, adapter := newAA93OwnershipReconciler(t, client, stateFile, api)
	return api, client, stateFile, r, adapter
}

func newAA93OwnershipReconciler(
	t *testing.T,
	client *proxmoxclient.Client,
	stateFile string,
	api *proxmoxManualRRsetAPI,
) (*Reconciler, *proxmoxclient.WorkloadListerAdapter) {
	t.Helper()
	dnsServer := httptest.NewServer(api.handler(t))
	t.Cleanup(dnsServer.Close)
	dnsProvider := newProxmoxManualRRsetTechnitiumProvider(t, dnsServer.URL)
	return newStatefulProxmoxTechnitiumReconcilerWithProvider(t, client, stateFile, dnsProvider, true)
}

func writeAA93LifecycleState(t *testing.T, stateFile string) {
	t.Helper()
	// Frozen from aa93af1398a09d5bc41ab8eb0acbb5ea304df4fd's
	// internal/proxmox/lifecycle.go lifecycleFile/lifecycleRecord JSON fields
	// and lifecycleIdentity format. Its schema is 1 and has no reconciler
	// ownership metadata; the first observed absence increments this record.
	const state = `{"schema":1,"instances":{"test-cluster|qemu|100":{"key":"test-cluster|qemu|100","vmid":100,"type":"qemu","name":"web","node":"pve-00","cluster_name":"test-cluster","generation":1,"status":"running+address","last_confirmed_ips":{"V4":"10.20.0.202","V6":""}}},"observed_non_empty":true}`
	if err := os.WriteFile(stateFile, []byte(state), 0o600); err != nil {
		t.Fatalf("write frozen aa93 lifecycle state: %v", err)
	}
}

func observeFirstAA93Absence(
	t *testing.T,
	adapter *proxmoxclient.WorkloadListerAdapter,
	ctx context.Context,
) {
	t.Helper()
	workloads, err := adapter.ListWorkloads(ctx)
	if err != nil {
		t.Fatalf("observe first complete absence: %v", err)
	}
	if len(workloads) != 1 || workloads[0].Metadata["proxmox.lifecycle.state"] != proxmoxclient.LifecycleStateAbsentPending {
		t.Fatalf("first absence did not retain the aa93 workload as absent-pending: %+v", workloads)
	}
}

func assertAA93LifecycleTombstone(t *testing.T, stateFile string) {
	t.Helper()
	data, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("read lifecycle tombstone: %v", err)
	}
	var persisted struct {
		Instances  map[string]json.RawMessage `json:"instances"`
		Tombstones map[string]uint64          `json:"tombstones"`
	}
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("decode lifecycle tombstone: %v", err)
	}
	if len(persisted.Instances) != 0 || persisted.Tombstones[reviewFollowupIdentity] != 1 {
		t.Fatalf("state file is not at confirmed absence: %s", data)
	}
}

func manualReplacementHTTPSRecord() proxmoxManualRRsetRecord {
	return proxmoxManualRRsetRecord{
		Name: reviewFollowupHost, Type: "HTTPS", Target: ".", TTL: 60,
		HTTPSPriority: 1, HTTPSTargetName: ".", HTTPSParams: "alpn|h3",
	}
}

func replaceHTTPSRecord(api *proxmoxManualRRsetAPI, replacement proxmoxManualRRsetRecord) bool {
	api.mu.Lock()
	defer api.mu.Unlock()
	for i := range api.records {
		if api.records[i].Name == replacement.Name && api.records[i].Type == "HTTPS" {
			api.records[i] = replacement
			return true
		}
	}
	return false
}

func assertRelatedHTTPSOwnershipMarker(t *testing.T, api *proxmoxManualRRsetAPI, hostname string) {
	t.Helper()
	records, _ := api.snapshot()
	for _, record := range records {
		if record.Type != "TXT" || !provider.IsOwnershipRecord(record.Name) ||
			provider.ExtractHostnameFromOwnership(record.Name) != hostname {
			continue
		}
		owned, _, member, metadata := provider.ParseMemberOwnershipValue(record.Target)
		if owned && member != nil && member.Type == provider.RecordTypeA && metadata[relatedHTTPSOwnershipKey] == "true" {
			return
		}
	}
	t.Fatalf("initial automatic HTTPS companion lacks durable related-record ownership metadata")
}

func assertRelatedHTTPSOwnershipInPendingRepair(t *testing.T, r *Reconciler) {
	t.Helper()
	for _, repair := range r.pendingMemberOwnership {
		if repair.Record.Metadata[relatedHTTPSOwnershipKey] == "true" &&
			repair.Record.Metadata[statefulProxmoxOwnershipKey] == "true" {
			return
		}
	}
	t.Fatalf("pending repair lacks the provider-created companion evidence: %+v", r.pendingMemberOwnership)
}

func countMemberOwnershipRecords(records []proxmoxManualRRsetRecord, hostname string) int {
	count := 0
	for _, record := range records {
		if record.Type == "TXT" && provider.IsOwnershipRecord(record.Name) &&
			provider.ExtractHostnameFromOwnership(record.Name) == hostname {
			count++
		}
	}
	return count
}

func assertManualReplacementUnchanged(t *testing.T, api *proxmoxManualRRsetAPI, want []proxmoxManualRRsetRecord) {
	t.Helper()
	got, writes := api.snapshot()
	if !slices.Equal(got, want) {
		t.Errorf("manual RRset changed: got=%+v want=%+v writes=%+v", got, want, writes)
	}
	if len(writes) != 0 {
		t.Errorf("conflicted stateful name caused provider writes: %+v", writes)
	}
}
