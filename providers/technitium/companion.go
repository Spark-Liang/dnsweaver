// Package technitium implements the DNSWeaver provider interface for Technitium DNS Server.
package technitium

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/maxfield-allison/dnsweaver/pkg/provider"
)

// companionHTTPSSvcPriority is the SvcPriority used for auto-created companion HTTPS records.
// Priority 1 = ServiceMode (as opposed to 0 = AliasMode).
const companionHTTPSSvcPriority = 1

// companionHTTPSTargetName is the SvcTargetName for companion HTTPS records.
// "." means the record's owner name (self-referential), per RFC 9460 Section 2.5.
const companionHTTPSTargetName = "."

const companionStateKey = "technitium.companion_https"
const protectedCompanionState = "protected"
const companionOwnershipMetadataKey = "dnsweaver.related.technitium-https"

func (p *Provider) AddRelatedOwnershipMetadata(metadata map[string]string, related []provider.Record) map[string]string {
	metadata = relatedOwnershipMetadata(metadata)
	for _, record := range related {
		fingerprint, ok := relatedHTTPSFingerprint(record)
		if !ok {
			continue
		}
		if metadata == nil {
			metadata = make(map[string]string)
		}
		metadata[companionOwnershipMetadataKey] = fingerprint
		return metadata
	}
	return metadata
}

func (p *Provider) MergeRelatedOwnershipMetadata(metadata, previous map[string]string) map[string]string {
	metadata = relatedOwnershipMetadata(metadata)
	if fingerprint := previous[companionOwnershipMetadataKey]; validRelatedHTTPSFingerprint(fingerprint) {
		if metadata == nil {
			metadata = make(map[string]string)
		}
		metadata[companionOwnershipMetadataKey] = fingerprint
	}
	return metadata
}

func (p *Provider) OwnsRelatedRecord(parent, related provider.Record, metadata map[string]string) bool {
	if (parent.Type != provider.RecordTypeA && parent.Type != provider.RecordTypeAAAA) ||
		related.Type != provider.RecordTypeHTTPS || related.HTTPS == nil ||
		!strings.EqualFold(strings.TrimSuffix(parent.Hostname, "."), strings.TrimSuffix(related.Hostname, ".")) {
		return false
	}
	fingerprint, ok := relatedHTTPSFingerprint(related)
	return ok && validRelatedHTTPSFingerprint(metadata[companionOwnershipMetadataKey]) &&
		metadata[companionOwnershipMetadataKey] == fingerprint
}

type relatedHTTPSIdentity struct {
	Hostname   string `json:"hostname"`
	TTL        int    `json:"ttl"`
	Target     string `json:"target"`
	Priority   uint16 `json:"priority"`
	TargetName string `json:"target_name"`
	ALPN       string `json:"alpn"`
}

func relatedHTTPSFingerprint(record provider.Record) (string, bool) {
	if record.Type != provider.RecordTypeHTTPS || record.HTTPS == nil {
		return "", false
	}
	identity := relatedHTTPSIdentity{
		Hostname:   strings.ToLower(strings.TrimSuffix(strings.TrimSpace(record.Hostname), ".")),
		TTL:        record.TTL,
		Target:     record.Target,
		Priority:   record.HTTPS.Priority,
		TargetName: record.HTTPS.TargetName,
		ALPN:       record.HTTPS.ALPN,
	}
	if identity.Hostname == "" {
		return "", false
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return "", false
	}
	checksum := sha256.Sum256(encoded)
	return hex.EncodeToString(checksum[:]), true
}

func validRelatedHTTPSFingerprint(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func relatedOwnershipMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	metadata = cloneMetadata(metadata)
	if !validRelatedHTTPSFingerprint(metadata[companionOwnershipMetadataKey]) {
		delete(metadata, companionOwnershipMetadataKey)
	}
	return metadata
}

func cloneMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return make(map[string]string)
	}
	cloned := make(map[string]string, len(metadata))
	for key, value := range metadata {
		cloned[key] = value
	}
	return cloned
}

// companionHTTPS identifies only the format dnsweaver generates. Multiple,
// disabled or customized HTTPS records are left under their existing control.
func companionHTTPS(records []apiRecord, hostname string) (*apiRecord, bool) {
	var companion *apiRecord
	for _, record := range records {
		if record.Type != "HTTPS" || !strings.EqualFold(strings.TrimSuffix(record.Name, "."), strings.TrimSuffix(hostname, ".")) {
			continue
		}
		params := string(record.RData.SvcParams)
		if companion != nil || record.Disabled || record.RData.SvcPriority != companionHTTPSSvcPriority ||
			record.RData.SvcTargetName != companionHTTPSTargetName ||
			!strings.HasPrefix(params, "alpn|") || len(params) == len("alpn|") ||
			strings.Count(params, "|") != 1 || strings.ContainsAny(params, " \t\r\n") {
			return nil, true
		}
		companion = &record
	}
	return companion, false
}

func companionState(record *apiRecord) string {
	if record == nil {
		return "missing"
	}
	return strconv.Itoa(record.TTL) + ":" + string(record.RData.SvcParams)
}

// RecordNeedsUpdate exposes companion drift to the normal ownership-gated
// update path, including when the parent address itself has not changed.
func (p *Provider) RecordNeedsUpdate(existing, desired provider.Record) bool {
	if !p.autoHTTPSRecords || !needsCompanionHTTPS(desired.Type) {
		return false
	}
	state, known := existing.Metadata[companionStateKey]
	if !known || state == protectedCompanionState {
		return false
	}
	ttl := desired.TTL
	if ttl <= 0 {
		ttl = p.ttl
	}
	return state != strconv.Itoa(ttl)+":"+buildSvcParams(p.autoHTTPSALPN)
}

// needsCompanionHTTPS returns true if the given record type should trigger
// companion HTTPS record creation (A or AAAA).
//
// CNAME is deliberately excluded: a CNAME is exclusive and cannot share an
// owner name with an HTTPS record (RFC 1034 §3.6.2, RFC 2181 §10.1). Technitium
// rejects the companion outright in that case, so attempting it only produced a
// warning on every reconcile (issue #165). Hosts fronted by a CNAME inherit the
// HTTPS record of the name they point at.
func needsCompanionHTTPS(recordType provider.RecordType) bool {
	switch recordType {
	case provider.RecordTypeA, provider.RecordTypeAAAA:
		return true
	default:
		return false
	}
}

// hadCompanionHTTPS returns true if a record of the given type may have a
// companion HTTPS record left over from an earlier dnsweaver release, which
// created companions for CNAME records too. Deletion stays broader than
// creation so those leftovers are still cleaned up when the record is removed.
func hadCompanionHTTPS(recordType provider.RecordType) bool {
	return needsCompanionHTTPS(recordType) || recordType == provider.RecordTypeCNAME
}

// createCompanionHTTPS creates a companion HTTPS record for the given hostname.
// This prevents ECH (Encrypted Client Hello) fallback errors in split-horizon DNS
// environments by providing a local HTTPS record that overrides public ECH parameters.
//
// The companion record format: HTTPS 1 . alpn="<alpn>"
// Example: app.example.com 300 IN HTTPS 1 . alpn="h2"
//
// Skips creation if:
//   - Auto HTTPS records are disabled
//   - The record type doesn't need a companion (not A/AAAA)
//   - A customized or ambiguous HTTPS record exists for the hostname
//
// An existing record matching the generated format is reconciled with the
// configured ALPN and TTL. Callers must hold management authority for the parent.
func (p *Provider) createCompanionHTTPS(ctx context.Context, hostname string, recordType provider.RecordType, ttl int) error {
	_, _, err := p.createCompanionHTTPSWithResult(ctx, hostname, recordType, ttl)
	return err
}

func (p *Provider) createCompanionHTTPSWithResult(ctx context.Context, hostname string, recordType provider.RecordType, ttl int) (provider.Record, bool, error) {
	if !p.autoHTTPSRecords {
		return provider.Record{}, false, nil
	}
	if !needsCompanionHTTPS(recordType) {
		return provider.Record{}, false, nil
	}

	// Re-read before mutation so a stale snapshot cannot overwrite a newly
	// customized or ambiguous HTTPS RRset.
	records, err := p.client.GetRecords(ctx, p.zone, hostname)
	if err != nil {
		return provider.Record{}, false, fmt.Errorf("checking companion HTTPS record for %s: %w", hostname, err)
	}
	existing, protected := companionHTTPS(records, hostname)
	if protected {
		p.logger.Debug("custom HTTPS records exist, skipping companion reconciliation",
			slog.String("hostname", hostname),
		)
		return provider.Record{}, false, nil
	}

	svcParams := buildSvcParams(p.autoHTTPSALPN)
	if existing != nil {
		if existing.TTL == ttl && string(existing.RData.SvcParams) == svcParams {
			return provider.Record{}, false, nil
		}
		return provider.Record{}, false, p.client.UpdateHTTPSRecord(ctx, p.zone, *existing, apiRecord{
			Name: hostname, Type: "HTTPS", TTL: ttl,
			RData: apiRData{SvcPriority: companionHTTPSSvcPriority, SvcTargetName: companionHTTPSTargetName, SvcParams: svcParamsValue(svcParams)},
		})
	}
	if err := p.client.AddHTTPSRecord(ctx, p.zone, hostname, companionHTTPSSvcPriority, companionHTTPSTargetName, svcParams, ttl); err != nil {
		// If Technitium reports conflict, treat as non-fatal (record already exists)
		if strings.Contains(err.Error(), "record already exists") || strings.Contains(err.Error(), "Identical record") {
			p.logger.Debug("companion HTTPS record already exists (conflict)", slog.String("hostname", hostname))
			return provider.Record{}, false, nil
		}
		return provider.Record{}, false, fmt.Errorf("creating companion HTTPS record for %s: %w", hostname, err)
	}
	confirmedRecords, err := p.client.GetRecords(ctx, p.zone, hostname)
	if err != nil {
		return provider.Record{}, false, fmt.Errorf("verifying created companion HTTPS record for %s: %w", hostname, err)
	}
	confirmed, protected := companionHTTPS(confirmedRecords, hostname)
	if protected || confirmed == nil || confirmed.TTL != ttl ||
		confirmed.RData.SvcPriority != companionHTTPSSvcPriority ||
		confirmed.RData.SvcTargetName != companionHTTPSTargetName ||
		string(confirmed.RData.SvcParams) != svcParams {
		return provider.Record{}, false, fmt.Errorf("verifying created companion HTTPS record for %s: read-back did not match the created member", hostname)
	}

	p.logger.Info("created companion HTTPS record",
		slog.String("hostname", hostname),
		slog.String("alpn", p.autoHTTPSALPN),
		slog.Int("ttl", ttl),
	)

	return provider.Record{
		Hostname: hostname,
		Type:     provider.RecordTypeHTTPS,
		Target:   confirmed.RData.SvcTargetName,
		TTL:      confirmed.TTL,
		HTTPS: &provider.HTTPSData{
			Priority:   uint16(min(max(0, confirmed.RData.SvcPriority), 65535)),
			TargetName: confirmed.RData.SvcTargetName,
			ALPN:       extractALPN(string(confirmed.RData.SvcParams)),
		},
	}, true, nil
}

// deleteCompanionHTTPS removes the companion HTTPS record for the given hostname.
// Only deletes records that match the auto-created format (priority 1, target
// ".", ALPN only). The observed ALPN may predate the current configuration.
func (p *Provider) deleteCompanionHTTPS(ctx context.Context, hostname string, recordType provider.RecordType) error {
	if !p.autoHTTPSRecords {
		return nil
	}
	if !hadCompanionHTTPS(recordType) {
		return nil
	}
	if needsCompanionHTTPS(recordType) {
		hasAddress, err := p.hasAddressRecord(ctx, hostname)
		if err != nil {
			return err
		}
		if hasAddress {
			p.logger.Debug("keeping companion HTTPS record for remaining address member",
				slog.String("hostname", hostname),
			)
			return nil
		}
	}

	records, err := p.client.GetRecords(ctx, p.zone, hostname)
	if err != nil {
		return fmt.Errorf("checking companion HTTPS record for deletion: %w", err)
	}
	existing, protected := companionHTTPS(records, hostname)
	if protected || existing == nil {
		return nil
	}
	svcParams := string(existing.RData.SvcParams)
	if err := p.client.DeleteHTTPSRecord(ctx, p.zone, hostname, companionHTTPSSvcPriority, companionHTTPSTargetName, svcParams); err != nil {
		return fmt.Errorf("deleting companion HTTPS record for %s: %w", hostname, err)
	}

	p.logger.Info("deleted companion HTTPS record",
		slog.String("hostname", hostname),
		slog.String("alpn", extractALPN(svcParams)),
	)

	return nil
}

// hasAddressRecord reports whether an A or AAAA member remains at hostname.
// Companion HTTPS records belong to the address RRsets as a whole, so deleting
// one round-robin member must not remove the companion while a sibling remains.
func (p *Provider) hasAddressRecord(ctx context.Context, hostname string) (bool, error) {
	records, err := p.client.GetRecords(ctx, p.zone, hostname)
	if err != nil {
		return false, fmt.Errorf("checking remaining address records for %s: %w", hostname, err)
	}
	for _, record := range records {
		if record.Type == string(provider.RecordTypeA) || record.Type == string(provider.RecordTypeAAAA) {
			return true, nil
		}
	}
	return false, nil
}
