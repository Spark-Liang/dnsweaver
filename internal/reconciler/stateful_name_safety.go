package reconciler

import (
	"log/slog"

	"github.com/maxfield-allison/dnsweaver/pkg/provider"
	"github.com/maxfield-allison/dnsweaver/pkg/source"
)

const statefulProxmoxOwnershipMetadataKey = "dnsweaver.stateful-proxmox"

func (r *Reconciler) blockStatefulNamesWithUnownedRecords(
	statefulClaims map[string][]*source.Hostname,
	cache *recordCache,
	retiredNames map[string]struct{},
) map[string]struct{} {
	blocked := make(map[string]struct{})
	if !r.config.OwnershipTracking || cache == nil {
		return blocked
	}

	claimsByName := make(map[string][]*source.Hostname, len(statefulClaims))
	for hostname, claims := range statefulClaims {
		claimsByName[source.NormalizeHostname(hostname)] = append(claimsByName[source.NormalizeHostname(hostname)], claims...)
	}
	evidenceByName := make(map[string]map[string]*provider.ProviderInstance)
	addStatefulEvidence := func(hostname string, instance *provider.ProviderInstance) {
		normalized := source.NormalizeHostname(hostname)
		if evidenceByName[normalized] == nil {
			evidenceByName[normalized] = make(map[string]*provider.ProviderInstance)
		}
		evidenceByName[normalized][instance.Name()] = instance
		if _, exists := claimsByName[normalized]; !exists {
			claimsByName[normalized] = nil
		}
	}
	for key, previousSet := range r.previousDesiredSnapshot() {
		if !previousSet.StatefulProxmox {
			continue
		}
		if instance, exists := r.providers.Get(previousSet.ProviderName); exists {
			addStatefulEvidence(key.Hostname, instance)
		}
	}
	for hostname := range retiredNames {
		normalized := source.NormalizeHostname(hostname)
		if normalized == "" {
			continue
		}
		if _, exists := claimsByName[normalized]; !exists {
			claimsByName[normalized] = nil
		}
		// Retired names come from the Proxmox lifecycle journal, but may no
		// longer have an ownership marker or an in-memory desired set. Inspect
		// only providers whose configured domains match that historical name.
		for _, instance := range r.providers.MatchingProviders(normalized) {
			addStatefulEvidence(normalized, instance)
		}
	}
	for _, instance := range r.providers.All() {
		for _, owned := range cache.ownedMembers(instance.Name(), instance.InstanceID) {
			if owned.Metadata[statefulProxmoxOwnershipMetadataKey] == "true" {
				addStatefulEvidence(owned.Member.Hostname, instance)
			}
		}
	}
	pendingByName := make(map[string][]memberOwnershipRepair)
	r.mu.RLock()
	for _, repair := range r.pendingMemberOwnership {
		normalized := source.NormalizeHostname(repair.Record.Hostname)
		pendingByName[normalized] = append(pendingByName[normalized], repair)
		if repair.StatefulProxmox {
			if _, exists := claimsByName[normalized]; !exists {
				claimsByName[normalized] = []*source.Hostname{{Name: repair.Record.Hostname}}
			}
		}
	}
	for key, recovery := range r.pendingReplacements {
		if !recovery.StatefulProxmox {
			continue
		}
		normalized := source.NormalizeHostname(key.Hostname)
		if _, exists := claimsByName[normalized]; !exists {
			claimsByName[normalized] = []*source.Hostname{{Name: key.Hostname}}
		}
	}
	r.mu.RUnlock()
	previous := r.previousDesiredSnapshot()

	for hostname, claims := range claimsByName {
		normalized := source.NormalizeHostname(hostname)
		instances := make(map[string]*provider.ProviderInstance)
		pendingMemberRecords := make(map[provider.ProviderIdentity][]provider.Record, len(pendingByName[normalized]))
		for _, repair := range pendingByName[normalized] {
			pendingMemberRecords[repair.Instance.Identity] = append(pendingMemberRecords[repair.Instance.Identity], repair.Record)
		}
		adoptionAllowed := true
		hasWriter := false

		for _, claim := range claims {
			if claim == nil {
				continue
			}
			candidates, _ := r.providersForClaim(claim)
			for _, instance := range candidates {
				hasWriter = true
				instances[instance.Name()] = instance
				if !r.effectiveAdoptExisting(claim, instance) {
					adoptionAllowed = false
				}
			}
		}
		for _, instance := range evidenceByName[normalized] {
			hasWriter = true
			instances[instance.Name()] = instance
			if !effectiveProviderAdoption(r, instance) {
				adoptionAllowed = false
			}
		}

		r.mu.RLock()
		for key, recovery := range r.pendingReplacements {
			if key.Hostname == normalized {
				instances[recovery.Instance.Name()] = recovery.Instance
				hasWriter = true
				if !effectiveProviderAdoption(r, recovery.Instance) {
					adoptionAllowed = false
				}
			}
		}
		r.mu.RUnlock()
		for _, repair := range pendingByName[normalized] {
			instances[repair.Instance.Name()] = repair.Instance
			hasWriter = true
			if !effectiveProviderAdoption(r, repair.Instance) {
				adoptionAllowed = false
			}
		}

		if !hasWriter || adoptionAllowed {
			continue
		}

		for _, instance := range r.providers.All() {
			instances[instance.Name()] = instance
		}
		unowned := false
		for _, instance := range instances {
			records, available := cache.getAllDataRecordsForHostname(instance.Name(), normalized)
			if !available {
				if instance.Matches(normalized) || hasPendingRoute(r, normalized, instance.Identity) {
					unowned = true
					break
				}
				continue
			}
			_, hasLegacyOwner := cache.legacyOwnershipRecord(instance.Name(), normalized, instance.InstanceID)
			for _, record := range records {
				if _, owned := cache.memberOwnershipRecord(instance.Name(), record, instance.InstanceID); owned {
					continue
				}
				if ownsRelatedRecord(instance, records, record, cache, pendingMemberRecords[instance.Identity]) {
					continue
				}
				if pendingOwnershipRepairMatches(instance, record, pendingMemberRecords[instance.Identity]) {
					continue
				}
				if !instance.Provider.Capabilities().SupportsOwnershipTXT && previousDesiredMemberMatches(previous, instance, record) {
					continue
				}
				// A legacy hostname marker can still prove one historically managed
				// member. It cannot authorize record types dnsweaver did not manage.
				if hasLegacyOwner && len(records) == 1 && legacyOwnershipCovers(record.Type) {
					continue
				}
				unowned = true
				break
			}
			if unowned {
				break
			}
		}
		if unowned {
			blocked[normalized] = struct{}{}
			r.warnOnce("stateful-unowned-name|"+normalized,
				"freezing stateful Proxmox DNS name because unowned provider records exist",
				slog.String("hostname", normalized),
			)
		}
	}
	return blocked
}

func ownsRelatedRecord(
	instance *provider.ProviderInstance,
	records []provider.Record,
	related provider.Record,
	cache *recordCache,
	pendingParents []provider.Record,
) bool {
	owner, ok := instance.Provider.(provider.RelatedRecordOwnership)
	if !ok {
		return false
	}
	for _, parent := range records {
		if parent.Type != provider.RecordTypeA && parent.Type != provider.RecordTypeAAAA {
			continue
		}
		if marker, owned := cache.memberOwnershipRecord(instance.Name(), parent, instance.InstanceID); owned {
			_, _, _, metadata := provider.ParseMemberOwnershipValue(marker.Target)
			if owner.OwnsRelatedRecord(parent, related, metadata) {
				return true
			}
		}
		for _, pending := range pendingParents {
			if !provider.SameRecordMember(parent, pending) || parent.TTL != pending.TTL || instance.RecordNeedsUpdate(parent, pending) {
				continue
			}
			if owner.OwnsRelatedRecord(parent, related, pending.Metadata) {
				return true
			}
		}
	}
	return false
}

func (r *Reconciler) hasStatefulPendingOwnershipRepairs() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, repair := range r.pendingMemberOwnership {
		if repair.StatefulProxmox {
			return true
		}
	}
	return false
}

func (r *Reconciler) hasStatefulPreviousDesired() bool {
	for _, previous := range r.previousDesiredSnapshot() {
		if previous.StatefulProxmox {
			return true
		}
	}
	return false
}

func (r *Reconciler) hasStatefulPendingReplacementRecoveries() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, recovery := range r.pendingReplacements {
		if recovery.StatefulProxmox {
			return true
		}
	}
	return false
}

func previousDesiredMemberMatches(
	previous map[desiredSetKey]previousDesiredSet,
	instance *provider.ProviderInstance,
	current provider.Record,
) bool {
	for key, set := range previous {
		if key.Identity != instance.Identity || key.Hostname != source.NormalizeHostname(current.Hostname) || set.ProviderName != instance.Name() {
			continue
		}
		for _, expected := range set.Records {
			if provider.SameRecordMember(current, expected) && current.TTL == expected.TTL && !instance.RecordNeedsUpdate(current, expected) {
				return true
			}
		}
	}
	return false
}

func pendingOwnershipRepairMatches(
	instance *provider.ProviderInstance,
	current provider.Record,
	pending []provider.Record,
) bool {
	for _, expected := range pending {
		if !provider.SameRecordMember(current, expected) || current.TTL != expected.TTL {
			continue
		}
		if !instance.RecordNeedsUpdate(current, expected) {
			return true
		}
	}
	return false
}

func effectiveProviderAdoption(r *Reconciler, instance *provider.ProviderInstance) bool {
	adopt := r.config.AdoptExisting
	if instance.AdoptExisting != nil {
		adopt = *instance.AdoptExisting
	}
	return adopt
}

func hasPendingRoute(r *Reconciler, hostname string, identity provider.ProviderIdentity) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key := range r.pendingReplacements {
		if key.Identity == identity && key.Hostname == hostname {
			return true
		}
	}
	for key, repair := range r.pendingMemberOwnership {
		if key.Identity == identity && source.NormalizeHostname(repair.Record.Hostname) == hostname {
			return true
		}
	}
	return false
}

func (r *Reconciler) heldReplacementRoutes(
	blockedNames map[string]struct{},
	rejected map[replacementKey]struct{},
) map[replacementKey]struct{} {
	held := make(map[replacementKey]struct{}, len(blockedNames)+len(rejected))
	for key := range rejected {
		held[key] = struct{}{}
	}
	for hostname := range blockedNames {
		normalized := source.NormalizeHostname(hostname)
		for _, instance := range r.providers.All() {
			if instance.Matches(normalized) {
				held[replacementKey{Identity: instance.Identity, Hostname: normalized}] = struct{}{}
			}
		}
	}

	r.mu.Lock()
	for key := range r.pendingReplacements {
		if _, blocked := blockedNames[source.NormalizeHostname(key.Hostname)]; blocked {
			held[key] = struct{}{}
		}
	}
	for key, repair := range r.pendingMemberOwnership {
		hostname := source.NormalizeHostname(repair.Record.Hostname)
		if _, blocked := blockedNames[hostname]; blocked {
			held[replacementKey{Identity: key.Identity, Hostname: hostname}] = struct{}{}
		}
	}
	r.mu.Unlock()
	return held
}

func (r *Reconciler) blockedOwnershipIdentities(
	blockedNames map[string]struct{},
	held map[replacementKey]struct{},
) map[provider.ProviderIdentity]struct{} {
	blocked := make(map[provider.ProviderIdentity]struct{})
	for key := range held {
		if _, frozen := blockedNames[source.NormalizeHostname(key.Hostname)]; frozen {
			blocked[key.Identity] = struct{}{}
		}
	}
	for hostname := range blockedNames {
		for _, instance := range r.providers.All() {
			if instance.Matches(hostname) {
				blocked[instance.Identity] = struct{}{}
			}
		}
	}
	return blocked
}
