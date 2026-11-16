package proxmox

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	// LifecycleModeLegacy preserves the v3.3.2 behavior.
	LifecycleModeLegacy = "legacy"
	// LifecycleModeStateful enables persisted Proxmox lifecycle handling.
	LifecycleModeStateful = "stateful"

	DefaultLifecycleStateFile       = "proxmox-lifecycle.json"
	DefaultLifecycleDeleteObservations = 2

	LifecycleStateRunningAddress = "running+address"
	LifecycleStateStopped        = "stopped"
	LifecycleStateQGAUnavailable = "qga-unavailable"
	LifecycleStateQGATimeout     = "qga-timeout"
	LifecycleStateSuccessfulEmpty = "successful-empty"
	LifecycleStateInventoryUnknown = "inventory-unknown"
	LifecycleStateAbsentPending  = "absent-pending"
	LifecycleStateConfirmedAbsent = "confirmed-absent/deleted"
	LifecycleStateNameCollision  = "name-collision"
)

const lifecycleSchemaVersion = 1

type lifecycleRecord struct {
	Key                 string       `json:"key"`
	VMID                int          `json:"vmid"`
	Type                string       `json:"type"`
	Name                string       `json:"name"`
	Node                string       `json:"node"`
	Generation          uint64       `json:"generation"`
	Status              string       `json:"status"`
	Collision           bool         `json:"collision,omitempty"`
	LastIPs             ResolvedIPs  `json:"last_confirmed_ips"`
	LastConfirmedAt     time.Time    `json:"last_confirmed_at,omitempty"`
	LastSeenAt          time.Time    `json:"last_seen_at,omitempty"`
	MissingObservations int          `json:"missing_observations,omitempty"`
}

type lifecycleFile struct {
	Schema             int                        `json:"schema"`
	Instances          map[string]lifecycleRecord `json:"instances"`
	Tombstones         map[string]uint64          `json:"tombstones,omitempty"`
	ObservedNonEmpty   bool                       `json:"observed_non_empty"`
}

type lifecycleStore struct {
	path string
	mu   sync.Mutex
	data lifecycleFile
}

func newLifecycleStore(path string) *lifecycleStore {
	if strings.TrimSpace(path) == "" {
		path = DefaultLifecycleStateFile
	}
	return &lifecycleStore{path: path}
}

func (s *lifecycleStore) snapshot() (lifecycleFile, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data.Schema == 0 {
		if err := s.loadLocked(); err != nil {
			return lifecycleFile{}, err
		}
	}
	return cloneLifecycleFile(s.data), nil
}

func (s *lifecycleStore) commit(next lifecycleFile) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if next.Schema == 0 {
		next.Schema = lifecycleSchemaVersion
	}
	if next.Instances == nil {
		next.Instances = make(map[string]lifecycleRecord)
	}
	if next.Tombstones == nil {
		next.Tombstones = make(map[string]uint64)
	}
	if err := s.saveLocked(next); err != nil {
		return err
	}
	s.data = cloneLifecycleFile(next)
	return nil
}

func (s *lifecycleStore) loadLocked() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			s.data = lifecycleFile{
				Schema:     lifecycleSchemaVersion,
				Instances:  make(map[string]lifecycleRecord),
				Tombstones: make(map[string]uint64),
			}
			return nil
		}
		return fmt.Errorf("reading proxmox lifecycle state: %w", err)
	}
	var file lifecycleFile
	if err := json.Unmarshal(data, &file); err != nil {
		return fmt.Errorf("decoding proxmox lifecycle state: %w", err)
	}
	if file.Schema != lifecycleSchemaVersion {
		return fmt.Errorf("unsupported proxmox lifecycle state schema %d", file.Schema)
	}
	if file.Instances == nil {
		file.Instances = make(map[string]lifecycleRecord)
	}
	if file.Tombstones == nil {
		file.Tombstones = make(map[string]uint64)
	}
	s.data = file
	return nil
}

func (s *lifecycleStore) saveLocked(file lifecycleFile) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("creating proxmox lifecycle state directory: %w", err)
	}
	data, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding proxmox lifecycle state: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".proxmox-lifecycle-*.tmp")
	if err != nil {
		return fmt.Errorf("creating proxmox lifecycle state temporary file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("setting proxmox lifecycle state permissions: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("writing proxmox lifecycle state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("syncing proxmox lifecycle state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing proxmox lifecycle state: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("replacing proxmox lifecycle state: %w", err)
	}
	return nil
}

func cloneLifecycleFile(file lifecycleFile) lifecycleFile {
	clone := file
	clone.Instances = make(map[string]lifecycleRecord, len(file.Instances))
	for key, record := range file.Instances {
		clone.Instances[key] = record
	}
	clone.Tombstones = make(map[string]uint64, len(file.Tombstones))
	for key, generation := range file.Tombstones {
		clone.Tombstones[key] = generation
	}
	return clone
}

func clusterIdentity(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Host == "" {
		return strings.ToLower(strings.TrimRight(strings.TrimSpace(raw), "/"))
	}
	return strings.ToLower(parsed.Scheme + "://" + parsed.Host + strings.TrimRight(parsed.EscapedPath(), "/"))
}

func lifecycleIdentity(cluster string, resource ClusterResource) string {
	return fmt.Sprintf("%s|%s|%d", cluster, resource.Type, resource.VMID)
}
