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

	DefaultLifecycleStateFile          = "proxmox-lifecycle.json"
	DefaultLifecycleDeleteObservations = 2

	LifecycleStateRunningAddress   = "running+address"
	LifecycleStateStopped          = "stopped"
	LifecycleStateQGAUnavailable   = "qga-unavailable"
	LifecycleStateQGATimeout       = "qga-timeout"
	LifecycleStateSuccessfulEmpty  = "successful-empty"
	LifecycleStateInventoryUnknown = "inventory-unknown"
	LifecycleStateAbsentPending    = "absent-pending"
	LifecycleStateConfirmedAbsent  = "confirmed-absent/deleted"
	LifecycleStateNameCollision    = "name-collision"
)

const lifecycleSchemaVersion = 2

type lifecycleRecord struct {
	Key                 string      `json:"key"`
	VMID                int         `json:"vmid"`
	Type                string      `json:"type"`
	Name                string      `json:"name"`
	Hostname            string      `json:"hostname,omitempty"`
	Node                string      `json:"node"`
	ClusterName         string      `json:"cluster_name,omitempty"`
	Generation          uint64      `json:"generation"`
	Status              string      `json:"status"`
	Collision           bool        `json:"collision,omitempty"`
	LastIPs             ResolvedIPs `json:"last_confirmed_ips"`
	LastConfirmedAt     time.Time   `json:"last_confirmed_at,omitempty"`
	LastSeenAt          time.Time   `json:"last_seen_at,omitempty"`
	MissingObservations int         `json:"missing_observations,omitempty"`
}

type lifecycleFile struct {
	Schema           int                             `json:"schema"`
	Instances        map[string]lifecycleRecord      `json:"instances"`
	Tombstones       map[string]uint64               `json:"tombstones,omitempty"`
	RetiredNames     map[string]lifecycleRetiredName `json:"retired_names,omitempty"`
	ObservedNonEmpty bool                            `json:"observed_non_empty"`
}

type lifecycleRetiredName struct {
	Key         string `json:"key"`
	VMID        int    `json:"vmid"`
	Type        string `json:"type"`
	Name        string `json:"name"`
	Hostname    string `json:"hostname,omitempty"`
	ClusterName string `json:"cluster_name,omitempty"`
	Generation  uint64 `json:"generation"`
}

type lifecycleStore struct {
	path    string
	mu      sync.Mutex
	data    lifecycleFile
	fileOps lifecycleFileOps
}

// lifecycleFileOps is scoped to one store so tests can fail the rename or
// directory sync boundary without replacing process-wide os functions.
type lifecycleFileOps struct {
	rename        func(oldPath, newPath string) error
	syncDirectory func(path string) error
}

func newLifecycleStore(path string) *lifecycleStore {
	if strings.TrimSpace(path) == "" {
		path = DefaultLifecycleStateFile
	}
	return &lifecycleStore{
		path: path,
		fileOps: lifecycleFileOps{
			rename:        os.Rename,
			syncDirectory: syncLifecycleDirectory,
		},
	}
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
	next.Schema = lifecycleSchemaVersion
	if next.Instances == nil {
		next.Instances = make(map[string]lifecycleRecord)
	}
	if next.Tombstones == nil {
		next.Tombstones = make(map[string]uint64)
	}
	if next.RetiredNames == nil {
		next.RetiredNames = make(map[string]lifecycleRetiredName)
	}
	renamed, err := s.saveLocked(next)
	if err != nil {
		if renamed {
			// The on-disk file now contains next even if directory sync failed.
			// Drop the old in-memory snapshot so a later observation reloads the
			// visible file instead of resurrecting deleted lifecycle state.
			s.data = lifecycleFile{}
		}
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
				Schema:       lifecycleSchemaVersion,
				Instances:    make(map[string]lifecycleRecord),
				Tombstones:   make(map[string]uint64),
				RetiredNames: make(map[string]lifecycleRetiredName),
			}
			return nil
		}
		return fmt.Errorf("reading proxmox lifecycle state: %w", err)
	}
	var file lifecycleFile
	if err := json.Unmarshal(data, &file); err != nil {
		return fmt.Errorf("decoding proxmox lifecycle state: %w", err)
	}
	if file.Schema != 1 && file.Schema != lifecycleSchemaVersion {
		return fmt.Errorf("unsupported proxmox lifecycle state schema %d", file.Schema)
	}
	if file.Instances == nil {
		file.Instances = make(map[string]lifecycleRecord)
	}
	if file.Tombstones == nil {
		file.Tombstones = make(map[string]uint64)
	}
	if file.RetiredNames == nil {
		file.RetiredNames = make(map[string]lifecycleRetiredName)
	}
	if file.Schema == 1 {
		// Schema 1 kept names only while an instance was active or absent-pending.
		// Preserve those identities before later observations can move them into
		// generation-only tombstones. A tombstone-only v1 file has no name to migrate.
		for key, record := range file.Instances {
			if record.Key == "" {
				record.Key = key
			}
			rememberLifecycleName(&file, record)
		}
		file.Schema = lifecycleSchemaVersion
	}
	s.data = file
	return nil
}

func (s *lifecycleStore) saveLocked(file lifecycleFile) (bool, error) {
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return false, fmt.Errorf("creating proxmox lifecycle state directory: %w", err)
	}
	data, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		return false, fmt.Errorf("encoding proxmox lifecycle state: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".proxmox-lifecycle-*.tmp")
	if err != nil {
		return false, fmt.Errorf("creating proxmox lifecycle state temporary file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return false, fmt.Errorf("setting proxmox lifecycle state permissions: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return false, fmt.Errorf("writing proxmox lifecycle state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return false, fmt.Errorf("syncing proxmox lifecycle state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return false, fmt.Errorf("closing proxmox lifecycle state: %w", err)
	}
	if err := s.fileOps.rename(tmpName, s.path); err != nil {
		return false, fmt.Errorf("replacing proxmox lifecycle state: %w", err)
	}
	if err := s.fileOps.syncDirectory(dir); err != nil {
		return true, err
	}
	return true, nil
}

func syncLifecycleDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening proxmox lifecycle state directory for sync: %w", err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return fmt.Errorf("syncing proxmox lifecycle state directory: %w", err)
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("closing proxmox lifecycle state directory: %w", err)
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
	clone.RetiredNames = make(map[string]lifecycleRetiredName, len(file.RetiredNames))
	for key, retired := range file.RetiredNames {
		clone.RetiredNames[key] = retired
	}
	return clone
}

func rememberLifecycleName(file *lifecycleFile, record lifecycleRecord) {
	hostname := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(record.Hostname), "."))
	if strings.TrimSpace(record.Name) == "" || hostname == "" || file == nil {
		return
	}
	if file.RetiredNames == nil {
		file.RetiredNames = make(map[string]lifecycleRetiredName)
	}
	if record.Generation == 0 {
		record.Generation = 1
	}
	key := record.Key
	if key == "" {
		key = lifecycleIdentity(record.ClusterName, ClusterResource{Type: record.Type, VMID: record.VMID})
	}
	retiredKey := fmt.Sprintf("%s|generation:%d|hostname:%s", key, record.Generation, hostname)
	file.RetiredNames[retiredKey] = lifecycleRetiredName{
		Key:         key,
		VMID:        record.VMID,
		Type:        record.Type,
		Name:        record.Name,
		Hostname:    hostname,
		ClusterName: record.ClusterName,
		Generation:  record.Generation,
	}
}

func clusterIdentity(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Host == "" {
		return strings.ToLower(strings.TrimRight(strings.TrimSpace(raw), "/"))
	}
	return strings.ToLower(parsed.Scheme + "://" + parsed.Host + strings.TrimRight(parsed.EscapedPath(), "/"))
}

func lifecycleIdentity(cluster string, resource ClusterResource) string {
	cluster = strings.ToLower(strings.Trim(strings.TrimSpace(cluster), "."))
	return fmt.Sprintf("%s|%s|%d", cluster, resource.Type, resource.VMID)
}
