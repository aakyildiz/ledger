package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

const (
	modeIdle    = "idle"
	modeRestore = "restore"
	modeNormal  = "normal"
	modeJoin    = "join"
	modeOffline = "offline"
)

// persisted is the agent's durable state (AGENT_STATE).
type persisted struct {
	Gen       uint64 `json:"gen"`
	Mode      string `json:"mode"`
	ClusterID string `json:"clusterId"`
	NodeID    uint64 `json:"nodeId"`
	Join      string `json:"join,omitempty"`
	// Running: the driver wants the child to run (start), as opposed to
	// stopped (stop, reset).
	Running   bool   `json:"running"`
	BootCount uint64 `json:"bootCount"`
	// OfflineRunning is set while ledgerctl store bootstrap runs; a
	// container restart then marks the attempt failed (restore.md §4.1).
	OfflineRunning bool           `json:"offlineRunning"`
	OfflineFailed  bool           `json:"offlineFailed"`
	Offline        *offlineResult `json:"offline,omitempty"`
	// ResumeNote says what the last boot did with the persisted state.
	ResumeNote string `json:"resumeNote,omitempty"`
}

type offlineResult struct {
	Gen         uint64   `json:"gen"`
	Exit        int      `json:"exit"`
	VerdictLine string   `json:"verdictLine"`
	Tail        []string `json:"tail"`
	Validate    bool     `json:"validate"`
}

func loadState(path string) (persisted, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return persisted{Mode: modeIdle}, nil
	}
	if err != nil {
		return persisted{}, err
	}
	var st persisted
	if err := json.Unmarshal(b, &st); err != nil {
		return persisted{}, fmt.Errorf("parsing %s: %w", path, err)
	}
	if st.Mode == "" {
		st.Mode = modeIdle
	}

	return st, nil
}

// saveState writes the state durably: temp file, fsync, rename, fsync dir.
func saveState(path string, st persisted) error {
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close() // the write error is the one reported

		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close() // the sync error is the one reported

		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := d.Sync()
	if err := d.Close(); err != nil {
		return err
	}

	return syncErr
}

// resumeDecision is what a container restart does with the persisted state
// (restore.md §4.1):
//   - restore: a RESTORED marker means finalize committed, so restore mode
//     is not restarted (it would refuse the directory); otherwise restore mode
//     restarts and the server reclaims staging and an unmarked checkpoints/0;
//   - normal, join: restart the same way;
//   - offline: an interrupted bootstrap is marked failed (the next attempt
//     discards its staging).
func resumeDecision(st persisted, markerPresent bool) (start bool, next persisted) {
	next = st
	next.ResumeNote = ""
	if next.OfflineRunning {
		next.OfflineRunning = false
		next.OfflineFailed = true
		next.ResumeNote = "offline bootstrap interrupted by a restart"
	}
	if !st.Running {
		return false, next
	}
	switch st.Mode {
	case modeRestore:
		if markerPresent {
			next.Running = false
			next.ResumeNote = "RESTORED marker present: finalize committed, restore mode not restarted"

			return false, next
		}

		return true, next
	case modeNormal, modeJoin:
		return true, next
	}
	next.Running = false

	return false, next
}

// markerStatus is the durable commit proof of a restore (EN-3029).
type markerStatus struct {
	Present              bool   `json:"present"`
	LastAppliedIndex     uint64 `json:"lastAppliedIndex"`
	LastAppliedTimestamp uint64 `json:"lastAppliedTimestamp"`
	Error                string `json:"error,omitempty"`
}

func readMarker(dataDir string) markerStatus {
	b, err := os.ReadFile(filepath.Join(dataDir, "RESTORED"))
	if errors.Is(err, os.ErrNotExist) {
		return markerStatus{}
	}
	if err != nil {
		return markerStatus{Present: true, Error: err.Error()}
	}
	var m struct {
		LastAppliedIndex     uint64 `json:"lastAppliedIndex"`
		LastAppliedTimestamp uint64 `json:"lastAppliedTimestamp"`
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return markerStatus{Present: true, Error: err.Error()}
	}

	return markerStatus{Present: true, LastAppliedIndex: m.LastAppliedIndex, LastAppliedTimestamp: m.LastAppliedTimestamp}
}

func exists(path string) bool {
	_, err := os.Stat(path)

	return err == nil
}

// dirEmpty is true for a missing or empty directory.
func dirEmpty(path string) bool {
	entries, err := os.ReadDir(path)

	return err != nil || len(entries) == 0
}

// wipeContents deletes everything inside dir (dir may be a mount point).
func wipeContents(dir string) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return os.MkdirAll(dir, 0o755)
	}
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}

	return nil
}

func uitoa(n uint64) string {
	return strconv.FormatUint(n, 10)
}
