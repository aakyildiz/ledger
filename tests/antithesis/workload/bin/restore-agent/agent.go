package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// wiper deletes every backup object of one namespace.
type wiper interface {
	wipe(ctx context.Context, bucketID string) (int, error)
}

type agent struct {
	cfg config
	out *lineWriter

	// opMu serializes mutations; mu guards the fields below.
	opMu  sync.Mutex
	mu    sync.Mutex
	state persisted
	child *proc
	off   *proc // a running ledgerctl store bootstrap
	exits []exitRecord

	wiper wiper
}

const maxExits = 64

func newAgent(cfg config) (*agent, error) {
	st, err := loadState(cfg.stateFile)
	if err != nil {
		return nil, err
	}

	return &agent{cfg: cfg, out: stdoutLines, state: st}, nil
}

// resume applies the restart rules once at boot.
func (a *agent) resume() {
	a.opMu.Lock()
	defer a.opMu.Unlock()
	a.mu.Lock()
	st := a.state
	a.mu.Unlock()
	start, next := resumeDecision(st, readMarker(a.cfg.dataDir).Present)
	next.BootCount++
	if err := a.save(next); err != nil {
		log.Printf("restore-agent: saving state at boot: %v", err)
	}
	if next.ResumeNote != "" {
		log.Printf("restore-agent: %s", next.ResumeNote)
	}
	if start {
		if err := a.startChildLocked(next.Mode, next.ClusterID, next.NodeID, next.Join); err != nil {
			log.Printf("restore-agent: resuming %s: %v", next.Mode, err)
		}
	}
}

func (a *agent) save(st persisted) error {
	if err := saveState(a.cfg.stateFile, st); err != nil {
		return err
	}
	a.mu.Lock()
	a.state = st
	a.mu.Unlock()

	return nil
}

func (a *agent) snapshot() persisted {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.state
}

func (a *agent) recordExit(rec exitRecord) {
	if !rec.ByAgent {
		log.Printf("restore-agent: child (%s, pid %d) exited on its own: code %d %s", rec.Mode, rec.Pid, rec.Code, rec.Signal)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.exits = append(a.exits, rec)
	if len(a.exits) > maxExits {
		a.exits = a.exits[len(a.exits)-maxExits:]
	}
}

// startChildLocked starts the server child. Caller holds opMu.
func (a *agent) startChildLocked(mode, clusterID string, nodeID uint64, join string) error {
	args := a.cfg.serverArgs(mode, clusterID, nodeID, join)
	log.Printf("restore-agent: starting %s %s", a.cfg.serverBin, strings.Join(args, " "))
	p, err := startProc(a.cfg.serverBin, args, a.cfg.childEnv, mode, a.out, nil, a.recordExit)
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.child = p
	a.mu.Unlock()

	return nil
}

// stopChildLocked stops the child, if any, and waits for its exit.
func (a *agent) stopChildLocked(graceful bool) {
	a.mu.Lock()
	p := a.child
	a.mu.Unlock()
	if p == nil {
		return
	}
	p.stop(graceful, a.cfg.stopTimeout)
	a.mu.Lock()
	if a.child == p {
		a.child = nil
	}
	a.mu.Unlock()
}

func (a *agent) stopOfflineLocked() {
	a.mu.Lock()
	p := a.off
	a.mu.Unlock()
	if p != nil {
		p.stop(false, 0)
	}
}

func (a *agent) shutdown(timeout time.Duration) {
	a.opMu.Lock()
	defer a.opMu.Unlock()
	a.stopOfflineLocked()
	a.mu.Lock()
	p := a.child
	a.mu.Unlock()
	if p != nil {
		p.stop(true, timeout)
	}
}

// ---------------------------------------------------------------- HTTP

func (a *agent) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", a.handleStatus)
	mux.HandleFunc("POST /reset", a.handleReset)
	mux.HandleFunc("POST /start", a.handleStart)
	mux.HandleFunc("POST /stop", a.handleStop)
	mux.HandleFunc("POST /offline-bootstrap", a.handleOffline)
	mux.HandleFunc("POST /wipe-namespace", a.handleWipe)

	return mux
}

type statusResponse struct {
	Gen         uint64         `json:"gen"`
	Mode        string         `json:"mode"`
	Running     bool           `json:"running"`
	ClusterID   string         `json:"clusterId"`
	NodeID      uint64         `json:"nodeId"`
	Join        string         `json:"join,omitempty"`
	Pid         int            `json:"pid"`
	Alive       bool           `json:"alive"`
	BootCount   uint64         `json:"bootCount"`
	ChildExits  []exitRecord   `json:"childExits"`
	Marker      markerStatus   `json:"marker"`
	Checkpoint0 bool           `json:"checkpoint0"`
	Staging     bool           `json:"staging"`
	WalEmpty    bool           `json:"walEmpty"`
	Offline     offlineStatus  `json:"offline"`
	ResumeNote  string         `json:"resumeNote,omitempty"`
	LastOffline *offlineResult `json:"lastOffline,omitempty"`
}

type offlineStatus struct {
	Running bool `json:"running"`
	Failed  bool `json:"failed"`
}

func (a *agent) status() statusResponse {
	a.mu.Lock()
	st := a.state
	p := a.child
	exits := append([]exitRecord{}, a.exits...)
	a.mu.Unlock()
	s := statusResponse{
		Gen: st.Gen, Mode: st.Mode, Running: st.Running, ClusterID: st.ClusterID, NodeID: st.NodeID, Join: st.Join,
		BootCount: st.BootCount, ChildExits: exits, ResumeNote: st.ResumeNote,
		Marker:      readMarker(a.cfg.dataDir),
		Checkpoint0: exists(filepath.Join(a.cfg.dataDir, "checkpoints", "0")),
		Staging:     exists(filepath.Join(a.cfg.dataDir, "restore-staging")),
		WalEmpty:    dirEmpty(a.cfg.walDir),
		Offline:     offlineStatus{Running: st.OfflineRunning, Failed: st.OfflineFailed},
		LastOffline: st.Offline,
	}
	if p != nil && p.alive() {
		s.Alive = true
		s.Pid = p.cmd.Process.Pid
	}

	return s
}

func (a *agent) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.status())
}

type command struct {
	Gen       uint64 `json:"gen"`
	Mode      string `json:"mode"`
	ClusterID string `json:"clusterId"`
	NodeID    uint64 `json:"nodeId"`
	Join      string `json:"join"`
	Graceful  bool   `json:"graceful"`
	BucketID  string `json:"bucketId"`
	Validate  bool   `json:"validate"`
}

func decode(w http.ResponseWriter, r *http.Request) (command, bool) {
	var c command
	if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})

		return c, false
	}

	return c, true
}

// errConflict answers 409.
type errConflict string

func (e errConflict) Error() string { return string(e) }

func (a *agent) reply(w http.ResponseWriter, body any, err error) {
	var conflict errConflict
	switch {
	case errors.As(err, &conflict):
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "status": a.status()})
	case err != nil:
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error(), "status": a.status()})
	default:
		writeJSON(w, http.StatusOK, body)
	}
}

// checkGen: a mutation must carry the current generation (reset may also
// carry a newer one).
func checkGen(cur, got uint64, newerOK bool) error {
	if got < cur {
		return errConflict(fmt.Sprintf("stale gen %d (current %d)", got, cur))
	}
	if got > cur && !newerOK {
		return errConflict(fmt.Sprintf("gen %d is newer than the current %d: reset first", got, cur))
	}

	return nil
}

func (a *agent) handleReset(w http.ResponseWriter, r *http.Request) {
	c, ok := decode(w, r)
	if !ok {
		return
	}
	a.reply(w, a.status, a.reset(c.Gen))
}

func (a *agent) reset(gen uint64) error {
	a.opMu.Lock()
	defer a.opMu.Unlock()
	st := a.snapshot()
	if err := checkGen(st.Gen, gen, true); err != nil {
		return err
	}
	a.stopOfflineLocked()
	a.stopChildLocked(true)
	if err := wipeContents(a.cfg.dataDir); err != nil {
		return fmt.Errorf("wiping %s: %w", a.cfg.dataDir, err)
	}
	if err := wipeContents(a.cfg.walDir); err != nil {
		return fmt.Errorf("wiping %s: %w", a.cfg.walDir, err)
	}
	next := persisted{Gen: gen, Mode: modeIdle, BootCount: st.BootCount}

	return a.save(next)
}

func (a *agent) handleStart(w http.ResponseWriter, r *http.Request) {
	c, ok := decode(w, r)
	if !ok {
		return
	}
	already, err := a.start(c)
	a.reply(w, map[string]any{"already": already, "status": a.status()}, err)
}

func (a *agent) start(c command) (already bool, err error) {
	switch c.Mode {
	case modeRestore, modeNormal:
	case modeJoin:
		if c.Join == "" {
			return false, errConflict("join mode needs join")
		}
	default:
		return false, errConflict(fmt.Sprintf("unknown mode %q", c.Mode))
	}
	if c.ClusterID == "" || c.NodeID == 0 {
		return false, errConflict("clusterId and nodeId are required")
	}
	a.opMu.Lock()
	defer a.opMu.Unlock()
	st := a.snapshot()
	if err := checkGen(st.Gen, c.Gen, false); err != nil {
		return false, err
	}
	if st.OfflineRunning {
		return false, errConflict("an offline bootstrap is running")
	}
	a.mu.Lock()
	p := a.child
	a.mu.Unlock()
	same := st.Running && st.Mode == c.Mode && st.ClusterID == c.ClusterID && st.NodeID == c.NodeID && st.Join == c.Join
	if p != nil && p.alive() && same {
		return true, nil
	}
	a.stopChildLocked(true)
	if c.Mode == modeRestore && readMarker(a.cfg.dataDir).Present {
		return false, errConflict("marker present: finalize committed, restore mode would refuse the data directory")
	}
	next := st
	next.Mode, next.ClusterID, next.NodeID, next.Join, next.Running = c.Mode, c.ClusterID, c.NodeID, c.Join, true
	next.OfflineFailed = false
	if err := a.save(next); err != nil {
		return false, err
	}

	return false, a.startChildLocked(c.Mode, c.ClusterID, c.NodeID, c.Join)
}

func (a *agent) handleStop(w http.ResponseWriter, r *http.Request) {
	c, ok := decode(w, r)
	if !ok {
		return
	}
	a.reply(w, a.status, a.stop(c))
}

func (a *agent) stop(c command) error {
	a.opMu.Lock()
	defer a.opMu.Unlock()
	st := a.snapshot()
	if err := checkGen(st.Gen, c.Gen, false); err != nil {
		return err
	}
	next := st
	next.Running = false
	if err := a.save(next); err != nil {
		return err
	}
	a.stopChildLocked(c.Graceful)

	return nil
}

func (a *agent) handleOffline(w http.ResponseWriter, r *http.Request) {
	c, ok := decode(w, r)
	if !ok {
		return
	}
	res, err := a.offline(c)
	a.reply(w, res, err)
}

// offline runs ledgerctl store bootstrap with the child stopped (restore.md
// §2.5). The verdict line is ledgerctl's last success or failure line.
func (a *agent) offline(c command) (*offlineResult, error) {
	if c.BucketID == "" {
		return nil, errConflict("bucketId is required (the ledgerctl default is the literal \"default\")")
	}
	a.opMu.Lock()
	st := a.snapshot()
	if err := checkGen(st.Gen, c.Gen, false); err != nil {
		a.opMu.Unlock()

		return nil, err
	}
	a.mu.Lock()
	busy := (a.child != nil && a.child.alive()) || a.off != nil
	a.mu.Unlock()
	if busy || st.OfflineRunning {
		a.opMu.Unlock()

		return nil, errConflict("the child or an offline bootstrap is running")
	}
	next := st
	next.Mode, next.Running, next.OfflineRunning, next.OfflineFailed, next.Offline = modeOffline, false, true, false, nil
	if err := a.save(next); err != nil {
		a.opMu.Unlock()

		return nil, err
	}
	var tailMu sync.Mutex
	var tail []string
	tee := func(line string) {
		tailMu.Lock()
		defer tailMu.Unlock()
		tail = append(tail, line)
		if len(tail) > 40 {
			tail = tail[len(tail)-40:]
		}
	}
	args := a.cfg.bootstrapArgs(c.BucketID, c.Validate)
	log.Printf("restore-agent: running %s %s", a.cfg.ledgerctlBin, strings.Join(args, " "))
	p, err := startProc(a.cfg.ledgerctlBin, args, a.cfg.childEnv, modeOffline, a.out, tee, nil)
	if err != nil {
		next.OfflineRunning, next.OfflineFailed = false, true
		_ = a.save(next) // the start error is the one reported
		a.opMu.Unlock()

		return nil, err
	}
	a.mu.Lock()
	a.off = p
	a.mu.Unlock()
	a.opMu.Unlock()

	<-p.done
	p.pumps.Wait()

	a.opMu.Lock()
	defer a.opMu.Unlock()
	a.mu.Lock()
	a.off = nil
	rec := p.exit
	a.mu.Unlock()
	tailMu.Lock()
	res := &offlineResult{Gen: c.Gen, Exit: rec.Code, Tail: tail, Validate: c.Validate, VerdictLine: verdictLine(tail)}
	tailMu.Unlock()
	cur := a.snapshot()
	if cur.Gen != c.Gen || !cur.OfflineRunning {
		// A reset interrupted the run.
		return res, errConflict("the offline bootstrap was interrupted by a reset")
	}
	cur.OfflineRunning = false
	cur.OfflineFailed = rec.Code != 0
	cur.Offline = res

	return res, a.save(cur)
}

// verdictLine is the last ledgerctl line that states an outcome.
func verdictLine(tail []string) string {
	for i := len(tail) - 1; i >= 0; i-- {
		l := tail[i]
		if strings.Contains(l, "Bootstrap complete") || strings.Contains(l, "Failed to validate backup") ||
			strings.Contains(l, "Backup is valid") || strings.Contains(l, "Error") || strings.Contains(l, "ERROR") {
			return l
		}
	}

	return ""
}

func (a *agent) handleWipe(w http.ResponseWriter, r *http.Request) {
	c, ok := decode(w, r)
	if !ok {
		return
	}
	n, err := a.wipeNamespace(r.Context(), c)
	a.reply(w, map[string]any{"deleted": n}, err)
}

// wipeNamespace deletes a chained-restore namespace. Only `restore-g*`
// namespaces (restore-target cluster ids) can be wiped, never the source's
// `default` or the cycle's `hegel-restore`.
func (a *agent) wipeNamespace(ctx context.Context, c command) (int, error) {
	if !strings.HasPrefix(c.BucketID, "restore-g") || strings.ContainsAny(c.BucketID, "/\\.") {
		return 0, errConflict(fmt.Sprintf("refusing to wipe namespace %q", c.BucketID))
	}
	st := a.snapshot()
	if err := checkGen(st.Gen, c.Gen, false); err != nil {
		return 0, err
	}
	if a.wiper == nil {
		return 0, errors.New("no backup storage configured")
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	return a.wiper.wipe(ctx, c.BucketID)
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	if f, ok := body.(func() statusResponse); ok {
		body = f()
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	// The client may be gone; nothing to do about it.
	_ = json.NewEncoder(w).Encode(body)
}
