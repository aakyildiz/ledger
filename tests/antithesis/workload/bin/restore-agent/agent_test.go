package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// syncBuffer is a goroutine-safe bytes.Buffer.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.b.String()
}

// fakeServer is a shell script standing in for ledger-server: it appends
// "start <args>" to $LOG, prints a JSON record and a panic line, and on
// SIGTERM appends "exit" after a short delay (so a restart that does not wait
// for the exit would log "start" before "exit").
const fakeServer = `#!/bin/sh
trap 'sleep 0.3; echo exit >> "$LOG"; exit 0' TERM
echo "start $*" >> "$LOG"
echo '{"level":"info","msg":"Leader ready","node":1}'
echo 'panic: x' >&2
while :; do sleep 0.05; done
`

func testAgent(t *testing.T, script string) (*agent, *syncBuffer, string) {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "server.sh")
	require.NoError(t, os.WriteFile(bin, []byte(script), 0o755))
	logFile := filepath.Join(dir, "log")
	cfg := config{
		dataDir:      filepath.Join(dir, "data", "app"),
		walDir:       filepath.Join(dir, "data", "raft"),
		stateFile:    filepath.Join(dir, "data", "agent.json"),
		serverBin:    bin,
		ledgerctlBin: bin,
		hostname:     "restore-0",
		stopTimeout:  5 * time.Second,
		childEnv:     []string{"PATH=" + os.Getenv("PATH"), "LOG=" + logFile},
	}
	require.NoError(t, os.MkdirAll(cfg.dataDir, 0o755))
	require.NoError(t, os.MkdirAll(cfg.walDir, 0o755))
	a, err := newAgent(cfg)
	require.NoError(t, err)
	out := &syncBuffer{}
	a.out = &lineWriter{w: out}
	t.Cleanup(func() { a.shutdown(time.Second) })

	return a, out, logFile
}

func post(t *testing.T, h http.Handler, path string, body any) (int, map[string]any) {
	t.Helper()
	b, err := json.Marshal(body)
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b)))
	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return rec.Code, m
}

func getStatus(t *testing.T, h http.Handler) statusResponse {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var s statusResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &s))

	return s
}

// starts counts the fake server's "start" lines.
func starts(log []byte) int {
	n := 0
	for _, l := range strings.Split(string(log), "\n") {
		if strings.HasPrefix(l, "start ") {
			n++
		}
	}

	return n
}

func writeMarker(t *testing.T, dataDir string, idx uint64) {
	t.Helper()
	b, err := json.Marshal(map[string]uint64{"lastAppliedIndex": idx, "lastAppliedTimestamp": 7})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "RESTORED"), b, 0o644))
}

func TestResumeDecisions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		st          persisted
		marker      bool
		wantStart   bool
		wantRunning bool
		wantFailed  bool
	}{
		{"restore without marker restarts", persisted{Mode: modeRestore, Running: true}, false, true, true, false},
		{"restore with marker: finalize committed, no restore mode", persisted{Mode: modeRestore, Running: true}, true, false, false, false},
		{"normal restarts", persisted{Mode: modeNormal, Running: true}, true, true, true, false},
		{"join restarts", persisted{Mode: modeJoin, Running: true}, false, true, true, false},
		{"stopped stays stopped", persisted{Mode: modeNormal, Running: false}, false, false, false, false},
		{"idle stays idle", persisted{Mode: modeIdle}, false, false, false, false},
		{"interrupted offline is failed", persisted{Mode: modeOffline, OfflineRunning: true}, false, false, false, true},
	} {
		start, next := resumeDecision(tc.st, tc.marker)
		require.Equal(t, tc.wantStart, start, tc.name)
		require.Equal(t, tc.wantRunning, next.Running, tc.name)
		require.Equal(t, tc.wantFailed, next.OfflineFailed, tc.name)
		require.False(t, next.OfflineRunning, tc.name)
	}
}

func TestStatePersistsAndResumes(t *testing.T) {
	t.Parallel()
	a, _, logFile := testAgent(t, fakeServer)
	h := a.routes()
	code, _ := post(t, h, "/reset", map[string]any{"gen": 3})
	require.Equal(t, http.StatusOK, code)
	code, _ = post(t, h, "/start", map[string]any{"gen": 3, "mode": "restore", "clusterId": "restore-g3", "nodeId": 1})
	require.Equal(t, http.StatusOK, code)
	require.Eventually(t, func() bool { b, _ := os.ReadFile(logFile); return starts(b) == 1 }, 5*time.Second, 10*time.Millisecond)
	st, err := loadState(a.cfg.stateFile)
	require.NoError(t, err)
	require.Equal(t, persisted{Gen: 3, Mode: modeRestore, ClusterID: "restore-g3", NodeID: 1, Running: true}, st)

	// A container restart after finalize committed: the marker blocks
	// restore mode, the agent reports it and starts nothing.
	a.shutdown(time.Second)
	writeMarker(t, a.cfg.dataDir, 42)
	b, err := newAgent(a.cfg)
	require.NoError(t, err)
	b.out = a.out
	b.resume()
	t.Cleanup(func() { b.shutdown(time.Second) })
	s := getStatus(t, b.routes())
	require.False(t, s.Alive)
	require.False(t, s.Running)
	require.True(t, s.Marker.Present)
	require.Equal(t, uint64(42), s.Marker.LastAppliedIndex)
	require.Equal(t, uint64(1), s.BootCount)
	require.Contains(t, s.ResumeNote, "marker")
	// Restore mode is refused while the marker exists.
	code, _ = post(t, b.routes(), "/start", map[string]any{"gen": 3, "mode": "restore", "clusterId": "restore-g3", "nodeId": 1})
	require.Equal(t, http.StatusConflict, code)
}

func TestStaleGenIsRefused(t *testing.T) {
	t.Parallel()
	a, _, _ := testAgent(t, fakeServer)
	h := a.routes()
	code, _ := post(t, h, "/reset", map[string]any{"gen": 5})
	require.Equal(t, http.StatusOK, code)
	for _, c := range []struct {
		path string
		body map[string]any
	}{
		{"/reset", map[string]any{"gen": 4}},
		{"/start", map[string]any{"gen": 4, "mode": "normal", "clusterId": "c", "nodeId": 1}},
		{"/start", map[string]any{"gen": 6, "mode": "normal", "clusterId": "c", "nodeId": 1}},
		{"/stop", map[string]any{"gen": 4, "graceful": true}},
		{"/offline-bootstrap", map[string]any{"gen": 4, "bucketId": "hegel-restore"}},
		{"/wipe-namespace", map[string]any{"gen": 4, "bucketId": "restore-g4"}},
	} {
		code, body := post(t, h, c.path, c.body)
		require.Equal(t, http.StatusConflict, code, "%s %v", c.path, c.body)
		require.Contains(t, body["error"], "gen", c.path)
	}
	// Reset with the same gen is allowed (it is idempotent in effect).
	code, _ = post(t, h, "/reset", map[string]any{"gen": 5})
	require.Equal(t, http.StatusOK, code)
	// Only restore-target namespaces can be wiped.
	for _, ns := range []string{"default", "hegel-restore", "restore-g5/x", ""} {
		code, _ := post(t, h, "/wipe-namespace", map[string]any{"gen": 5, "bucketId": ns})
		require.Equal(t, http.StatusConflict, code, ns)
	}
}

func TestRestartWaitsForTheOldChildToExit(t *testing.T) {
	t.Parallel()
	a, _, logFile := testAgent(t, fakeServer)
	h := a.routes()
	post(t, h, "/reset", map[string]any{"gen": 1})
	code, _ := post(t, h, "/start", map[string]any{"gen": 1, "mode": "restore", "clusterId": "restore-g1", "nodeId": 1})
	require.Equal(t, http.StatusOK, code)
	require.Eventually(t, func() bool { b, _ := os.ReadFile(logFile); return starts(b) == 1 }, 5*time.Second, 10*time.Millisecond)
	// Same command again: idempotent, no restart.
	code, body := post(t, h, "/start", map[string]any{"gen": 1, "mode": "restore", "clusterId": "restore-g1", "nodeId": 1})
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, true, body["already"])
	// A different mode: the old child is stopped and has exited before the
	// new one starts.
	code, _ = post(t, h, "/start", map[string]any{"gen": 1, "mode": "normal", "clusterId": "restore-g1", "nodeId": 1})
	require.Equal(t, http.StatusOK, code)
	require.Eventually(t, func() bool { b, _ := os.ReadFile(logFile); return starts(b) == 2 }, 5*time.Second, 10*time.Millisecond)
	b, err := os.ReadFile(logFile)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	require.Len(t, lines, 3)
	require.True(t, strings.HasPrefix(lines[0], "start run --restore"))
	require.Equal(t, "exit", lines[1], "the restart waited for the exit")
	require.True(t, strings.HasPrefix(lines[2], "start run --node-id"))
	s := getStatus(t, h)
	require.True(t, s.Alive)
	require.Len(t, s.ChildExits, 1)
	require.True(t, s.ChildExits[0].ByAgent)

	// Reset stops the child and wipes the data and WAL contents.
	require.NoError(t, os.WriteFile(filepath.Join(a.cfg.dataDir, "x"), nil, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(a.cfg.walDir, "y"), nil, 0o644))
	code, _ = post(t, h, "/reset", map[string]any{"gen": 2})
	require.Equal(t, http.StatusOK, code)
	s = getStatus(t, h)
	require.False(t, s.Alive)
	require.True(t, s.WalEmpty)
	require.True(t, dirEmpty(a.cfg.dataDir))
	require.Equal(t, uint64(2), s.Gen)
	require.Equal(t, modeIdle, s.Mode)
}

func TestUnrequestedExitIsReported(t *testing.T) {
	t.Parallel()
	a, _, _ := testAgent(t, "#!/bin/sh\necho 'first start requires --bootstrap or --join'\nexit 1\n")
	h := a.routes()
	post(t, h, "/reset", map[string]any{"gen": 1})
	post(t, h, "/start", map[string]any{"gen": 1, "mode": "normal", "clusterId": "restore-g1", "nodeId": 1})
	require.Eventually(t, func() bool { return len(getStatus(t, h).ChildExits) == 1 }, 5*time.Second, 10*time.Millisecond)
	e := getStatus(t, h).ChildExits[0]
	require.False(t, e.ByAgent)
	require.Equal(t, 1, e.Code)
	require.Equal(t, modeNormal, e.Mode)
}

func TestChildOutputPassesThroughUnchanged(t *testing.T) {
	t.Parallel()
	a, out, _ := testAgent(t, fakeServer)
	h := a.routes()
	post(t, h, "/reset", map[string]any{"gen": 1})
	post(t, h, "/start", map[string]any{"gen": 1, "mode": "normal", "clusterId": "restore-g1", "nodeId": 1})
	require.Eventually(t, func() bool {
		s := out.String()
		return strings.Contains(s, "panic: x\n") && strings.Contains(s, "Leader ready")
	}, 5*time.Second, 10*time.Millisecond)
	lines := strings.Split(out.String(), "\n")
	require.Contains(t, lines, `{"level":"info","msg":"Leader ready","node":1}`)
	require.Contains(t, lines, "panic: x")
}

func TestPumpLinesKeepsLinesWhole(t *testing.T) {
	t.Parallel()
	out := &syncBuffer{}
	var teed []string
	pumpLines(strings.NewReader("a\n{\"b\":1}\nno newline"), &lineWriter{w: out}, func(l string) { teed = append(teed, l) })
	require.Equal(t, "a\n{\"b\":1}\nno newline\n", out.String())
	require.Equal(t, []string{"a", `{"b":1}`, "no newline"}, teed)
}

func TestCommandLinesPerMode(t *testing.T) {
	t.Parallel()
	c := config{dataDir: "/data/app", walDir: "/data/raft", hostname: "restore-1", s3Bucket: "backups", s3Endpoint: "http://minio:9000", s3Region: "us-east-1", s3AccessKey: "k", s3SecretKey: "s"}
	common := "--node-id 2 --cluster-id restore-g7 --data-dir /data/app --wal-dir /data/raft --advertise-addr restore-1:7777"
	require.Equal(t, "run --restore --restore-listen 0.0.0.0 "+common, strings.Join(c.serverArgs(modeRestore, "restore-g7", 2, ""), " "))
	require.Equal(t, "run "+common, strings.Join(c.serverArgs(modeNormal, "restore-g7", 2, ""), " "))
	require.Equal(t, "run "+common+" --join restore-0:7777", strings.Join(c.serverArgs(modeJoin, "restore-g7", 2, "restore-0:7777"), " "))
	got := strings.Join(c.bootstrapArgs("hegel-restore", true), " ")
	require.Equal(t, "store bootstrap --driver s3 --s3-bucket backups --s3-endpoint http://minio:9000 --s3-region us-east-1 --s3-access-key-id k --s3-secret-access-key s --bucket-id hegel-restore --data-dir /data/app --validate --yes", got)
	require.NotContains(t, strings.Join(c.bootstrapArgs("hegel-restore", false), " "), "--validate")
}

func TestChildEnvIsBuiltFromScratch(t *testing.T) {
	t.Parallel()
	env := childEnv([]string{"CLUSTER_ID=default", "NODE_ID=1", "DATA_DIR=/data/app", "PATH=/bin", "PEBBLE_CACHE_SIZE=32Mi", "INSECURE=true", "AGENT_ADDR=:8090", "LOG_FORWARD_ADDR=driver:7777"})
	require.Equal(t, []string{"PATH=/bin", "PEBBLE_CACHE_SIZE=32Mi", "INSECURE=true"}, env)
}

func TestOfflineBootstrap(t *testing.T) {
	t.Parallel()
	ctl := "#!/bin/sh\necho \"args $*\"\necho 'Backup is valid - no integrity errors found'\necho 'Bootstrap complete (index=5, ledgers=3). Start the server with --bootstrap to use this data.'\n"
	a, out, _ := testAgent(t, ctl)
	h := a.routes()
	post(t, h, "/reset", map[string]any{"gen": 2})
	code, _ := post(t, h, "/offline-bootstrap", map[string]any{"gen": 2, "validate": true})
	require.Equal(t, http.StatusConflict, code, "bucketId is required")
	code, body := post(t, h, "/offline-bootstrap", map[string]any{"gen": 2, "bucketId": "hegel-restore", "validate": true})
	require.Equal(t, http.StatusOK, code, body)
	require.EqualValues(t, 0, body["exit"])
	require.Contains(t, body["verdictLine"], "Bootstrap complete")
	require.Contains(t, out.String(), "--bucket-id hegel-restore")
	s := getStatus(t, h)
	require.False(t, s.Offline.Running)
	require.False(t, s.Offline.Failed)
	require.Equal(t, modeOffline, s.Mode)
	require.NotNil(t, s.LastOffline)
}
