package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/bin/oracle/servicetest"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
)

// Offline replay (DESIGN §4.12):
//
//	go test ./bin/oracle -run TestReplayJournal -args -journal=<file>
//
// re-feeds every request of a journal (ORACLE_JOURNAL_FILE, JSONL) in n order
// into a fresh service with the same boot id and requires identical answers.

var journalFlag = flag.String("journal", "", "oracle journal (JSONL) to replay")

func readJournalFile(t *testing.T, path string) []journalEntry {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	var out []journalEntry
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<30)
	for sc.Scan() {
		var e journalEntry
		require.NoError(t, json.Unmarshal(sc.Bytes(), &e))
		out = append(out, e)
	}
	require.NoError(t, sc.Err())

	return out
}

// replayEntries feeds entries into a fresh server and returns the first
// mismatch (nil when every answer is identical).
func replayEntries(entries []journalEntry, out *bytes.Buffer) error {
	if len(entries) == 0 {
		return nil
	}
	s, err := newServer(options{BootID: entries[0].Boot, Out: out})
	if err != nil {
		return err
	}
	for _, e := range entries {
		if _, el := isElided(e.Req); el {
			return fmt.Errorf("entry %d (%s): request elided; replay needs ORACLE_JOURNAL_FILE", e.N, e.Ep)
		}
		var body []byte
		if string(e.Req) != "null" {
			body = e.Req
		}
		status, resp := s.do(e.Ep, body)
		for _, v := range e.Violations {
			fmt.Fprintf(out, "entry %d %s: violation %s\n", e.N, e.Ep, v)
		}
		if strings.HasPrefix(e.Ep, "GET /journal") {
			continue // the journal itself carries wall-clock ms
		}
		if status != e.Status {
			return fmt.Errorf("entry %d (%s): status %d, journal %d", e.N, e.Ep, status, e.Status)
		}
		if el, ok := isElided(e.Resp); ok {
			sum := sha256.Sum256(resp)
			if hex.EncodeToString(sum[:]) != el.Sha256 {
				return fmt.Errorf("entry %d (%s): elided response hash differs", e.N, e.Ep)
			}

			continue
		}
		if !bytes.Equal(compactJSON(resp), compactJSON(e.Resp)) {
			return fmt.Errorf("entry %d (%s): response differs:\n got %s\nwant %s", e.N, e.Ep, resp, e.Resp)
		}
	}

	return nil
}

func TestReplayJournal(t *testing.T) {
	if *journalFlag == "" {
		t.Skip("pass -args -journal=<file>")
	}
	var out bytes.Buffer
	err := replayEntries(readJournalFile(t, *journalFlag), &out)
	t.Log(out.String())
	require.NoError(t, err)
}

// scriptSession drives a representative session: writes, failures, reads,
// views, quiet periods, refusals.
func scriptSession(t *testing.T, c *servicetest.Client, bigViews bool) {
	t.Helper()
	c.Setup("replay")
	l1 := c.Ledger(1)
	c.Write(false, testutil.CreateLedgerReq(l1))
	a := c.BeginWrite(true, testutil.TxReq(l1, c.Prefix+"ref1", "world", "t-1:1", "COIN", 10))
	b := c.BeginWrite(false, testutil.TxReq(l1, "", "t-1:1", "t-1:2", "COIN", 10))
	outB := c.Execute(b)
	outA := c.Execute(a)
	c.EndWrite(b, outB)
	rb := c.BeginRead("GetAccount", map[string]any{"name": "seek", "ledgerPick": 0, "table": "volumes", "probe": "t-1:0"})
	c.EndWrite(a, outA)
	c.EndRead(uint64(rb["ticket"].(float64)), "GetAccount", smokeRequest(l1, "t-1:1"), outA)
	c.Do("POST", "/write/late", map[string]any{"ticket": a, "outcome": outA, "via": map[string]any{}})
	c.Do("POST", "/write/begin", c.BeginWriteBody(false, testutil.EmptyTxReq("r-ns-x-1")))
	c.OK("POST", "/quiet/begin", map[string]any{"epoch": 1})
	c.OK("POST", "/quiet/end", map[string]any{"epoch": 1})
	if bigViews {
		views := make([]any, 0, 300)
		for i := range 300 {
			views = append(views, map[string]any{"name": "seek", "ledger": l1, "table": "volumes", "probe": fmt.Sprintf("t-1:%d", i)})
		}
		c.OK("POST", "/view", map[string]any{"views": views})
	}
	c.OK("GET", "/dump", nil)
	c.OK("GET", "/journal?from=1&limit=3", nil)
}

func TestReplay_ScriptedSessionFromFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "oracle.jsonl")
	s, err := newServer(options{BootID: testBoot, JournalFile: path, Out: &bytes.Buffer{}})
	require.NoError(t, err)
	scriptSession(t, servicetest.New(t, s), true)

	entries := readJournalFile(t, path)
	require.Greater(t, len(entries), 10)
	var out bytes.Buffer
	require.NoError(t, replayEntries(entries, &out))

	// A divergent journal is caught.
	entries[3].Resp = json.RawMessage(`{"bootId":"x"}`)
	require.Error(t, replayEntries(entries, &out))
}

func TestReplay_ElidedResponsesComparedByHash(t *testing.T) {
	t.Parallel()
	s, c := newTestServer(t)
	scriptSession(t, c, true)

	// Read the ring back through GET /journal: the big /view answer is elided.
	// One page holds the whole (short) ring; paging further would chase the
	// entries the GET /journal calls themselves append.
	var entries []journalEntry
	m := c.OK("GET", "/journal?from=1&limit=10000", nil)
	for _, raw := range m["entries"].([]any) {
		b, err := json.Marshal(raw)
		require.NoError(t, err)
		var e journalEntry
		require.NoError(t, json.Unmarshal(b, &e))
		entries = append(entries, e)
	}
	elidedSeen := false
	for _, e := range entries {
		if _, ok := isElided(e.Resp); ok {
			elidedSeen = true
		}
	}
	require.True(t, elidedSeen)
	_ = s
	var out bytes.Buffer
	require.NoError(t, replayEntries(entries, &out))
}
