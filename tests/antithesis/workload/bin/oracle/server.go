package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// options configures a server.
type options struct {
	// BootID is the boot id; empty means the decimal nanoseconds now.
	BootID string
	// QueryCheckpointLimit is the env QUERY_CHECKPOINT_LIMIT ("" = unset).
	QueryCheckpointLimit string
	JournalRingBytes     int
	JournalFile          string
	DumpBatches          bool
	// Out receives [journal] and [batch-dump] lines (stdout by default).
	Out io.Writer
}

type readInfo struct {
	rid     string
	kind    string
	params  json.RawMessage
	views   []json.RawMessage
	snap    oracle.GlobalState
	version uint64
}

// server is the oracle service: one mutex around the tracker and everything
// that touches it (DESIGN §5.5).
type server struct {
	mu      sync.Mutex
	opts    options
	boot    string
	started time.Time

	tracker   *oc.Tracker
	setup     *setupReq
	setupResp map[string]any

	journal *journal
	cache   *idemCache
	reads   map[uint64]*readInfo
	dumped  bool

	quietSections, quietViolations int
}

func newServer(opts options) (*server, error) {
	if opts.BootID == "" {
		opts.BootID = strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	if opts.Out == nil {
		opts.Out = log.Writer()
	}
	j, err := newJournal(opts.JournalRingBytes, opts.JournalFile)
	if err != nil {
		return nil, err
	}

	return &server{
		opts: opts, boot: opts.BootID, started: time.Now(), journal: j, cache: newIdemCache(),
		reads: map[uint64]*readInfo{},
	}, nil
}

// ServeHTTP runs every request through do.
func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}
	ep := r.Method + " " + r.URL.Path
	if r.URL.RawQuery != "" {
		ep += "?" + r.URL.RawQuery
	}
	status, resp := s.do(ep, body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(resp) // the client went away: nothing to do
}

// result is what one handler produced.
type result struct {
	status int
	body   map[string]any
	evs    []oc.Event
	// cacheKey, when set, caches a 200 answer for repeats of the same body.
	cacheKey string
}

func ok(body map[string]any, evs []oc.Event) result {
	return result{status: http.StatusOK, body: body, evs: evs}
}

func refused(ref *oc.Refusal, evs []oc.Event) result {
	return result{status: ref.HTTP, body: errorBody(ref), evs: evs}
}

// do handles one request: ep is "METHOD /path[?query]".
func (s *server) do(ep string, body []byte) (status int, resp []byte) {
	arrived := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	var res result
	func() {
		defer func() {
			if p := recover(); p != nil {
				assert.Unreachable("oracle protocol: checker panicked", map[string]any{
					"endpoint": ep, "panic": fmt.Sprint(p), "stack": string(debug.Stack()),
				})
				res = refused(&oc.Refusal{HTTP: http.StatusInternalServerError, Code: oc.CodePanic,
					Message: fmt.Sprintf("checker panicked: %v", p), Details: map[string]any{"endpoint": ep}}, nil)
			}
		}()
		res = s.dispatch(ep, body)
		assertEvents(res.evs)
	}()

	res.body["bootId"] = s.boot
	resp, err := json.Marshal(res.body)
	if err != nil {
		resp = []byte(fmt.Sprintf(`{"bootId":%q,"error":{"code":"panic","message":%q,"details":{}}}`, s.boot, err.Error()))
		res.status = http.StatusInternalServerError
	}

	var findings []string
	for _, e := range res.evs {
		if e.IsFinding() {
			findings = append(findings, string(e.ID))
		}
	}
	if res.status == http.StatusInternalServerError {
		findings = append(findings, "oracle protocol: checker panicked")
	}
	var req json.RawMessage
	switch {
	case len(body) == 0:
		req = json.RawMessage("null")
	case json.Valid(body):
		req = compactJSON(body)
	default:
		req, _ = json.Marshal(string(body))
	}
	s.journal.append(journalEntry{Boot: s.boot, Ep: ep, Req: req, Status: res.status, Resp: resp, Violations: findings,
		Ms: time.Since(s.started).Milliseconds(), Us: time.Since(arrived).Microseconds()})
	if len(findings) > 0 && !s.dumped {
		s.dumped = true
		for _, line := range s.journal.newest(findingDumpBytes) {
			fmt.Fprintf(s.opts.Out, "[journal] %s\n", line)
		}
	}

	return res.status, resp
}

func compactJSON(b []byte) json.RawMessage {
	var buf bytes.Buffer
	if err := json.Compact(&buf, b); err != nil {
		return b
	}

	return buf.Bytes()
}

func protocolErr(msg string, d map[string]any) result { return refused(oc.Protocol(msg, d), nil) }

func (s *server) dispatch(ep string, body []byte) result {
	path, query, _ := strings.Cut(ep, "?")
	switch path {
	case "GET /health":
		return ok(map[string]any{"ready": s.tracker != nil}, nil)
	case "POST /setup":
		return s.handleSetup(body)
	}
	if s.tracker == nil {
		return refused(&oc.Refusal{HTTP: http.StatusConflict, Code: oc.CodeSetup, Message: "setup not done", Details: map[string]any{}}, nil)
	}

	cacheKey := s.cacheKey(path, body)
	if cacheKey != "" {
		if c, hit := s.cache.get(cacheKey, body); hit {
			return result{status: c.status, body: cloneBody(c.body)}
		}
	}

	var res result
	switch path {
	case "POST /view":
		res = s.handleView(body)
	case "POST /write/begin":
		res = s.handleWriteBegin(body)
	case "POST /write/end":
		res = s.handleWriteEnd(body)
	case "POST /write/late":
		res = s.handleWriteLate(body)
	case "POST /write/abandon":
		res = s.handleWriteAbandon(body)
	case "POST /read/begin":
		res = s.handleReadBegin(body)
	case "POST /read/end":
		res = s.handleReadEnd(body)
	case "POST /read/abandon":
		res = s.handleReadAbandon(body)
	case "GET /quiet/status":
		in, rd, pend := s.tracker.Outstanding()
		_, open := s.tracker.QuietEpoch()
		res = ok(map[string]any{"inflight": in, "reads": rd, "pending": pend, "quiet": open}, nil)
	case "POST /quiet/begin":
		res = s.handleQuietBegin(body)
	case "POST /quiet/section":
		res = s.handleQuietSection(body)
	case "POST /quiet/end":
		res = s.handleQuietEnd(body)
	case "GET /tickets":
		res = ok(map[string]any{"inflight": nonNil(s.tracker.InflightTickets()), "reads": nonNil(s.tracker.OpenReads())}, nil)
	case "GET /journal":
		res = s.handleJournal(query)
	case "GET /dump":
		res = s.handleDump()
	default:
		res = refused(&oc.Refusal{HTTP: http.StatusNotFound, Code: oc.CodeProtocol, Message: "unknown endpoint", Details: map[string]any{"endpoint": path}}, nil)
	}
	if cacheKey != "" && res.status == http.StatusOK {
		s.cache.put(cacheKey, body, res.status, cloneBody(res.body))
	}

	return res
}

func nonNil[T any](l []T) []T {
	if l == nil {
		return []T{}
	}

	return l
}

func cloneBody(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}

	return out
}

// cacheKey derives the idempotency key of a mutating call (DESIGN §4.3).
func (s *server) cacheKey(path string, body []byte) string {
	var k struct {
		Wid     string  `json:"wid"`
		Rid     string  `json:"rid"`
		Ticket  *uint64 `json:"ticket"`
		Epoch   *uint64 `json:"epoch"`
		Section string  `json:"section"`
		Seq     uint64  `json:"seq"`
	}
	if json.Unmarshal(body, &k) != nil {
		return ""
	}
	ticket := func(p string) string {
		if k.Ticket == nil {
			return ""
		}

		return p + strconv.FormatUint(*k.Ticket, 10)
	}
	epoch := func(p string) string {
		if k.Epoch == nil {
			return ""
		}

		return p + strconv.FormatUint(*k.Epoch, 10)
	}
	switch path {
	case "POST /write/begin":
		if k.Wid != "" {
			return "wb:" + k.Wid
		}
	case "POST /write/end":
		return ticket("we:")
	case "POST /write/late":
		return ticket("wl:")
	case "POST /write/abandon":
		return ticket("wa:")
	case "POST /read/begin":
		if k.Rid != "" {
			return "rb:" + k.Rid
		}
	case "POST /read/end":
		return ticket("re:")
	case "POST /read/abandon":
		return ticket("ra:")
	case "POST /quiet/begin":
		return epoch("qb:")
	case "POST /quiet/section":
		if e := epoch("qs:"); e != "" {
			return e + ":" + k.Section + ":" + strconv.FormatUint(k.Seq, 10)
		}
	case "POST /quiet/end":
		return epoch("qe:")
	}

	return ""
}

// ---- handlers ----

func (s *server) handleSetup(body []byte) result {
	var req setupReq
	if err := decodeStrict(body, &req); err != nil {
		return protocolErr("cannot decode /setup", map[string]any{"error": err.Error()})
	}
	if s.setup != nil {
		if req.RunID == s.setup.RunID {
			return ok(cloneBody(s.setupResp), nil)
		}

		return refused(&oc.Refusal{HTTP: http.StatusConflict, Code: oc.CodeSetup, Message: "setup already done with a different runId",
			Details: map[string]any{"runId": s.setup.RunID, "requestedRunId": req.RunID}}, nil)
	}
	limit, err := strconv.ParseUint(req.QueryCheckpointLimit, 10, 64)
	if err != nil {
		return protocolErr("queryCheckpointLimit is not a decimal", map[string]any{"queryCheckpointLimit": req.QueryCheckpointLimit})
	}
	if env := s.opts.QueryCheckpointLimit; env != "" && env != req.QueryCheckpointLimit {
		d := map[string]any{"driver": req.QueryCheckpointLimit, "oracle": env}
		assert.Unreachable("oracle protocol: the driver and the oracle agree on the query checkpoint limit", d)

		return refused(&oc.Refusal{HTTP: http.StatusConflict, Code: oc.CodeSetup, Message: "query checkpoint limit mismatch", Details: d}, nil)
	}
	if req.RunID == "" || req.TrackedPrefix == "" {
		return protocolErr("runId and trackedPrefix are required", nil)
	}
	s.tracker = oc.NewTracker(oc.Config{RunID: req.RunID, TrackedPrefix: req.TrackedPrefix, QueryCheckpointLimit: limit})
	s.setup = &req
	s.setupResp = map[string]any{"runId": req.RunID, "trackedPrefix": req.TrackedPrefix, "version": 0}

	return ok(cloneBody(s.setupResp), nil)
}

func (s *server) handleView(body []byte) result {
	var req viewReq
	if err := decodeStrict(body, &req); err != nil {
		return protocolErr("cannot decode /view", map[string]any{"error": err.Error()})
	}
	views, err := s.tracker.ComputeViews(s.tracker.Model(), req.Views)
	if err != nil {
		return protocolErr(err.Error(), nil)
	}

	return ok(map[string]any{"version": s.tracker.Version(), "views": nonNil(views)}, nil)
}

// DecodeRequests parses servicepb.Request protojson.
func decodeRequests(raw []json.RawMessage) ([]*servicepb.Request, error) {
	out := make([]*servicepb.Request, 0, len(raw))
	for i, r := range raw {
		req := &servicepb.Request{}
		if err := protojson.Unmarshal(r, req); err != nil {
			return nil, fmt.Errorf("request %d: %w", i, err)
		}
		out = append(out, req)
	}

	return out, nil
}

func (s *server) handleWriteBegin(body []byte) result {
	var req writeBeginReq
	if err := decodeStrict(body, &req); err != nil {
		return protocolErr("cannot decode /write/begin", map[string]any{"error": err.Error()})
	}
	if req.Wid == "" {
		return protocolErr("wid is required", nil)
	}
	reqs, err := decodeRequests(req.Requests)
	if err != nil {
		return refused(&oc.Refusal{HTTP: http.StatusBadRequest, Code: oc.CodeUnmodeled, Message: err.Error(), Details: map[string]any{"wid": req.Wid}}, nil)
	}
	ticket, evs, predicted, ref := s.tracker.BeginWrite(oc.WriteRecord{
		Wid: req.Wid, Key: req.IdempotencyKey, Role: req.Role, Of: req.Of, Family: req.Family,
		TrackKey: req.TrackKey, Signed: req.Signed, Requests: reqs,
	})
	if ref != nil {
		return refused(ref, evs)
	}

	return ok(map[string]any{"ticket": ticket, "version": s.tracker.Version(), "predicted": predicted}, evs)
}

func (s *server) handleWriteEnd(body []byte) result {
	var req writeEndReq
	if err := decodeStrict(body, &req); err != nil {
		return protocolErr("cannot decode /write/end", map[string]any{"error": err.Error()})
	}
	status, evs, ref := s.tracker.EndWrite(req.Ticket, req.Outcome)
	if ref != nil {
		return refused(ref, evs)
	}
	if s.opts.DumpBatches {
		if rec, ok := s.tracker.RecordByTicket(req.Ticket); ok {
			if line := oc.BatchDumpLine(req.Ticket, rec, req.Outcome); line != "" {
				fmt.Fprintln(s.opts.Out, line)
			}
		}
	}

	return ok(map[string]any{"status": status, "verdict": verdictOf(evs, false)}, evs)
}

func (s *server) handleWriteLate(body []byte) result {
	var req writeLateReq
	if err := decodeStrict(body, &req); err != nil {
		return protocolErr("cannot decode /write/late", map[string]any{"error": err.Error()})
	}
	evs, ref := s.tracker.LateWrite(req.Ticket, req.Outcome)
	if ref != nil {
		return refused(ref, evs)
	}

	return ok(map[string]any{"verdict": verdictOf(evs, false)}, evs)
}

func (s *server) handleWriteAbandon(body []byte) result {
	var req writeAbandonReq
	if err := decodeStrict(body, &req); err != nil {
		return protocolErr("cannot decode /write/abandon", map[string]any{"error": err.Error()})
	}
	evs, ref := s.tracker.AbandonWrite(req.Ticket, req.Attempts)
	if ref != nil {
		return refused(ref, evs)
	}

	return ok(map[string]any{"verdict": verdictOf(evs, false)}, evs)
}

func (s *server) handleReadBegin(body []byte) result {
	var req readBeginReq
	if err := decodeStrict(body, &req); err != nil {
		return protocolErr("cannot decode /read/begin", map[string]any{"error": err.Error()})
	}
	if _, known := oc.LookupReadKind(req.Kind); !known {
		return protocolErr("unknown read kind", map[string]any{"kind": req.Kind})
	}
	if req.Params != nil && !isObject(req.Params) {
		return protocolErr("params must be an object", nil)
	}
	ticket, snap, version := s.tracker.BeginRead(req.Kind, req.Rid)
	views, err := s.tracker.ComputeViews(snap, req.Views)
	if err != nil {
		evs := s.tracker.FinishRead(ticket)

		return refused(oc.Protocol(err.Error(), nil), evs)
	}
	s.reads[ticket] = &readInfo{rid: req.Rid, kind: req.Kind, params: req.Params, views: views, snap: snap, version: version}

	return ok(map[string]any{"ticket": ticket, "version": version, "views": nonNil(views)}, nil)
}

func isObject(b json.RawMessage) bool {
	var m map[string]json.RawMessage

	return json.Unmarshal(b, &m) == nil
}

// mergeParams overlays end params on begin params (both objects).
func mergeParams(begin, end json.RawMessage) json.RawMessage {
	m := map[string]json.RawMessage{}
	_ = json.Unmarshal(begin, &m) // validated objects, or absent
	var e map[string]json.RawMessage
	if json.Unmarshal(end, &e) == nil {
		for k, v := range e {
			m[k] = v
		}
	}
	b, _ := json.Marshal(m)

	return b
}

func (s *server) handleReadEnd(body []byte) result {
	var req readEndReq
	if err := decodeStrict(body, &req); err != nil {
		return protocolErr("cannot decode /read/end", map[string]any{"error": err.Error()})
	}
	info, open := s.reads[req.Ticket]
	if !open || !s.tracker.ReadOpen(req.Ticket) {
		return refused(oc.UnknownTicket(req.Ticket), nil)
	}
	kind, known := oc.LookupReadKind(req.Kind)
	if !known {
		return protocolErr("unknown read kind", map[string]any{"kind": req.Kind})
	}
	if req.Kind != info.kind {
		return protocolErr("the read kind differs from /read/begin", map[string]any{"begin": info.kind, "end": req.Kind})
	}
	if req.Params != nil && !isObject(req.Params) {
		return protocolErr("params must be an object", nil)
	}

	maxTicket := s.tracker.ReadFrontier()
	var evs []oc.Event
	skipped := req.Outcome.Transient(kind.SendsWrites)
	if !skipped {
		evs = kind.Validate(&oc.ReadCtx{
			T: s.tracker, Ticket: req.Ticket, MaxTicket: maxTicket, Version: info.version, Snapshot: info.snap,
			Views: info.views, Kind: req.Kind, Request: req.Request, Outcome: req.Outcome, Route: req.Route,
			Hint: req.Hint, Params: mergeParams(info.params, req.Params),
		})
	}
	delete(s.reads, req.Ticket)
	evs = append(evs, s.tracker.FinishRead(req.Ticket)...)

	return ok(map[string]any{"verdict": verdictOf(evs, skipped), "coverage": coverageOf(evs)}, evs)
}

func (s *server) handleReadAbandon(body []byte) result {
	var req readAbandonReq
	if err := decodeStrict(body, &req); err != nil {
		return protocolErr("cannot decode /read/abandon", map[string]any{"error": err.Error()})
	}
	if !s.tracker.ReadOpen(req.Ticket) {
		return refused(oc.UnknownTicket(req.Ticket), nil)
	}
	delete(s.reads, req.Ticket)
	evs := s.tracker.FinishRead(req.Ticket)

	return ok(map[string]any{"verdict": verdictOf(evs, false)}, evs)
}

func (s *server) handleQuietBegin(body []byte) result {
	var req quietBeginReq
	if err := decodeStrict(body, &req); err != nil {
		return protocolErr("cannot decode /quiet/begin", map[string]any{"error": err.Error()})
	}
	endedOld, ref := s.tracker.BeginQuiet(req.Epoch)
	if ref != nil {
		return refused(ref, nil)
	}
	if endedOld != 0 {
		log.Printf("oracle: quiet epoch %d begun while epoch %d was open; ended it", req.Epoch, endedOld)
	}
	s.quietSections, s.quietViolations = 0, 0

	snap := s.tracker.Model()
	live, deleted := []string{}, []string{}
	expect := map[string]any{}
	for _, name := range s.tracker.LedgerNames() {
		lc, exists := snap.Lifecycle(name)
		if !exists {
			continue
		}
		if lc.Deleted {
			deleted = append(deleted, name)

			continue
		}
		live = append(live, name)
		ls := snap.Ledger(name)
		expect[name] = map[string]any{"txCount": ls.Txs().Len(), "logCount": len(ls.LogRows())}
	}
	cursor := s.tracker.AuditCursor()

	return ok(map[string]any{
		"epoch": req.Epoch, "version": s.tracker.Version(), "auditCursor": cursor,
		"ledgers":   map[string]any{"live": live, "deleted": deleted},
		"expect":    expect,
		"providers": s.tracker.Providers(),
	}, nil)
}

func (s *server) handleQuietSection(body []byte) result {
	var req quietSectionReq
	if err := decodeStrict(body, &req); err != nil {
		return protocolErr("cannot decode /quiet/section", map[string]any{"error": err.Error()})
	}
	epoch, open := s.tracker.QuietEpoch()
	if !open || epoch != req.Epoch {
		return protocolErr("no open quiet period with this epoch", map[string]any{"epoch": req.Epoch})
	}
	fn, known := oc.LookupQuietSection(req.Section)
	if !known {
		return protocolErr("unknown quiet section", map[string]any{"section": req.Section})
	}
	next, evs, ref := fn(s.tracker, req.Epoch, req.Seq, req.Body)
	if ref != nil {
		return refused(ref, evs)
	}
	s.quietSections++
	v := verdictOf(evs, false)
	s.quietViolations += len(v.Violations)
	if next == nil {
		next = map[string]any{}
	}

	return ok(map[string]any{"verdict": v, "next": next}, evs)
}

func (s *server) handleQuietEnd(body []byte) result {
	var req quietEndReq
	if err := decodeStrict(body, &req); err != nil {
		return protocolErr("cannot decode /quiet/end", map[string]any{"error": err.Error()})
	}
	s.tracker.EndQuiet(req.Epoch)

	return ok(map[string]any{"summary": map[string]any{"sections": s.quietSections, "violations": s.quietViolations}}, nil)
}

func (s *server) handleJournal(query string) result {
	q, _ := url.ParseQuery(query)
	from, _ := strconv.ParseUint(q.Get("from"), 10, 64)
	limit, _ := strconv.Atoi(q.Get("limit"))
	entries, next := s.journal.page(from, limit)

	return ok(map[string]any{"entries": entries, "next": next}, nil)
}

func (s *server) handleDump() result {
	snap := s.tracker.Model()
	ledgers := []map[string]any{}
	for _, name := range s.tracker.LedgerNames() {
		lc, exists := snap.Lifecycle(name)
		ls := snap.Ledger(name)
		ledgers = append(ledgers, map[string]any{"name": name, "live": exists && !lc.Deleted, "txCount": ls.Txs().Len(), "logCount": len(ls.LogRows())})
	}
	fp := strings.NewReplacer("{", "", "}", "", " ", "").Replace(fmt.Sprintf("%016x", snap.Fingerprint()))

	return ok(map[string]any{
		"version": s.tracker.Version(), "fingerprint": fp, "ledgers": ledgers,
		"inflight": nonNil(s.tracker.InflightTickets()), "pending": nonNil(s.tracker.PendingTickets()),
		"reads": nonNil(s.tracker.OpenReads()),
	}, nil)
}
