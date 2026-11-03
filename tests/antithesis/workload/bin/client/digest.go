package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// POST /digest (DESIGN §7 C6): deterministic digests of the server's listings,
// so the driver can compare nodes (task 15) or a restored cluster with its
// source (task 18) without paging or canonicalising in Rust.
//
//	<- {"opId", "route", "node", "consistency", "sections": [...], "ledgers": [...],
//	    "auditFrom": "N", "items": bool, "timeoutMs": n, "pageSize": n}
//	-> {"boot", "opId", "ok", "sections": {name: {"count", "sha256", "lastKey",
//	    "items"?, "ledgers"?: {ledger: {"count", "sha256", "lastKey"}}}},
//	    "error"?, "outcome"?}
//
// Every item is hashed as an 8-byte big-endian length followed by its
// proto.MarshalOptions{Deterministic: true} bytes, in listing order
// (MD/eventually_cross_node_identity/observation.go digestMessage). Per-ledger
// sections walk the ledgers in order and hash each ledger name (same framing)
// before that ledger's items, and also report a digest per ledger. lastKey is
// the key of the last item ("<ledger>/<key>" in per-ledger sections).
//
// Ledgers: "ledgers" when given (in that order, and the ledgers section keeps
// only those rows), otherwise every ledger ListLedgers returns; deleted ledgers
// are skipped by the per-ledger sections. Every RPC goes through the requested
// route (or node) with the requested consistency; the first failing RPC ends
// the digest with ok=false, "error" and that RPC's "outcome".

type digestRequest struct {
	OpID        uint64   `json:"opId"`
	Route       string   `json:"route"`
	Node        string   `json:"node"`
	Consistency string   `json:"consistency"`
	Sections    []string `json:"sections"`
	Ledgers     []string `json:"ledgers"`
	// AuditFrom (decimal string) restricts the audit section to sequences >= it.
	AuditFrom string `json:"auditFrom"`
	Items     bool   `json:"items"`
	TimeoutMS int64  `json:"timeoutMs"`
	PageSize  uint32 `json:"pageSize"`
}

type ledgerDigest struct {
	Count   uint64 `json:"count"`
	SHA256  string `json:"sha256"`
	LastKey string `json:"lastKey"`
}

type sectionDigest struct {
	Count   uint64                  `json:"count"`
	SHA256  string                  `json:"sha256"`
	LastKey string                  `json:"lastKey"`
	Items   []json.RawMessage       `json:"items,omitempty"`
	Ledgers map[string]ledgerDigest `json:"ledgers,omitempty"`
}

type digestResponse struct {
	Boot     string                    `json:"boot"`
	OpID     uint64                    `json:"opId"`
	OK       bool                      `json:"ok"`
	Sections map[string]*sectionDigest `json:"sections"`
	Error    string                    `json:"error,omitempty"`
	Outcome  *outcome                  `json:"outcome,omitempty"`
}

// Section names, in the order they are computed when the request names none.
var digestSections = []string{
	"ledgers", "accounts", "transactions", "logs", "stats", "aggregates", "audit",
	"numscripts", "preparedQueries", "indexes", "signingKeys", "sinks", "schemas",
	"checkpoints",
}

// perLedgerSections are walked once per ledger.
var perLedgerSections = map[string]bool{
	"accounts": true, "transactions": true, "logs": true, "stats": true,
	"aggregates": true, "numscripts": true, "preparedQueries": true, "schemas": true,
}

const defaultDigestPageSize = 100

func (s *server) handleDigest(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var req digestRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := validateDigest(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	timeout := time.Duration(req.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	resp := digestResponse{Boot: boot, OpID: req.OpID, Sections: map[string]*sectionDigest{}}
	conn, err := s.conns.pick(req.Route, req.Node)
	if err != nil {
		out := localOutcome(codes.Internal, "dial: "+err.Error())
		resp.Error, resp.Outcome = out.Message, &out
		writeJSON(w, resp)
		return
	}
	resp.Sections, err = runDigest(ctx, conn, req)
	if err != nil {
		resp.Sections = map[string]*sectionDigest{}
		resp.Error = err.Error()
		var re *rpcError
		if errors.As(err, &re) {
			s.abortIfOwnClose(re.out)
			resp.Outcome = &re.out
		}
	} else {
		resp.OK = true
	}
	writeJSON(w, resp)
}

func validateDigest(req *digestRequest) error {
	if !validRoute(req.Route) {
		return fmt.Errorf("unknown route %q", req.Route)
	}
	if len(req.Sections) == 0 {
		req.Sections = digestSections
	}
	known := map[string]bool{}
	for _, name := range digestSections {
		known[name] = true
	}
	for _, name := range req.Sections {
		if !known[name] {
			return fmt.Errorf("unknown section %q", name)
		}
	}
	if req.AuditFrom != "" {
		if _, err := strconv.ParseUint(req.AuditFrom, 10, 64); err != nil {
			return fmt.Errorf("auditFrom: %w", err)
		}
	}
	if req.PageSize == 0 {
		req.PageSize = defaultDigestPageSize
	}

	return nil
}

// rpcError is a failed RPC of a digest, with its outcome.
type rpcError struct {
	section, ledger string
	out             outcome
}

func (e *rpcError) Error() string {
	where := e.section
	if e.ledger != "" {
		where += " " + e.ledger
	}

	return fmt.Sprintf("%s: %s %s: %s", where, e.out.Code, e.out.Reason, e.out.Message)
}

// digester holds the state of one digest run.
type digester struct {
	bucket  servicepb.BucketServiceClient
	cluster clusterpb.ClusterServiceClient
	node    string
	req     digestRequest

	section, ledger string
}

// rpc asserts the classification of one RPC result and wraps a failure.
func (d *digester) rpc(fullMethod string, err error) error {
	out := outcome{OK: true}
	if err != nil {
		out = errOutcome(err)
	}
	assertClassified(fullMethod, d.node, out)
	if err != nil {
		return &rpcError{section: d.section, ledger: d.ledger, out: out}
	}

	return nil
}

func runDigest(ctx context.Context, conn grpc.ClientConnInterface, req digestRequest) (map[string]*sectionDigest, error) {
	if req.Consistency != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-consistency", req.Consistency)
	}
	d := &digester{
		bucket:  servicepb.NewBucketServiceClient(conn),
		cluster: clusterpb.NewClusterServiceClient(conn),
		node:    req.Node,
		req:     req,
	}
	needLedgers := false
	for _, name := range req.Sections {
		if name == "ledgers" || (perLedgerSections[name] && len(req.Ledgers) == 0) {
			needLedgers = true
		}
	}
	var infos []*commonpb.LedgerInfo
	if needLedgers {
		d.section = "ledgers"
		var err error
		if infos, err = d.listLedgers(ctx); err != nil {
			return nil, err
		}
	}
	ledgers := req.Ledgers
	if len(ledgers) == 0 {
		for _, info := range infos {
			if info.GetDeletedAt() == nil {
				ledgers = append(ledgers, info.GetName())
			}
		}
	}

	out := map[string]*sectionDigest{}
	for _, name := range req.Sections {
		if _, done := out[name]; done {
			continue
		}
		d.section, d.ledger = name, ""
		acc := newAccumulator(req.Items, perLedgerSections[name])
		var err error
		if perLedgerSections[name] {
			for _, l := range ledgers {
				d.ledger = l
				acc.beginLedger(l)
				if err = d.ledgerSection(ctx, name, l, acc); err != nil {
					break
				}
			}
		} else {
			err = d.globalSection(ctx, name, infos, acc)
		}
		if err != nil {
			return nil, err
		}
		out[name] = acc.result()
	}

	return out, nil
}

func (d *digester) listLedgers(ctx context.Context) ([]*commonpb.LedgerInfo, error) {
	var infos []*commonpb.LedgerInfo
	err := d.pages(ctx, servicepb.BucketService_ListLedgers_FullMethodName, func(ctx context.Context, cursor string) (recvStream, error) {
		return d.bucket.ListLedgers(ctx, &servicepb.ListLedgersRequest{Options: d.options(cursor)})
	}, func() proto.Message { return &commonpb.LedgerInfo{} }, func(m proto.Message) error {
		infos = append(infos, m.(*commonpb.LedgerInfo))
		return nil
	})

	return infos, err
}

func (d *digester) options(cursor string) *commonpb.ListOptions {
	return &commonpb.ListOptions{PageSize: d.req.PageSize, Cursor: cursor}
}

func (d *digester) globalSection(ctx context.Context, name string, infos []*commonpb.LedgerInfo, acc *accumulator) error {
	switch name {
	case "ledgers":
		keep := map[string]bool{}
		for _, l := range d.req.Ledgers {
			keep[l] = true
		}
		for _, info := range infos {
			if len(keep) == 0 || keep[info.GetName()] {
				if err := acc.add(info.GetName(), info); err != nil {
					return err
				}
			}
		}

		return nil
	case "audit":
		opts := func(cursor string) *commonpb.ListOptions {
			o := d.options(cursor)
			if d.req.AuditFrom != "" {
				from, _ := strconv.ParseUint(d.req.AuditFrom, 10, 64) // validated
				o.Filter = &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{Audit: &commonpb.AuditCondition{
					Field:     commonpb.AuditField_AUDIT_FIELD_SEQUENCE,
					Condition: &commonpb.AuditCondition_UintCond{UintCond: &commonpb.UintCondition{Min: &from}},
				}}}
			}

			return o
		}

		return d.pages(ctx, servicepb.BucketService_ListAuditEntries_FullMethodName, func(ctx context.Context, cursor string) (recvStream, error) {
			return d.bucket.ListAuditEntries(ctx, &servicepb.ListAuditEntriesRequest{Options: opts(cursor)})
		}, func() proto.Message { return &auditpb.AuditEntry{} }, func(m proto.Message) error {
			e := m.(*auditpb.AuditEntry)
			// Items are stripped, as ListAuditEntries' contract allows.
			e.Items = nil

			return acc.add(strconv.FormatUint(e.GetSequence(), 10), e)
		})
	case "indexes":
		keep := map[string]bool{}
		for _, l := range d.req.Ledgers {
			keep[l] = true
		}

		return d.stream(ctx, servicepb.BucketService_ListIndexes_FullMethodName, func(ctx context.Context) (recvStream, error) {
			return d.bucket.ListIndexes(ctx, &servicepb.ListIndexesRequest{Scope: servicepb.ListIndexesRequest_SCOPE_ALL})
		}, func() proto.Message { return &commonpb.Index{} }, func(m proto.Message) error {
			idx := m.(*commonpb.Index)
			if len(keep) > 0 && idx.GetLedger() != "" && !keep[idx.GetLedger()] {
				return nil
			}

			return acc.add(idx.GetLedger()+"/"+compactJSON(idx.GetId()), idx)
		})
	case "signingKeys":
		return d.pages(ctx, servicepb.BucketService_ListSigningKeys_FullMethodName, func(ctx context.Context, cursor string) (recvStream, error) {
			return d.bucket.ListSigningKeys(ctx, &servicepb.ListSigningKeysRequest{Options: d.options(cursor)})
		}, func() proto.Message { return &commonpb.SigningKey{} }, func(m proto.Message) error {
			return acc.add(m.(*commonpb.SigningKey).GetKeyId(), m)
		})
	case "sinks":
		resp, err := d.bucket.GetEventsSinks(ctx, &servicepb.GetEventsSinksRequest{})
		if err := d.rpc(servicepb.BucketService_GetEventsSinks_FullMethodName, err); err != nil {
			return err
		}
		// Configurations only: delivery statuses move independently.
		for _, sink := range resp.GetSinks() {
			if err := acc.add(sink.GetName(), sink); err != nil {
				return err
			}
		}

		return nil
	case "checkpoints":
		resp, err := d.cluster.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
		if err := d.rpc(clusterpb.ClusterService_ListQueryCheckpoints_FullMethodName, err); err != nil {
			return err
		}
		for _, c := range resp.GetCheckpoints() {
			if err := acc.add(strconv.FormatUint(c.GetCheckpointId(), 10), c); err != nil {
				return err
			}
		}

		return nil
	}

	return fmt.Errorf("section %q is not global", name)
}

func (d *digester) ledgerSection(ctx context.Context, name, ledger string, acc *accumulator) error {
	switch name {
	case "accounts":
		return d.pages(ctx, servicepb.BucketService_ListAccounts_FullMethodName, func(ctx context.Context, cursor string) (recvStream, error) {
			return d.bucket.ListAccounts(ctx, &servicepb.ListAccountsRequest{Ledger: ledger, Options: d.options(cursor)})
		}, func() proto.Message { return &commonpb.Account{} }, func(m proto.Message) error {
			return acc.add(m.(*commonpb.Account).GetAddress(), m)
		})
	case "transactions":
		return d.pages(ctx, servicepb.BucketService_ListTransactions_FullMethodName, func(ctx context.Context, cursor string) (recvStream, error) {
			return d.bucket.ListTransactions(ctx, &servicepb.ListTransactionsRequest{Ledger: ledger, Options: d.options(cursor)})
		}, func() proto.Message { return &commonpb.Transaction{} }, func(m proto.Message) error {
			return acc.add(strconv.FormatUint(m.(*commonpb.Transaction).GetId(), 10), m)
		})
	case "logs":
		return d.pages(ctx, servicepb.BucketService_ListLogs_FullMethodName, func(ctx context.Context, cursor string) (recvStream, error) {
			return d.bucket.ListLogs(ctx, &servicepb.ListLogsRequest{Ledger: ledger, Options: d.options(cursor)})
		}, func() proto.Message { return &commonpb.Log{} }, func(m proto.Message) error {
			return acc.add(strconv.FormatUint(m.(*commonpb.Log).GetSequence(), 10), m)
		})
	case "numscripts":
		return d.pages(ctx, servicepb.BucketService_ListNumscripts_FullMethodName, func(ctx context.Context, cursor string) (recvStream, error) {
			return d.bucket.ListNumscripts(ctx, &servicepb.ListNumscriptsRequest{Ledger: ledger, Options: d.options(cursor)})
		}, func() proto.Message { return &commonpb.NumscriptInfo{} }, func(m proto.Message) error {
			n := m.(*commonpb.NumscriptInfo)
			return acc.add(n.GetName()+"@"+n.GetVersion(), n)
		})
	case "stats":
		resp, err := d.bucket.GetLedgerStats(ctx, &servicepb.GetLedgerStatsRequest{Ledger: ledger})
		if err := d.rpc(servicepb.BucketService_GetLedgerStats_FullMethodName, err); err != nil {
			return err
		}

		return acc.add(ledger, statsDigestView(resp))
	case "aggregates":
		resp, err := d.bucket.AggregateVolumes(ctx, &servicepb.AggregateVolumesRequest{Ledger: ledger})
		if err := d.rpc(servicepb.BucketService_AggregateVolumes_FullMethodName, err); err != nil {
			return err
		}

		return acc.add(ledger, resp)
	case "preparedQueries":
		resp, err := d.bucket.ListPreparedQueries(ctx, &servicepb.ListPreparedQueriesRequest{Ledger: ledger})
		if err := d.rpc(servicepb.BucketService_ListPreparedQueries_FullMethodName, err); err != nil {
			return err
		}
		for _, q := range resp.GetQueries() {
			if err := acc.add(q.GetName(), q); err != nil {
				return err
			}
		}

		return nil
	case "schemas":
		resp, err := d.bucket.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{Ledger: ledger})
		if err := d.rpc(servicepb.BucketService_GetMetadataSchemaStatus_FullMethodName, err); err != nil {
			return err
		}

		return acc.add(ledger, resp)
	}

	return fmt.Errorf("section %q is not per ledger", name)
}

// statsDigestView keeps the stats fields that are synchronous with the
// committed state. The usage counters (volumes, references, postings, …) are
// maintained asynchronously on both sides of a comparison.
func statsDigestView(s *commonpb.LedgerStats) *commonpb.LedgerStats {
	return &commonpb.LedgerStats{TransactionCount: s.GetTransactionCount(), LogCount: s.GetLogCount()}
}

// recvStream is the part of a server stream the digest uses.
type recvStream interface {
	RecvMsg(m any) error
	Trailer() metadata.MD
}

// stream reads one server stream to EOF.
func (d *digester) stream(ctx context.Context, fullMethod string, open func(context.Context) (recvStream, error), newItem func() proto.Message, consume func(proto.Message) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	st, err := open(ctx)
	if err != nil {
		return d.rpc(fullMethod, err)
	}
	for {
		item := newItem()
		err := st.RecvMsg(item)
		if errors.Is(err, io.EOF) {
			return d.rpc(fullMethod, nil)
		}
		if err != nil {
			return d.rpc(fullMethod, err)
		}
		if err := consume(item); err != nil {
			return err
		}
	}
}

// pages reads every page of a listing, following x-next-cursor until the
// server omits it. A repeated cursor fails the digest.
func (d *digester) pages(ctx context.Context, fullMethod string, open func(context.Context, string) (recvStream, error), newItem func() proto.Message, consume func(proto.Message) error) error {
	seen := map[string]bool{"": true}
	cursor := ""
	for {
		var trailer metadata.MD
		err := d.stream(ctx, fullMethod, func(ctx context.Context) (recvStream, error) {
			st, err := open(ctx, cursor)
			if err != nil {
				return nil, err
			}

			return trailerCapture{recvStream: st, trailer: &trailer}, nil
		}, newItem, consume)
		if err != nil {
			return err
		}
		next := trailer.Get("x-next-cursor")
		if len(next) == 0 {
			return nil
		}
		if len(next) != 1 || next[0] == "" || seen[next[0]] {
			return fmt.Errorf("%s %s: invalid or repeated cursor %q", d.section, d.ledger, next)
		}
		cursor = next[0]
		seen[cursor] = true
	}
}

// trailerCapture records the trailer once the stream ended.
type trailerCapture struct {
	recvStream
	trailer *metadata.MD
}

func (t trailerCapture) RecvMsg(m any) error {
	err := t.recvStream.RecvMsg(m)
	if errors.Is(err, io.EOF) {
		*t.trailer = t.recvStream.Trailer()
	}

	return err
}

// accumulator folds one section.
type accumulator struct {
	items     bool
	perLedger bool

	h       hash.Hash
	count   uint64
	lastKey string
	list    []json.RawMessage

	ledger  string
	lh      hash.Hash
	ld      ledgerDigest
	ledgers map[string]ledgerDigest
}

func newAccumulator(items, perLedger bool) *accumulator {
	return &accumulator{items: items, perLedger: perLedger, h: sha256.New(), ledgers: map[string]ledgerDigest{}}
}

func (a *accumulator) beginLedger(name string) {
	a.endLedger()
	a.ledger, a.lh, a.ld = name, sha256.New(), ledgerDigest{}
	writeFramed(a.h, []byte(name))
}

func (a *accumulator) endLedger() {
	if a.lh == nil {
		return
	}
	a.ld.SHA256 = hex.EncodeToString(a.lh.Sum(nil))
	a.ledgers[a.ledger] = a.ld
	a.lh = nil
}

func (a *accumulator) add(key string, m proto.Message) error {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", key, err)
	}
	writeFramed(a.h, data)
	a.count++
	a.lastKey = key
	if a.perLedger {
		a.lastKey = a.ledger + "/" + key
		writeFramed(a.lh, data)
		a.ld.Count++
		a.ld.LastKey = key
	}
	if a.items {
		b, err := marshal.Marshal(m)
		if err != nil {
			return fmt.Errorf("marshal %s: %w", key, err)
		}
		a.list = append(a.list, b)
	}

	return nil
}

func (a *accumulator) result() *sectionDigest {
	a.endLedger()
	out := &sectionDigest{Count: a.count, SHA256: hex.EncodeToString(a.h.Sum(nil)), LastKey: a.lastKey, Items: a.list}
	if a.perLedger {
		out.Ledgers = a.ledgers
	}

	return out
}

// writeFramed writes an 8-byte big-endian length and the data.
func writeFramed(h hash.Hash, data []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(data)))
	_, _ = h.Write(length[:]) // hash.Hash.Write never fails
	_, _ = h.Write(data)
}

// compactJSON renders a key message as compact protojson (protojson adds
// random whitespace; compacting makes it stable).
func compactJSON(m proto.Message) string {
	b, err := marshal.Marshal(m)
	if err != nil {
		return "?"
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, b); err != nil {
		return string(b)
	}

	return buf.String()
}
