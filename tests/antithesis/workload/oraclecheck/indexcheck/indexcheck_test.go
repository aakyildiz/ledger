package indexcheck

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// ---- builders

const acct = commonpb.TargetType_TARGET_TYPE_ACCOUNT

func createIndex(ledger string, id *commonpb.IndexID) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_CreateIndex{CreateIndex: &servicepb.CreateIndexRequest{Ledger: ledger, Id: id}}}
}

func dropIndex(ledger string, id *commonpb.IndexID) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_DropIndex{DropIndex: &servicepb.DropIndexRequest{Ledger: ledger, Id: id}}}
}

func setType(ledger, key string, t commonpb.MetadataType) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_SetMetadataFieldType{SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
		Ledger: ledger, TargetType: acct, Key: key, Type: t}}}
}

func pj(m proto.Message) json.RawMessage {
	b, err := protojson.Marshal(m)
	if err != nil {
		panic(err)
	}

	return b
}

func has(evs []oc.Event, id oc.EventID) bool {
	return slices.ContainsFunc(evs, func(e oc.Event) bool { return e.ID == id })
}

type fixture struct {
	t *testing.T
	s *testutil.ScriptedSession
	L string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	s := testutil.NewSession("")
	f := &fixture{t: t, s: s, L: s.Ledger(1)}
	f.write(testutil.CreateLedgerReq(f.L))

	return f
}

func (f *fixture) write(reqs ...*servicepb.Request) []oc.Event {
	f.t.Helper()
	evs := f.s.Write(false, reqs...)
	require.Empty(f.t, testutil.Findings(evs), "%v", testutil.IDs(evs))

	return evs
}

func (f *fixture) ctx(kind string, ticket uint64, req proto.Message, out oc.Outcome, params any) *oc.ReadCtx {
	var p json.RawMessage
	if params != nil {
		p, _ = json.Marshal(params)
	}

	return &oc.ReadCtx{T: f.s.T, Ticket: ticket, MaxTicket: f.s.T.ReadFrontier(), Snapshot: f.s.T.Model(),
		Kind: kind, Request: pj(req), Outcome: out, Params: p}
}

// read runs one validated read the way the service does: validate, then
// FinishRead (drain + settle hooks).
func (f *fixture) read(kind string, req proto.Message, out oc.Outcome, params any, validate func(*oc.ReadCtx) []oc.Event) []oc.Event {
	tk := f.s.Read(kind)
	evs := validate(f.ctx(kind, tk, req, out, params))

	return append(evs, f.s.T.FinishRead(tk)...)
}

// nodeStat renders one node's status response.
func nodeStat(lastIndexed uint64, entries map[*commonpb.IndexID][2]uint32) oc.Outcome {
	resp := &servicepb.GetIndexStatusResponse{LastIndexedSequence: lastIndexed, LastLogSequence: lastIndexed}
	ids := make([]*commonpb.IndexID, 0, len(entries))
	for id := range entries {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b *commonpb.IndexID) int {
		if indexes.Canonical(a) < indexes.Canonical(b) {
			return -1
		}

		return 1
	})
	for _, id := range ids {
		v := entries[id]
		resp.Indexes = append(resp.Indexes, &servicepb.IndexEntry{Index: &commonpb.Index{Id: id}, CurrentVersion: v[0], PendingVersion: v[1]})
	}

	return oc.Outcome{OK: true, Response: pj(resp)}
}

var allNodes = []string{"ledger-0:8888", "ledger-1:8888", "ledger-2:8888"}

func (f *fixture) poll(nodes []string, per map[string]oc.Outcome) []oc.Event {
	payload := PollPayload{}
	for _, n := range nodes {
		payload.PerNode = append(payload.PerNode, NodeOutcome{Node: n, Outcome: per[n]})
	}
	b, _ := json.Marshal(payload)

	return f.read(KindIndexStatusPoll, &servicepb.GetIndexStatusRequest{Ledger: f.L}, oc.Outcome{OK: true, Response: b},
		PollParams{Nodes: nodes, AllNodes: allNodes}, ValidateIndexStatusPoll)
}

func everyNode(o oc.Outcome) map[string]oc.Outcome {
	m := map[string]oc.Outcome{}
	for _, n := range allNodes {
		m[n] = o
	}

	return m
}

func (f *fixture) state(canon string) (exists, active bool) {
	return f.s.T.Model().Ledger(f.L).IndexState(canon)
}

// ---- readiness

func TestPollNotReadyDemotes(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.write(createIndex(f.L, match.AssetIndexID()))
	seq, ok := StateOf(f.s.T).CreateSeq(f.L, match.AssetIndexCanonical)
	require.True(t, ok)

	evs := f.poll(allNodes, everyNode(nodeStat(seq, map[*commonpb.IndexID][2]uint32{match.AssetIndexID(): {0, 0}})))
	require.True(t, has(evs, EvDemoted))
	require.False(t, has(evs, EvPromoted))
	exists, active := f.state(match.AssetIndexCanonical)
	require.True(t, exists)
	require.False(t, active)
	require.False(t, StateOf(f.s.T).PendingPromotion(f.L, match.AssetIndexCanonical))
}

func TestPollAllNodesReadyDeferredThenPromoted(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.write(createIndex(f.L, match.AssetIndexID()))
	seq, _ := StateOf(f.s.T).CreateSeq(f.L, match.AssetIndexCanonical)
	fpBefore := f.s.T.Model().BusinessFingerprint()

	earlier := f.s.Read("GetAccount") // a read ticketed before the confirmation
	ready := nodeStat(seq, map[*commonpb.IndexID][2]uint32{match.AssetIndexID(): {1, 0}})
	evs := f.poll(allNodes, everyNode(ready))
	require.Empty(t, testutil.Findings(evs))
	require.True(t, has(evs, EvPromotionDeferred), "%v", testutil.IDs(evs))
	_, active := f.state(match.AssetIndexCanonical)
	require.False(t, active, "deferred behind the earlier read")
	require.True(t, StateOf(f.s.T).PendingPromotion(f.L, match.AssetIndexCanonical))

	evs = f.s.T.FinishRead(earlier)
	require.True(t, has(evs, EvPromoted), "%v", testutil.IDs(evs))
	_, active = f.state(match.AssetIndexCanonical)
	require.True(t, active)
	require.Equal(t, fpBefore, f.s.T.Model().BusinessFingerprint(), "MutateModel never changes the business fingerprint")

	// A later not-ready report demotes it again.
	evs = f.poll(allNodes[:1], map[string]oc.Outcome{allNodes[0]: {Code: "Unavailable", Unknown: "timeout"}})
	require.True(t, has(evs, EvDemoted))
	_, active = f.state(match.AssetIndexCanonical)
	require.False(t, active)
	require.Equal(t, fpBefore, f.s.T.Model().BusinessFingerprint())
}

func TestPollSingleNodePollsNeverPromote(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.write(createIndex(f.L, match.AssetIndexID()))
	seq, _ := StateOf(f.s.T).CreateSeq(f.L, match.AssetIndexCanonical)
	ready := nodeStat(seq, map[*commonpb.IndexID][2]uint32{match.AssetIndexID(): {1, 0}})
	for _, n := range allNodes {
		evs := f.poll([]string{n}, map[string]oc.Outcome{n: ready})
		require.False(t, has(evs, EvPromoted))
		require.False(t, has(evs, EvDemoted))
	}
	_, active := f.state(match.AssetIndexCanonical)
	require.False(t, active, "A, then B, then C ready in three polls is no all-replica proof")
	require.False(t, StateOf(f.s.T).PendingPromotion(f.L, match.AssetIndexCanonical))

	// A poll naming every node but answered by fewer is not full either.
	evs := f.read(KindIndexStatusPoll, &servicepb.GetIndexStatusRequest{Ledger: f.L},
		oc.Outcome{OK: true, Response: mustJSON(PollPayload{PerNode: []NodeOutcome{{Node: allNodes[0], Outcome: ready}}})},
		PollParams{Nodes: allNodes, AllNodes: allNodes}, ValidateIndexStatusPoll)
	require.False(t, has(evs, EvPromoted))
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}

	return b
}

func TestPollRecreateMovesFrontier(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.write(createIndex(f.L, match.AssetIndexID()))
	oldSeq, _ := StateOf(f.s.T).CreateSeq(f.L, match.AssetIndexCanonical)
	f.write(dropIndex(f.L, match.AssetIndexID()))
	f.write(createIndex(f.L, match.AssetIndexID()))
	newSeq, _ := StateOf(f.s.T).CreateSeq(f.L, match.AssetIndexCanonical)
	require.Greater(t, newSeq, oldSeq)

	// Every node reports the OLD incarnation ready: stale, not this one.
	stale := nodeStat(oldSeq, map[*commonpb.IndexID][2]uint32{match.AssetIndexID(): {3, 0}})
	evs := f.poll(allNodes, everyNode(stale))
	require.False(t, has(evs, EvPromoted))
	require.True(t, has(evs, EvDemoted))
	_, active := f.state(match.AssetIndexCanonical)
	require.False(t, active)

	// A pending promotion of the old incarnation is discarded by the settle
	// hook once the frontier moved.
	g := newFixture(t)
	g.write(createIndex(g.L, match.AssetIndexID()))
	s1, _ := StateOf(g.s.T).CreateSeq(g.L, match.AssetIndexCanonical)
	earlier := g.s.Read("GetAccount")
	g.poll(allNodes, everyNode(nodeStat(s1, map[*commonpb.IndexID][2]uint32{match.AssetIndexID(): {1, 0}})))
	require.True(t, StateOf(g.s.T).PendingPromotion(g.L, match.AssetIndexCanonical))
	g.s.T.FinishRead(earlier)
	_, active = g.state(match.AssetIndexCanonical)
	require.True(t, active)

	h := newFixture(t)
	h.write(createIndex(h.L, match.AssetIndexID()))
	s1, _ = StateOf(h.s.T).CreateSeq(h.L, match.AssetIndexCanonical)
	earlier = h.s.Read("GetAccount")
	h.poll(allNodes, everyNode(nodeStat(s1, map[*commonpb.IndexID][2]uint32{match.AssetIndexID(): {1, 0}})))
	tk := h.s.Begin(false, dropIndex(h.L, match.AssetIndexID()))
	h.s.End(tk)
	tk = h.s.Begin(false, createIndex(h.L, match.AssetIndexID()))
	h.s.End(tk)
	evs = h.s.T.FinishRead(earlier)
	require.False(t, has(evs, EvPromoted), "the recreated incarnation is not promoted by the old proof")
	_, active = h.state(match.AssetIndexCanonical)
	require.False(t, active)
}

func TestPollSelfConsistency(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	bad := &servicepb.GetIndexStatusResponse{LastIndexedSequence: 3, LastLogSequence: 9, Lag: 2, IndexFileSize: 10}
	evs := f.poll(allNodes[:1], map[string]oc.Outcome{allNodes[0]: {OK: true, Response: pj(bad)}})
	require.True(t, has(evs, EvStatusNotSelfConsistent))
	require.True(t, has(evs, EvStatusFile))
	good := &servicepb.GetIndexStatusResponse{LastIndexedSequence: 9, LastLogSequence: 3, Lag: 0}
	evs = f.poll(allNodes[:1], map[string]oc.Outcome{allNodes[0]: {OK: true, Response: pj(good)}})
	require.Empty(t, testutil.Findings(evs))
}

// ---- retype closure

func retypeFixture(t *testing.T) (*fixture, *commonpb.IndexID, string) {
	f := newFixture(t)
	id := indexes.MetadataID(acct, "k0")
	f.write(setType(f.L, "k0", commonpb.MetadataType_METADATA_TYPE_STRING))
	f.write(createIndex(f.L, id))
	f.write(testutil.AccountMetaReq(f.L, "users:1", "k0", "5"))
	f.write(setType(f.L, "k0", commonpb.MetadataType_METADATA_TYPE_INT64))
	canon := indexes.Canonical(id)
	_, open := f.s.T.Model().Ledger(f.L).RetypeWindow(canon)
	require.True(t, open)
	require.True(t, StateOf(f.s.T).RetypeOpen(f.L, canon))

	return f, id, canon
}

func TestRetypeClosureIsTwoPhasePerNode(t *testing.T) {
	t.Parallel()
	f, id, canon := retypeFixture(t)
	head := f.s.NextSeq - 1
	clear := nodeStat(head, map[*commonpb.IndexID][2]uint32{id: {2, 0}})

	// Phase one only: the same poll that proves the fold does not confirm.
	evs := f.poll(allNodes, everyNode(clear))
	require.False(t, has(evs, EvRetypeClosed))
	// Node by node, single-node polls confirm each node.
	for i, n := range allNodes {
		evs = f.poll([]string{n}, map[string]oc.Outcome{n: clear})
		require.Equal(t, i == len(allNodes)-1, has(evs, EvRetypeClosed), "node %d", i)
	}
	_, open := f.s.T.Model().Ledger(f.L).RetypeWindow(canon)
	require.False(t, open)
	require.False(t, StateOf(f.s.T).RetypeOpen(f.L, canon))
}

func TestRetypeClosureUnlistedOrPendingDoesNotConfirm(t *testing.T) {
	t.Parallel()
	f, id, canon := retypeFixture(t)
	head := f.s.NextSeq - 1
	f.poll(allNodes, everyNode(nodeStat(head, map[*commonpb.IndexID][2]uint32{id: {2, 3}})))
	evs := f.poll(allNodes, everyNode(nodeStat(head, map[*commonpb.IndexID][2]uint32{id: {2, 3}})))
	require.False(t, has(evs, EvRetypeClosed), "rewrite pending")
	evs = f.poll(allNodes, everyNode(nodeStat(head, nil)))
	require.False(t, has(evs, EvRetypeClosed), "unlisted is no proof")
	_, open := f.s.T.Model().Ledger(f.L).RetypeWindow(canon)
	require.True(t, open)
}

func TestRetypeClosureReArmsOnChainedRetype(t *testing.T) {
	t.Parallel()
	f, id, canon := retypeFixture(t)
	head := f.s.NextSeq - 1
	clear := nodeStat(head, map[*commonpb.IndexID][2]uint32{id: {2, 0}})
	f.poll(allNodes, everyNode(clear)) // armed everywhere
	f.write(setType(f.L, "k0", commonpb.MetadataType_METADATA_TYPE_BOOL))
	// The old fold point no longer arms: the chained retype's log is later.
	evs := f.poll(allNodes, everyNode(clear))
	require.False(t, has(evs, EvRetypeClosed))
	evs = f.poll(allNodes, everyNode(clear))
	require.False(t, has(evs, EvRetypeClosed), "still below the new openSeq")
	newHead := f.s.NextSeq - 1
	newClear := nodeStat(newHead, map[*commonpb.IndexID][2]uint32{id: {3, 0}})
	f.poll(allNodes, everyNode(newClear))
	evs = f.poll(allNodes, everyNode(newClear))
	require.True(t, has(evs, EvRetypeClosed))
	_, open := f.s.T.Model().Ledger(f.L).RetypeWindow(canon)
	require.False(t, open)
}

func TestRetypeClosureWaitsForEarlierReads(t *testing.T) {
	t.Parallel()
	f, id, canon := retypeFixture(t)
	head := f.s.NextSeq - 1
	clear := nodeStat(head, map[*commonpb.IndexID][2]uint32{id: {2, 0}})
	f.poll(allNodes, everyNode(clear))
	earlier := f.s.Read("ListAccounts")
	evs := f.poll(allNodes, everyNode(clear))
	require.False(t, has(evs, EvRetypeClosed))
	evs = f.s.T.FinishRead(earlier)
	require.True(t, has(evs, EvRetypeClosed))
	_, open := f.s.T.Model().Ledger(f.L).RetypeWindow(canon)
	require.False(t, open)
}

// pollAt posts a poll on an already begun ticket (a poll that overlaps
// others), then finishes it.
func (f *fixture) pollAt(tk uint64, nodes []string, per map[string]oc.Outcome) []oc.Event {
	payload := PollPayload{}
	for _, n := range nodes {
		payload.PerNode = append(payload.PerNode, NodeOutcome{Node: n, Outcome: per[n]})
	}
	b, _ := json.Marshal(payload)
	evs := ValidateIndexStatusPoll(f.ctx(KindIndexStatusPoll, tk, &servicepb.GetIndexStatusRequest{Ledger: f.L},
		oc.Outcome{OK: true, Response: b}, PollParams{Nodes: nodes, AllNodes: allNodes}))

	return append(evs, f.s.T.FinishRead(tk)...)
}

func TestRetypeClosureIgnoresPollsBegunBeforeArming(t *testing.T) {
	t.Parallel()
	f, id, canon := retypeFixture(t)
	head := f.s.NextSeq - 1
	clear := nodeStat(head, map[*commonpb.IndexID][2]uint32{id: {2, 0}})
	// B begins first (it may sample a node before the fold), then A arms
	// every node and ends, then B ends with clear-looking samples.
	tkB := f.s.Read(KindIndexStatusPoll)
	evs := f.poll(allNodes, everyNode(clear))
	require.False(t, has(evs, EvRetypeClosed))
	evs = f.pollAt(tkB, allNodes, everyNode(clear))
	require.False(t, has(evs, EvRetypeClosed), "a poll begun before the arming poll ended is no proof")
	_, open := f.s.T.Model().Ledger(f.L).RetypeWindow(canon)
	require.True(t, open)
	// A poll begun after the arming poll ended confirms.
	evs = f.poll(allNodes, everyNode(clear))
	require.True(t, has(evs, EvRetypeClosed))
}

func TestRetypeClosurePhaseTwoNeedsTheFold(t *testing.T) {
	t.Parallel()
	f, id, _ := retypeFixture(t)
	head := f.s.NextSeq - 1
	f.poll(allNodes, everyNode(nodeStat(head, map[*commonpb.IndexID][2]uint32{id: {2, 0}})))
	// A later sample below the fold (a node that restarted from an older
	// state) does not confirm, however its pending reads.
	evs := f.poll(allNodes, everyNode(nodeStat(head-1, map[*commonpb.IndexID][2]uint32{id: {2, 0}})))
	require.False(t, has(evs, EvRetypeClosed))
	evs = f.poll(allNodes, everyNode(nodeStat(head, map[*commonpb.IndexID][2]uint32{id: {2, 0}})))
	require.True(t, has(evs, EvRetypeClosed))
}

func TestRetypeClosureSameTypeRedeclareDoesNotReArm(t *testing.T) {
	t.Parallel()
	f, id, canon := retypeFixture(t)
	head := f.s.NextSeq - 1
	clear := nodeStat(head, map[*commonpb.IndexID][2]uint32{id: {2, 0}})
	f.poll(allNodes, everyNode(clear)) // armed everywhere
	f.write(setType(f.L, "k0", commonpb.MetadataType_METADATA_TYPE_INT64))
	evs := f.poll(allNodes, everyNode(clear))
	require.True(t, has(evs, EvRetypeClosed), "a same-type re-declaration keeps the observation")
	_, open := f.s.T.Model().Ledger(f.L).RetypeWindow(canon)
	require.False(t, open)
}

func TestRetypeClosureReArmsOnRoundTripInOneBulk(t *testing.T) {
	t.Parallel()
	f, id, _ := retypeFixture(t)
	head := f.s.NextSeq - 1
	clear := nodeStat(head, map[*commonpb.IndexID][2]uint32{id: {2, 0}})
	f.poll(allNodes, everyNode(clear))
	// INT64 → BOOL → INT64 in one bulk: same type before and after, yet a
	// new rewrite chain.
	f.write(setType(f.L, "k0", commonpb.MetadataType_METADATA_TYPE_BOOL), setType(f.L, "k0", commonpb.MetadataType_METADATA_TYPE_INT64))
	evs := f.poll(allNodes, everyNode(clear))
	require.False(t, has(evs, EvRetypeClosed))
	evs = f.poll(allNodes, everyNode(clear))
	require.False(t, has(evs, EvRetypeClosed), "re-armed at the round trip's log")
}

func TestRetypedKeys(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.write(setType(f.L, "k0", commonpb.MetadataType_METADATA_TYPE_STRING))
	before := f.s.T.Model()
	rm := &servicepb.Request{Type: &servicepb.Request_RemoveMetadataFieldType{RemoveMetadataFieldType: &servicepb.RemoveMetadataFieldTypeRequest{
		Ledger: f.L, TargetType: acct, Key: "k0"}}}
	k0 := retypeKey(f.L, indexes.Canonical(indexes.MetadataID(acct, "k0")))
	k1 := retypeKey(f.L, indexes.Canonical(indexes.MetadataID(acct, "k1")))
	require.Empty(t, retypedKeys([]*servicepb.Request{setType(f.L, "k0", commonpb.MetadataType_METADATA_TYPE_STRING)}, before))
	require.True(t, retypedKeys([]*servicepb.Request{setType(f.L, "k0", commonpb.MetadataType_METADATA_TYPE_INT64)}, before)[k0])
	require.True(t, retypedKeys([]*servicepb.Request{setType(f.L, "k1", commonpb.MetadataType_METADATA_TYPE_STRING)}, before)[k1], "first declaration")
	require.True(t, retypedKeys([]*servicepb.Request{rm, setType(f.L, "k0", commonpb.MetadataType_METADATA_TYPE_STRING)}, before)[k0], "remove then declare")
}

// ---- introspection

func listOutcome(entries ...*commonpb.Index) oc.Outcome {
	out := oc.Outcome{OK: true, Complete: true}
	for _, e := range entries {
		out.Items = append(out.Items, pj(e))
	}

	return out
}

func TestListIndexesLedgerScope(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.write(createIndex(f.L, match.AssetIndexID()))
	req := &servicepb.ListIndexesRequest{Scope: servicepb.ListIndexesRequest_SCOPE_LEDGER, Ledger: f.L}
	entry := &commonpb.Index{Id: match.AssetIndexID(), Ledger: f.L}

	evs := f.read(KindListIndexes, req, listOutcome(entry), nil, ValidateListIndexes)
	require.True(t, has(evs, EvListingValidated), "%v", testutil.IDs(evs))

	extra := &commonpb.Index{Id: match.LogDateIndexID(), Ledger: f.L}
	require.True(t, has(f.read(KindListIndexes, req, listOutcome(entry, extra), nil, ValidateListIndexes), EvListingOutsideModel))
	require.True(t, has(f.read(KindListIndexes, req, listOutcome(entry, entry), nil, ValidateListIndexes), EvListingRepeated))
	other := &commonpb.Index{Id: match.AssetIndexID(), Ledger: "o-test-l9"}
	require.True(t, has(f.read(KindListIndexes, req, listOutcome(other), nil, ValidateListIndexes), EvListingScope))
	require.True(t, has(f.read(KindListIndexes, req, oc.Outcome{Code: "Internal", Reason: "X"}, nil, ValidateListIndexes), EvListIndexesError))

	absent := &servicepb.ListIndexesRequest{Scope: servicepb.ListIndexesRequest_SCOPE_LEDGER, Ledger: "o-test-absent-7"}
	require.True(t, has(f.read(KindListIndexes, absent, oc.Outcome{Code: "NotFound"}, nil, ValidateListIndexes), EvListingAbsentNotFound))
	require.True(t, has(f.read(KindListIndexes, absent, listOutcome(), nil, ValidateListIndexes), EvListingOutsideFleet))
}

func TestListIndexesFleetScopes(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.write(createIndex(f.L, match.AssetIndexID()))
	all := &servicepb.ListIndexesRequest{Scope: servicepb.ListIndexesRequest_SCOPE_ALL}
	mine := &commonpb.Index{Id: match.AssetIndexID(), Ledger: f.L}
	foreign := &commonpb.Index{Id: match.AssetIndexID(), Ledger: "r-ns-x-1"}
	scenario := &commonpb.Index{Id: match.LogDateIndexID(), Ledger: "marketplace"}

	require.True(t, has(f.read(KindListIndexes, all, listOutcome(foreign, mine, scenario), nil, ValidateListIndexes), EvFleetListingValidated))
	require.True(t, has(f.read(KindListIndexes, all, listOutcome(foreign), nil, ValidateListIndexes), EvBucketListingOutsideModel))
	require.True(t, has(f.read(KindListIndexes, all, listOutcome(mine, mine), nil, ValidateListIndexes), EvBucketListingRepeated))
	require.True(t, has(f.read(KindListIndexes, all, listOutcome(&commonpb.Index{Ledger: f.L}), nil, ValidateListIndexes), EvBucketListingNoID))
	require.True(t, has(f.read(KindListIndexes, all, oc.Outcome{Code: "Internal"}, nil, ValidateListIndexes), EvBucketListIndexesError))

	bucket := &servicepb.ListIndexesRequest{Scope: servicepb.ListIndexesRequest_SCOPE_BUCKET}
	require.True(t, has(f.read(KindListIndexes, bucket, listOutcome(&commonpb.Index{Id: match.AssetIndexID()}), nil, ValidateListIndexes), EvBucketListingValid))
	require.True(t, has(f.read(KindListIndexes, bucket, listOutcome(mine), nil, ValidateListIndexes), EvBucketScopedListingLedger))
}

func TestGetIndexAndStatus(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.write(createIndex(f.L, match.AssetIndexID()))
	have := &servicepb.GetIndexRequest{Ledger: f.L, Id: match.AssetIndexID()}
	missing := &servicepb.GetIndexRequest{Ledger: f.L, Id: match.LogDateIndexID()}
	entry := &commonpb.Index{Id: match.AssetIndexID(), Ledger: f.L}

	require.True(t, has(f.read(KindGetIndex, have, oc.Outcome{OK: true, Response: pj(entry)}, nil, ValidateGetIndex), EvLookupValidated))
	require.True(t, has(f.read(KindGetIndex, missing, oc.Outcome{Code: "NotFound"}, nil, ValidateGetIndex), EvLookupValidated))
	require.True(t, has(f.read(KindGetIndex, have, oc.Outcome{Code: "NotFound"}, nil, ValidateGetIndex), EvPresenceOutsideModel))
	require.True(t, has(f.read(KindGetIndex, missing, oc.Outcome{OK: true, Response: pj(&commonpb.Index{Id: match.LogDateIndexID(), Ledger: f.L})}, nil, ValidateGetIndex), EvPresenceOutsideModel))
	require.True(t, has(f.read(KindGetIndex, missing, oc.Outcome{OK: true, Response: pj(entry)}, nil, ValidateGetIndex), EvLookupAnotherEntry))
	require.True(t, has(f.read(KindGetIndex, have, oc.Outcome{Code: "Internal"}, nil, ValidateGetIndex), EvLookupError))

	absent := &servicepb.GetIndexRequest{Ledger: "o-test-absent-1", Id: match.AssetIndexID()}
	require.True(t, has(f.read(KindGetIndex, absent, oc.Outcome{Code: "NotFound"}, nil, ValidateGetIndex), EvLookupAbsentNotFound))
	require.True(t, has(f.read(KindGetIndex, absent, oc.Outcome{OK: true, Response: pj(entry)}, nil, ValidateGetIndex), EvLookupOutsideFleet))

	bucket := &servicepb.GetIndexRequest{Id: match.AssetIndexID()}
	require.True(t, has(f.read(KindGetIndex, bucket, oc.Outcome{Code: "NotFound"}, nil, ValidateGetIndex), EvBucketLookupNotFound))
	require.True(t, has(f.read(KindGetIndex, bucket, oc.Outcome{OK: true, Response: pj(entry)}, nil, ValidateGetIndex), EvBucketLookupLedgerEntry))
	require.True(t, has(f.read(KindGetIndex, bucket, oc.Outcome{Code: "Internal"}, nil, ValidateGetIndex), EvBucketLookupError))

	st := &servicepb.GetIndexEntryStatusRequest{Ledger: f.L, Id: match.AssetIndexID()}
	ie := &servicepb.IndexEntry{Ledger: f.L, Index: entry, CurrentVersion: 1}
	require.True(t, has(f.read(KindGetIndexStatus, st, oc.Outcome{OK: true, Response: pj(ie)}, nil, ValidateGetIndexStatus), EvStatusValidated))
	wrong := &servicepb.IndexEntry{Ledger: f.L, Index: &commonpb.Index{Id: match.LogDateIndexID(), Ledger: f.L}}
	require.True(t, has(f.read(KindGetIndexStatus, st, oc.Outcome{OK: true, Response: pj(wrong)}, nil, ValidateGetIndexStatus), EvLookupAnotherEntry))
}

func TestLookupNotFoundOnConcurrentlyDeletedLedger(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.write(createIndex(f.L, match.AssetIndexID()))
	have := &servicepb.GetIndexRequest{Ledger: f.L, Id: match.AssetIndexID()}
	tk := f.s.Read(KindGetIndex)
	del := f.s.Begin(false, testutil.DeleteLedgerReq(f.L)) // in flight while the read is served
	evs := ValidateGetIndex(f.ctx(KindGetIndex, tk, have, oc.Outcome{Code: "NotFound"}, nil))
	require.Empty(t, testutil.Findings(evs), "%v", testutil.IDs(evs))
	f.s.T.FinishRead(tk)
	f.s.End(del)
}

// ---- inspection

func inspectReq(ledger, key string, mode servicepb.InspectIndexMode) *servicepb.InspectIndexRequest {
	return &servicepb.InspectIndexRequest{Ledger: ledger, TargetType: acct, MetadataKey: key, Mode: mode, PageSize: 10}
}

func summary(card, withKey, withNull uint64) oc.Outcome {
	return oc.Outcome{OK: true, Response: pj(&servicepb.InspectIndexResponse{Result: &servicepb.InspectIndexResponse_Summary{
		Summary: &servicepb.InspectSummary{Cardinality: card, EntitiesWithKey: withKey, EntitiesWithNull: withNull}}})}
}

func TestInspectSummaryCountsUnconvertible(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.write(testutil.AccountMetaReq(f.L, "users:1", "k0", "5"))
	f.write(testutil.AccountMetaReq(f.L, "users:2", "k0", "not-a-number"))
	f.write(testutil.AccountMetaReq(f.L, "users:3", "k0", "5"))
	f.write(setType(f.L, "k0", commonpb.MetadataType_METADATA_TYPE_INT64))
	f.write(createIndex(f.L, indexes.MetadataID(acct, "k0")))
	want := match.ModelInspectCounts(f.s.T.Model().Ledger(f.L), commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, "k0", commonpb.MetadataType_METADATA_TYPE_INT64)
	require.Equal(t, match.InspectCounts{Cardinality: 2, EntitiesWithKey: 2, EntitiesWithNull: 1}, want)

	req := inspectReq(f.L, "k0", servicepb.InspectIndexMode_INSPECT_INDEX_MODE_SUMMARY)
	evs := f.read(KindInspectIndex, req, summary(2, 2, 1), nil, ValidateInspectIndex)
	for _, id := range []oc.EventID{EvSummaryMatched, EvSummaryPopulated, EvSummaryUnconvertible, EvInspectValidated} {
		require.True(t, has(evs, id), "%s in %v", id, testutil.IDs(evs))
	}
	evs = f.read(KindInspectIndex, req, summary(2, 3, 0), nil, ValidateInspectIndex)
	require.True(t, has(evs, EvInspectSummaryOutsideModel))
	evs = f.read(KindInspectIndex, req, oc.Outcome{OK: true, Response: pj(&servicepb.InspectIndexResponse{})}, nil, ValidateInspectIndex)
	require.True(t, has(evs, EvSummaryOtherArm), "%v", testutil.IDs(evs))
	require.False(t, has(evs, EvInspectContract), "the catalogued arm literal fires, not the generic contract")
	evs = f.read(KindInspectIndex, req, oc.Outcome{OK: true, Response: pj(&servicepb.InspectIndexResponse{Result: &servicepb.InspectIndexResponse_Facets{
		Facets: &servicepb.InspectFacets{}}})}, nil, ValidateInspectIndex)
	require.True(t, has(evs, EvSummaryOtherArm), "%v", testutil.IDs(evs))

	// A retype window accepts any summary.
	f.write(setType(f.L, "k0", commonpb.MetadataType_METADATA_TYPE_STRING))
	evs = f.read(KindInspectIndex, req, summary(9, 9, 9), nil, ValidateInspectIndex)
	require.Empty(t, testutil.Findings(evs))
	require.True(t, has(evs, EvInspectValidated))
}

func TestInspectPagesAndErrors(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	dv := servicepb.InspectIndexMode_INSPECT_INDEX_MODE_DISTINCT_VALUES
	req := inspectReq(f.L, "k0", dv)
	page := func(n int, more bool, next string) oc.Outcome {
		vals := make([]*commonpb.MetadataValue, n)
		for i := range vals {
			vals[i] = &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: fmt.Sprint(i)}}
		}

		return oc.Outcome{OK: true, Response: pj(&servicepb.InspectIndexResponse{Result: &servicepb.InspectIndexResponse_DistinctValues{
			DistinctValues: &servicepb.InspectDistinctValues{Values: vals, HasMore: more, NextCursor: next}}})}
	}
	require.True(t, has(f.read(KindInspectIndex, req, page(3, true, "c"), nil, ValidateInspectIndex), EvInspectValidated))
	require.True(t, has(f.read(KindInspectIndex, req, page(11, false, ""), nil, ValidateInspectIndex), EvInspectContract))
	require.True(t, has(f.read(KindInspectIndex, req, page(1, true, ""), nil, ValidateInspectIndex), EvInspectContract))
	require.True(t, has(f.read(KindInspectIndex, req, oc.Outcome{Code: "Unavailable", Reason: "INDEX_BUILDING"}, nil, ValidateInspectIndex), EvInspectGated))
	require.True(t, has(f.read(KindInspectIndex, req, oc.Outcome{Code: "FailedPrecondition", Reason: "INDEX_NOT_FOUND"}, nil, ValidateInspectIndex), EvInspectGated))
	require.True(t, has(f.read(KindInspectIndex, req, oc.Outcome{Code: "Internal"}, nil, ValidateInspectIndex), EvInspectError))

	resumed := proto.Clone(req).(*servicepb.InspectIndexRequest)
	resumed.Cursor = "c"
	require.True(t, has(f.read(KindInspectIndex, resumed, page(2, false, ""), nil, ValidateInspectIndex), EvInspectResumed))
	require.True(t, has(f.read(KindInspectIndex, resumed, page(2, false, "z"), nil, ValidateInspectIndex), EvResumeContract))
	require.True(t, has(f.read(KindInspectIndex, resumed, oc.Outcome{Code: "Internal"}, nil, ValidateInspectIndex), EvResumeError))
	require.Empty(t, f.read(KindInspectIndex, resumed, oc.Outcome{Code: "Unavailable", Reason: "INDEX_BUILDING"}, nil, ValidateInspectIndex))

	absent := inspectReq("o-test-absent-3", "k0", dv)
	require.True(t, has(f.read(KindInspectIndex, absent, oc.Outcome{Code: "NotFound"}, nil, ValidateInspectIndex), EvInspectAbsentNotFound))
	require.True(t, has(f.read(KindInspectIndex, absent, page(0, false, ""), nil, ValidateInspectIndex), EvInspectOutsideFleet))

	cp := inspectReq(f.L, "k0", servicepb.InspectIndexMode_INSPECT_INDEX_MODE_SUMMARY)
	cp.CheckpointId = 3
	evs := f.read(KindInspectIndex, cp, summary(42, 1, 0), nil, ValidateInspectIndex)
	require.True(t, has(evs, EvInspectCheckpoint), "checkpoint summaries are not compared with the live model")
	require.Empty(t, f.read(KindInspectIndex, cp, oc.Outcome{Code: "NotFound"}, nil, ValidateInspectIndex))
}

// ---- view, hooks

func TestIndexGapsView(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.write(setType(f.L, "k1", commonpb.MetadataType_METADATA_TYPE_STRING))
	f.write(setType(f.L, "own-score1", commonpb.MetadataType_METADATA_TYPE_INT64))
	f.write(createIndex(f.L, match.AssetIndexID()))
	res, err := viewIndexGaps(f.s.T.Model(), f.s.T, json.RawMessage(`{}`))
	require.NoError(t, err)
	gaps := res.(map[string]any)["gaps"].(map[string][]GapEntry)[f.L]
	var canons []string
	for _, g := range gaps {
		canons = append(canons, g.Canonical)
		var id commonpb.IndexID
		require.NoError(t, protojson.Unmarshal(g.ID, &id))
		require.Equal(t, g.Canonical, indexes.Canonical(&id))
	}
	require.NotContains(t, canons, match.AssetIndexCanonical)
	require.Contains(t, canons, indexes.Canonical(indexes.MetadataID(acct, "k1")))
	require.NotContains(t, canons, indexes.Canonical(indexes.MetadataID(acct, "own-score1")))
	require.Len(t, canons, len(match.WorkloadIndexes())-1+1)
}

// The gaps view is where index_op's "metadata indexes only on declared
// keys" guarantee lives: every metadata gap is a key the model declares, and
// reserved keys appear only with owned.
func TestIndexGapsViewOnlyDeclaredKeys(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	tx := commonpb.TargetType_TARGET_TYPE_TRANSACTION
	f.write(setType(f.L, "k1", commonpb.MetadataType_METADATA_TYPE_STRING))
	f.write(setType(f.L, "k2", commonpb.MetadataType_METADATA_TYPE_INT64))
	f.write(setType(f.L, "own-score7", commonpb.MetadataType_METADATA_TYPE_INT64))
	f.write(setType(f.L, "own-bad", commonpb.MetadataType_METADATA_TYPE_INT64))
	f.write(&servicepb.Request{Type: &servicepb.Request_SetMetadataFieldType{SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
		Ledger: f.L, TargetType: tx, Key: "k3", Type: commonpb.MetadataType_METADATA_TYPE_BOOL}}})
	f.write(&servicepb.Request{Type: &servicepb.Request_RemoveMetadataFieldType{RemoveMetadataFieldType: &servicepb.RemoveMetadataFieldTypeRequest{
		Ledger: f.L, TargetType: acct, Key: "k2"}}})
	// Values without a declaration never make a gap.
	f.write(testutil.AccountMetaReq(f.L, "users:1", "k0", "5"))
	f.write(testutil.AccountMetaReq(f.L, "users:1", "own-other", "5"))

	for _, owned := range []bool{false, true} {
		params, _ := json.Marshal(map[string]any{"owned": owned})
		res, err := viewIndexGaps(f.s.T.Model(), f.s.T, params)
		require.NoError(t, err)
		ls := f.s.T.Model().Ledger(f.L)
		sawOwn := false
		for _, g := range res.(map[string]any)["gaps"].(map[string][]GapEntry)[f.L] {
			var id commonpb.IndexID
			require.NoError(t, protojson.Unmarshal(g.ID, &id))
			md := id.GetMetadata()
			if md == nil {
				continue
			}
			_, declared := ls.FieldTypeFor(md.GetTarget(), md.GetKey())
			require.True(t, declared, "gap on undeclared key %s (owned=%v)", g.Canonical, owned)
			if oc.IsReservedKey(md.GetKey()) {
				require.True(t, owned, "reserved key %s without owned", g.Canonical)
				sawOwn = true
			}
		}
		require.Equal(t, owned, sawOwn)
	}
}

func TestFieldNotInSchemaFailureHook(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	evs := f.s.Write(false, createIndex(f.L, indexes.MetadataID(acct, "undeclared-k1")))
	require.Empty(t, testutil.Findings(evs))
	require.True(t, has(evs, EvFieldNotInSchema))
}

func TestDropOfAbsentIndexIsANoOpSuccess(t *testing.T) {
	t.Parallel()
	g := oracle.NewGlobalState()
	g = g.Apply(oracle.Bulk{Requests: []*servicepb.Request{testutil.CreateLedgerReq("L")}}).State
	res := g.Apply(oracle.Bulk{Requests: []*servicepb.Request{dropIndex("L", match.AssetIndexID())}})
	require.True(t, res.OK, "the server tombstones an absent id and logs a DroppedIndexLog (processDropIndex)")
}

func TestDeleteLedgerForgetsObservations(t *testing.T) {
	t.Parallel()
	f, _, canon := retypeFixture(t)
	f.write(createIndex(f.L, match.AssetIndexID()))
	f.write(testutil.DeleteLedgerReq(f.L))
	st := StateOf(f.s.T)
	_, ok := st.CreateSeq(f.L, match.AssetIndexCanonical)
	require.False(t, ok)
	require.False(t, st.RetypeOpen(f.L, canon))
}
