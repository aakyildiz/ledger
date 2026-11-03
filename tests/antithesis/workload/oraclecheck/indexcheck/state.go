package indexcheck

import (
	"encoding/json"
	"slices"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// extKey names the family side table in Tracker.Ext.
const extKey = "indexcheck"

// Read kinds of the family.
const (
	KindIndexStatusPoll = "IndexStatusPoll"
	KindListIndexes     = "ListIndexes"
	KindGetIndex        = "GetIndex"
	KindGetIndexStatus  = "GetIndexStatus"
	KindInspectIndex    = "InspectIndex"
)

// ViewIndexGaps is the family's aiming view.
const ViewIndexGaps = "indexGaps"

func init() {
	oc.RegisterDrainHook("07-indexes", drainHook)
	oc.RegisterFailureHook("07-indexes", failureHook)
	oc.RegisterSettleHook("07-indexes", settleHook)
	oc.RegisterView(ViewIndexGaps, viewIndexGaps)
	oc.RegisterReadKind(oc.ReadKind{Name: KindIndexStatusPoll, Validate: ValidateIndexStatusPoll})
	oc.RegisterReadKind(oc.ReadKind{Name: KindListIndexes, Validate: ValidateListIndexes})
	oc.RegisterReadKind(oc.ReadKind{Name: KindGetIndex, Validate: ValidateGetIndex})
	oc.RegisterReadKind(oc.ReadKind{Name: KindGetIndexStatus, Validate: ValidateGetIndexStatus})
	oc.RegisterReadKind(oc.ReadKind{Name: KindInspectIndex, Validate: ValidateInspectIndex})
}

// promotion is a confirmed-ready index awaiting promotion to active
// (M/checker.go indexPromotion): one all-nodes poll found it ready within
// createSeq's incarnation; it is applied once every operation ticketed at or
// before confirmedAt finished.
type promotion struct {
	createSeq   uint64
	confirmedAt uint64
	deferred    bool // the deferral coverage was reported
}

// retypeObs drives one retype window's closure, two-phase per node
// (M/checker.go retypeObservation): a poll proving the retype's own log folded
// (lastIndexedSequence >= openSeq) arms the node; a later poll listing the
// index with pendingVersion == 0 confirms it. Nodes are service addresses.
type retypeObs struct {
	ledger, canonical string
	openSeq           uint64
	foldSeen          map[string]bool
	pendClear         map[string]bool
	confirmedAt       uint64
}

// State is the family's side table (observation state only).
type State struct {
	// createSeq is each index's create frontier: the committed log sequence of
	// the CreateIndex that made the current incarnation (M/validate.go
	// recordIndexCreates).
	createSeq  map[string]map[string]uint64
	promotions map[string]map[string]*promotion
	retype     map[string]*retypeObs
}

func newState() any {
	return &State{
		createSeq:  map[string]map[string]uint64{},
		promotions: map[string]map[string]*promotion{},
		retype:     map[string]*retypeObs{},
	}
}

// StateOf returns the family side table of t.
func StateOf(t *oc.Tracker) *State { return t.Ext(extKey, newState).(*State) }

// CreateSeq returns an index's create frontier.
func (s *State) CreateSeq(ledger, canonical string) (uint64, bool) {
	seq, ok := s.createSeq[ledger][canonical]

	return seq, ok
}

// PendingPromotion reports whether a promotion awaits its settle point.
func (s *State) PendingPromotion(ledger, canonical string) bool {
	_, ok := s.promotions[ledger][canonical]

	return ok
}

// RetypeOpen reports whether a closure observation is tracked.
func (s *State) RetypeOpen(ledger, canonical string) bool {
	_, ok := s.retype[retypeKey(ledger, canonical)]

	return ok
}

func retypeKey(ledger, canonical string) string { return ledger + "\x00" + canonical }

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)

	return out
}

// drainHook records create frontiers and opens retype observations for a
// drained bulk (M/validate.go:90-116, :440-458; M/checker.go:424-440), and
// forgets a deleted ledger's observations.
func drainHook(t *oc.Tracker, rec oc.WriteRecord, resp *servicepb.ApplyResponse, _ oracle.GlobalState, after oracle.GlobalState) []oc.Event {
	st := StateOf(t)
	logs := resp.GetLogs()
	for i, req := range rec.Bulk.Requests {
		switch {
		case req.GetCreateIndex() != nil:
			ci := req.GetCreateIndex()
			if i >= len(logs) || ci.GetId() == nil || logs[i].GetSequence() == 0 {
				continue
			}
			ledger := ci.GetLedger()
			if st.createSeq[ledger] == nil {
				st.createSeq[ledger] = map[string]uint64{}
			}
			st.createSeq[ledger][indexes.Canonical(ci.GetId())] = logs[i].GetSequence()
		case req.GetDeleteLedger() != nil:
			name := req.GetDeleteLedger().GetName()
			delete(st.createSeq, name)
			delete(st.promotions, name)
			for k, o := range st.retype {
				if o.ledger == name {
					delete(st.retype, k)
				}
			}
		case req.GetSetMetadataFieldType() != nil:
			smft := req.GetSetMetadataFieldType()
			tt := smft.GetTargetType()
			if tt != commonpb.TargetType_TARGET_TYPE_ACCOUNT && tt != commonpb.TargetType_TARGET_TYPE_TRANSACTION {
				continue
			}
			ledger := smft.GetLedger()
			canonical := indexes.Canonical(indexes.MetadataID(tt, smft.GetKey()))
			if _, open := after.Ledger(ledger).RetypeWindow(canonical); open {
				st.noteRetypeCommit(ledger, canonical, oc.MaxLogSequence(logs))
			}
		}
	}

	return nil
}

// noteRetypeCommit registers (or re-arms, on a chained retype) the closure
// observation of a committed retype.
func (s *State) noteRetypeCommit(ledger, canonical string, seq uint64) {
	key := retypeKey(ledger, canonical)
	o, ok := s.retype[key]
	if !ok {
		o = &retypeObs{ledger: ledger, canonical: canonical}
		s.retype[key] = o
	}
	if seq > o.openSeq {
		o.openSeq = seq
	}
	o.foldSeen = map[string]bool{}
	o.pendClear = map[string]bool{}
	o.confirmedAt = 0
}

// failureHook reports the rejection coverage of the family.
func failureHook(_ *oc.Tracker, rec oc.WriteRecord, _ oracle.ApplyResult, reason string) []oc.Event {
	if reason != "METADATA_FIELD_NOT_IN_SCHEMA" {
		return nil
	}
	for _, r := range rec.Bulk.Requests {
		if r.GetCreateIndex() != nil {
			return []oc.Event{oc.Reached(EvFieldNotInSchema, map[string]any{"wid": rec.Wid, "ledger": r.GetCreateIndex().GetLedger()})}
		}
	}

	return nil
}

// settleHook applies deferred promotions and retype-window closures once
// every operation ticketed at or before their confirmation finished
// (M/indexes.go:686-708, :890-905).
func settleHook(t *oc.Tracker) []oc.Event {
	st := StateOf(t)
	earliest, outstanding := t.EarliestOutstanding()
	blocked := func(confirmedAt uint64) bool { return outstanding && earliest <= confirmedAt }
	model := t.Model()

	var evs []oc.Event
	for _, ledger := range sortedKeys(st.promotions) {
		ps := st.promotions[ledger]
		ls := model.Ledger(ledger)
		for _, canon := range sortedKeys(ps) {
			p := ps[canon]
			exists, active := ls.IndexState(canon)
			if !exists || active || st.createSeq[ledger][canon] != p.createSeq {
				delete(ps, canon)

				continue
			}
			if blocked(p.confirmedAt) {
				if !p.deferred {
					p.deferred = true
					evs = append(evs, oc.Reached(EvPromotionDeferred, map[string]any{"ledger": ledger, "index": canon,
						"confirmedAt": p.confirmedAt, "earliest": earliest}))
				}

				continue
			}
			delete(ps, canon)
			t.MutateModel("index-promote", func(g *oracle.GlobalState) { g.SetIndexActive(ledger, canon) })
			evs = append(evs, oc.Reached(EvPromoted, map[string]any{"ledger": ledger, "index": canon}))
		}
		if len(ps) == 0 {
			delete(st.promotions, ledger)
		}
	}

	for _, key := range sortedKeys(st.retype) {
		o := st.retype[key]
		if _, open := model.Ledger(o.ledger).RetypeWindow(o.canonical); !open {
			delete(st.retype, key) // the window died with its index

			continue
		}
		if o.confirmedAt == 0 || blocked(o.confirmedAt) {
			continue
		}
		delete(st.retype, key)
		ledger, canon := o.ledger, o.canonical
		t.MutateModel("retype-close", func(g *oracle.GlobalState) { g.CloseRetypeWindow(ledger, canon) })
		evs = append(evs, oc.Reached(EvRetypeClosed, map[string]any{"ledger": ledger, "index": canon}))
	}

	return evs
}

// ---- view ----

type gapsParams struct {
	Ledger string `json:"ledger"`
	Owned  bool   `json:"owned"`
}

// GapEntry is one absent workload index of a ledger.
type GapEntry struct {
	Canonical string          `json:"canonical"`
	ID        json.RawMessage `json:"id"`
}

// viewIndexGaps answers {"gaps": {ledger: [{canonical, id}…]}} over the live
// tracked ledgers (or the requested one): the workload indexes
// (match.LedgerIndexPool) each ledger lacks. Reserved (own-) keys are
// excluded unless owned.
func viewIndexGaps(snap oracle.GlobalState, t *oc.Tracker, params json.RawMessage) (any, error) {
	var p gapsParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	ledgers := t.LiveLedgers(snap, p.Owned)
	if p.Ledger != "" {
		ledgers = []string{p.Ledger}
	}
	gaps := map[string][]GapEntry{}
	for _, ledger := range ledgers {
		ls := snap.Ledger(ledger)
		out := []GapEntry{}
		for _, w := range match.AbsentIndexes(ls, match.LedgerIndexPool(ls)) {
			if !p.Owned && oc.IsReservedKey(w.ID.GetMetadata().GetKey()) {
				continue
			}
			if len(out) >= oc.ViewCap {
				break
			}
			b, err := protojson.Marshal(w.ID)
			if err != nil {
				return nil, err
			}
			out = append(out, GapEntry{Canonical: w.Canonical, ID: b})
		}
		slices.SortFunc(out, func(a, b GapEntry) int { return strings.Compare(a.Canonical, b.Canonical) })
		gaps[ledger] = out
	}

	return map[string]any{"gaps": gaps}, nil
}
