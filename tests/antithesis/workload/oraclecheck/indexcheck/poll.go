package indexcheck

import (
	"encoding/json"
	"slices"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// PollParams are the IndexStatusPoll begin params: the nodes this poll
// covers and the full node set the driver discovered.
type PollParams struct {
	Nodes    []string `json:"nodes"`
	AllNodes []string `json:"allNodes"`
}

// NodeOutcome is one node's stale GetIndexStatus outcome.
type NodeOutcome struct {
	Node    string     `json:"node"`
	Outcome oc.Outcome `json:"outcome"`
}

// PollPayload is the outcome response of an IndexStatusPoll read.
type PollPayload struct {
	PerNode []NodeOutcome `json:"perNode"`
}

var unmarshal = protojson.UnmarshalOptions{DiscardUnknown: true}

type nodeStatus struct {
	ok          bool
	lastIndexed uint64
	version     map[string]uint32
	pending     map[string]uint32
}

func sameSet(a, b []string) bool {
	x, y := slices.Clone(a), slices.Clone(b)
	slices.Sort(x)
	slices.Sort(y)

	return slices.Equal(slices.Compact(x), slices.Compact(y))
}

// ValidateIndexStatusPoll folds one readiness poll (stale, per node; the only
// stale read kind, DESIGN §4.7) into the observation state: lifted from
// M/indexes.go reconcileIndexes / applyIndexReadiness and the two-phase
// retype closure. A node is ready for an index iff it lists it with
// currentVersion != 0 and lastIndexedSequence >= the index's create
// frontier; a node whose outcome is not a decodable success is not ready.
//
//   - Not ready on any polled node (single-node polls included) demotes the
//     index at once (MutateModel) and cancels its pending promotion.
//   - Ready on every node of a poll that covered the whole node set
//     (params.nodes == params.allNodes, every node answered) records a
//     promotion confirmed at MaxTicket; the settle hook applies it once
//     every operation ticketed at or before it finished. Verdicts are never
//     aggregated across polls.
//   - Retype closure: per node, a poll with lastIndexed >= openSeq arms it
//     (at the poll's MaxTicket); a poll that began after that (Ticket >
//     armedAt), still sees lastIndexed >= openSeq and lists the index with
//     pendingVersion == 0 confirms it; once
//     every node of allNodes confirmed, the closure waits for the settle
//     point like a promotion.
func ValidateIndexStatusPoll(c *oc.ReadCtx) []oc.Event {
	var req servicepb.GetIndexStatusRequest
	if err := unmarshal.Unmarshal(c.Request, &req); err != nil {
		return nil
	}
	ledger := req.GetLedger()
	var params PollParams
	_ = json.Unmarshal(c.Params, &params) // absent params: no full-poll verdict
	var payload PollPayload
	if !c.Outcome.OK || json.Unmarshal(c.Outcome.Response, &payload) != nil {
		return nil
	}

	var evs []oc.Event
	statuses := make(map[string]nodeStatus, len(payload.PerNode))
	polled := make([]string, 0, len(payload.PerNode))
	for _, pn := range payload.PerNode {
		polled = append(polled, pn.Node)
		ns := nodeStatus{}
		if pn.Outcome.OK {
			resp := &servicepb.GetIndexStatusResponse{}
			if err := unmarshal.Unmarshal(pn.Outcome.Response, resp); err == nil {
				ns = nodeStatus{ok: true, lastIndexed: resp.GetLastIndexedSequence(),
					version: map[string]uint32{}, pending: map[string]uint32{}}
				for _, e := range resp.GetIndexes() {
					canon := indexes.Canonical(e.GetIndex().GetId())
					ns.version[canon] = e.GetCurrentVersion()
					ns.pending[canon] = e.GetPendingVersion()
				}
				if why := match.IndexStatusViolation(resp); why != "" {
					evs = append(evs, oc.Violation(EvStatusNotSelfConsistent, map[string]any{
						"ledger": ledger, "node": pn.Node, "violation": why,
						"lastIndexed": resp.GetLastIndexedSequence(), "lastLog": resp.GetLastLogSequence(),
						"lag": resp.GetLag(), "indexFileSize": resp.GetIndexFileSize(),
					}))
				}
				if resp.GetIndexFileSize() > 0 {
					evs = append(evs, oc.Reached(EvStatusFile, map[string]any{"ledger": ledger, "node": pn.Node}))
				}
			}
		}
		statuses[pn.Node] = ns
	}
	if len(polled) == 0 || ledger == "" {
		return evs
	}
	fullPoll := len(params.AllNodes) > 0 && sameSet(params.Nodes, params.AllNodes) && sameSet(polled, params.AllNodes)

	st := StateOf(c.T)
	ls, live := match.LiveLedgerState(c.T.Model(), ledger)
	if live {
		canons := make([]string, 0, ls.Indexes().Len())
		for canon := range ls.Indexes().All() {
			canons = append(canons, canon)
		}
		slices.Sort(canons)
		for _, canon := range canons {
			createSeq, known := st.createSeq[ledger][canon]
			if !known {
				continue
			}
			readyAll := true
			for _, node := range polled {
				ns := statuses[node]
				if !ns.ok || ns.version[canon] == 0 || ns.lastIndexed < createSeq {
					readyAll = false

					break
				}
			}
			_, wasActive := ls.IndexState(canon)
			if !readyAll {
				if ps := st.promotions[ledger]; ps != nil {
					delete(ps, canon)
				}
				if wasActive {
					c.T.MutateModel("index-demote", func(g *oracle.GlobalState) { g.SetIndexAmbiguous(ledger, canon) })
				}
				evs = append(evs, oc.Reached(EvDemoted, map[string]any{"ledger": ledger, "index": canon, "wasActive": wasActive, "nodes": polled}))

				continue
			}
			if !fullPoll || wasActive {
				continue
			}
			if st.promotions[ledger] == nil {
				st.promotions[ledger] = map[string]*promotion{}
			}
			if p, ok := st.promotions[ledger][canon]; !ok || p.createSeq != createSeq {
				st.promotions[ledger][canon] = &promotion{createSeq: createSeq, confirmedAt: c.MaxTicket}
			}
		}
	}

	for _, key := range sortedKeys(st.retype) {
		o := st.retype[key]
		if o.ledger != ledger {
			continue
		}
		if _, open := c.T.Model().Ledger(ledger).RetypeWindow(o.canonical); !open {
			delete(st.retype, key)

			continue
		}
		for _, node := range polled {
			ns := statuses[node]
			switch {
			case o.pendClear[node], !ns.ok:
			case !o.foldSeen[node]:
				if ns.lastIndexed >= o.openSeq {
					o.foldSeen[node] = true
					o.armedAt[node] = c.MaxTicket
				}
			case c.Ticket > o.armedAt[node] && ns.lastIndexed >= o.openSeq:
				// Phase two only from a poll that began after the arming
				// poll ended (polls run concurrently: a poll that began
				// earlier may have sampled the node before the fold), and
				// that itself sees the fold. Presence is part of the proof:
				// an unlisted index reads as pending 0 through the map's
				// zero value.
				if p, listed := ns.pending[o.canonical]; listed && p == 0 {
					o.pendClear[node] = true
				}
			}
		}
		confirmed := len(params.AllNodes) > 0
		for _, node := range params.AllNodes {
			if !o.pendClear[node] {
				confirmed = false
			}
		}
		if confirmed && o.confirmedAt == 0 {
			o.confirmedAt = c.MaxTicket
		}
	}

	return evs
}
