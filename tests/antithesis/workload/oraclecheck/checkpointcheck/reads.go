package checkpointcheck

import (
	"encoding/json"
	"strconv"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/pagecursor"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
)

// The arms of CheckpointRead (M/checkpoint_reads.go:54-342, draws 2-9). The
// arm is named in the read params; everything else comes from the request.
const (
	ArmGetAccount       = "GetAccount"
	ArmGetTransaction   = "GetTransaction"
	ArmListAccounts     = "ListAccounts"
	ArmListTransactions = "ListTransactions"
	ArmAggregateVolumes = "AggregateVolumes"
	ArmGetLedgerStats   = "GetLedgerStats"
	ArmGetLog           = "GetLog"
	ArmListLedgers      = "ListLedgers"
)

// UnassignedSlack: a GetLog probe at least this far past a checkpoint's max
// sequence names a log the frozen store never held (DESIGN §3.2 uses 2^40;
// the driver sends maxSequence + 2^40).
const UnassignedSlack = uint64(1) << 39

var unmarshal = protojson.UnmarshalOptions{DiscardUnknown: true}

func decode(b []byte, m proto.Message) error {
	if len(b) == 0 {
		return nil
	}

	return unmarshal.Unmarshal(b, m)
}

func rpcError(o oc.Outcome) match.RPCError {
	if o.OK {
		return match.NoError
	}

	return match.RPCErrorFrom(o.Code, o.Reason, o.Metadata)
}

func rawOrNull(b []byte) json.RawMessage {
	if len(b) == 0 {
		return json.RawMessage("null")
	}

	return json.RawMessage(b)
}

type readParams struct {
	Arm string `json:"arm"`
	ID  string `json:"id"`
}

func paramsOf(c *oc.ReadCtx) readParams {
	var p readParams
	_ = json.Unmarshal(c.Params, &p) // absent or malformed params: zero value, handled by callers
	return p
}

// cursorKey decodes a request page token to its key ("" = first page).
func cursorKey(token string) (string, bool) {
	cur, err := pagecursor.Decode(token)
	if err != nil || cur.Back {
		return "", false
	}

	return cur.Key, true
}

func decodeItems[T any, P interface {
	*T
	proto.Message
}](items []json.RawMessage) ([]P, bool) {
	out := make([]P, 0, len(items))
	for _, it := range items {
		m := P(new(T))
		if err := decode(it, m); err != nil {
			return nil, false
		}
		out = append(out, m)
	}

	return out, true
}

// armResult is one arm's verdict on the served data against the frozen
// snapshot.
type armResult struct {
	ledger        string // "" for cluster-wide reads: a deleted ledger never explains NotFound
	frozenMatches bool
	concrete      bool // the answer carried model-verified content (coverage)
	skip          bool // nothing to judge (truncated stream, unknown sequence)
	fleetRows     int  // ListLedgers: rows served
	details       map[string]any
}

// ValidateCheckpointRead checks one checkpoint-scoped read on a pinned node
// (M/checkpoint_reads.go runCheckpointRead). Success must match the snapshot
// frozen at the checkpoint's creation; NotFound is legal when the frozen
// result explains it, or when some candidate base has deleted the checkpoint
// or (ledger-scoped reads) the ledger. Live business data never substitutes
// for frozen data (checkpointLedgerReadOutcomeMatches).
func ValidateCheckpointRead(c *oc.ReadCtx) []oc.Event {
	p := paramsOf(c)
	res, id, ok := checkpointArm(c, p.Arm)
	if !ok {
		return []oc.Event{oc.Violation(EvProtocolMalformedRead, map[string]any{"kind": KindCheckpointRead, "arm": p.Arm, "request": rawOrNull(c.Request)})}
	}
	if res.skip {
		return nil
	}
	err := rpcError(c.Outcome)
	if !match.CheckpointLedgerReadOutcomeMatches(id, res.ledger, res.frozenMatches, err, c.Matches) {
		d := res.details
		d["checkpoint"], d["arm"], d["ledger"], d["maxTicket"] = id, p.Arm, res.ledger, c.MaxTicket
		d["code"], d["reason"], d["message"] = c.Outcome.Code, c.Outcome.Reason, c.Outcome.Message

		return []oc.Event{oc.Violation(EvReadOutsideFrozen, d)}
	}
	var evs []oc.Event
	deleted := !c.Snapshot.QueryCheckpointExists(id)
	// An absent entity can return NotFound even from a live checkpoint; count
	// deletion coverage only when the frozen result cannot explain it.
	if deleted && !res.frozenMatches && match.CheckpointNotFound(err) {
		evs = append(evs, coverage(CovDeletedRead, map[string]any{"checkpoint": id, "arm": p.Arm})...)
	}
	if res.concrete {
		evs = append(evs, coverage(CovFrozenRead, map[string]any{"checkpoint": id, "arm": p.Arm})...)
	}
	if p.Arm == ArmListLedgers && res.frozenMatches && c.Outcome.OK && res.fleetRows > 0 {
		evs = append(evs, oc.Reached(EvFrozenFleetListing, map[string]any{"checkpoint": id, "rows": res.fleetRows}))
	}

	return evs
}

// checkpointArm decodes the request of arm and judges the outcome against
// the frozen snapshot of the checkpoint it names. ok=false: the request does
// not decode or names no checkpoint.
func checkpointArm(c *oc.ReadCtx, arm string) (res armResult, id uint64, ok bool) {
	res.details = map[string]any{}
	out := c.Outcome
	switch arm {
	case ArmGetAccount:
		var req servicepb.GetAccountRequest
		if decode(c.Request, &req) != nil || req.GetCheckpointId() == 0 {
			return res, 0, false
		}
		id = req.GetCheckpointId()
		frozen, known := c.T.FrozenCheckpoint(id)
		if !known {
			return armResult{skip: true}, id, true
		}
		res.ledger = req.GetLedger()
		var acct *commonpb.Account
		if out.OK {
			acct = &commonpb.Account{}
			if decode(out.Response, acct) != nil {
				acct = nil
			}
		}
		res.frozenMatches = match.CheckpointAccountReadMatches(frozen, res.ledger, req.GetAddress(), acct, out.OK, req.GetCollapseColors())
		res.concrete = out.OK && acct != nil && match.ModelKnowsAccount(frozen.Ledger(res.ledger), req.GetAddress())
		res.details["address"], res.details["collapseColors"], res.details["returned"] = req.GetAddress(), req.GetCollapseColors(), rawOrNull(out.Response)

	case ArmGetTransaction:
		var req servicepb.GetTransactionRequest
		if decode(c.Request, &req) != nil || req.GetCheckpointId() == 0 {
			return res, 0, false
		}
		id = req.GetCheckpointId()
		frozen, known := c.T.FrozenCheckpoint(id)
		if !known {
			return armResult{skip: true}, id, true
		}
		res.ledger = req.GetLedger()
		var tx *commonpb.Transaction
		if out.OK {
			resp := &servicepb.GetTransactionResponse{}
			if decode(out.Response, resp) == nil {
				tx = resp.GetTransaction()
			}
		}
		res.frozenMatches = match.CheckpointTransactionReadMatches(frozen, res.ledger, req.GetTransactionId(), tx, out.OK)
		res.concrete = out.OK && tx != nil
		res.details["transactionId"], res.details["returned"] = req.GetTransactionId(), rawOrNull(out.Response)

	case ArmGetLedgerStats:
		var req servicepb.GetLedgerStatsRequest
		if decode(c.Request, &req) != nil || req.GetCheckpointId() == 0 {
			return res, 0, false
		}
		id = req.GetCheckpointId()
		frozen, known := c.T.FrozenCheckpoint(id)
		if !known {
			return armResult{skip: true}, id, true
		}
		res.ledger = req.GetLedger()
		stats := &commonpb.LedgerStats{}
		if out.OK && decode(out.Response, stats) == nil {
			// A frozen store answers the boundaries it froze, exactly.
			ls := frozen.Ledger(res.ledger)
			res.frozenMatches = match.LedgerIsLive(frozen, res.ledger) &&
				stats.GetTransactionCount() == uint64(ls.Txs().Len()) &&
				stats.GetLogCount() == uint64(len(ls.LogDates()))
			res.concrete = res.frozenMatches && stats.GetLogCount() > 0
		}
		res.details["returned"] = rawOrNull(out.Response)

	case ArmAggregateVolumes:
		var req servicepb.AggregateVolumesRequest
		if decode(c.Request, &req) != nil || req.GetCheckpointId() == 0 {
			return res, 0, false
		}
		id = req.GetCheckpointId()
		frozen, known := c.T.FrozenCheckpoint(id)
		if !known {
			return armResult{skip: true}, id, true
		}
		res.ledger = req.GetLedger()
		if req.GetFilter() != nil {
			// The driver never filters a checkpoint aggregate (index readiness
			// stays out of the frozen fold); nothing to judge.
			return armResult{skip: true}, id, true
		}
		opts := match.AggOptions{CollapseColors: req.GetCollapseColors(), UseMaxPrecision: req.GetUseMaxPrecision(), GroupByPrefixes: req.GetGroupByPrefixes()}
		agg := &commonpb.AggregateResult{}
		served := out.OK && decode(out.Response, agg) == nil
		res.frozenMatches, res.concrete = match.CheckpointAggregateMatches(frozen.Ledger(res.ledger), opts, agg, served && match.LedgerIsLive(frozen, res.ledger))
		res.details["prefixes"] = req.GetGroupByPrefixes()
		if served {
			res.details["returned"] = match.DescribeCheckpointAggregate(opts, agg)
		}

	case ArmGetLog:
		var req servicepb.GetLogRequest
		if decode(c.Request, &req) != nil || req.GetCheckpointId() == 0 {
			return res, 0, false
		}
		id = req.GetCheckpointId()
		frozen, known := c.T.FrozenCheckpoint(id)
		maxSeq, _ := c.T.FrozenCheckpointMaxSequence(id)
		if !known {
			return armResult{skip: true}, id, true
		}
		seq := req.GetSequence()
		res.details["sequence"] = seq
		// GetLog is sequence-scoped and the global stream outlives its ledger,
		// so ledger stays empty: a deleted ledger never excuses a NotFound.
		if entry, learned := frozen.Log(seq); learned {
			target := match.CommittedLogTarget{Ledger: entry.Ledger, ID: entry.ID, Sequence: seq}
			log := &commonpb.Log{}
			if out.OK && decode(out.Response, log) == nil {
				rows := match.ServerLogRows([]*commonpb.Log{log})
				res.frozenMatches = len(rows) == 1 && match.CommittedLogMatches(frozen, target, rows[0])
			}
			res.concrete = res.frozenMatches
			res.details["logLedger"], res.details["returned"] = entry.Ledger, rawOrNull(out.Response)
		} else if seq >= maxSeq+UnassignedSlack {
			// A sequence the frozen store never held must resolve NotFound.
			res.frozenMatches = out.IsNotFound()
			res.concrete = res.frozenMatches
		} else {
			// A sequence the model does not hold (e.g. a deleted ledger's
			// retired stream): nothing to judge.
			return armResult{skip: true}, id, true
		}

	case ArmListAccounts:
		var req servicepb.ListAccountsRequest
		if decode(c.Request, &req) != nil || req.GetOptions().GetRead().GetCheckpointId() == 0 {
			return res, 0, false
		}
		opts := req.GetOptions()
		id = opts.GetRead().GetCheckpointId()
		frozen, known := c.T.FrozenCheckpoint(id)
		cursor, cursorOK := cursorKey(opts.GetCursor())
		if !known || opts.GetFilter() != nil || !cursorOK || (out.OK && !out.Complete) {
			return armResult{skip: true}, id, true
		}
		res.ledger = req.GetLedger()
		pageSize := match.EffectivePageSize(int(opts.GetPageSize()))
		res.details["cursor"], res.details["pageSize"], res.details["reverse"] = cursor, pageSize, opts.GetReverse()
		if out.OK {
			rows, ok := decodeItems[commonpb.Account](out.Items)
			next := match.NextCursorOf(out.NextCursor)
			ls := frozen.Ledger(res.ledger)
			want, more := match.CheckpointAccountWindow(ls, cursor, pageSize, opts.GetReverse())
			res.details["expectedAddresses"], res.details["served"], res.details["nextCursor"] = want, len(out.Items), next
			res.frozenMatches = ok && match.LedgerIsLive(frozen, res.ledger) && len(rows) == len(want) &&
				match.NextCursorLegal(next, more, match.LastAccountKey(rows), len(rows), pageSize)
			for i := 0; res.frozenMatches && i < len(want); i++ {
				res.frozenMatches = match.CheckpointAccountReadMatches(frozen, res.ledger, want[i], rows[i], true, false)
			}
			res.concrete = res.frozenMatches && len(rows) > 0
		}

	case ArmListTransactions:
		var req servicepb.ListTransactionsRequest
		if decode(c.Request, &req) != nil || req.GetOptions().GetRead().GetCheckpointId() == 0 {
			return res, 0, false
		}
		opts := req.GetOptions()
		id = opts.GetRead().GetCheckpointId()
		frozen, known := c.T.FrozenCheckpoint(id)
		cursor, cursorOK := cursorKey(opts.GetCursor())
		var afterID uint64
		if cursorOK && cursor != "" {
			n, err := strconv.ParseUint(cursor, 10, 64)
			cursorOK = err == nil
			afterID = n
		}
		if !known || opts.GetFilter() != nil || !cursorOK || (out.OK && !out.Complete) {
			return armResult{skip: true}, id, true
		}
		res.ledger = req.GetLedger()
		pageSize := match.EffectivePageSize(int(opts.GetPageSize()))
		res.details["afterId"], res.details["pageSize"], res.details["reverse"] = afterID, pageSize, opts.GetReverse()
		if out.OK {
			rows, ok := decodeItems[commonpb.Transaction](out.Items)
			next := match.NextCursorOf(out.NextCursor)
			res.details["served"], res.details["nextCursor"] = len(out.Items), next
			res.frozenMatches = ok && match.LedgerIsLive(frozen, res.ledger) &&
				match.TxWindowMatches(frozen.Ledger(res.ledger), nil, afterID, pageSize, opts.GetReverse(), rows, next)
			res.concrete = res.frozenMatches && len(rows) > 0
		}

	case ArmListLedgers:
		var req servicepb.ListLedgersRequest
		if decode(c.Request, &req) != nil || req.GetOptions().GetRead().GetCheckpointId() == 0 {
			return res, 0, false
		}
		opts := req.GetOptions()
		id = opts.GetRead().GetCheckpointId()
		frozen, known := c.T.FrozenCheckpoint(id)
		if !known || opts.GetFilter() != nil || (out.OK && !out.Complete) {
			return armResult{skip: true}, id, true
		}
		if out.OK {
			rows, ok := decodeItems[commonpb.LedgerInfo](out.Items)
			lreq := match.ListLedgersReq{Cursor: opts.GetCursor(), PageSize: opts.GetPageSize(), Reverse: opts.GetReverse()}
			names := make([]string, len(rows))
			for i, r := range rows {
				names[i] = r.GetName()
			}
			// A checkpoint freezes foreign ledgers too, so the frozen listing
			// is judged foreign-tolerantly (DESIGN §3.2), not by exact names
			// (M/checkpoint_reads.go:189-245 compared names exactly).
			var why string
			res.frozenMatches, why = false, "undecodable rows"
			if ok {
				res.frozenMatches, why = match.LedgerListingMatches(frozen, c.T.Config().TrackedPrefix, lreq, rows, out.NextCursor)
			}
			for _, r := range rows {
				if res.frozenMatches && match.LedgerInfoStructureViolation(r) != "" {
					res.frozenMatches, why = false, "malformed row "+r.GetName()
				}
			}
			res.fleetRows = len(rows)
			res.concrete = res.frozenMatches && len(rows) > 0
			res.details["served"], res.details["why"], res.details["nextCursor"] = names, why, out.NextCursor
			res.details["pageSize"], res.details["reverse"] = opts.GetPageSize(), opts.GetReverse()
		}

	default:
		return res, 0, false
	}

	return res, id, true
}
