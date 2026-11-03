package pointcheck

import (
	"encoding/json"
	"strconv"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// UnassignedSeqSlack is how far past the highest learned global sequence the
// GetLog rule probes an unassigned sequence (DESIGN §3.2): foreign writers
// share the global stream, so the singleton model's 1_000_000 is replaced by
// 2^40, which no run can hand out.
const UnassignedSeqSlack uint64 = 1 << 40

// LogSeqsNewest is how many of the newest learned sequences the logSeqs view
// always lists; older ones are sampled evenly up to oc.ViewCap in total.
const LogSeqsNewest = 32

// learnedSeqs lists the snapshot's learned global sequences, ascending.
func learnedSeqs(snap oracle.GlobalState) []uint64 {
	var out []uint64
	for seq := range snap.Logs() {
		out = append(out, seq)
	}

	return out
}

// SampleSeqs keeps the newest LogSeqsNewest sequences plus every ⌈n/32⌉-th
// older one (deterministic, ascending, at most oc.ViewCap).
func SampleSeqs(all []uint64) []uint64 {
	if len(all) <= LogSeqsNewest {
		return append([]uint64(nil), all...)
	}
	older := all[:len(all)-LogSeqsNewest]
	step := (len(older) + (oc.ViewCap - LogSeqsNewest) - 1) / (oc.ViewCap - LogSeqsNewest)
	out := make([]uint64, 0, oc.ViewCap)
	for i := 0; i < len(older); i += step {
		out = append(out, older[i])
	}

	return append(out, all[len(all)-LogSeqsNewest:]...)
}

// viewLogSeqs answers {"seqs": [decimal…], "maxLearnedSeq": "N"} on the
// read's snapshot.
func viewLogSeqs(snap oracle.GlobalState, _ *oc.Tracker, _ json.RawMessage) (any, error) {
	all := learnedSeqs(snap)
	var maxSeq uint64
	if len(all) > 0 {
		maxSeq = all[len(all)-1]
	}
	seqs := []string{}
	for _, s := range SampleSeqs(all) {
		seqs = append(seqs, strconv.FormatUint(s, 10))
	}

	return map[string]any{"seqs": seqs, "maxLearnedSeq": strconv.FormatUint(maxSeq, 10)}, nil
}

// ValidateGetLog checks one GetLog by global sequence (M/getlog.go runGetLog).
// A sequence the snapshot learned must resolve, on every base, to the log the
// model recorded there (history is permanent and the global stream outlives
// its ledger, so NotFound is a lost log); a sequence at least
// UnassignedSeqSlack past every learned one must resolve to nothing. Which
// case applies is recomputed from the request and the snapshot.
func ValidateGetLog(c *oc.ReadCtx) []oc.Event {
	var req servicepb.GetLogRequest
	if err := decode(c.Request, &req); err != nil {
		return []oc.Event{oc.Violation(EvGetLogAnotherLog, map[string]any{"error": "undecodable request: " + err.Error()})}
	}
	seq := req.GetSequence()
	entry, learned := c.Snapshot.Log(seq)
	var maxSeq uint64
	if all := learnedSeqs(c.Snapshot); len(all) > 0 {
		maxSeq = all[len(all)-1]
	}
	unassigned := !learned && seq >= maxSeq+UnassignedSeqSlack

	switch {
	case c.Outcome.IsNotFound():
		switch {
		case learned:
			return []oc.Event{oc.Violation(EvGetLogLostCommitted, map[string]any{
				"sequence": seq, "wantLedger": entry.Ledger, "wantLogID": entry.ID,
			})}
		case unassigned:
			return []oc.Event{oc.Reached(EvGetLogUnassignedNotFound, map[string]any{"sequence": seq})}
		}

		return nil
	case !c.Outcome.OK:
		d := outcomeDetails(c.Outcome)
		d["sequence"], d["learned"] = seq, learned

		return []oc.Event{oc.Violation(EvGetLogUnexpectedError, d)}
	}

	log := &commonpb.Log{}
	if err := decode(c.Outcome.Response, log); err != nil {
		return []oc.Event{oc.Violation(EvGetLogAnotherLog, map[string]any{"sequence": seq, "error": "undecodable response: " + err.Error()})}
	}

	if unassigned {
		return []oc.Event{oc.Violation(EvGetLogServedUnassigned, map[string]any{
			"sequence":     seq,
			"maxLearned":   maxSeq,
			"servedLedger": log.GetPayload().GetApply().GetLedgerName(),
			"servedLogID":  log.GetPayload().GetApply().GetLog().GetId(),
		})}
	}
	if !learned {
		return nil // not a sequence the rule aims at; nothing to compare
	}

	target := match.CommittedLogTarget{Ledger: entry.Ledger, ID: entry.ID, Sequence: seq}
	got := match.ServerLogRows([]*commonpb.Log{log})[0]
	if c.Matches(func(base oracle.GlobalState) bool {
		return match.CommittedLogMatches(base, target, got)
	}) {
		return []oc.Event{oc.Reached(EvGetLogValidated, map[string]any{"ledger": target.Ledger, "kind": got.Kind})}
	}

	return []oc.Event{oc.Violation(EvGetLogAnotherLog, map[string]any{
		"sequence":     seq,
		"wantLedger":   target.Ledger,
		"wantLogID":    target.ID,
		"servedLedger": got.Ledger,
		"servedLogID":  got.ID,
		"servedSeq":    got.Sequence,
		"servedRow":    match.DescribeServerLogRows([]match.ServerLogRow{got}),
		"modelRow":     entry.Ledger + "/" + strconv.FormatUint(entry.ID, 10) + ":" + entry.Row.Kind + "[" + entry.Row.Payload + "]",
	})}
}
