package oraclecheck

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// logSequences extracts the log sequences from a response, in order.
func logSequences(logs []*commonpb.Log) []uint64 {
	out := make([]uint64, len(logs))
	for i, l := range logs {
		out[i] = l.GetSequence()
	}

	return out
}

// sequencesEqual reports whether two sequence slices are identical in order.
func sequencesEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// replayOrdersMatch reports whether the server's response logs echo the frozen
// orders — the transaction identity that defines "same result": assigned ids,
// echoed references and postings, per-cell post-commit volumes, and stored
// metadata. It reuses the leaf predicates crossCheckCommit asserts on; the two
// stay separate because crossCheckCommit needs a distinct assert callsite per
// field (Antithesis catalogues by callsite) while a replay diverges as a whole.
func replayOrdersMatch(bulk oracle.Bulk, orders []oracle.OrderResult, logs []*commonpb.Log) bool {
	if !checkpointOrdersMatch(bulk, orders, logs) {
		return false
	}
	if len(logs) != len(orders) || len(bulk.Requests) != len(orders) {
		return false
	}
	for i, order := range orders {
		if i >= len(logs) {
			return false
		}

		req := bulk.Requests[i]
		if err := validateLifecycleLog(req, order.PreparedQueryLog, logs[i].GetPayload()); err != nil {
			return false
		}
		entry := logs[i].GetPayload().GetApply()
		data := entry.GetLog().GetData()
		if order.LogID != 0 && (entry.GetLedgerName() != oracle.LedgerOf(req) || entry.GetLog().GetId() != order.LogID) {
			return false
		}
		if (order.Skipped != nil) != (data.GetOrderSkipped() != nil) {
			return false
		}
		if order.Skipped != nil {
			if !order.Skipped.EqualVT(data.GetOrderSkipped()) {
				return false
			}

			continue
		}
		if mode := requestedEnforcementMode(req); mode != nil {
			if data.GetUpdatedDefaultEnforcementMode() == nil || data.GetUpdatedDefaultEnforcementMode().GetEnforcementMode() != *mode {
				return false
			}
		}

		if !chartResponseMatches(req, data) {
			return false
		}

		switch {
		case order.Revert != nil:
			rt := data.GetRevertedTransaction()
			revTx := rt.GetRevertTransaction()
			if revTx.GetId() != order.TxID ||
				rt.GetRevertedTransactionId() != order.Revert.RevertedID() ||
				!postingsEqual(order.Revert.Postings(), revTx.GetPostings()) {
				return false
			}

		case order.TxID != 0:
			tx := data.GetCreatedTransaction().GetTransaction()
			ct := req.GetApply().GetAction().GetCreateTransaction()
			if tx.GetId() != order.TxID ||
				tx.GetReference() != ct.GetReference() ||
				!postingsEqual(ct.GetPostings(), tx.GetPostings()) ||
				!accountMetaMapEqual(ct.GetAccountMetadata(), data.GetCreatedTransaction().GetAccountMetadata()) {
				return false
			}
		}

		if order.PCV != nil {
			if !pcvMatches(order.PCV, serverPCV(data)) {
				return false
			}
		}

		if order.Meta != nil {
			if !metaMapEqual(order.Meta.Saved(), responseMetaEffect(req, logs[i])) {
				return false
			}
		}
	}

	return true
}

// pcvMatches reports whether every model cell equals the server's post-commit
// volume for that cell.
func pcvMatches(model map[oracle.VolumeKey]oracle.VolumePair, server *commonpb.PostCommitVolumes) bool {
	for key, vp := range model {
		gotIn, gotOut, ok := postCommitVolume(server, key)
		if !ok || vp.Input.Cmp(&gotIn) != 0 || vp.Output.Cmp(&gotOut) != 0 {
			return false
		}
	}

	return true
}

// serverPCV pulls the post-commit volumes off a create or revert response log.
// PCV rides on the transaction itself (CreatedTransaction.Transaction /
// RevertedTransaction.RevertTransaction) and is present on every committed
// transaction.
func serverPCV(data *commonpb.LedgerLogPayload) *commonpb.PostCommitVolumes {
	switch {
	case data.GetCreatedTransaction() != nil:
		return data.GetCreatedTransaction().GetTransaction().GetPostCommitVolumes()
	case data.GetRevertedTransaction() != nil:
		return data.GetRevertedTransaction().GetRevertTransaction().GetPostCommitVolumes()
	default:
		return nil
	}
}

// chartResponseMatches validates both chart request forms against their echoed
// payloads, including normal outcomes of requests that opted into skipping.
// Non-chart requests match by definition so callers can use it uniformly.
func chartResponseMatches(req *servicepb.Request, data *commonpb.LedgerLogPayload) bool {
	var added *commonpb.AccountType
	var removed *string
	switch r := req.GetType().(type) {
	case *servicepb.Request_AddAccountType:
		added = r.AddAccountType.GetAccountType()
	case *servicepb.Request_RemoveAccountType:
		name := r.RemoveAccountType.GetName()
		removed = &name
	case *servicepb.Request_Apply:
		switch action := r.Apply.GetAction().GetData().(type) {
		case *servicepb.LedgerAction_AddAccountType:
			added = action.AddAccountType.GetAccountType()
		case *servicepb.LedgerAction_RemoveAccountType:
			name := action.RemoveAccountType.GetName()
			removed = &name
		}
	}
	if added != nil {
		return data.GetAddedAccountType() != nil && added.EqualVT(data.GetAddedAccountType().GetAccountType())
	}
	if removed != nil {
		return data.GetRemovedAccountType() != nil && data.GetRemovedAccountType().GetName() == *removed
	}

	return true
}
