package oraclecheck

import (
	"fmt"
	"maps"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Lifted from M/validate.go (validateBulkSuccess, crossCheckCommit,
// validateEmptyCommit). Each assert.Unreachable became a returned Violation
// with the same suffix; index bookkeeping (recordIndexCreates, retype
// observations) and lifecycle coverage moved to family drain hooks.

// validateBulkSuccess cross-checks a drained bulk; on agreement the model has
// advanced and the drain hooks run.
func (t *Tracker) validateBulkSuccess(rec *WriteRecord, resp *servicepb.ApplyResponse) []Event {
	before := t.modelState
	evs := t.crossCheckCommit(rec.Bulk, rec.Wid, resp)
	t.keepResponse(rec.Wid, resp)
	if HasFinding(evs) {
		return evs
	}

	return append(evs, t.runDrainHooks(*rec, resp, before, t.modelState)...)
}

// crossCheckCommit validates a committed bulk against the forward model.
// Bulks drain in log-sequence order, so the drained model is this bulk's exact
// predecessor and the prediction is deterministic. The model advances only on
// agreement.
func (t *Tracker) crossCheckCommit(bulk oracle.Bulk, wid string, resp *servicepb.ApplyResponse) []Event {
	res := t.modelState.Apply(bulk)

	if !res.OK {
		return []Event{Violation(EvModelRejectsCommittedBulk, map[string]any{
			"ledgers": bulkLedgers(bulk),
			"reason":  res.Reason,
			"kinds":   requestKinds(bulk),
			"meta":    bulkMeta(bulk),
			"wid":     wid,
		})}
	}

	logs := resp.GetLogs()
	if !checkpointOrdersMatch(bulk, res.Orders, logs) {
		return []Event{Violation(EvCheckpointCommitOutsideModel, map[string]any{"kinds": requestKinds(bulk), "logSeqs": logSeqs(logs), "wid": wid})}
	}
	if len(logs) != len(res.Orders) {
		return []Event{Violation(EvCommittedLogCountMismatch, map[string]any{"expected": len(res.Orders), "actual": len(logs), "wid": wid})}
	}
	for i, order := range res.Orders {
		entry := logs[i].GetPayload().GetApply()
		if order.LogID != 0 && (entry.GetLog().GetId() != order.LogID || entry.GetLedgerName() != oracle.LedgerOf(bulk.Requests[i])) {
			return []Event{Violation(EvCommittedLogIdentityMismatch, map[string]any{"order": i, "expected": order.LogID, "actual": entry.GetLog().GetId(), "wid": wid})}
		}
		data := logs[i].GetPayload().GetApply().GetLog().GetData()
		if (data.GetOrderSkipped() != nil) != (order.Skipped != nil) ||
			(order.Skipped != nil && (data.GetOrderSkipped().GetReason() != order.Skipped.GetReason() || data.GetOrderSkipped().GetCode() != order.Skipped.GetCode() || !maps.Equal(data.GetOrderSkipped().GetFacts(), order.Skipped.GetFacts()))) {
			return []Event{Violation(EvSkippedOrderMismatch, map[string]any{"order": i, "expected": fmt.Sprint(order.Skipped), "actual": fmt.Sprint(data.GetOrderSkipped()), "wid": wid})}
		}
		if mode := requestedEnforcementMode(bulk.Requests[i]); mode != nil {
			if data.GetUpdatedDefaultEnforcementMode() == nil || data.GetUpdatedDefaultEnforcementMode().GetEnforcementMode() != *mode {
				return []Event{Violation(EvEnforcementModeMismatch, map[string]any{"order": i, "expected": mode.String(), "actual": fmt.Sprint(data.GetUpdatedDefaultEnforcementMode()), "wid": wid})}
			}
		}
	}
	for i, req := range bulk.Requests {
		var payload *commonpb.LogPayload
		if i < len(logs) {
			payload = logs[i].GetPayload()
		}
		if err := validateLifecycleLog(req, res.Orders[i].PreparedQueryLog, payload); err != nil {
			return []Event{Violation(EvLifecycleResponseMismatch, map[string]any{
				"ledger": oracle.LedgerOf(req),
				"kind":   requestKinds(oracle.Bulk{Requests: []*servicepb.Request{req}}),
				"error":  err.Error(),
				"wid":    wid,
			})}
		}
	}
	for i, order := range res.Orders {
		if order.PCV == nil || i >= len(logs) {
			continue
		}

		data := logs[i].GetPayload().GetApply().GetLog().GetData()
		var pcv *commonpb.PostCommitVolumes
		switch {
		case data.GetCreatedTransaction() != nil:
			pcv = data.GetCreatedTransaction().GetTransaction().GetPostCommitVolumes()
		case data.GetRevertedTransaction() != nil:
			pcv = data.GetRevertedTransaction().GetRevertTransaction().GetPostCommitVolumes()
		default:
			continue
		}

		for _, key := range sortedVolumeKeys(order.PCV) {
			vp := order.PCV[key]
			gotIn, gotOut, ok := postCommitVolume(pcv, key)
			if !ok || vp.Input.Cmp(&gotIn) != 0 || vp.Output.Cmp(&gotOut) != 0 {
				return []Event{Violation(EvPostCommitVolumeMismatch, map[string]any{
					"cell":      key.Address + "/" + key.Asset + "/" + key.Color,
					"modelIn":   vp.Input.Dec(),
					"modelOut":  vp.Output.Dec(),
					"serverIn":  gotIn.Dec(),
					"serverOut": gotOut.Dec(),
					"present":   ok,
					"wid":       wid,
				})}
			}
		}
	}

	for i, order := range res.Orders {
		if order.Meta == nil || i >= len(logs) {
			continue
		}

		gotSaved := responseMetaEffect(bulk.Requests[i], logs[i])
		if !metaMapEqual(order.Meta.Saved(), gotSaved) {
			return []Event{Violation(EvResponseMetadataMismatch, map[string]any{
				"ledger":      oracle.LedgerOf(bulk.Requests[i]),
				"kinds":       requestKinds(bulk),
				"meta":        bulkMeta(bulk),
				"modelSaved":  renderMetaMap(order.Meta.Saved()),
				"serverSaved": renderMetaMap(gotSaved),
				"wid":         wid,
			})}
		}
	}

	// droppedInBulk collects the (ledger, canonical) pairs earlier orders of
	// this bulk already removed, so a duplicate remove expects no second drop.
	droppedInBulk := map[string]bool{}

	for i := range res.Orders {
		if i >= len(logs) {
			continue
		}

		req := bulk.Requests[i]
		data := logs[i].GetPayload().GetApply().GetLog().GetData()

		order := res.Orders[i]
		if order.Skipped != nil {
			continue
		}

		if order.Revert != nil {
			rt := data.GetRevertedTransaction()
			revTx := rt.GetRevertTransaction()

			if revTx.GetId() != order.TxID {
				return []Event{Violation(EvRevertTxIDMismatch, map[string]any{
					"ledger":   oracle.LedgerOf(req),
					"modelId":  strconv.FormatUint(order.TxID, 10),
					"serverId": strconv.FormatUint(revTx.GetId(), 10),
					"wid":      wid,
				})}
			}
			if rt.GetRevertedTransactionId() != order.Revert.RevertedID() {
				return []Event{Violation(EvRevertedTxIDMismatch, map[string]any{
					"ledger":   oracle.LedgerOf(req),
					"modelId":  strconv.FormatUint(order.Revert.RevertedID(), 10),
					"serverId": strconv.FormatUint(rt.GetRevertedTransactionId(), 10),
					"wid":      wid,
				})}
			}
			if !postingsEqual(order.Revert.Postings(), revTx.GetPostings()) {
				return []Event{Violation(EvRevertPostingsMismatch, map[string]any{
					"ledger":   oracle.LedgerOf(req),
					"model":    renderPostings(order.Revert.Postings()),
					"returned": renderPostings(revTx.GetPostings()),
					"wid":      wid,
				})}
			}
		} else if order.TxID != 0 {
			tx := data.GetCreatedTransaction().GetTransaction()
			ct := req.GetApply().GetAction().GetCreateTransaction()

			if tx.GetId() != order.TxID {
				return []Event{Violation(EvTxIDMismatch, map[string]any{
					"ledger":   oracle.LedgerOf(req),
					"modelId":  strconv.FormatUint(order.TxID, 10),
					"serverId": strconv.FormatUint(tx.GetId(), 10),
					"wid":      wid,
				})}
			}
			if tx.GetReference() != ct.GetReference() {
				return []Event{Violation(EvTxReferenceMismatch, map[string]any{
					"ledger":    oracle.LedgerOf(req),
					"requested": ct.GetReference(),
					"returned":  tx.GetReference(),
					"wid":       wid,
				})}
			}
			if !postingsEqual(ct.GetPostings(), tx.GetPostings()) {
				return []Event{Violation(EvTxPostingsMismatch, map[string]any{
					"ledger":    oracle.LedgerOf(req),
					"requested": renderPostings(ct.GetPostings()),
					"returned":  renderPostings(tx.GetPostings()),
					"wid":       wid,
				})}
			}
			if !accountMetaMapEqual(ct.GetAccountMetadata(), data.GetCreatedTransaction().GetAccountMetadata()) {
				return []Event{Violation(EvTxAccountMetadataMismatch, map[string]any{
					"ledger":    oracle.LedgerOf(req),
					"requested": len(ct.GetAccountMetadata()),
					"returned":  len(data.GetCreatedTransaction().GetAccountMetadata()),
					"wid":       wid,
				})}
			}
		}

		if dm := req.GetApply().GetAction().GetDeleteMetadata(); dm != nil {
			logDM := data.GetDeletedMetadata()
			if logDM.GetKey() != dm.GetKey() || metaTargetLabel(logDM.GetTarget()) != metaTargetLabel(dm.GetTarget()) {
				return []Event{Violation(EvDeleteMetadataMismatch, map[string]any{
					"ledger":    oracle.LedgerOf(req),
					"requested": metaTargetLabel(dm.GetTarget()) + "/" + dm.GetKey(),
					"returned":  metaTargetLabel(logDM.GetTarget()) + "/" + logDM.GetKey(),
					"wid":       wid,
				})}
			}
		}

		if !chartResponseMatches(req, data) {
			return []Event{Violation(EvChartResponseMismatch, map[string]any{"order": i, "wid": wid})}
		}
		switch r := req.GetType().(type) {
		case *servicepb.Request_SetMetadataFieldType:
			lg, rq := data.GetSetMetadataFieldType(), r.SetMetadataFieldType
			if lg.GetTargetType() != rq.GetTargetType() || lg.GetKey() != rq.GetKey() || lg.GetType() != rq.GetType() {
				return []Event{Violation(EvSetFieldTypeMismatch, map[string]any{
					"ledger":    oracle.LedgerOf(req),
					"requested": fmt.Sprintf("tgt%d/%s=%v", rq.GetTargetType(), rq.GetKey(), rq.GetType()),
					"returned":  fmt.Sprintf("tgt%d/%s=%v", lg.GetTargetType(), lg.GetKey(), lg.GetType()),
					"wid":       wid,
				})}
			}

		case *servicepb.Request_RemoveMetadataFieldType:
			lg, rq := data.GetRemovedMetadataFieldType(), r.RemoveMetadataFieldType
			if lg.GetTargetType() != rq.GetTargetType() || lg.GetKey() != rq.GetKey() {
				return []Event{Violation(EvRemoveFieldTypeMismatch, map[string]any{
					"ledger":    oracle.LedgerOf(req),
					"requested": fmt.Sprintf("tgt%d/%s", rq.GetTargetType(), rq.GetKey()),
					"returned":  fmt.Sprintf("tgt%d/%s", lg.GetTargetType(), lg.GetKey()),
					"wid":       wid,
				})}
			}

			// A metadata index cannot outlive its field declaration: the server
			// reports the dropped index exactly when one existed before the bulk,
			// minus keys an earlier order of this bulk already removed.
			canonical := indexes.Canonical(indexes.MetadataID(rq.GetTargetType(), rq.GetKey()))
			bulkKey := oracle.LedgerOf(req) + "\x00" + canonical
			earlierInBulk := droppedInBulk[bulkKey]
			hadIndex := false
			if exists, _ := t.modelState.Ledger(oracle.LedgerOf(req)).IndexState(canonical); exists && !earlierInBulk {
				hadIndex = true
			}
			droppedInBulk[bulkKey] = true

			if (lg.GetDroppedIndex() != nil) != hadIndex {
				return []Event{Violation(EvRemoveFieldTypeDroppedMismatch, map[string]any{
					"ledger":        oracle.LedgerOf(req),
					"key":           rq.GetKey(),
					"target":        rq.GetTargetType().String(),
					"canonical":     canonical,
					"modelHadIdx":   hadIndex,
					"serverDropped": lg.GetDroppedIndex() != nil,
					"modelIndexes":  modelIndexDump(t.modelState.Ledger(oracle.LedgerOf(req))),
					"earlierInBulk": earlierInBulk,
					"wid":           wid,
				})}
			}
			if lg.GetDroppedIndex() != nil && indexes.Canonical(lg.GetDroppedIndex()) != canonical {
				return []Event{Violation(EvRemoveFieldTypeDroppedWrong, map[string]any{
					"ledger":   oracle.LedgerOf(req),
					"key":      rq.GetKey(),
					"returned": indexes.Canonical(lg.GetDroppedIndex()),
					"wid":      wid,
				})}
			}
		}
	}

	t.modelState = res.State
	learnTxStamps(&t.modelState, bulk, logs)
	t.learnLedgerLogSequences(bulk, logs)
	t.recordCommittedBulk(bulk, wid, logs)
	t.learnLedgers(bulk, logs)
	if bulk.IdempotencyKey != "" {
		t.rememberReplayable(*t.records[wid], ReplayEntry{Wid: wid, Key: bulk.IdempotencyKey, Status: "committed", LogSeqs: logSequences(logs)})
	}
	t.recordCheckpoints(logs)

	return nil
}

// learnLedgers records created ledger names and forced transactions.
func (t *Tracker) learnLedgers(bulk oracle.Bulk, logs []*commonpb.Log) {
	for i, req := range bulk.Requests {
		if cl := req.GetCreateLedger(); cl != nil && strings.HasPrefix(cl.GetName(), t.cfg.TrackedPrefix) {
			t.ledgerNames[cl.GetName()] = struct{}{}
		}
		ct := req.GetApply().GetAction().GetCreateTransaction()
		if ct == nil || !ct.GetForce() || i >= len(logs) {
			continue
		}
		id := logs[i].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().GetId()
		if id == 0 {
			continue
		}
		ledger := req.GetApply().GetLedger()
		if t.forcedTxs[ledger] == nil {
			t.forcedTxs[ledger] = map[uint64]bool{}
		}
		t.forcedTxs[ledger][id] = true
	}
}

// validateEmptyCommit: a success with no sequenced log is always a finding.
func validateEmptyCommit(bulk oracle.Bulk) []Event {
	return []Event{Violation(EvEmptyCommit, map[string]any{
		"ledgers": bulkLedgers(bulk),
		"kinds":   requestKinds(bulk),
	})}
}
