package oraclecheck

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// checkpointOrdersMatch checks the independent lifecycle prediction against
// the actual global log payloads. No snapshots are published on disagreement.
func checkpointOrdersMatch(bulk oracle.Bulk, orders []oracle.OrderResult, logs []*commonpb.Log) bool {
	for i, req := range bulk.Requests {
		if req.GetCreateQueryCheckpoint() == nil && req.GetDeleteQueryCheckpoint() == nil && req.GetSetQueryCheckpointSchedule() == nil && req.GetDeleteQueryCheckpointSchedule() == nil {
			continue
		}
		if i >= len(orders) || i >= len(logs) || logs[i].GetSequence() == 0 {
			return false
		}
		payload := logs[i].GetPayload()
		switch {
		case req.GetCreateQueryCheckpoint() != nil:
			cp := payload.GetCreatedQueryCheckpoint()
			if cp == nil || cp.GetCheckpointId() != orders[i].CheckpointID || cp.GetMaxSequence() != logs[i].GetSequence()-1 {
				return false
			}
		case req.GetDeleteQueryCheckpoint() != nil:
			cp := payload.GetDeletedQueryCheckpoint()
			if cp == nil || cp.GetCheckpointId() != orders[i].CheckpointID {
				return false
			}
		case req.GetSetQueryCheckpointSchedule() != nil:
			schedule := payload.GetSetQueryCheckpointSchedule()
			if schedule == nil || schedule.GetCron() != req.GetSetQueryCheckpointSchedule().GetCron() {
				return false
			}
		case req.GetDeleteQueryCheckpointSchedule() != nil:
			if payload.GetDeleteQueryCheckpointSchedule() == nil {
				return false
			}
		}
	}

	return true
}
