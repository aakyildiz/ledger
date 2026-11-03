// Package testutil builds synthetic server responses and scripted protocol
// sessions for oraclecheck tests (DESIGN §10).
package testutil

import (
	"fmt"
	"slices"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// SimulateCommit predicts bulk on state and renders the ApplyResponse a
// correct server returns for it: log i carries sequence firstSeq+i and date
// stamps+firstSeq+i (microseconds). It panics if the model rejects the bulk.
func SimulateCommit(state oracle.GlobalState, bulk oracle.Bulk, firstSeq uint64, stamps uint64) *servicepb.ApplyResponse {
	res := state.Apply(bulk)
	if !res.OK {
		panic(fmt.Sprintf("SimulateCommit: the model rejects the bulk: %s", res.Reason))
	}

	resp := &servicepb.ApplyResponse{}
	for i, req := range bulk.Requests {
		seq := firstSeq + uint64(i)
		date := &commonpb.Timestamp{Data: stamps + seq}
		resp.Logs = append(resp.Logs, &commonpb.Log{Sequence: seq, Payload: logPayload(state, req, res.Orders[i], seq, date)})
	}

	return resp
}

func pcvOf(m map[oracle.VolumeKey]oracle.VolumePair) *commonpb.PostCommitVolumes {
	if m == nil {
		return nil
	}
	keys := make([]oracle.VolumeKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, oracle.CompareVolumeKey)
	out := &commonpb.PostCommitVolumes{VolumesByAccount: map[string]*commonpb.VolumesByAssets{}}
	for _, k := range keys {
		vp := m[k]
		by := out.VolumesByAccount[k.Address]
		if by == nil {
			by = &commonpb.VolumesByAssets{}
			out.VolumesByAccount[k.Address] = by
		}
		by.Volumes = append(by.Volumes, &commonpb.VolumeEntry{Asset: k.Asset, Color: k.Color, Volumes: &commonpb.Volumes{
			Input:  commonpb.NewBigUintFromUint256(&vp.Input),
			Output: commonpb.NewBigUintFromUint256(&vp.Output),
		}})
	}

	return out
}

func schemaOf(commands []*commonpb.SetMetadataFieldTypeCommand) *commonpb.MetadataSchema {
	if len(commands) == 0 {
		return nil
	}
	schema := &commonpb.MetadataSchema{}
	for _, cmd := range commands {
		field := &commonpb.MetadataFieldSchema{Type: cmd.GetType()}
		switch cmd.GetTargetType() {
		case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
			if schema.AccountFields == nil {
				schema.AccountFields = map[string]*commonpb.MetadataFieldSchema{}
			}
			schema.AccountFields[cmd.GetKey()] = field
		case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
			if schema.TransactionFields == nil {
				schema.TransactionFields = map[string]*commonpb.MetadataFieldSchema{}
			}
			schema.TransactionFields[cmd.GetKey()] = field
		case commonpb.TargetType_TARGET_TYPE_LEDGER:
			if schema.LedgerFields == nil {
				schema.LedgerFields = map[string]*commonpb.MetadataFieldSchema{}
			}
			schema.LedgerFields[cmd.GetKey()] = field
		}
	}

	return schema
}

func logPayload(before oracle.GlobalState, req *servicepb.Request, order oracle.OrderResult, seq uint64, date *commonpb.Timestamp) *commonpb.LogPayload {
	if order.PreparedQueryLog != nil {
		return order.PreparedQueryLog
	}
	switch r := req.GetType().(type) {
	case *servicepb.Request_CreateLedger:
		cl := r.CreateLedger
		types := map[string]*commonpb.AccountType{}
		for name, ty := range cl.GetAccountTypes() {
			c := ty.CloneVT()
			c.Name = name
			types[name] = c
		}
		if len(types) == 0 {
			types = nil
		}

		return &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{
			Name: cl.GetName(), CreatedAt: date, MetadataSchema: schemaOf(cl.GetInitialSchema()), Mode: cl.GetMode(),
			MirrorSource: cl.GetMirrorSource(), AccountTypes: types, DefaultEnforcementMode: cl.GetDefaultEnforcementMode(),
			Id: uint32(seq), Metadata: cl.GetMetadata(),
		}}}
	case *servicepb.Request_DeleteLedger:
		return &commonpb.LogPayload{Type: &commonpb.LogPayload_DeleteLedger{DeleteLedger: &commonpb.DeletedLedgerLog{Name: r.DeleteLedger.GetName(), DeletedAt: date}}}
	case *servicepb.Request_PromoteLedger:
		return &commonpb.LogPayload{Type: &commonpb.LogPayload_PromoteLedger{PromoteLedger: &commonpb.PromotedLedgerLog{Name: r.PromoteLedger.GetLedger()}}}
	case *servicepb.Request_SetMaintenanceMode:
		return &commonpb.LogPayload{Type: &commonpb.LogPayload_SetMaintenanceMode{SetMaintenanceMode: &commonpb.SetMaintenanceModeLog{Enabled: r.SetMaintenanceMode.GetEnabled()}}}
	case *servicepb.Request_CreateQueryCheckpoint:
		return &commonpb.LogPayload{Type: &commonpb.LogPayload_CreatedQueryCheckpoint{CreatedQueryCheckpoint: &commonpb.CreatedQueryCheckpointLog{
			CheckpointId: order.CheckpointID, MaxSequence: seq - 1, CreatedAt: date,
		}}}
	case *servicepb.Request_DeleteQueryCheckpoint:
		return &commonpb.LogPayload{Type: &commonpb.LogPayload_DeletedQueryCheckpoint{DeletedQueryCheckpoint: &commonpb.DeletedQueryCheckpointLog{CheckpointId: order.CheckpointID}}}
	case *servicepb.Request_SetQueryCheckpointSchedule:
		return &commonpb.LogPayload{Type: &commonpb.LogPayload_SetQueryCheckpointSchedule{SetQueryCheckpointSchedule: &commonpb.SetQueryCheckpointScheduleLog{Cron: r.SetQueryCheckpointSchedule.GetCron()}}}
	case *servicepb.Request_DeleteQueryCheckpointSchedule:
		return &commonpb.LogPayload{Type: &commonpb.LogPayload_DeleteQueryCheckpointSchedule{DeleteQueryCheckpointSchedule: &commonpb.DeletedQueryCheckpointScheduleLog{}}}
	case *servicepb.Request_SaveLedgerMetadata:
		return &commonpb.LogPayload{Type: &commonpb.LogPayload_SavedLedgerMetadata{SavedLedgerMetadata: &commonpb.SavedLedgerMetadataLog{
			Ledger: r.SaveLedgerMetadata.GetLedger(), Metadata: r.SaveLedgerMetadata.GetMetadata(),
		}}}
	case *servicepb.Request_DeleteLedgerMetadata:
		return &commonpb.LogPayload{Type: &commonpb.LogPayload_DeletedLedgerMetadata{DeletedLedgerMetadata: &commonpb.DeletedLedgerMetadataLog{
			Ledger: r.DeleteLedgerMetadata.GetLedger(), Key: r.DeleteLedgerMetadata.GetKey(),
		}}}
	}

	data := ledgerData(before, req, order, date)

	return &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		LedgerName: oracle.LedgerOf(req),
		Log:        &commonpb.LedgerLog{Id: order.LogID, Date: date, Data: data},
	}}}
}

func ledgerData(before oracle.GlobalState, req *servicepb.Request, order oracle.OrderResult, date *commonpb.Timestamp) *commonpb.LedgerLogPayload {
	if order.Skipped != nil {
		return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_OrderSkipped{OrderSkipped: order.Skipped}}
	}
	if order.Revert != nil {
		return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RevertedTransaction{RevertedTransaction: &commonpb.RevertedTransaction{
			RevertedTransactionId: order.Revert.RevertedID(),
			RevertTransaction: &commonpb.Transaction{
				Id: order.TxID, Postings: order.Revert.Postings(), Timestamp: date, InsertedAt: date, UpdatedAt: date,
				RevertsTransaction: order.Revert.RevertedID(), PostCommitVolumes: pcvOf(order.PCV),
			},
		}}}
	}
	if mode := enforcementMode(req); mode != nil {
		return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_UpdatedDefaultEnforcementMode{UpdatedDefaultEnforcementMode: &commonpb.UpdatedDefaultEnforcementModeLog{EnforcementMode: *mode}}}
	}
	switch r := req.GetType().(type) {
	case *servicepb.Request_Apply:
		a := r.Apply.GetAction()
		switch {
		case a.GetCreateTransaction() != nil:
			ct := a.GetCreateTransaction()
			ts := ct.GetTimestamp()
			if ts == nil {
				ts = date
			}
			var meta map[string]*commonpb.MetadataValue
			if order.Meta != nil {
				meta = order.Meta.Saved()
			}

			return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{
				Transaction: &commonpb.Transaction{
					Id: order.TxID, Postings: ct.GetPostings(), Reference: ct.GetReference(), Metadata: meta,
					Timestamp: ts, InsertedAt: date, UpdatedAt: date, PostCommitVolumes: pcvOf(order.PCV),
				},
				AccountMetadata: ct.GetAccountMetadata(),
			}}}
		case a.GetAddMetadata() != nil:
			var meta map[string]*commonpb.MetadataValue
			if order.Meta != nil {
				meta = order.Meta.Saved()
			}

			return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_SavedMetadata{SavedMetadata: &commonpb.SavedMetadata{Target: a.GetAddMetadata().GetTarget(), Metadata: meta}}}
		case a.GetDeleteMetadata() != nil:
			return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_DeletedMetadata{DeletedMetadata: &commonpb.DeletedMetadata{Target: a.GetDeleteMetadata().GetTarget(), Key: a.GetDeleteMetadata().GetKey()}}}
		case a.GetAddAccountType() != nil:
			return addedType(a.GetAddAccountType().GetAccountType())
		case a.GetRemoveAccountType() != nil:
			return removedType(a.GetRemoveAccountType().GetName())
		}
	case *servicepb.Request_AddAccountType:
		return addedType(r.AddAccountType.GetAccountType())
	case *servicepb.Request_RemoveAccountType:
		return removedType(r.RemoveAccountType.GetName())
	case *servicepb.Request_SetMetadataFieldType:
		s := r.SetMetadataFieldType

		return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_SetMetadataFieldType{SetMetadataFieldType: &commonpb.SetMetadataFieldTypeLog{TargetType: s.GetTargetType(), Key: s.GetKey(), Type: s.GetType()}}}
	case *servicepb.Request_RemoveMetadataFieldType:
		s := r.RemoveMetadataFieldType
		log := &commonpb.RemovedMetadataFieldTypeLog{TargetType: s.GetTargetType(), Key: s.GetKey()}
		id := indexes.MetadataID(s.GetTargetType(), s.GetKey())
		if exists, _ := before.Ledger(s.GetLedger()).IndexState(indexes.Canonical(id)); exists {
			log.DroppedIndex = id
		}

		return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RemovedMetadataFieldType{RemovedMetadataFieldType: log}}
	case *servicepb.Request_CreateIndex:
		return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreateIndex{CreateIndex: &commonpb.CreatedIndexLog{Id: r.CreateIndex.GetId()}}}
	case *servicepb.Request_DropIndex:
		return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_DropIndex{DropIndex: &commonpb.DroppedIndexLog{Id: r.DropIndex.GetId()}}}
	}

	return &commonpb.LedgerLogPayload{}
}

func addedType(t *commonpb.AccountType) *commonpb.LedgerLogPayload {
	return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_AddedAccountType{AddedAccountType: &commonpb.AddedAccountTypeLog{AccountType: t}}}
}

func removedType(name string) *commonpb.LedgerLogPayload {
	return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RemovedAccountType{RemovedAccountType: &commonpb.RemovedAccountTypeLog{Name: name}}}
}

func enforcementMode(req *servicepb.Request) *commonpb.ChartEnforcementMode {
	if s := req.GetSetDefaultEnforcementMode(); s != nil {
		m := s.GetEnforcementMode()

		return &m
	}
	if s := req.GetApply().GetAction().GetSetDefaultEnforcementMode(); s != nil {
		m := s.GetEnforcementMode()

		return &m
	}

	return nil
}

// ---- builders ----

// CreateLedgerReq creates a ledger.
func CreateLedgerReq(name string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: name}}}
}

// DeleteLedgerReq deletes a ledger.
func DeleteLedgerReq(name string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_DeleteLedger{DeleteLedger: &servicepb.DeleteLedgerRequest{Name: name}}}
}

// MaintenanceReq toggles maintenance mode.
func MaintenanceReq(enabled bool) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_SetMaintenanceMode{SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{Enabled: enabled}}}
}

// CreateCheckpointReq creates a query checkpoint.
func CreateCheckpointReq() *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{}}}
}

// Posting builds a posting.
func Posting(src, dst, asset string, amount uint64) *commonpb.Posting {
	return &commonpb.Posting{Source: src, Destination: dst, Asset: asset, Amount: commonpb.NewUint256FromUint64(amount)}
}

// TxReq creates a transaction on ledger.
func TxReq(ledger, ref, src, dst, asset string, amount uint64) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
		Ledger: ledger,
		Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: &servicepb.CreateTransactionPayload{
			Postings: []*commonpb.Posting{Posting(src, dst, asset, amount)}, Reference: ref,
		}}},
	}}}
}

// EmptyTxReq is a create with no postings (VALIDATION at admission).
func EmptyTxReq(ledger string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
		Ledger: ledger,
		Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: &servicepb.CreateTransactionPayload{}}},
	}}}
}

// RevertReq reverts a transaction.
func RevertReq(ledger string, id uint64) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
		Ledger: ledger,
		Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_RevertTransaction{RevertTransaction: &servicepb.RevertTransactionPayload{TransactionId: id}}},
	}}}
}

// AccountMetaReq sets one string metadata on an account.
func AccountMetaReq(ledger, addr, key, value string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
		Ledger: ledger,
		Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{AddMetadata: &commonpb.SaveMetadataCommand{
			Target:   &commonpb.Target{Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: addr}}},
			Metadata: map[string]*commonpb.MetadataValue{key: commonpb.NewStringValue(value)},
		}}},
	}}}
}

// AddTypeReq adds an account type (top-level form).
func AddTypeReq(ledger, name, pattern string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_AddAccountType{AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
		Ledger: ledger, AccountType: &commonpb.AccountType{Name: name, Pattern: pattern},
	}}}
}

// RequestJSON renders requests as protojson.
func RequestJSON(reqs ...*servicepb.Request) []string {
	out := make([]string, 0, len(reqs))
	for _, r := range reqs {
		b, err := protojson.Marshal(r)
		if err != nil {
			panic(err)
		}
		out = append(out, string(b))
	}

	return out
}

// ---- outcomes ----

// OK renders a success outcome around a response.
func OK(resp *servicepb.ApplyResponse) oraclecheck.Outcome {
	b, err := protojson.Marshal(resp)
	if err != nil {
		panic(err)
	}

	return oraclecheck.Outcome{OK: true, Response: b}
}

// Fail renders a definitive (or not) server error outcome.
func Fail(code, reason string) oraclecheck.Outcome {
	return oraclecheck.Outcome{Code: code, Reason: reason, Message: reason}
}

// Unknown renders the driver's no-answer outcome.
func Unknown(why string) oraclecheck.Outcome { return oraclecheck.Outcome{Unknown: why} }
