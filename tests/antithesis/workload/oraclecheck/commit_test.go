package oraclecheck_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

// bareLedgerSession: empty tracked prefix, ledger "L" created (oracletest's
// default ledger).
func bareLedgerSession(t *testing.T) *testutil.ScriptedSession {
	t.Helper()
	s := testutil.NewBareSession()
	require.Empty(t, testutil.Findings(s.Write(false, testutil.CreateLedgerReq("L"))))

	return s
}

// commitMutated sends reqs, lets the fake server commit them, applies mutate
// to the response, posts it and returns the findings of the drain.
func commitMutated(t *testing.T, s *testutil.ScriptedSession, mutate func(*servicepb.ApplyResponse), reqs ...*servicepb.Request) []oc.EventID {
	t.Helper()
	tk := s.Begin(false, reqs...)
	out := s.Execute(tk)
	require.True(t, out.OK, "the fake server must commit: %s", out.Reason)
	resp, err := oc.DecodeApplyResponse(out.Response)
	require.NoError(t, err)
	mutate(resp)
	status, evs, ref := s.T.EndWrite(tk, testutil.OK(resp))
	require.Nil(t, ref)
	require.Equal(t, "pending", status)

	return testutil.IDs(testutil.Findings(evs))
}

func ledgerData(resp *servicepb.ApplyResponse, i int) *commonpb.LedgerLogPayload {
	return resp.Logs[i].GetPayload().GetApply().GetLog().GetData()
}

func setMeta(target commonpb.TargetType, key string, ty commonpb.MetadataType) *servicepb.Request {
	return oracletest.SetFieldTypeReq(target, key, ty)
}

func nestedAddType(name string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: "L", Action: &servicepb.LedgerAction{
		Data: &servicepb.LedgerAction_AddAccountType{AddAccountType: &servicepb.AddAccountTypeRequest{AccountType: &commonpb.AccountType{Name: name, Pattern: name + ":{id}"}}},
	}}}}
}

func nestedRemoveType(name string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: "L", Action: &servicepb.LedgerAction{
		Data: &servicepb.LedgerAction_RemoveAccountType{RemoveAccountType: &servicepb.RemoveAccountTypeRequest{Name: name}},
	}}}}
}

func enforcementReq(mode commonpb.ChartEnforcementMode) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_SetDefaultEnforcementMode{SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeLedgerRequest{Ledger: "L", EnforcementMode: mode}}}
}

func deleteAccountMeta(addr, key string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: "L", Action: &servicepb.LedgerAction{
		Data: &servicepb.LedgerAction_DeleteMetadata{DeleteMetadata: &commonpb.DeleteMetadataCommand{
			Target: &commonpb.Target{Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: addr}}}, Key: key,
		}},
	}}}}
}

func TestCrossCheckCommit_EachCheck(t *testing.T) {
	t.Parallel()

	type tc struct {
		id     oc.EventID
		setup  [][]*servicepb.Request
		reqs   []*servicepb.Request
		mutate func(*servicepb.ApplyResponse)
	}
	acct := commonpb.TargetType_TARGET_TYPE_ACCOUNT
	cases := []tc{
		{oc.EvCheckpointCommitOutsideModel, nil, []*servicepb.Request{testutil.CreateCheckpointReq()}, func(r *servicepb.ApplyResponse) {
			r.Logs[0].GetPayload().GetCreatedQueryCheckpoint().CheckpointId++
		}},
		{oc.EvCommittedLogCountMismatch, nil, []*servicepb.Request{oracletest.TxReq("world", "a:1", "USD", 1)}, func(r *servicepb.ApplyResponse) {
			r.Logs = append(r.Logs, r.Logs[0].CloneVT())
		}},
		{oc.EvCommittedLogIdentityMismatch, nil, []*servicepb.Request{oracletest.TxReq("world", "a:1", "USD", 1)}, func(r *servicepb.ApplyResponse) {
			r.Logs[0].GetPayload().GetApply().GetLog().Id++
		}},
		{oc.EvSkippedOrderMismatch, nil, []*servicepb.Request{oracletest.TxReq("world", "a:1", "USD", 1)}, func(r *servicepb.ApplyResponse) {
			r.Logs[0].GetPayload().GetApply().GetLog().Data = &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_OrderSkipped{OrderSkipped: &commonpb.OrderSkippedLog{}}}
		}},
		{oc.EvEnforcementModeMismatch, nil, []*servicepb.Request{enforcementReq(commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT)}, func(r *servicepb.ApplyResponse) {
			ledgerData(r, 0).GetUpdatedDefaultEnforcementMode().EnforcementMode = commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT
		}},
		{oc.EvLifecycleResponseMismatch, nil, []*servicepb.Request{testutil.CreateLedgerReq("L2")}, func(r *servicepb.ApplyResponse) {
			r.Logs[0].GetPayload().GetCreateLedger().Name = "other"
		}},
		{oc.EvLifecycleResponseMismatch, nil, []*servicepb.Request{testutil.MaintenanceReq(true)}, func(r *servicepb.ApplyResponse) {
			r.Logs[0].GetPayload().GetSetMaintenanceMode().Enabled = false
		}},
		{oc.EvPostCommitVolumeMismatch, nil, []*servicepb.Request{oracletest.TxReq("world", "a:1", "USD", 1)}, func(r *servicepb.ApplyResponse) {
			ledgerData(r, 0).GetCreatedTransaction().GetTransaction().GetPostCommitVolumes().GetVolumesByAccount()["a:1"].Volumes[0].Volumes.Input = commonpb.NewBigUintFromUint256(nil)
		}},
		{oc.EvResponseMetadataMismatch, nil, []*servicepb.Request{oracletest.AddAccountMetaReq("a:1", "k", commonpb.NewStringValue("v"))}, func(r *servicepb.ApplyResponse) {
			ledgerData(r, 0).GetSavedMetadata().Metadata["k"] = commonpb.NewStringValue("w")
		}},
		{oc.EvRevertTxIDMismatch, [][]*servicepb.Request{{oracletest.TxReq("world", "a:1", "USD", 3)}}, []*servicepb.Request{oracletest.RevertReqL("L", 1, true)}, func(r *servicepb.ApplyResponse) {
			ledgerData(r, 0).GetRevertedTransaction().GetRevertTransaction().Id++
		}},
		{oc.EvRevertedTxIDMismatch, [][]*servicepb.Request{{oracletest.TxReq("world", "a:1", "USD", 3)}}, []*servicepb.Request{oracletest.RevertReqL("L", 1, true)}, func(r *servicepb.ApplyResponse) {
			ledgerData(r, 0).GetRevertedTransaction().RevertedTransactionId++
		}},
		{oc.EvRevertPostingsMismatch, [][]*servicepb.Request{{oracletest.TxReq("world", "a:1", "USD", 3)}}, []*servicepb.Request{oracletest.RevertReqL("L", 1, true)}, func(r *servicepb.ApplyResponse) {
			ledgerData(r, 0).GetRevertedTransaction().GetRevertTransaction().Postings[0].Asset = "EUR"
		}},
		{oc.EvTxIDMismatch, nil, []*servicepb.Request{oracletest.TxReq("world", "a:1", "USD", 1)}, func(r *servicepb.ApplyResponse) {
			ledgerData(r, 0).GetCreatedTransaction().GetTransaction().Id++
		}},
		{oc.EvTxReferenceMismatch, nil, []*servicepb.Request{oracletest.TxReqRefL("L", "ref-1", "world", "a:1", "USD", 1)}, func(r *servicepb.ApplyResponse) {
			ledgerData(r, 0).GetCreatedTransaction().GetTransaction().Reference = "ref-2"
		}},
		{oc.EvTxPostingsMismatch, nil, []*servicepb.Request{oracletest.TxReq("world", "a:1", "USD", 1)}, func(r *servicepb.ApplyResponse) {
			ledgerData(r, 0).GetCreatedTransaction().GetTransaction().Postings[0].Destination = "a:2"
		}},
		{oc.EvTxAccountMetadataMismatch, nil, []*servicepb.Request{oracletest.TxReq("world", "a:1", "USD", 1)}, func(r *servicepb.ApplyResponse) {
			ledgerData(r, 0).GetCreatedTransaction().AccountMetadata = map[string]*commonpb.MetadataMap{"a:1": {Values: map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue("v")}}}
		}},
		{oc.EvDeleteMetadataMismatch, [][]*servicepb.Request{{oracletest.AddAccountMetaReq("a:1", "k", commonpb.NewStringValue("v"))}}, []*servicepb.Request{deleteAccountMeta("a:1", "k")}, func(r *servicepb.ApplyResponse) {
			ledgerData(r, 0).GetDeletedMetadata().Key = "other"
		}},
		{oc.EvChartResponseMismatch, nil, []*servicepb.Request{oracletest.AddTypeReq("T")}, func(r *servicepb.ApplyResponse) {
			ledgerData(r, 0).GetAddedAccountType().AccountType.Pattern = "x:{id}"
		}},
		{oc.EvChartResponseMismatch, nil, []*servicepb.Request{nestedAddType("U")}, func(r *servicepb.ApplyResponse) {
			ledgerData(r, 0).GetAddedAccountType().AccountType.Pattern = "x:{id}"
		}},
		{oc.EvChartResponseMismatch, [][]*servicepb.Request{{oracletest.AddTypeReq("V")}}, []*servicepb.Request{oracletest.RemoveTypeReq("V")}, func(r *servicepb.ApplyResponse) {
			ledgerData(r, 0).GetRemovedAccountType().Name = "W"
		}},
		{oc.EvChartResponseMismatch, [][]*servicepb.Request{{oracletest.AddTypeReq("X")}}, []*servicepb.Request{nestedRemoveType("X")}, func(r *servicepb.ApplyResponse) {
			ledgerData(r, 0).GetRemovedAccountType().Name = "W"
		}},
		{oc.EvSetFieldTypeMismatch, nil, []*servicepb.Request{setMeta(acct, "k", commonpb.MetadataType_METADATA_TYPE_STRING)}, func(r *servicepb.ApplyResponse) {
			ledgerData(r, 0).GetSetMetadataFieldType().Type = commonpb.MetadataType_METADATA_TYPE_INT64
		}},
		{oc.EvRemoveFieldTypeMismatch, [][]*servicepb.Request{{setMeta(acct, "k", commonpb.MetadataType_METADATA_TYPE_STRING)}}, []*servicepb.Request{oracletest.RemoveFieldTypeReq(acct, "k")}, func(r *servicepb.ApplyResponse) {
			ledgerData(r, 0).GetRemovedMetadataFieldType().Key = "other"
		}},
		{oc.EvRemoveFieldTypeDroppedMismatch, [][]*servicepb.Request{{setMeta(acct, "k", commonpb.MetadataType_METADATA_TYPE_STRING)}}, []*servicepb.Request{oracletest.RemoveFieldTypeReq(acct, "k")}, func(r *servicepb.ApplyResponse) {
			ledgerData(r, 0).GetRemovedMetadataFieldType().DroppedIndex = indexes.MetadataID(acct, "k")
		}},
		{oc.EvRemoveFieldTypeDroppedWrong, [][]*servicepb.Request{{setMeta(acct, "k", commonpb.MetadataType_METADATA_TYPE_STRING)}, {oracletest.CreateIndexReq(indexes.MetadataID(acct, "k"))}}, []*servicepb.Request{oracletest.RemoveFieldTypeReq(acct, "k")}, func(r *servicepb.ApplyResponse) {
			ledgerData(r, 0).GetRemovedMetadataFieldType().DroppedIndex = indexes.MetadataID(acct, "j")
		}},
	}

	for _, c := range cases {
		t.Run(string(c.id), func(t *testing.T) {
			t.Parallel()
			s := bareLedgerSession(t)
			for _, reqs := range c.setup {
				require.Empty(t, testutil.Findings(s.Write(false, reqs...)))
			}
			// Unmutated, the same shape is accepted.
			before := s.T.Model().Fingerprint()
			ids := commitMutated(t, s, c.mutate, c.reqs...)
			require.Equal(t, []oc.EventID{c.id}, ids)
			require.Equal(t, before, s.T.Model().Fingerprint(), "a rejected response must not advance the model")
		})
	}
}

func TestCrossCheckCommit_ModelRejectsCommittedBulk(t *testing.T) {
	t.Parallel()
	s := bareLedgerSession(t)
	good := s.Begin(false, oracletest.TxReq("world", "a:1", "USD", 1))
	bad := s.Begin(false, oracletest.TxReq("b:1", "a:1", "USD", 1)) // b:1 has no funds
	out := s.Execute(good)
	_, _, ref := s.T.EndWrite(good, out)
	require.Nil(t, ref)
	// The server claims the unfunded bulk committed, with fresh sequences.
	resp, _ := oc.DecodeApplyResponse(out.Response)
	for _, l := range resp.Logs {
		l.Sequence += 100
	}
	_, evs, ref := s.T.EndWrite(bad, testutil.OK(resp))
	require.Nil(t, ref)
	require.Equal(t, []oc.EventID{oc.EvModelRejectsCommittedBulk}, testutil.IDs(testutil.Findings(evs)))
}

func TestCrossCheckCommit_EmptyCommit(t *testing.T) {
	t.Parallel()
	s := bareLedgerSession(t)
	tk := s.Begin(false, oracletest.TxReq("world", "a:1", "USD", 1))
	status, evs, ref := s.T.EndWrite(tk, testutil.OK(&servicepb.ApplyResponse{}))
	require.Nil(t, ref)
	require.Equal(t, "validated", status)
	require.Equal(t, []oc.EventID{oc.EvEmptyCommit}, testutil.IDs(testutil.Findings(evs)))
}

// Every accepted shape of the table above, unmutated, advances the model and
// fills the side tables.
func TestCrossCheckCommit_AcceptsSimulatedCommits(t *testing.T) {
	t.Parallel()
	s := bareLedgerSession(t)
	acct := commonpb.TargetType_TARGET_TYPE_ACCOUNT
	bulks := [][]*servicepb.Request{
		{oracletest.TxReqRefL("L", "r1", "world", "a:1", "USD", 5), oracletest.AddAccountMetaReq("a:1", "k", commonpb.NewStringValue("v"))},
		{oracletest.RevertReqL("L", 1, true)},
		{oracletest.AddTypeReq("T"), nestedAddType("U"), enforcementReq(commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT)},
		{setMeta(acct, "k", commonpb.MetadataType_METADATA_TYPE_STRING)},
		{oracletest.CreateIndexReq(indexes.MetadataID(acct, "k"))},
		{oracletest.RemoveFieldTypeReq(acct, "k")},
		{deleteAccountMeta("a:1", "k")},
		{testutil.CreateCheckpointReq()},
		{testutil.MaintenanceReq(true)},
		{testutil.MaintenanceReq(false)},
		{testutil.DeleteLedgerReq("L")},
	}
	for i, b := range bulks {
		require.Empty(t, testutil.Findings(s.Write(true, b...)), "bulk %d", i)
	}
	require.EqualValues(t, len(bulks)+1, s.T.Version())
	_, ok := s.T.FrozenCheckpoint(1)
	require.True(t, ok)
	require.NotEmpty(t, s.T.CommittedBulks())
	require.NotEmpty(t, s.T.LedgerIdentities())
	require.Len(t, s.T.Replayable(), len(bulks))
}
