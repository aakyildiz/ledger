package match

import (
	"errors"
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCheckpointReadsRejectLiveMutation(t *testing.T) {
	t.Parallel()
	frozen := buildGlobal(t, oracletest.TxReqL("L", "world", "acc:1", "USD", 5))
	result := frozen.Apply(oracle.Bulk{Requests: []*servicepb.Request{oracletest.RevertReqL("L", 1, true)}})
	require.True(t, result.OK)
	account := &commonpb.Account{Address: "acc:1", Volumes: []*commonpb.AccountVolume{{Asset: "USD", Volumes: &commonpb.VolumesWithBalance{Input: commonpb.MustBigUintFromDecimal("5"), Output: commonpb.MustBigUintFromDecimal("0"), Balance: commonpb.MustSignedBigIntFromDecimal("5")}}}}
	require.True(t, CheckpointAccountReadMatches(frozen, "L", "acc:1", account, true, false))
	account.Volumes[0].Volumes.Output = commonpb.MustBigUintFromDecimal("5")
	account.Volumes[0].Volumes.Balance = commonpb.MustSignedBigIntFromDecimal("0")
	require.True(t, CheckpointAccountReadMatches(result.State, "L", "acc:1", account, true, false))
	require.False(t, CheckpointAccountReadMatches(frozen, "L", "acc:1", account, true, false))
	require.True(t, CheckpointTransactionReadMatches(frozen, "L", 1, serverTxFromRec(frozen.Ledger("L").Txs().Get(0)), true))
	require.False(t, CheckpointTransactionReadMatches(frozen, "L", 1, serverTxFromRec(result.State.Ledger("L").Txs().Get(0)), true))
	require.False(t, CheckpointTransactionReadMatches(frozen, "L", 1, nil, false))
	require.True(t, CheckpointTransactionReadMatches(frozen, "L", 2, nil, false))
}

func TestCheckpointMetadataOnlyAccountWithLaterVolume(t *testing.T) {
	t.Parallel()
	value := &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: "before"}}
	frozen := buildGlobal(t, oracletest.TxReqL("L", "world", "z", "USD", 5), oracletest.AddAccountMetaReq("a", "phase", value))
	account := &commonpb.Account{Address: "a", Metadata: map[string]*commonpb.MetadataValue{"phase": value}}
	require.True(t, CheckpointAccountReadMatches(frozen, "L", "a", account, true, false))
	require.False(t, CheckpointAccountReadMatches(frozen, "L", "a", nil, false, false))
}

func TestCheckpointReadAcceptsEmptyAccountWithoutInventingState(t *testing.T) {
	t.Parallel()
	frozen := oracle.NewGlobalState()
	require.True(t, CheckpointAccountReadMatches(frozen, "L", "empty", &commonpb.Account{Address: "empty"}, true, false))
	require.False(t, CheckpointAccountReadMatches(frozen, "L", "empty", &commonpb.Account{Address: "empty", Metadata: checkpointMetadata("ghost")}, true, false))
}

// Adapted from M/checkpoint_reads_test.go: the status code alone decides,
// now read off an RPCError.
func TestCheckpointMissingRequiresNotFoundStatus(t *testing.T) {
	t.Parallel()
	require.True(t, CheckpointNotFound(RPCErrorOf(status.Error(codes.NotFound, "query checkpoint 7 not found"))))
	require.True(t, CheckpointNotFound(RPCErrorFrom("NotFound", "", nil)))
	require.False(t, CheckpointNotFound(NoError))
	require.False(t, CheckpointNotFound(RPCErrorOf(errors.New("query checkpoint 7 not found"))))
	for _, code := range []codes.Code{codes.Internal, codes.FailedPrecondition, codes.Unavailable, codes.InvalidArgument} {
		require.False(t, CheckpointNotFound(RPCErrorOf(status.Error(code, "query checkpoint 7 not found"))))
	}
}

// Adapted from M/checkpoint_reads_test.go: the Checker's checkpoint
// bookkeeping (frozen snapshot capture at drain) is task 09's; here the frozen
// snapshot and the candidate bases are built directly.
func TestDeletedCheckpointReadAcceptsOnlyFrozenSuccessOrNotFound(t *testing.T) {
	t.Parallel()
	frozen := buildGlobal(t, oracletest.TxReqL("L", "world", "acc:1", "USD", 5))
	created := frozen.Apply(bulkOf(&servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{}}}))
	require.True(t, created.OK, created.Reason)
	require.True(t, created.State.QueryCheckpointExists(1))
	require.False(t, CheckpointReadOutcomeMatches(1, false, RPCErrorFrom("NotFound", "", nil), candidatesOf(created.State)), "a live checkpoint is never missing")
	deleted := created.State.Apply(bulkOf(&servicepb.Request{Type: &servicepb.Request_DeleteQueryCheckpoint{DeleteQueryCheckpoint: &servicepb.DeleteQueryCheckpointRequest{CheckpointId: 1}}}))
	require.True(t, deleted.OK, deleted.Reason)
	bases := candidatesOf(deleted.State)
	account := &commonpb.Account{Address: "acc:1", Volumes: []*commonpb.AccountVolume{{Asset: "USD", Volumes: &commonpb.VolumesWithBalance{Input: commonpb.MustBigUintFromDecimal("5"), Output: commonpb.MustBigUintFromDecimal("0"), Balance: commonpb.MustSignedBigIntFromDecimal("5")}}}}
	require.True(t, CheckpointReadOutcomeMatches(1, CheckpointAccountReadMatches(frozen, "L", "acc:1", account, true, false), NoError, bases), "a replica may still serve the frozen checkpoint after deletion")
	account.Volumes[0].Volumes.Input = commonpb.MustBigUintFromDecimal("9")
	account.Volumes[0].Volumes.Balance = commonpb.MustSignedBigIntFromDecimal("9")
	require.False(t, CheckpointReadOutcomeMatches(1, CheckpointAccountReadMatches(frozen, "L", "acc:1", account, true, false), NoError, bases), "deletion must never permit newer business data")
	require.True(t, CheckpointReadOutcomeMatches(1, false, RPCErrorFrom("NotFound", "", nil), bases))
	require.False(t, CheckpointReadOutcomeMatches(1, true, RPCErrorFrom("Internal", "", nil), bases))
}

// Adapted from M/checkpoint_reads_test.go (see above).
func TestLiveCheckpointTransactionAbsenceMatchesFrozenState(t *testing.T) {
	t.Parallel()
	tx := newModel(t, "L").Apply(bulkOf(oracletest.TxReqL("L", "world", "acc:1", "USD", 5)))
	require.True(t, tx.OK, tx.Reason)
	frozen := tx.State
	created := frozen.Apply(bulkOf(&servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{}}}))
	require.True(t, created.OK, created.Reason)
	live := candidatesOf(created.State)
	notFound := RPCErrorFrom("NotFound", "", nil)
	require.True(t, CheckpointReadOutcomeMatches(1, CheckpointTransactionReadMatches(frozen, "L", 2, nil, false), notFound, live), "NotFound also describes a transaction absent from a live frozen checkpoint")
	require.False(t, CheckpointReadOutcomeMatches(1, CheckpointTransactionReadMatches(frozen, "L", 1, nil, false), notFound, live), "a known frozen transaction cannot disappear while the checkpoint remains live")
	deleted := created.State.Apply(bulkOf(&servicepb.Request{Type: &servicepb.Request_DeleteLedger{DeleteLedger: &servicepb.DeleteLedgerRequest{Name: "L"}}}))
	require.True(t, deleted.OK)
	afterDelete := candidatesOf(deleted.State)
	require.False(t, CheckpointReadOutcomeMatches(1, CheckpointTransactionReadMatches(frozen, "L", 1, nil, false), notFound, afterDelete), "deleting the live ledger cannot make a frozen transaction disappear")
	require.True(t, CheckpointLedgerReadOutcomeMatches(1, "L", CheckpointTransactionReadMatches(frozen, "L", 1, nil, false), notFound, afterDelete), "a ledger-scoped read may say NotFound once the ledger is deleted")
	require.False(t, CheckpointLedgerReadOutcomeMatches(1, "L", false, notFound, live), "the live ledger explains no NotFound")
	require.False(t, CheckpointReadOutcomeMatches(1, CheckpointTransactionReadMatches(frozen, "L", 2, nil, false), RPCErrorFrom("Internal", "", nil), live), "absence does not excuse an unrelated error")
}
