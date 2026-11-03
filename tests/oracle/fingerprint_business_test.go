package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

// TestBusinessFingerprint pins what BusinessFingerprint leaves out
// (observation state) and what it keeps (every business collection).
func TestBusinessFingerprint(t *testing.T) {
	t.Parallel()

	indexed := applyOK(t, NewGlobalState(), bulkOf(
		oracletest.SetFieldTypeReq(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k", commonpb.MetadataType_METADATA_TYPE_STRING),
		oracletest.CreateIndexReq(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k")),
		oracletest.TxReq("world", "a:1", "USD", 5),
	))
	canonical := indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k"))

	t.Run("equal across observation state", func(t *testing.T) {
		t.Parallel()

		active := indexed
		active.SetIndexActive("L", canonical)

		retyped := applyOK(t, indexed, bulkOf(oracletest.SetFieldTypeReq(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k", commonpb.MetadataType_METADATA_TYPE_INT64)))
		_, open := retyped.Ledger("L").RetypeWindow(canonical)
		require.True(t, open)
		closed := retyped
		closed.CloseRetypeWindow("L", canonical)

		stamped := indexed
		stamped.LearnTxStamps("L", 1, &commonpb.Timestamp{Data: 11}, &commonpb.Timestamp{Data: 12}, nil)
		logID := uint64(len(indexed.Ledger("L").LogIDs()))
		stamped.LearnLogDate("L", logID, &commonpb.Timestamp{Data: 13})
		stamped.LearnLogSequence("L", logID, 42)

		// An at-effective-date revert inherits the learned timestamp: still
		// observation state on both the revert and the original's revertedAt.
		revert := oracletest.RevertReqL("L", 1, true)
		revert.GetApply().GetAction().GetRevertTransaction().AtEffectiveDate = true
		revertedLearned := applyOK(t, stamped, bulkOf(revert))
		revertedPlain := applyOK(t, indexed, bulkOf(revert))
		learnedLater := revertedPlain
		learnedLater.LearnTxStamps("L", 1, nil, nil, &commonpb.Timestamp{Data: 77})
		learnedLater.LearnTxStamps("L", 2, &commonpb.Timestamp{Data: 77}, &commonpb.Timestamp{Data: 78}, nil)

		for _, pair := range []struct {
			name string
			a, b GlobalState
		}{
			{"index readiness", indexed, active},
			{"retype window", retyped, closed},
			{"learned stamps, log date and sequence", indexed, stamped},
			{"revert inheriting a learned timestamp", revertedLearned, revertedPlain},
			{"stamps learned after the revert", revertedPlain, learnedLater},
		} {
			require.NotEqual(t, pair.a.Fingerprint(), pair.b.Fingerprint(), "%s: Fingerprint covers it", pair.name)
			require.Equal(t, pair.a.BusinessFingerprint(), pair.b.BusinessFingerprint(), pair.name)
		}
	})

	t.Run("different across business state", func(t *testing.T) {
		t.Parallel()

		withTS := func(ts uint64) GlobalState {
			req := oracletest.TxReq("world", "b:1", "USD", 1)
			req.GetApply().GetAction().GetCreateTransaction().Timestamp = &commonpb.Timestamp{Data: ts}

			return applyOK(t, indexed, bulkOf(req))
		}
		_, failure := fail(t, indexed, keyedBulk("k", oracletest.TxReq("nobody", "b:1", "USD", 1)), domain.ErrReasonInsufficientFunds)

		for _, other := range []struct {
			name string
			g    GlobalState
		}{
			{"volumes and txs", applyOK(t, indexed, bulkOf(oracletest.TxReq("world", "a:1", "USD", 1)))},
			{"account metadata", applyOK(t, indexed, bulkOf(oracletest.AddAccountMetaReq("a:1", "k", &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: "v"}})))},
			{"chart", applyOK(t, indexed, bulkOf(oracletest.AddTypeReq("t")))},
			{"field types", applyOK(t, indexed, bulkOf(oracletest.SetFieldTypeReq(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "k", commonpb.MetadataType_METADATA_TYPE_STRING)))},
			{"index set", applyOK(t, indexed, bulkOf(oracletest.DropIndexReq(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k"))))},
			{"prepared queries", applyOK(t, indexed, bulkOf(pqReq("q", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, nil)))},
			{"frozen failure", indexed.RecordFailure(keyedBulk("k", oracletest.TxReq("nobody", "b:1", "USD", 1)), failure)},
			{"frozen success", applyOK(t, indexed, keyedBulk("s", oracletest.AddTypeReq("t")))},
			{"checkpoints", applyOK(t, indexed, bulkOf(createCheckpointRequest()))},
			{"lifecycle", applyOK(t, indexed, bulkOf(&servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: "M"}}}))},
		} {
			require.NotEqual(t, indexed.BusinessFingerprint(), other.g.BusinessFingerprint(), other.name)
		}

		// A client-supplied timestamp is business state.
		require.NotEqual(t, withTS(1).BusinessFingerprint(), withTS(2).BusinessFingerprint())
	})
}
