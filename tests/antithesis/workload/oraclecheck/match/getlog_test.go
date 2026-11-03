package match

import (
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
	"github.com/stretchr/testify/require"
)

// topLevelLog builds a response log with no Apply arm — the shape ledger
// metadata takes, which carries a sequence but no ledger-local id.
func topLevelLog(seq uint64) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_SavedLedgerMetadata{
			SavedLedgerMetadata: &commonpb.SavedLedgerMetadataLog{},
		}},
	}
}

func TestCommittedLogMatchesTheModelsRow(t *testing.T) {
	t.Parallel()

	gs := committedStateWithSequences(t, 42)
	target := CommittedLogTarget{Ledger: "L", ID: 1, Sequence: 42}
	got := servedRows(gs.Ledger("L"), "L", 1)[0]

	require.True(t, CommittedLogMatches(gs, target, got))

	other := got
	other.Sequence = 43
	require.False(t, CommittedLogMatches(gs, target, other), "a different global sequence is another log")

	other = got
	other.ID = 2
	require.False(t, CommittedLogMatches(gs, target, other), "a different ledger-local id is another log")

	other = got
	other.Kind = "created_transaction"
	require.False(t, CommittedLogMatches(gs, target, other), "the served row is compared field by field")

	require.False(t, CommittedLogMatches(gs, CommittedLogTarget{Ledger: "L", ID: 2, Sequence: 42}, got), "an id the stream does not hold at that sequence matches nothing")
	require.False(t, CommittedLogMatches(gs, CommittedLogTarget{Ledger: "L", ID: 1, Sequence: 43}, got), "a sequence the stream never learned matches nothing")
}

func TestServerLogKindMatchesModelVocabulary(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t,
		oracletest.AddTypeReq("acc"),
		oracletest.TxReq("world", "acc:a", "USD", 1),
	)

	rows := ls.LogRows()
	require.Len(t, rows, 2)
	modelKind := rows[1].Kind

	served := &commonpb.Log{
		Sequence: 2,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: "L",
				Log: &commonpb.LedgerLog{Id: 2, Data: &commonpb.LedgerLogPayload{
					Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
						CreatedTransaction: &commonpb.CreatedTransaction{},
					},
				}},
			},
		}},
	}

	require.Equal(t, modelKind, ServerLogKind(served))
	require.Equal(t, "other", ServerLogKind(topLevelLog(3)))
}
