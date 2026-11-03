package main

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func TestParseRequiresOutcome(t *testing.T) {
	t.Parallel()

	raw, err := (&servicepb.ApplyRequest{SkipResponse: true}).MarshalVT()
	require.NoError(t, err)
	encoded := base64.StdEncoding.EncodeToString(raw)
	path := filepath.Join(t.TempDir(), "dump.log")
	content := "[batch-dump] seq=1 b64=" + encoded + "\n" +
		"[batch-dump] ticket=2 seq=2 outcome=OK b64=" + encoded + "\n"
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	batches, err := parse(path)
	require.NoError(t, err)
	require.Len(t, batches, 1)
	require.EqualValues(t, 2, batches[0].ticket)
	require.Equal(t, "OK", batches[0].outcome)
}

func applyRequest(key string, reqs ...*servicepb.Request) *servicepb.ApplyRequest {
	return &servicepb.ApplyRequest{Variant: &servicepb.ApplyRequest_Unsigned{
		Unsigned: &servicepb.ApplyBatch{Requests: reqs, IdempotencyKey: key},
	}}
}

// TestReplaySubmitsFreezesFailures replays a single-worker stream in which the
// server froze a rejected keyed bulk: after the source is funded, the same key
// and body still replay INSUFFICIENT_FUNDS, and a different body under the key
// conflicts. Before failure freezing the model predicted OK for the replay.
func TestReplaySubmitsFreezesFailures(t *testing.T) {
	t.Parallel()

	overdraw := oracletest.TxReq("alice", "bob", "USD", 10)
	submits := []batch{
		{ticket: 1, outcome: domain.ErrReasonInsufficientFunds, req: applyRequest("k", overdraw)},
		{ticket: 2, outcome: "OK", req: applyRequest("", oracletest.TxReq("world", "alice", "USD", 10))},
		{ticket: 3, outcome: domain.ErrReasonInsufficientFunds, req: applyRequest("k", overdraw)},
		{ticket: 4, outcome: domain.ErrReasonIdempotencyKeyConflict, req: applyRequest("k", oracletest.TxReq("alice", "bob", "USD", 5))},
		// An unkeyed failure freezes nothing: the same body commits once funded.
		{ticket: 5, outcome: domain.ErrReasonInsufficientFunds, req: applyRequest("", oracletest.TxReq("carol", "bob", "USD", 1))},
		{ticket: 6, outcome: "OK", req: applyRequest("", oracletest.TxReq("world", "carol", "USD", 1))},
		{ticket: 7, outcome: "OK", req: applyRequest("", oracletest.TxReq("carol", "bob", "USD", 1))},
	}

	var out bytes.Buffer
	require.Zero(t, replaySubmits(&out, submits, ""), out.String())

	// Without the freeze the third bulk would commit: the stream above must
	// fail if the server had not frozen it.
	committed := []batch{submits[0], submits[1], {ticket: 3, outcome: "OK", req: applyRequest("k", overdraw)}}
	out.Reset()
	require.Equal(t, 1, replaySubmits(&out, committed, ""), out.String())
}
