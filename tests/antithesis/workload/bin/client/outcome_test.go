package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const fixtureDir = "../../../hegel/protocol-fixtures/core"

func readOutcomeFixture(t *testing.T, shape string) json.RawMessage {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(fixtureDir, "outcome."+shape+".json"))
	require.NoError(t, err)
	var f struct {
		Response json.RawMessage `json:"response"`
	}
	require.NoError(t, json.Unmarshal(b, &f))

	return f.Response
}

// The client writes every outcome shape exactly as the core fixtures (the
// "unknown" shape is the driver's own and has no client counterpart).
func TestOutcomeJSONMatchesFixtures(t *testing.T) {
	t.Parallel()
	for _, shape := range []string{"ok-unary", "ok-stream", "err-definitive", "err-nondefinitive", "err-local"} {
		want := readOutcomeFixture(t, shape)
		var out outcome
		dec := json.NewDecoder(bytesReader(want))
		dec.DisallowUnknownFields()
		require.NoError(t, dec.Decode(&out), shape)
		got, err := json.Marshal(out)
		require.NoError(t, err)
		require.JSONEq(t, string(want), string(got), shape)
	}
}

// Outcomes built by the client's own conversion equal the fixtures.
func TestOutcomeConstructionMatchesFixtures(t *testing.T) {
	t.Parallel()
	st, err := status.New(codes.FailedPrecondition, "insufficient funds: account world:COIN").WithDetails(&errdetails.ErrorInfo{
		Reason:   "INSUFFICIENT_FUNDS",
		Metadata: map[string]string{"account": "t-3:7", "asset": "COIN"},
	})
	require.NoError(t, err)
	out := errOutcome(st.Err())
	out.Peer, out.ElapsedMS = "ledger-2:8888", 21
	got, err := json.Marshal(out)
	require.NoError(t, err)
	require.JSONEq(t, string(readOutcomeFixture(t, "err-definitive")), string(got))

	st, err = status.New(codes.Unavailable, "proposal is stale, retry").WithDetails(&errdetails.ErrorInfo{Reason: "STALE_PROPOSAL"})
	require.NoError(t, err)
	out = errOutcome(st.Err())
	out.Peer, out.ElapsedMS = "ledger-0:8888", 9
	got, err = json.Marshal(out)
	require.NoError(t, err)
	require.JSONEq(t, string(readOutcomeFixture(t, "err-nondefinitive")), string(got))

	out = errOutcome(status.Error(codes.ResourceExhausted, "grpc: received message larger than max (70000000 vs. 67108864)"))
	out.ElapsedMS = 412
	got, err = json.Marshal(out)
	require.NoError(t, err)
	require.JSONEq(t, string(readOutcomeFixture(t, "err-local")), string(got))

	// A server ResourceExhausted with a reason is never local.
	st, err = status.New(codes.ResourceExhausted, "grpc: received message larger than max").WithDetails(&errdetails.ErrorInfo{Reason: "WRITES_BLOCKED_DISK_FULL"})
	require.NoError(t, err)
	require.False(t, errOutcome(st.Err()).Local)
}

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }
