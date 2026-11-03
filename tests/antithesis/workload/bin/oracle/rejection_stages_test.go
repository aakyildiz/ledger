package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Stage/freeze conformance (DESIGN §10): for every rejection fixture, the
// model's stage, freeze decision and UncommittedReasons membership must
// predict the server's audit presence, replay outcome and conflict outcome.
// Fixture format: oraclecheck/testdata/rejections/README.md.

const rejectionsDir = "../../oraclecheck/testdata/rejections"

type rejectionFixture struct {
	Name         string            `json:"name"`
	Setup        []json.RawMessage `json:"setup"`
	Requests     []json.RawMessage `json:"requests"`
	ExpectReason string            `json:"expectReason"`
}

const rejectionKey = "k1"

// differentBody builds a different body under the same key: the fixture's
// requests plus a trailing addMetadata on the last ledger. A cluster-scoped
// fixture names no ledger (an addMetadata on "" is an admission VALIDATION
// that never reaches the idempotency check), so it gets a leading
// createLedger instead (00d).
func differentBody(reqs []*servicepb.Request) []*servicepb.Request {
	ledger := lastLedger(reqs)
	if ledger == "" {
		return append([]*servicepb.Request{actions.CreateLedgerAction("o-rej-tail", nil)}, reqs...)
	}

	return append(append([]*servicepb.Request(nil), reqs...),
		actions.SaveAccountMetadataAction(ledger, "users:own-x", map[string]string{"own-probe": "1"}))
}

func lastLedger(reqs []*servicepb.Request) string {
	for i := len(reqs) - 1; i >= 0; i-- {
		if l := oracleLedgerOf(reqs[i]); l != "" {
			return l
		}
	}

	return ""
}

func oracleLedgerOf(r *servicepb.Request) (l string) {
	defer func() {
		if recover() != nil {
			l = ""
		}
	}()

	return oracle.LedgerOf(r)
}

// auditFailuresForKey waits until a marker bulk's success entry is listed
// (the audit projection is eventually consistent and ordered), then counts
// the failure entries carrying key for a bulk of orders orders (the
// different body's conflict, one order longer, is audited too).
func auditFailuresForKey(t *testing.T, ctx context.Context, client servicepb.BucketServiceClient, key, markerKey string, orders int) int {
	t.Helper()
	var entries []*auditpb.AuditEntry
	require.Eventually(t, func() bool {
		var err error
		entries, err = actions.ListAuditEntries(ctx, client, false)
		if err != nil {
			return false
		}
		for _, e := range entries {
			if e.GetIdempotency().GetKey() == markerKey && e.GetSuccess() != nil {
				return true
			}
		}

		return false
	}, 20*time.Second, 20*time.Millisecond)
	n := 0
	for _, e := range entries {
		if e.GetIdempotency().GetKey() == key && e.GetFailure() != nil && int(e.GetOrderCount()) == orders {
			n++
		}
	}

	return n
}

func TestRejectionStages(t *testing.T) {
	if testing.Short() {
		t.Skip("in-process server")
	}
	paths, err := filepath.Glob(filepath.Join(rejectionsDir, "*.json"))
	require.NoError(t, err)
	require.NotEmpty(t, paths)

	for _, p := range paths {
		b, err := os.ReadFile(p)
		require.NoError(t, err)
		var fx rejectionFixture
		require.NoError(t, json.Unmarshal(b, &fx), p)
		t.Run(strings.TrimSuffix(filepath.Base(p), ".json"), func(t *testing.T) {
			setup, err := decodeRequests(fx.Setup)
			require.NoError(t, err)
			reqs, err := decodeRequests(fx.Requests)
			require.NoError(t, err)
			require.NotEmpty(t, reqs)

			ctx, client := singleNodeServer(t, "oracle-rejections")
			// The in-process server runs with the default query-checkpoint
			// limit (10, cmd/server/server.go); the model must agree for
			// CHECKPOINT_LIMIT_REACHED (task 09's request to 99).
			model := oracle.NewGlobalState().WithQueryCheckpointLimit(10)
			for i, r := range setup {
				_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", r))
				require.NoError(t, err, "setup %d", i)
				res := model.Apply(oracle.Bulk{Requests: []*servicepb.Request{r}})
				require.True(t, res.OK, "setup %d: model %s", i, res.Reason)
				model = res.State
			}

			// The keyed rejection.
			bulk := oracle.Bulk{Requests: reqs, IdempotencyKey: rejectionKey}
			mres := model.Apply(bulk)
			require.False(t, mres.OK, "the model must reject the fixture")
			first := outcomeOf(client.Apply(ctx, servicepb.UnsignedApplyRequest(rejectionKey, reqs...)))
			require.False(t, first.OK)
			require.Equal(t, fx.ExpectReason, first.Reason, "server reason")
			require.Equal(t, fx.ExpectReason, mres.Reason, "model reason")

			// Replay of the same body, then a different body under the key.
			replay := outcomeOf(client.Apply(ctx, servicepb.UnsignedApplyRequest(rejectionKey, reqs...)))
			diff := differentBody(reqs)
			conflict := outcomeOf(client.Apply(ctx, servicepb.UnsignedApplyRequest(rejectionKey, diff...)))

			marker := "marker"
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest(marker, &servicepb.Request{Type: &servicepb.Request_SetMaintenanceMode{SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{Enabled: model.MaintenanceMode()}}}))
			require.NoError(t, err)
			audited := auditFailuresForKey(t, ctx, client, rejectionKey, marker, len(reqs))

			// The server's three observations agree with one another.
			require.Equal(t, fx.ExpectReason, replay.Reason, "a replay returns the same failure")
			serverFrozen := audited > 0
			if serverFrozen {
				require.Equal(t, 1, audited, "one failure entry for the key")
				require.Equal(t, "IDEMPOTENCY_KEY_CONFLICT", conflict.Reason, "a frozen key refuses a different body")
			} else {
				fresh := model.Apply(oracle.Bulk{Requests: diff})
				if fresh.OK {
					require.True(t, conflict.OK, "an unfrozen key executes a different body normally: %+v", conflict)
				} else {
					require.Equal(t, fresh.Reason, conflict.Reason, "an unfrozen key executes a different body normally")
				}
			}

			// The model predicts it (strict once 00d wires the task-01 seams).
			modelFrozen := !oc.IsAdmissionStage(mres) && !oc.UncommittedReason(mres.Reason) && oc.IsDefinitiveReason(mres.Reason)
			if oc.StageSeamWired {
				require.Equal(t, serverFrozen, modelFrozen, "model freeze decision vs server audit presence")
				after := oc.RecordFailure(model, bulk, mres).Apply(oracle.Bulk{Requests: diff, IdempotencyKey: rejectionKey})
				if serverFrozen {
					require.Equal(t, "IDEMPOTENCY_KEY_CONFLICT", after.Reason)
				}
			} else {
				t.Logf("seams not wired: model frozen=%v server frozen=%v (strict in 00d)", modelFrozen, serverFrozen)
			}
		})
	}
}
