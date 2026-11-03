package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

// outcome is the part of an ApplyResult every freezing case pins.
type outcome struct {
	ok       bool
	reason   string
	stage    Stage
	replayed bool
}

func requireOutcome(t *testing.T, want outcome, got ApplyResult) {
	t.Helper()
	require.Equal(t, want, outcome{ok: got.OK, reason: got.Reason, stage: got.Stage, replayed: got.Replayed}, "reason=%s", got.Reason)
}

// applyOK applies bulk and requires it to commit, returning the new state.
func applyOK(t *testing.T, g GlobalState, bulk Bulk) GlobalState {
	t.Helper()
	res := g.Apply(bulk)
	require.True(t, res.OK, res.Reason)

	return res.State
}

// fail applies bulk, requires the given fresh FSM failure, checks Apply left
// the state alone, then records the failure the way the checker does and
// returns the state after RecordFailure.
func fail(t *testing.T, g GlobalState, bulk Bulk, reason string) (GlobalState, ApplyResult) {
	t.Helper()
	res := g.Apply(bulk)
	requireOutcome(t, outcome{reason: reason, stage: StageFSM}, res)
	require.Equal(t, g.Fingerprint(), res.State.Fingerprint(), "Apply must return State == g on failure")

	return g.RecordFailure(bulk, res), res
}

func funded(t *testing.T, addr string, amount int64) GlobalState {
	t.Helper()

	return applyOK(t, NewGlobalState(), bulkOf(oracletest.TxReq("world", addr, "USD", amount)))
}

// TestIdempotencyFailureFreezing is the server's failure-freezing contract
// (machine.go dedup gate and recordIdempotencyFailure) case by case.
func TestIdempotencyFailureFreezing(t *testing.T) {
	t.Parallel()

	overdraw := func() *servicepb.Request { return oracletest.TxReq("alice", "bob", "USD", 10) }

	cases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"1 frozen INSUFFICIENT_FUNDS replays after funding", func(t *testing.T) {
			g := NewGlobalState()
			bulk := keyedBulk("k", overdraw())
			frozen, _ := fail(t, g, bulk, domain.ErrReasonInsufficientFunds)
			require.NotEqual(t, g.Fingerprint(), frozen.Fingerprint(), "the freeze is state")

			fundedState := applyOK(t, frozen, bulkOf(oracletest.TxReq("world", "alice", "USD", 10)))
			// Control: without the key the same body now commits.
			require.True(t, fundedState.Apply(bulkOf(overdraw())).OK)

			replay := fundedState.Apply(keyedBulk("k", overdraw()))
			requireOutcome(t, outcome{reason: domain.ErrReasonInsufficientFunds, stage: StageFSM, replayed: true}, replay)
			require.Nil(t, replay.Orders)
			require.Equal(t, fundedState.Fingerprint(), replay.State.Fingerprint())
		}},
		{"2 different body after a frozen failure conflicts and is not frozen", func(t *testing.T) {
			frozen, _ := fail(t, NewGlobalState(), keyedBulk("k", overdraw()), domain.ErrReasonInsufficientFunds)
			other := keyedBulk("k", oracletest.TxReq("world", "bob", "USD", 1))
			conflict := frozen.Apply(other)
			requireOutcome(t, outcome{reason: domain.ErrReasonIdempotencyKeyConflict, stage: StageFSM}, conflict)
			after := frozen.RecordFailure(other, conflict)
			require.Equal(t, frozen.Fingerprint(), after.Fingerprint(), "live entry: no-op")
			// The original failure is still what the key replays.
			requireOutcome(t, outcome{reason: domain.ErrReasonInsufficientFunds, stage: StageFSM, replayed: true}, after.Apply(keyedBulk("k", overdraw())))
		}},
		{"3 conflict over a frozen success is never frozen", func(t *testing.T) {
			success := keyedBulk("k", oracletest.TxReq("world", "a", "USD", 1))
			g := applyOK(t, NewGlobalState(), success)
			other := keyedBulk("k", oracletest.TxReq("world", "a", "USD", 2))
			conflict := g.Apply(other)
			requireOutcome(t, outcome{reason: domain.ErrReasonIdempotencyKeyConflict, stage: StageFSM}, conflict)
			require.Equal(t, g.Fingerprint(), g.RecordFailure(other, conflict).Fingerprint())
			// Even on a state where the key is absent, a conflict result never freezes.
			require.Equal(t, NewGlobalState().Fingerprint(), NewGlobalState().RecordFailure(other, conflict).Fingerprint())
			replay := g.Apply(success)
			requireOutcome(t, outcome{ok: true, stage: StageFSM, replayed: true}, replay)
		}},
		{"4 admission failures are never frozen", func(t *testing.T) {
			empty := &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
				Ledger: "L",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: &servicepb.CreateTransactionPayload{}}},
			}}}
			badSkip := skipRequest(oracletest.TxReq("world", "a", "USD", 1), commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS)
			maintenance := applyOK(t, NewGlobalState(), bulkOf(&servicepb.Request{Type: &servicepb.Request_SetMaintenanceMode{SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{Enabled: true}}}))
			for _, sub := range []struct {
				name   string
				base   GlobalState
				req    *servicepb.Request
				reason string
			}{
				{"empty create", NewGlobalState(), empty, domain.ErrReasonValidation},
				{"invalid skippable reason", NewGlobalState(), badSkip, domain.ErrReasonValidation},
				{"maintenance gate", maintenance, oracletest.TxReq("world", "a", "USD", 1), domain.ErrReasonMaintenanceMode},
			} {
				bulk := keyedBulk("k", sub.req)
				res := sub.base.Apply(bulk)
				requireOutcome(t, outcome{reason: sub.reason, stage: StageAdmission}, res)
				after := sub.base.RecordFailure(bulk, res)
				require.Equal(t, sub.base.Fingerprint(), after.Fingerprint(), sub.name)
				// A later valid body under the same key applies normally.
				valid := keyedBulk("k", oracletest.TxReq("world", "a", "USD", 1))
				if sub.reason == domain.ErrReasonMaintenanceMode {
					after = applyOK(t, after, bulkOf(&servicepb.Request{Type: &servicepb.Request_SetMaintenanceMode{SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{Enabled: false}}}))
				}
				requireOutcome(t, outcome{ok: true, stage: StageFSM}, after.Apply(valid))
			}
		}},
		{"5 non-freezable kinds are never frozen", func(t *testing.T) {
			g := NewGlobalState()
			bulk := keyedBulk("k", oracletest.TxReq("world", "a", "USD", 1))
			for _, reason := range []string{
				domain.ErrReasonMaintenanceMode,     // Unavailable
				domain.ErrReasonIndexBuilding,       // Unavailable
				domain.ErrReasonSequenceExhausted,   // ResourceExhausted
				domain.ErrReasonStorageOperation,    // Internal
				domain.ErrReasonPreloadUnavailable,  // Unavailable
				domain.ErrReasonAuditDisabled + "X", // unknown reason: UNSPECIFIED → Internal
			} {
				require.False(t, domain.IsFreezableFailure(domain.KindForReason(domain.ReasonCode(reason))), reason)
				after := g.RecordFailure(bulk, ApplyResult{Reason: reason, Stage: StageFSM})
				require.Equal(t, g.Fingerprint(), after.Fingerprint(), reason)
			}
			// Control: the same synthetic result with a freezable reason freezes.
			frozen := g.RecordFailure(bulk, ApplyResult{Reason: domain.ErrReasonInsufficientFunds, Stage: StageFSM})
			require.NotEqual(t, g.Fingerprint(), frozen.Fingerprint())
			// A result without a stage is never frozen.
			require.Equal(t, g.Fingerprint(), g.RecordFailure(bulk, ApplyResult{Reason: domain.ErrReasonInsufficientFunds}).Fingerprint())
		}},
		{"6 TRANSIENT_ACCOUNT_NON_ZERO is frozen", func(t *testing.T) {
			g := applyOK(t, NewGlobalState(), bulkOf(oracletest.AddTypeReqP("tmp", commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT)))
			bulk := keyedBulk("k", oracletest.TxReq("world", "tmp:1", "USD", 10))
			frozen, _ := fail(t, g, bulk, domain.ErrReasonTransientAccountNonZero)
			require.NotEqual(t, g.Fingerprint(), frozen.Fingerprint())
			// Removing the transient type would let the body commit fresh; the
			// key still replays the frozen failure.
			untyped := applyOK(t, frozen, bulkOf(oracletest.RemoveTypeReq("tmp")))
			require.True(t, untyped.Apply(bulkOf(oracletest.TxReq("world", "tmp:1", "USD", 10))).OK)
			replay := untyped.Apply(keyedBulk("k", oracletest.TxReq("world", "tmp:1", "USD", 10)))
			requireOutcome(t, outcome{reason: domain.ErrReasonTransientAccountNonZero, stage: StageFSM, replayed: true}, replay)
			require.Equal(t, untyped.Fingerprint(), replay.State.Fingerprint())
		}},
		{"7 a replayed failure is not re-frozen", func(t *testing.T) {
			bulk := keyedBulk("k", overdraw())
			frozen, _ := fail(t, NewGlobalState(), bulk, domain.ErrReasonInsufficientFunds)
			replay := frozen.Apply(bulk)
			requireOutcome(t, outcome{reason: domain.ErrReasonInsufficientFunds, stage: StageFSM, replayed: true}, replay)
			require.Equal(t, frozen.Fingerprint(), frozen.RecordFailure(bulk, replay).Fingerprint())
			// Even where the key is absent, a replay result never freezes.
			require.Equal(t, NewGlobalState().Fingerprint(), NewGlobalState().RecordFailure(bulk, replay).Fingerprint())
		}},
		{"8 unkeyed bulk: RecordFailure is a no-op", func(t *testing.T) {
			g := NewGlobalState()
			bulk := bulkOf(overdraw())
			res := g.Apply(bulk)
			requireOutcome(t, outcome{reason: domain.ErrReasonInsufficientFunds, stage: StageFSM}, res)
			require.Equal(t, g.Fingerprint(), g.RecordFailure(bulk, res).Fingerprint())
		}},
		{"9 frozen failures are identity and commute", func(t *testing.T) {
			g := NewGlobalState()
			b1 := keyedBulk("k1", overdraw())
			b2 := keyedBulk("k2", oracletest.TxReq("carol", "bob", "USD", 3))
			only1, r1 := fail(t, g, b1, domain.ErrReasonInsufficientFunds)
			require.NotEqual(t, g.Fingerprint(), only1.Fingerprint())

			_, r2 := fail(t, g, b2, domain.ErrReasonInsufficientFunds)
			order12 := g.RecordFailure(b1, r1).RecordFailure(b2, r2)
			order21 := g.RecordFailure(b2, r2).RecordFailure(b1, r1)
			require.Equal(t, order12.Fingerprint(), order21.Fingerprint())
			require.NotEqual(t, only1.Fingerprint(), order12.Fingerprint())

			// A frozen failure and a frozen success under one key differ.
			success := applyOK(t, funded(t, "alice", 10), b1)
			failure := funded(t, "alice", 10).RecordFailure(b1, r1)
			require.NotEqual(t, success.Fingerprint(), failure.Fingerprint())
		}},
		{"10 RequestsEqual normalisation replays a frozen failure", func(t *testing.T) {
			for _, nestedFirst := range []bool{false, true} {
				g := applyOK(t, NewGlobalState(), bulkOf(oracletest.AddTypeReq("known")))
				top := oracletest.AddTypeReq("known")
				nested := &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
					Ledger: "L",
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddAccountType{AddAccountType: &servicepb.AddAccountTypeRequest{AccountType: top.GetAddAccountType().GetAccountType()}}},
				}}}
				first, second := top, nested
				if nestedFirst {
					first, second = nested, top
				}
				frozen, _ := fail(t, g, keyedBulk("k", first), domain.ErrReasonAccountTypeAlreadyExists)
				// Once the type is gone the body would commit fresh.
				removed := applyOK(t, frozen, bulkOf(oracletest.RemoveTypeReq("known")))
				replay := removed.Apply(keyedBulk("k", second))
				requireOutcome(t, outcome{reason: domain.ErrReasonAccountTypeAlreadyExists, stage: StageFSM, replayed: true}, replay)
				require.Equal(t, removed.Fingerprint(), replay.State.Fingerprint())
			}
		}},
		{"11 uncommitted FILTER_COMPILATION_ERROR is not frozen", func(t *testing.T) {
			require.True(t, UncommittedReasons[domain.ErrReasonFilterCompilation])
			logs := applyOK(t, NewGlobalState(), bulkOf(pqReq("q", commonpb.QueryTarget_QUERY_TARGET_LOGS, nil)))
			// An address condition passes admission's shape gate but is invalid
			// for the stored LOGS target: the FSM rejects the update.
			update := keyedBulk("k", pqUpdateReq("q", pqFilter("a:")))
			after, _ := fail(t, logs, update, domain.ErrReasonFilterCompilation)
			require.Equal(t, logs.Fingerprint(), after.Fingerprint(), "uncommitted: not frozen")

			// Replace the stored query by one with a compatible target: the same
			// body under the same key now commits.
			replaced := applyOK(t, after, bulkOf(pqDeleteReq("q"), pqReq("q", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, nil)))
			requireOutcome(t, outcome{ok: true, stage: StageFSM}, replaced.Apply(update))
		}},
		{"extra: transaction-id 0 is VALIDATION at the server's stage", func(t *testing.T) {
			g := funded(t, "a", 5)
			revert := keyedBulk("k", oracletest.RevertReqL("L", 0, true))
			res := g.Apply(revert)
			requireOutcome(t, outcome{reason: domain.ErrReasonValidation, stage: StageAdmission}, res)
			require.Equal(t, g.Fingerprint(), g.RecordFailure(revert, res).Fingerprint())

			meta := keyedBulk("m", oracletest.AddTxMetaReq(0, map[string]*commonpb.MetadataValue{"x": {Type: &commonpb.MetadataValue_StringValue{StringValue: "y"}}}))
			frozen, _ := fail(t, g, meta, domain.ErrReasonValidation)
			require.NotEqual(t, g.Fingerprint(), frozen.Fingerprint(), "FSM VALIDATION is freezable")
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
