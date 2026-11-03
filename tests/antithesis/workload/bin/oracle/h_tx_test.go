package main

import (
	"os"
	"testing"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/sdktest"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	txcheck "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/applysurface"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Each rejection-coverage branch of task 02's failure hook emits its literal
// Reachable through the SDK; the apply-surface probe goes out as a Raw
// assertion.
func TestTxAsserters_EmitLiterals(t *testing.T) {
	t.Parallel()
	if !assert.Enabled {
		t.Skip("SDK emission needs -tags enable_antithesis_sdk")
	}
	reasons := map[string]oc.EventID{
		"INSUFFICIENT_FUNDS":             txcheck.EvInsufficientFundsExercised,
		"TRANSACTION_REFERENCE_CONFLICT": txcheck.EvReferenceConflictExercised,
		"VOLUME_OVERFLOW":                txcheck.EvVolumeOverflowExercised,
		"VALIDATION":                     txcheck.EvValidationExercised,
		"TRANSIENT_ACCOUNT_NON_ZERO":     txcheck.EvTransientNonZeroExercised,
	}
	events := sdktest.Capture(t, func() {
		for reason := range reasons {
			assertEvents(txcheck.RejectionCoverage(nil, oc.WriteRecord{Wid: "w1", Family: "tx"}, oracle.ApplyResult{}, reason))
		}
		assertEvents([]oc.Event{oc.Raw(txcheck.EvSkipRefConflictContinued, true, nil)})
	})
	if os.Getenv("LEDGER_SDK_TEST_CHILD") == t.Name() {
		return // the child process only runs the scenario
	}
	require.NotEmpty(t, events, "the SDK emitted nothing")
	for _, id := range reasons {
		require.True(t, sdktest.Find(t, events, string(id)).Hit, id)
	}
	require.True(t, sdktest.Find(t, events, string(txcheck.EvSkipRefConflictContinued)).Condition)
	sdktest.Absent(t, events, "oracle protocol: unmapped event")
}
