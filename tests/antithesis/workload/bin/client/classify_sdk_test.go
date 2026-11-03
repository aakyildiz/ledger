//go:build enable_antithesis_sdk

package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/sdktest"
)

// Run with -tags enable_antithesis_sdk (the SDK is a no-op without it).
func TestClassificationAssertionLiteral(t *testing.T) {
	events := sdktest.Capture(t, func() {
		assertClassified("/x/Y", "", outcome{Code: "Internal"})
	})
	if events == nil {
		return // child process
	}
	ev := sdktest.Find(t, events, "client: every RPC error the client returns is classified")
	require.False(t, ev.Condition)
	require.Equal(t, "Internal", ev.Details["code"])
	require.Equal(t, "/x/Y", ev.Details["method"])
}
