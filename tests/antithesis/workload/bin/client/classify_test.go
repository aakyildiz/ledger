package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClassification(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		out  outcome
		node string
		want bool
	}{
		{"ok", outcome{OK: true}, "", true},
		{"unavailable", outcome{Code: "Unavailable"}, "", true},
		{"unavailable stale proposal", outcome{Code: "Unavailable", Reason: "STALE_PROPOSAL"}, "", true},
		{"deadline", outcome{Code: "DeadlineExceeded"}, "", true},
		{"canceled", outcome{Code: "Canceled"}, "", true},
		{"not found", outcome{Code: "NotFound", Reason: "LEDGER_NOT_FOUND"}, "", true},
		{"already exists", outcome{Code: "AlreadyExists"}, "", true},
		{"invalid argument", outcome{Code: "InvalidArgument"}, "", true},
		{"failed precondition", outcome{Code: "FailedPrecondition", Reason: "INSUFFICIENT_FUNDS"}, "", true},
		{"disk full", outcome{Code: "ResourceExhausted", Reason: "WRITES_BLOCKED_DISK_FULL"}, "", true},
		{"resource exhausted other", outcome{Code: "ResourceExhausted"}, "", false},
		{"resource exhausted other reason", outcome{Code: "ResourceExhausted", Reason: "BALANCE_NOT_PRELOADED"}, "", false},
		{"resource exhausted local", outcome{Code: "ResourceExhausted", Local: true}, "", true},
		{"unauthenticated", outcome{Code: "Unauthenticated"}, "", true},
		{"permission denied", outcome{Code: "PermissionDenied"}, "", true},
		{"unimplemented restore node", outcome{Code: "Unimplemented"}, "restore-0:8888", true},
		{"unimplemented ledger node", outcome{Code: "Unimplemented"}, "ledger-0:8888", false},
		{"unimplemented shared", outcome{Code: "Unimplemented"}, "", false},
		{"aborted", outcome{Code: "Aborted"}, "", false},
		{"internal", outcome{Code: "Internal"}, "", false},
		{"unknown", outcome{Code: "Unknown"}, "", false},
		{"internal local", outcome{Code: "Internal", Local: true}, "", true},
		{"external service error any code", outcome{Code: "Internal", Reason: "EXTERNAL_SERVICE_ERROR"}, "", true},
		{"out of range", outcome{Code: "OutOfRange"}, "", false},
		{"data loss", outcome{Code: "DataLoss"}, "", false},
	}
	for _, c := range cases {
		require.Equal(t, c.want, classified(c.out, c.node), c.name)
	}
}
