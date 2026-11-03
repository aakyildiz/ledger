package main

import (
	"encoding/json"
	"testing"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/bin/oracle/servicetest"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/sdktest"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	ac "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/auditcheck"
)

// The audit read kinds and the auditSamples view are registered and validate
// through the protocol.
func TestAudit_ReadKindsThroughTheService(t *testing.T) {
	t.Parallel()
	_, c := setupWithLedger(t)

	// An unfiltered first page of foreign entries only: validated, sampled.
	foreign := &auditpb.AuditEntry{Sequence: 1, OrderCount: 1, Ledgers: []string{"default"},
		Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: 1, MaxLogSequence: 1}}}
	b, err := protojson.Marshal(foreign)
	require.NoError(t, err)
	req := map[string]any{"options": map[string]any{"pageSize": 10}}
	tk := uint64(c.BeginRead(ac.KindList)["ticket"].(float64))
	m := c.EndRead(tk, ac.KindList, req, oc.Outcome{OK: true, Complete: true, Items: []json.RawMessage{b}})
	okv, viol := servicetest.Verdict(m)
	require.True(t, okv, "%v", viol)
	require.Contains(t, m["coverage"], string(ac.EvAuditPageValidated))

	r := c.BeginRead(ac.KindGet, map[string]any{"name": ac.ViewSamples})
	views := r["views"].([]any)
	samples := views[0].(map[string]any)["samples"].([]any)
	require.Len(t, samples, 1)
	require.Equal(t, "1", samples[0].(map[string]any)["seq"])
	require.Equal(t, c.Prefix+"k", views[0].(map[string]any)["keyPrefix"])

	// A foreign entry naming a tracked ledger is a finding.
	bad := &auditpb.AuditEntry{Sequence: 2, OrderCount: 1, Ledgers: []string{c.Ledger(1)},
		Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: 2, MaxLogSequence: 2}}}
	bb, err := protojson.Marshal(bad)
	require.NoError(t, err)
	tk = uint64(r["ticket"].(float64))
	m = c.EndRead(tk, ac.KindGet, map[string]any{"sequence": "2"}, oc.Outcome{OK: true, Response: bb})
	okv, viol = servicetest.Verdict(m)
	require.False(t, okv)
	require.Equal(t, []string{string(ac.EvAuditForeignTouchedTracked)}, viol)

	// Transient outcomes are skipped.
	tk = uint64(c.BeginRead(ac.KindList)["ticket"].(float64))
	m = c.EndRead(tk, ac.KindList, req, oc.Outcome{Code: "Unavailable", Message: "transient"})
	require.True(t, m["verdict"].(map[string]any)["skipped"].(bool))
}

func TestAudit_AssertersEmitLiterals(t *testing.T) {
	t.Parallel()
	if !assert.Enabled {
		t.Skip("SDK emission needs -tags enable_antithesis_sdk")
	}
	events := sdktest.Capture(t, func() {
		assertEvents([]oc.Event{
			oc.Reached(ac.EvAuditMalformedCursorRejected, nil),
			oc.Violation(ac.EvAuditRejectedBulkHasSuccess, map[string]any{"entry": "x"}),
		})
	})
	if events == nil {
		return
	}
	require.True(t, sdktest.Find(t, events, string(ac.EvAuditMalformedCursorRejected)).Hit)
	require.False(t, sdktest.Find(t, events, string(ac.EvAuditRejectedBulkHasSuccess)).Condition)
}
