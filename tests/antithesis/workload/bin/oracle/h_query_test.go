package main

import (
	"encoding/json"
	"testing"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/sdktest"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match/matchtest"
	qc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/querycheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/bin/oracle/servicetest"
)

func queryItems(t *testing.T, txs []*commonpb.Transaction) []json.RawMessage {
	t.Helper()
	var out []json.RawMessage
	for _, tx := range txs {
		b, err := protojson.Marshal(tx)
		require.NoError(t, err)
		out = append(out, b)
	}

	return out
}

// The three list read kinds are registered and validate through the protocol.
func TestQuery_ListTransactionsThroughTheService(t *testing.T) {
	t.Parallel()
	s, c := setupWithLedger(t)
	l := c.Ledger(1)
	for range 3 {
		m := c.Write(false, testutil.TxReq(l, "", "world", "users:1", "COIN", 5))
		okv, viol := servicetest.Verdict(m)
		require.True(t, okv, "%v", viol)
	}

	txs, next := matchtest.RenderTxPage(s.tracker.Model(), l, matchtest.TxPageReq{PageSize: 2, Reverse: true})
	require.Len(t, txs, 2)
	req := map[string]any{"ledger": l, "options": map[string]any{"pageSize": 2, "reverse": true}}

	tk := uint64(c.BeginRead("ListTransactions", map[string]any{"name": "stamps", "ledger": l})["ticket"].(float64))
	m := c.EndRead(tk, "ListTransactions", req, oc.Outcome{OK: true, Complete: true, Items: queryItems(t, txs), NextCursor: next})
	okv, viol := servicetest.Verdict(m)
	require.True(t, okv, "%v", viol)

	// A dropped row is a finding.
	tk = uint64(c.BeginRead("ListTransactions")["ticket"].(float64))
	m = c.EndRead(tk, "ListTransactions", req, oc.Outcome{OK: true, Complete: true, Items: queryItems(t, txs[1:]), NextCursor: next})
	okv, viol = servicetest.Verdict(m)
	require.False(t, okv)
	require.Equal(t, []string{string(qc.EvTransactionQueryOutsideModel)}, viol)

	for _, kind := range []string{"ListAccounts", "ListLogs"} {
		tk = uint64(c.BeginRead(kind)["ticket"].(float64))
		m = c.EndRead(tk, kind, map[string]any{"ledger": l, "options": map[string]any{"cursor": "not-a-cursor"}},
			oc.Outcome{Code: "Unavailable", Message: "transient"})
		require.True(t, m["verdict"].(map[string]any)["skipped"].(bool), kind)
	}
}

func TestQuery_AssertersEmitLiterals(t *testing.T) {
	t.Parallel()
	if !assert.Enabled {
		t.Skip("SDK emission needs -tags enable_antithesis_sdk")
	}
	events := sdktest.Capture(t, func() {
		assertEvents([]oc.Event{
			oc.Reached(qc.EvMalformedTxCursorRejected, nil),
			oc.Violation(qc.EvLogQueryOutsideModel, map[string]any{"ledger": "x"}),
			oc.Raw(qc.CoverageRetypeID(), false, nil),
		})
	})
	if events == nil {
		return
	}
	require.True(t, sdktest.Find(t, events, string(qc.EvMalformedTxCursorRejected)).Hit)
	require.False(t, sdktest.Find(t, events, string(qc.EvLogQueryOutsideModel)).Condition)
	sdktest.Find(t, events, string(qc.CoverageRetypeID()))
}
