package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func digestFake(stats map[string]*commonpb.LedgerStats) *fakeBucket {
	return &fakeBucket{
		ledgers: []string{"a", "b", "c"},
		accounts: map[string][]string{
			"a": {"a:1", "a:2", "a:3", "a:4", "a:5"},
			"b": {"b:1"},
			"c": {},
		},
		stats: stats,
	}
}

func baseStats() map[string]*commonpb.LedgerStats {
	return map[string]*commonpb.LedgerStats{
		"a": {TransactionCount: 5, LogCount: 7, VolumeCount: 3, PostingCount: 9},
		"b": {TransactionCount: 1, LogCount: 2},
		"c": {},
	}
}

func runDigestOn(t *testing.T, f *fakeBucket, req digestRequest) digestResponse {
	t.Helper()
	addr := startFake(t, f)
	cs, err := newConns(addr)
	require.NoError(t, err)
	t.Cleanup(cs.close)
	s := &server{conns: cs}
	body, err := json.Marshal(req)
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	s.handleDigest(rec, httptest.NewRequest(http.MethodPost, "/digest", strings.NewReader(string(body))))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var resp digestResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

func TestDigestDeterministicAndPaged(t *testing.T) {
	t.Parallel()
	req := digestRequest{Route: "plain", Sections: []string{"ledgers", "accounts", "stats"}}
	one := runDigestOn(t, digestFake(baseStats()), req)
	require.True(t, one.OK, one.Error)
	require.Equal(t, uint64(3), one.Sections["ledgers"].Count)
	require.Equal(t, "c", one.Sections["ledgers"].LastKey)
	acc := one.Sections["accounts"]
	require.Equal(t, uint64(6), acc.Count)
	require.Equal(t, "b/b:1", acc.LastKey)
	require.Equal(t, uint64(5), acc.Ledgers["a"].Count)
	require.Equal(t, "a:5", acc.Ledgers["a"].LastKey)
	require.Equal(t, uint64(0), acc.Ledgers["c"].Count)
	require.Nil(t, acc.Items, "items only on request")

	// Same items on another server, read in pages of 2: same digests.
	req.PageSize = 2
	two := runDigestOn(t, digestFake(baseStats()), req)
	require.True(t, two.OK, two.Error)
	for _, name := range req.Sections {
		require.Equal(t, one.Sections[name].SHA256, two.Sections[name].SHA256, name)
		require.Equal(t, one.Sections[name].Count, two.Sections[name].Count, name)
	}

	// Order matters.
	f := digestFake(baseStats())
	f.accounts["a"] = []string{"a:2", "a:1", "a:3", "a:4", "a:5"}
	three := runDigestOn(t, f, req)
	require.NotEqual(t, one.Sections["accounts"].SHA256, three.Sections["accounts"].SHA256)
	require.NotEqual(t, one.Sections["accounts"].Ledgers["a"].SHA256, three.Sections["accounts"].Ledgers["a"].SHA256)
	require.Equal(t, one.Sections["accounts"].Ledgers["b"].SHA256, three.Sections["accounts"].Ledgers["b"].SHA256)
}

func TestDigestStatsIgnoresUsageCounters(t *testing.T) {
	t.Parallel()
	req := digestRequest{Sections: []string{"stats"}, Ledgers: []string{"a", "b"}, Items: true}
	base := runDigestOn(t, digestFake(baseStats()), req)
	require.True(t, base.OK, base.Error)
	require.Len(t, base.Sections["stats"].Items, 2)

	usage := baseStats()
	usage["a"].VolumeCount, usage["a"].PostingCount, usage["a"].RevertCount = 100, 200, 3
	other := runDigestOn(t, digestFake(usage), req)
	require.Equal(t, base.Sections["stats"].SHA256, other.Sections["stats"].SHA256)

	counts := baseStats()
	counts["a"].LogCount = 8
	diff := runDigestOn(t, digestFake(counts), req)
	require.NotEqual(t, base.Sections["stats"].SHA256, diff.Sections["stats"].SHA256)
}

func TestDigestExplicitLedgersAndFailure(t *testing.T) {
	t.Parallel()
	resp := runDigestOn(t, digestFake(baseStats()), digestRequest{Sections: []string{"ledgers", "accounts"}, Ledgers: []string{"b"}})
	require.True(t, resp.OK, resp.Error)
	require.Equal(t, uint64(1), resp.Sections["ledgers"].Count)
	require.Equal(t, uint64(1), resp.Sections["accounts"].Count)

	resp = runDigestOn(t, digestFake(baseStats()), digestRequest{Sections: []string{"accounts"}, Ledgers: []string{"b", "zz"}})
	require.False(t, resp.OK)
	require.Empty(t, resp.Sections)
	require.NotNil(t, resp.Outcome)
	require.Equal(t, "NotFound", resp.Outcome.Code)
	require.Contains(t, resp.Error, "accounts zz")
}

func TestDigestRejectsBadRequests(t *testing.T) {
	t.Parallel()
	require.ErrorContains(t, validateDigest(&digestRequest{Sections: []string{"bogus"}}), "unknown section")
	require.ErrorContains(t, validateDigest(&digestRequest{Route: "x"}), "unknown route")
	require.ErrorContains(t, validateDigest(&digestRequest{AuditFrom: "-1"}), "auditFrom")
	req := digestRequest{}
	require.NoError(t, validateDigest(&req))
	require.Equal(t, digestSections, req.Sections)
}

func TestAccumulatorFraming(t *testing.T) {
	t.Parallel()
	digest := func(items ...string) string {
		a := newAccumulator(false, false)
		for _, it := range items {
			require.NoError(t, a.add(it, &commonpb.Account{Address: it}))
		}

		return a.result().SHA256
	}
	require.Equal(t, digest("x", "y"), digest("x", "y"))
	require.NotEqual(t, digest("x", "y"), digest("y", "x"))
	require.NotEqual(t, digest("x"), digest("x", "x"))

	// Per-ledger sections hash ledger boundaries: moving an item between
	// ledgers changes the section digest.
	perLedger := func(split int) string {
		a := newAccumulator(false, true)
		items := []string{"p", "q", "r"}
		a.beginLedger("l1")
		for _, it := range items[:split] {
			require.NoError(t, a.add(it, &commonpb.Account{Address: it}))
		}
		a.beginLedger("l2")
		for _, it := range items[split:] {
			require.NoError(t, a.add(it, &commonpb.Account{Address: it}))
		}

		return a.result().SHA256
	}
	require.NotEqual(t, perLedger(1), perLedger(2))
}
