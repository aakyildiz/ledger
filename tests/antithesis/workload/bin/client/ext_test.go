package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMuxServesOpAndExtensions(t *testing.T) {
	// Not parallel: s.mux() sets the process-wide extension server.
	f := &fakeBucket{name: "a"}
	addr := startFake(t, f)
	cs, err := newConns(addr)
	require.NoError(t, err)
	t.Cleanup(cs.close)
	s := &server{conns: cs}

	registerExt("POST /ext-test", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]bool{"same": extServer() == s})
	})
	mux := s.mux()

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/ext-test", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"same":true}`, rec.Body.String())

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/op",
		strings.NewReader(`{"opId":9,"service":"bucket","method":"GetLedger","request":{"ledger":"x"},"route":"plain","timeoutMs":5000}`)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var ev event
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ev))
	require.Equal(t, uint64(9), ev.OpID)
	require.True(t, ev.Outcome.OK)
	require.Equal(t, addr, ev.Outcome.Peer)

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/op",
		strings.NewReader(`{"opId":10,"service":"bucket","method":"GetLedger","route":"bogus"}`)))
	require.Equal(t, http.StatusBadRequest, rec.Code)
}
