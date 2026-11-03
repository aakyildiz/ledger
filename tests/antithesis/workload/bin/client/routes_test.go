package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// Not parallel: it replaces the package-level newClient seam.
func TestDialOptionsCarryMessageLimits(t *testing.T) {
	var recv, send int
	for _, o := range defaultCallOptions() {
		switch o := o.(type) {
		case grpc.MaxRecvMsgSizeCallOption:
			recv = o.MaxRecvMsgSize
		case grpc.MaxSendMsgSizeCallOption:
			send = o.MaxSendMsgSize
		}
	}
	require.Equal(t, 64<<20, recv)
	require.Equal(t, 64<<20, send)

	type dialed struct {
		target string
		opts   []grpc.DialOption
	}
	var got []dialed
	orig := newClient
	t.Cleanup(func() { newClient = orig })
	newClient = func(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
		got = append(got, dialed{target, opts})
		return orig(target, opts...)
	}
	cs, err := newConns("127.0.0.1:1,127.0.0.1:2")
	require.NoError(t, err)
	t.Cleanup(cs.close)
	_, err = cs.pick("", "127.0.0.1:3")
	require.NoError(t, err)

	require.Len(t, got, 3, "lib, plain and one node connection")
	common := len(commonDialOptions())
	// Every connection starts with the common options, which carry
	// WithDefaultCallOptions(defaultCallOptions()...).
	require.Len(t, got[0].opts, common+2, "lib: + service config + resolver")
	require.Len(t, got[1].opts, common+2, "plain: + service config + resolver")
	require.Len(t, got[2].opts, common, "node: common options only")
	require.Equal(t, "passthrough:///127.0.0.1:3", got[2].target)
}

func getLedger(t *testing.T, s *server, route, node string) outcome {
	t.Helper()
	method, err := lookupMethod("bucket", "GetLedger")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	return s.callRoute(ctx, route, node, "", method, &servicepb.GetLedgerRequest{Ledger: "x"}, 0)
}

func ledgerName(t *testing.T, out outcome) string {
	t.Helper()
	require.True(t, out.OK, "outcome: %+v", out)
	var info struct {
		Name string `json:"name"`
	}
	require.NoError(t, json.Unmarshal(out.Response, &info))

	return info.Name
}

func TestRouteLibRetriesPlainDoesNot(t *testing.T) {
	t.Parallel()
	f := &fakeBucket{name: "a"}
	addr := startFake(t, f)
	cs, err := newConns(addr)
	require.NoError(t, err)
	t.Cleanup(cs.close)
	s := &server{conns: cs}

	f.mu.Lock()
	f.unavailable = 2
	f.mu.Unlock()
	out := getLedger(t, s, "lib", "")
	require.Equal(t, "a", ledgerName(t, out))
	f.mu.Lock()
	require.Equal(t, 3, f.calls, "lib route retried Unavailable twice")
	f.unavailable, f.calls = 1, 0
	f.mu.Unlock()

	out = getLedger(t, s, "", "")
	require.True(t, out.OK, "empty route is lib")

	f.mu.Lock()
	f.unavailable, f.calls = 1, 0
	f.mu.Unlock()
	out = getLedger(t, s, "plain", "")
	require.False(t, out.OK)
	require.Equal(t, "Unavailable", out.Code)
	require.False(t, out.Local)
	require.Equal(t, addr, out.Peer)
	f.mu.Lock()
	require.Equal(t, 1, f.calls, "plain route sends once")
	f.mu.Unlock()
	require.True(t, classified(out, ""))
}

func TestRouteNodePinsOneNode(t *testing.T) {
	t.Parallel()
	a, b := &fakeBucket{name: "a"}, &fakeBucket{name: "b"}
	addrA, addrB := startFake(t, a), startFake(t, b)
	cs, err := newConns(addrA + "," + addrB)
	require.NoError(t, err)
	t.Cleanup(cs.close)
	s := &server{conns: cs}

	for range 4 {
		out := getLedger(t, s, "plain", addrB)
		require.Equal(t, "b", ledgerName(t, out), "node overrides the route")
		require.Equal(t, addrB, out.Peer, "peer is the dialed node address")
	}
	seen := map[string]bool{}
	for range 6 {
		out := getLedger(t, s, "plain", "")
		seen[ledgerName(t, out)] = true
		require.Contains(t, []string{addrA, addrB}, out.Peer)
	}
	require.Len(t, seen, 2, "plain is round-robin over every node")
}

func TestRouteUnknownAndRestoreNeedsNode(t *testing.T) {
	t.Parallel()
	_, _, err := prepareOp(opRequest{Service: "bucket", Method: "GetLedger", Route: "fast"})
	require.ErrorContains(t, err, "unknown route")
	_, _, err = prepareOp(opRequest{Service: "restore", Method: "PreviewRestore"})
	require.ErrorContains(t, err, "must set node")
	m, _, err := prepareOp(opRequest{Service: "restore", Method: "PreviewRestore", Node: "restore-0:8888"})
	require.NoError(t, err)
	require.Equal(t, "PreviewRestore", string(m.Name()))
}

func TestMessageLimitsOnEveryRoute(t *testing.T) {
	t.Parallel()
	f := &fakeBucket{name: "a", metadataBytes: 5 << 20}
	addr := startFake(t, f)
	cs, err := newConns(addr)
	require.NoError(t, err)
	t.Cleanup(cs.close)
	s := &server{conns: cs}

	routes := []struct{ route, node string }{{"lib", ""}, {"plain", ""}, {"", addr}}
	for _, r := range routes {
		out := getLedger(t, s, r.route, r.node)
		require.True(t, out.OK, "a 5 MiB response passes on %s: %+v", routeLabel(r.route, r.node), out.Message)
	}

	f.mu.Lock()
	f.metadataBytes = 65 << 20
	f.mu.Unlock()
	for _, r := range routes {
		out := getLedger(t, s, r.route, r.node)
		require.False(t, out.OK)
		require.Equal(t, "ResourceExhausted", out.Code)
		require.True(t, strings.HasPrefix(out.Message, "grpc: received message larger than max"), out.Message)
		require.True(t, out.Local, "a client-side size limit is local")
		require.True(t, classified(out, ""))
	}
}

func TestDialFailureIsLocal(t *testing.T) {
	t.Parallel()
	cs, err := newConns("127.0.0.1:1")
	require.NoError(t, err)
	t.Cleanup(cs.close)
	s := &server{conns: cs}
	method, err := lookupMethod("bucket", "GetLedger")
	require.NoError(t, err)
	// An invalid target makes grpc.NewClient fail.
	out := s.callRoute(t.Context(), "", "%%bad", "", method, &servicepb.GetLedgerRequest{}, 0)
	require.False(t, out.OK)
	require.True(t, out.Local)
	require.True(t, classified(out, ""))
}
