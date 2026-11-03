package main

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// shutdownHarness is a client served on a local port, with a fake node and a
// UDP listener standing in for the driver's outcome socket.
type shutdownHarness struct {
	f      *fakeBucket
	addr   string // the fake node
	url    string // the client's HTTP base URL
	udp    *net.UDPConn
	stop   chan struct{}
	served chan error
	s      *server
}

func newShutdownHarness(t *testing.T, grace time.Duration) *shutdownHarness {
	t.Helper()
	f := &fakeBucket{name: "a", hold: make(chan struct{})}
	addr := startFake(t, f)
	cs, err := newConns(addr)
	require.NoError(t, err)
	t.Cleanup(cs.close)

	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = udp.Close() })
	out, err := net.DialUDP("udp", nil, udp.LocalAddr().(*net.UDPAddr))
	require.NoError(t, err)
	t.Cleanup(func() { _ = out.Close() })

	s := &server{conns: cs, udp: out}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	h := &shutdownHarness{
		f: f, addr: addr, url: "http://" + lis.Addr().String(), udp: udp,
		stop: make(chan struct{}), served: make(chan error, 1), s: s,
	}
	go func() { h.served <- s.serve(lis, h.stop, grace) }()

	return h
}

// post sends an /op GetLedger on route and returns the decoded event, or the
// transport error.
func (h *shutdownHarness) post(opID uint64, route string, timeout time.Duration) (event, error) {
	body, _ := json.Marshal(map[string]any{
		"opId": opID, "service": "bucket", "method": "GetLedger",
		"request": map[string]string{"ledger": "x"}, "timeoutMs": 20000, "route": route,
	})
	c := &http.Client{Timeout: timeout}
	resp, err := c.Post(h.url+"/op", "application/json", bytes.NewReader(body))
	if err != nil {
		return event{}, err
	}
	defer resp.Body.Close()
	var ev event
	err = json.NewDecoder(resp.Body).Decode(&ev)

	return ev, err
}

// waitCalls waits until the fake has received n GetLedger calls.
func (h *shutdownHarness) waitCalls(t *testing.T, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		h.f.mu.Lock()
		defer h.f.mu.Unlock()
		return h.f.calls >= n
	}, 10*time.Second, 5*time.Millisecond)
}

// events reads the UDP outcome events that arrive within wait.
func (h *shutdownHarness) events(t *testing.T, wait time.Duration) []event {
	t.Helper()
	var evs []event
	buf := make([]byte, 65536)
	require.NoError(t, h.udp.SetReadDeadline(time.Now().Add(wait)))
	for {
		n, err := h.udp.Read(buf)
		if err != nil {
			return evs
		}
		var ev event
		require.NoError(t, json.Unmarshal(buf[:n], &ev))
		evs = append(evs, ev)
	}
}

// An RPC in flight at shutdown that finishes within the grace is answered
// normally, over HTTP and UDP, including one whose HTTP request has already
// given up: the connections close only after it.
func TestShutdownWaitsForInFlightRPCs(t *testing.T) {
	t.Parallel()
	h := newShutdownHarness(t, 10*time.Second)

	type res struct {
		ev  event
		err error
	}
	answered := make(chan res, 1)
	go func() {
		ev, err := h.post(1, "plain", 15*time.Second)
		answered <- res{ev, err}
	}()
	// Op 2's HTTP request gives up before the RPC ends; it is reported over
	// UDP only.
	_, err := h.post(2, "plain", 200*time.Millisecond)
	require.Error(t, err)
	h.waitCalls(t, 2)

	close(h.stop)
	require.Never(t, func() bool { return h.s.conns.closing.Load() || len(h.served) > 0 },
		200*time.Millisecond, 5*time.Millisecond, "serve keeps the connections open while RPCs run")
	close(h.f.hold)

	r := <-answered
	require.NoError(t, r.err)
	require.Equal(t, uint64(1), r.ev.OpID)
	require.True(t, r.ev.Outcome.OK, "outcome: %+v", r.ev.Outcome)
	require.NoError(t, <-h.served)
	require.True(t, h.s.conns.closing.Load())

	got := map[uint64]bool{}
	for _, ev := range h.events(t, time.Second) {
		require.True(t, ev.Outcome.OK, "op %d: %+v", ev.OpID, ev.Outcome)
		got[ev.OpID] = true
	}
	require.Equal(t, map[uint64]bool{1: true, 2: true}, got)
}

// An RPC still running when the grace expires fails with the client's own
// connection-closing error. That is not a server answer: the driver gets no
// HTTP outcome (the connection drops) and no UDP event, so it sees no answer
// rather than a remote Canceled.
func TestShutdownNeverReportsOwnConnectionClose(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"lib", "plain"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			h := newShutdownHarness(t, 200*time.Millisecond)
			t.Cleanup(func() { close(h.f.hold) })

			answered := make(chan error, 1)
			go func() {
				ev, err := h.post(1, route, 15*time.Second)
				if err == nil {
					t.Errorf("got an outcome for an RPC the client cut off: %+v", ev.Outcome)
				}
				answered <- err
			}()
			h.waitCalls(t, 1)
			close(h.stop)
			require.NoError(t, <-h.served)
			require.Error(t, <-answered)
			require.Empty(t, h.events(t, 500*time.Millisecond), "no UDP event for an RPC the client cut off")
		})
	}
}

// The error grpc-go gives an RPC whose client connection closes is marked
// connClosing on every route, unary and pending; ownClose holds only once the
// client itself is closing.
func TestOwnConnectionCloseIsRecognised(t *testing.T) {
	t.Parallel()
	f := &fakeBucket{name: "a", hold: make(chan struct{})}
	t.Cleanup(func() { close(f.hold) })
	addr := startFake(t, f)
	cs, err := newConns(addr)
	require.NoError(t, err)
	s := &server{conns: cs}

	routes := []struct{ route, node string }{{"lib", ""}, {"plain", ""}, {"", addr}}
	outs := make(chan outcome, len(routes))
	for _, r := range routes {
		go func() { outs <- getLedger(t, s, r.route, r.node) }()
	}
	require.Eventually(t, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.calls == len(routes)
	}, 10*time.Second, 5*time.Millisecond)
	cs.close()
	for range routes {
		out := <-outs
		require.False(t, out.OK)
		require.True(t, out.connClosing, "outcome: %+v", out)
		require.True(t, s.ownClose(out))
		require.True(t, classified(out, ""))
	}

	// A call started after close fails before reaching any node.
	out := getLedger(t, s, "plain", "")
	require.True(t, s.ownClose(out), "outcome: %+v", out)
	out = getLedger(t, s, "", "127.0.0.1:9")
	require.True(t, out.Local, "a new node connection after close is a local dial error: %+v", out)
	require.Contains(t, out.Message, "dial:")
}

// A server that answers Canceled with grpc's connection-closing message (one
// of its own connections closed) is a server answer: while the client runs it
// stays remote and is reported like any other.
func TestServerConnectionClosingAnswerIsReported(t *testing.T) {
	t.Parallel()
	h := newShutdownHarness(t, time.Second)
	close(h.f.hold)
	h.f.mu.Lock()
	h.f.err = status.Error(codes.Canceled, "grpc: the client connection is closing")
	h.f.mu.Unlock()

	ev, err := h.post(1, "plain", 15*time.Second)
	require.NoError(t, err)
	require.False(t, ev.Outcome.OK)
	require.Equal(t, "Canceled", ev.Outcome.Code)
	require.False(t, ev.Outcome.Local)
	require.Equal(t, h.addr, ev.Outcome.Peer)
	evs := h.events(t, time.Second)
	require.Len(t, evs, 1)
	require.Equal(t, "Canceled", evs[0].Outcome.Code)
	require.False(t, evs[0].Outcome.Local)

	close(h.stop)
	require.NoError(t, <-h.served)
}

func TestConnClosingError(t *testing.T) {
	t.Parallel()
	require.True(t, errOutcome(grpc.ErrClientConnClosing).connClosing)
	require.True(t, errOutcome(status.Error(codes.Unavailable, grpc.ErrClientConnClosing.Error())).connClosing)
	require.False(t, errOutcome(status.Error(codes.Canceled, "context canceled")).connClosing)
	require.False(t, errOutcome(status.Error(codes.Unavailable, "transport is closing")).connClosing)
	require.False(t, errOutcome(status.Error(codes.Internal, clientConnClosing)).connClosing)

	// ownClose needs the client to be closing.
	cs, err := newConns("127.0.0.1:1")
	require.NoError(t, err)
	s := &server{conns: cs}
	out := errOutcome(grpc.ErrClientConnClosing)
	require.False(t, s.ownClose(out))
	cs.close()
	require.True(t, s.ownClose(out))
	require.False(t, s.ownClose(errOutcome(status.Error(codes.Canceled, "context canceled"))))
}
