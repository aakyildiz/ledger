package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"

	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
)

// maxMsgSize is the gRPC message limit of every connection, both directions.
// The server sends up to 64 MiB (internal/adapter/grpc/server.go
// serviceMaxMsgSize); with gRPC's default 4 MiB receive limit a large
// successful response would become a reasonless ResourceExhausted that never
// resolves (DESIGN §7 C1).
const maxMsgSize = 64 << 20

// Routes (DESIGN §7 C1). A request names one with opRequest.route; node, when
// set, overrides it.
const (
	// routeLib is the production-configured client: round-robin over every
	// node with actions.GRPCRetryPolicy, as ledgerctl dials.
	routeLib = "lib"
	// routePlain is round-robin over every node without a retry policy, so
	// the driver is the only retry.
	routePlain = "plain"
)

// defaultCallOptions are the per-call defaults every connection dials with.
// Tests inspect them through this function.
func defaultCallOptions() []grpc.CallOption {
	return []grpc.CallOption{
		grpc.MaxCallRecvMsgSize(maxMsgSize),
		grpc.MaxCallSendMsgSize(maxMsgSize),
	}
}

// newClient is the test seam over grpc.NewClient: tests replace it to capture
// the dial options of each connection kind.
var newClient = grpc.NewClient

// commonDialOptions are shared by every connection: the protocol revision
// header (as ledgerctl sends it), plaintext, the 64 MiB limits, and a dialer
// that keeps the dialed address name as the connection's remote address, so
// grpc.Peer reports "ledger-1:8888" rather than an IP.
func commonDialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpcprotocol.ClientOption(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(defaultCallOptions()...),
		grpc.WithContextDialer(namedDial),
	}
}

// dialShared connects round-robin to every node in addrs (comma-separated).
// withRetry adds ledgerctl's retry policy (the lib route).
func dialShared(addrs string, withRetry bool) (*grpc.ClientConn, error) {
	policy := map[string]any{}
	if withRetry {
		if err := json.Unmarshal([]byte(actions.GRPCRetryPolicy), &policy); err != nil {
			return nil, fmt.Errorf("parsing retry policy: %w", err)
		}
	}
	policy["loadBalancingConfig"] = []any{map[string]any{"round_robin": map[string]any{}}}
	serviceConfig, err := json.Marshal(policy)
	if err != nil {
		return nil, err
	}
	r := manual.NewBuilderWithScheme("ledger")
	var state resolver.State
	for _, a := range strings.Split(addrs, ",") {
		if a = strings.TrimSpace(a); a != "" {
			state.Addresses = append(state.Addresses, resolver.Address{Addr: a})
		}
	}
	r.InitialState(state)

	opts := append(commonDialOptions(),
		grpc.WithDefaultServiceConfig(string(serviceConfig)),
		grpc.WithResolvers(r),
	)

	return newClient("ledger:///", opts...)
}

// dialNode connects directly to one node, without retries, so a per-node call
// reports that node's own answer.
func dialNode(addr string) (*grpc.ClientConn, error) {
	return newClient("passthrough:///"+addr, commonDialOptions()...)
}

// conns holds the client's connections: the two shared ones and a direct
// connection per node, created on first use.
type conns struct {
	lib   *grpc.ClientConn
	plain *grpc.ClientConn

	nodesMu sync.Mutex
	nodes   map[string]*grpc.ClientConn

	// closing is set when close starts (client shutdown, serve).
	closing atomic.Bool
}

func newConns(addrs string) (*conns, error) {
	lib, err := dialShared(addrs, true)
	if err != nil {
		return nil, fmt.Errorf("dial lib: %w", err)
	}
	plain, err := dialShared(addrs, false)
	if err != nil {
		_ = lib.Close()
		return nil, fmt.Errorf("dial plain: %w", err)
	}

	return &conns{lib: lib, plain: plain, nodes: map[string]*grpc.ClientConn{}}, nil
}

// validRoute reports whether route names a known route.
func validRoute(route string) bool {
	switch route {
	case "", routeLib, routePlain:
		return true
	}

	return false
}

// pick returns the connection for a route and node. A dial error is the
// client's own (the outcome is local).
func (c *conns) pick(route, node string) (*grpc.ClientConn, error) {
	if node != "" {
		return c.node(node)
	}
	switch route {
	case "", routeLib:
		return c.lib, nil
	case routePlain:
		return c.plain, nil
	}

	return nil, fmt.Errorf("unknown route %q", route)
}

func (c *conns) node(addr string) (*grpc.ClientConn, error) {
	c.nodesMu.Lock()
	defer c.nodesMu.Unlock()
	if c.closing.Load() {
		// No new connection once close has run; the outcome is a local
		// "dial:" error, which the driver reads as no answer.
		return nil, errors.New("client is shutting down")
	}
	if conn, ok := c.nodes[addr]; ok {
		return conn, nil
	}
	conn, err := dialNode(addr)
	if err != nil {
		return nil, err
	}
	c.nodes[addr] = conn

	return conn, nil
}

func (c *conns) close() {
	c.closing.Store(true)
	for _, conn := range []*grpc.ClientConn{c.lib, c.plain} {
		_ = conn.Close()
	}
	c.nodesMu.Lock()
	defer c.nodesMu.Unlock()
	for _, conn := range c.nodes {
		_ = conn.Close()
	}
}

// routeLabel is the route as reported in logs: "node:<addr>", "plain" or "lib".
func routeLabel(route, node string) string {
	if node != "" {
		return "node:" + node
	}
	if route == "" {
		return routeLib
	}

	return route
}

// namedDial dials TCP and reports the dialed address (a host name, as the
// driver knows the nodes) as the remote address.
func namedDial(ctx context.Context, addr string) (net.Conn, error) {
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	return namedConn{Conn: c, remote: namedAddr(addr)}, nil
}

type namedAddr string

func (namedAddr) Network() string  { return "tcp" }
func (a namedAddr) String() string { return string(a) }

type namedConn struct {
	net.Conn
	remote namedAddr
}

func (c namedConn) RemoteAddr() net.Addr { return c.remote }
