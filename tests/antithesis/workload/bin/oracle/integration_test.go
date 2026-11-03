package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/bin/oracle/servicetest"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
)

// singleNodeServer starts a real one-node ledger server in process
// (M/skippable_integration_test.go:187-214 pattern).
func singleNodeServer(t *testing.T, clusterID string) (context.Context, servicepb.BucketServiceClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	lease := testserver.AllocateNodeLease()
	instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{NodeID: 1, ClusterID: clusterID, Ports: lease.Ports(), WalDir: t.TempDir(), DataDir: t.TempDir(), Output: io.Discard})
	instruments = append(instruments, testserver.WithBootstrap())
	server := lease.NewService(cmdserver.NewRunCommandWithBindings, testservice.WithInstruments(instruments...))
	require.NoError(t, server.Start(ctx))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		require.NoError(t, server.Stop(stopCtx))
	})
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", lease.Ports().GRPC()), grpcprotocol.ClientOption(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	cluster := clusterpb.NewClusterServiceClient(conn)
	require.Eventually(t, func() bool {
		state, err := cluster.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})

		return err == nil && state.GetLeader() != 0
	}, 10*time.Second, 10*time.Millisecond)
	client := servicepb.NewBucketServiceClient(conn)
	testserver.WaitForWriteAdmission(t, ctx, client)

	return ctx, client
}

// outcomeOf renders a real Apply result as the client program does.
func outcomeOf(resp *servicepb.ApplyResponse, err error) oc.Outcome {
	if err == nil {
		b, mErr := protojson.Marshal(resp)
		if mErr != nil {
			panic(mErr)
		}

		return oc.Outcome{OK: true, Response: b}
	}
	st, ok := status.FromError(err)
	if !ok {
		return oc.Outcome{Code: "Unknown", Message: err.Error(), Local: true}
	}
	out := oc.Outcome{Code: st.Code().String(), Message: st.Message()}
	for _, d := range st.Details() {
		var info *errdetails.ErrorInfo
		if i, ok := d.(*errdetails.ErrorInfo); ok {
			info = i
		}
		if info != nil {
			out.Reason = info.GetReason()
			out.Metadata = info.GetMetadata()
		}
	}

	return out
}

var errNotDefinitive = errors.New("non-definitive outcome")

// applyUntilDefinitive sends the same bytes until the outcome is definitive.
func applyUntilDefinitive(ctx context.Context, client servicepb.BucketServiceClient, key string, reqs []*servicepb.Request) (oc.Outcome, error) {
	for range 50 {
		out := outcomeOf(client.Apply(ctx, servicepb.UnsignedApplyRequest(key, reqs...)))
		if out.Definitive() {
			return out, nil
		}
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}

	return oc.Outcome{}, errNotDefinitive
}

type plannedWrite struct {
	body   map[string]any
	reqs   []*servicepb.Request
	ticket uint64
	out    oc.Outcome
}

func TestIntegration_RealServer(t *testing.T) {
	if testing.Short() {
		t.Skip("in-process server")
	}
	ctx, client := singleNodeServer(t, "oracle-integration")
	_, c := newTestServer(t)
	c.Setup("integ")
	l := []string{c.Ledger(1), c.Ledger(2)}

	send := func(ws []*plannedWrite, order []int) {
		for _, w := range ws {
			m := c.OK("POST", "/write/begin", w.body)
			w.ticket = uint64(m["ticket"].(float64))
		}
		for _, i := range order {
			w := ws[i]
			out, err := applyUntilDefinitive(ctx, client, w.body["idempotencyKey"].(string), w.reqs)
			require.NoError(t, err)
			w.out = out
		}
		for _, i := range order {
			w := ws[i]
			m := c.EndWrite(w.ticket, w.out)
			ok, viol := servicetest.Verdict(m)
			require.True(t, ok, "%v (outcome %+v)", viol, w.out)
		}
	}
	plan := func(trackKey bool, reqs ...*servicepb.Request) *plannedWrite {
		return &plannedWrite{body: c.BeginWriteBody(trackKey, reqs...), reqs: reqs}
	}

	send([]*plannedWrite{plan(false, testutil.CreateLedgerReq(l[0])), plan(false, testutil.CreateLedgerReq(l[1]))}, []int{1, 0})

	rng := rand.New(rand.NewPCG(1, 2))
	addr := func() string {
		if rng.IntN(3) == 0 {
			return "world"
		}

		return fmt.Sprintf("t-%d:%d", rng.IntN(3), rng.IntN(4))
	}
	order := func(ledger string) *servicepb.Request {
		switch rng.IntN(6) {
		case 0:
			return testutil.AccountMetaReq(ledger, addr(), fmt.Sprintf("k%d", rng.IntN(3)), fmt.Sprintf("v%d", rng.IntN(3)))
		case 1:
			return testutil.TxReq(ledger, fmt.Sprintf("%sref%d", c.Prefix, rng.IntN(20)), addr(), addr(), "COIN", uint64(rng.IntN(20)+1))
		default:
			return testutil.TxReq(ledger, "", addr(), addr(), "COIN", uint64(rng.IntN(20)+1))
		}
	}

	sent := 0
	for sent < 50 {
		fan := 1 + rng.IntN(3)
		var ws []*plannedWrite
		for range fan {
			var reqs []*servicepb.Request
			for range 1 + rng.IntN(3) {
				reqs = append(reqs, order(l[rng.IntN(2)]))
			}
			ws = append(ws, plan(rng.IntN(4) == 0, reqs...))
		}
		send(ws, rng.Perm(len(ws)))
		sent += fan

		// A GetAccount read between rounds.
		rb := c.BeginRead("GetAccount")
		a := addr()
		ledger := l[rng.IntN(2)]
		acct, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: ledger, Address: a})
		var out oc.Outcome
		if err == nil {
			b, _ := protojson.Marshal(acct)
			out = oc.Outcome{OK: true, Response: b}
		} else {
			out = outcomeOf(nil, err)
		}
		m := c.EndRead(uint64(rb["ticket"].(float64)), "GetAccount", map[string]any{"ledger": ledger, "address": a}, out)
		ok, viol := servicetest.Verdict(m)
		require.True(t, ok, "%v", viol)
	}

	st := c.OK("GET", "/quiet/status", nil)
	require.EqualValues(t, 0, st["pending"])
	d := c.OK("GET", "/dump", nil)
	require.Greater(t, d["version"].(float64), float64(2))
	t.Logf("drained version %v", d["version"])
}
