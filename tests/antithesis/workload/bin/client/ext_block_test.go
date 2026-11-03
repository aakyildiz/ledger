package main

import (
	"context"
	"encoding/json"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/scenario"
)

// The 26 registered block names (HEGEL-INVENTORY "Registered block
// checklist"; kept in sync with the driver's rules/blocks.rs).
var expectedBlockNames = []string{
	"gaming/buy_item", "gaming/clawback", "gaming/p2p_trade", "gaming/promotion", "gaming/topup",
	"lending/disburse", "lending/fund_pool", "lending/provision", "lending/repay", "lending/write_off",
	"marketplace/deposit", "marketplace/metadata", "marketplace/payout", "marketplace/purchase", "marketplace/revert",
	"multicurrency/fund", "multicurrency/fx", "multicurrency/vendor_pay",
	"payroll/cost_alloc", "payroll/distribute", "payroll/fund_clearing", "payroll/pay_salary",
	"subscription/charge", "subscription/credit", "subscription/fund", "subscription/recognize",
}

func TestBlockRegistryResolvesEveryName(t *testing.T) {
	t.Parallel()
	require.Equal(t, expectedBlockNames, blockNames())
	require.ElementsMatch(t, []string{"gaming", "lending", "marketplace", "multicurrency", "payroll", "subscription"}, keys(blockGroups))
	for _, name := range expectedBlockNames {
		b, ok := blocksByName[name]
		require.True(t, ok, name)
		require.Equal(t, name, b.Name)
		g := blockGroups[scenario.BlockScenario(name)]
		require.NotNil(t, g, name)
		require.NotEmpty(t, g.Setup(), name)
	}
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	return out
}

func TestValueRandomIsDeterministicAndFlagsExhaustion(t *testing.T) {
	t.Parallel()
	values := []uint64{7, 100, 1 << 63, ^uint64(0)}
	draw := func() ([]int64, *valueRandom) {
		r := newValueRandom(values)
		got := []int64{int64(r.Intn(5)), r.Int63n(30), int64(r.Uint64() >> 1), int64(r.Intn(10))}

		return got, r
	}
	a, ra := draw()
	b, _ := draw()
	require.Equal(t, a, b)
	// The legacy one-draw modulo mapping of scenario.RandFunc.
	require.Equal(t, []int64{7 % 5, 100 % 30, int64(uint64(1<<63) >> 1), int64(^uint64(0) % 10)}, a)
	require.Equal(t, 4, ra.used)
	require.False(t, ra.exhausted)

	// The same mapping as scenario.RandFunc over the same values.
	i := 0
	legacy := scenario.RandFunc(func() uint64 { v := values[i]; i++; return v })
	require.Equal(t, int(a[0]), legacy.Intn(5))
	require.Equal(t, a[1], legacy.Int63n(30))

	// Exhaustion: zeros and the flag.
	ra2 := newValueRandom([]uint64{3})
	require.Equal(t, 3, ra2.Intn(10))
	require.False(t, ra2.exhausted)
	require.Equal(t, 0, ra2.Intn(10))
	require.Equal(t, int64(0), ra2.Int63n(10))
	require.Equal(t, uint64(0), ra2.Uint64())
	require.True(t, ra2.exhausted)
	require.Equal(t, 1, ra2.used)
}

// blockFakeServer is a scripted BucketService for block runs: every account
// holds `balance` of every asset, ListTransactions serves two non-reverted
// transactions, Apply records requests and answers with a created
// transaction carrying post-commit volumes.
type blockFakeServer struct {
	servicepb.UnimplementedBucketServiceServer

	balance  int64
	applyErr error

	mu       sync.Mutex
	requests []*servicepb.ApplyRequest
}

func (f *blockFakeServer) Apply(_ context.Context, req *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, req)
	if f.applyErr != nil {
		return nil, f.applyErr
	}
	in, err := commonpb.NewBigUint(big.NewInt(10))
	if err != nil {
		return nil, err
	}
	out, err := commonpb.NewBigUint(big.NewInt(3))
	if err != nil {
		return nil, err
	}
	vol := &commonpb.Volumes{Input: in, Output: out}
	tx := &commonpb.Transaction{Id: 1, PostCommitVolumes: &commonpb.PostCommitVolumes{VolumesByAccount: map[string]*commonpb.VolumesByAssets{
		"customer:1": {Volumes: []*commonpb.VolumeEntry{{Asset: "USD/2", Volumes: vol}}},
	}}}
	log := &commonpb.Log{Sequence: 9, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{Transaction: tx}}}},
	}}}}

	return &servicepb.ApplyResponse{Logs: []*commonpb.Log{log}}, nil
}

func (f *blockFakeServer) GetAccount(_ context.Context, req *servicepb.GetAccountRequest) (*commonpb.Account, error) {
	var volumes []*commonpb.AccountVolume
	for _, asset := range []string{"USD/2", "EUR/2", "GBP/2", "COINS"} {
		volumes = append(volumes, &commonpb.AccountVolume{Asset: asset, Volumes: &commonpb.VolumesWithBalance{Balance: commonpb.NewSignedBigInt(big.NewInt(f.balance))}})
	}

	return &commonpb.Account{Address: req.GetAddress(), Volumes: volumes}, nil
}

func (f *blockFakeServer) ListTransactions(_ *servicepb.ListTransactionsRequest, st grpc.ServerStreamingServer[commonpb.Transaction]) error {
	for _, tx := range []*commonpb.Transaction{{Id: 4, Reverted: true}, {Id: 5}, {Id: 6}} {
		if err := st.Send(tx); err != nil {
			return err
		}
	}

	return nil
}

func (f *blockFakeServer) applied() []*servicepb.ApplyRequest {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]*servicepb.ApplyRequest(nil), f.requests...)
}

// bufconnClient serves f over bufconn, as M/setup_test.go does.
func bufconnClient(t *testing.T, f *blockFakeServer) servicepb.BucketServiceClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	servicepb.RegisterBucketServiceServer(srv, f)
	go func() {
		_ = srv.Serve(listener) // Stop terminates Serve with an expected error.
	}()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	return servicepb.NewBucketServiceClient(classifyingConn{cc: conn})
}

func TestBlockRunUsesOnlyPoppedValues(t *testing.T) {
	t.Parallel()
	f := &blockFakeServer{balance: 1_000_000}
	client := bufconnClient(t, f)

	// marketplace/deposit: customer = 1 + v0 % 50, amount = 100000 + v1 % 1000000.
	res := runBlock(context.Background(), client, blocksByName["marketplace/deposit"], []uint64{53, 1_000_007, 99, 98})
	require.True(t, res.out.OK, res.out.Message)
	require.Equal(t, 2, res.used)
	require.False(t, res.exhausted)
	reqs := f.applied()
	require.Len(t, reqs, 1)
	require.Empty(t, reqs[0].GetUnsigned().GetIdempotencyKey(), "blocks send unkeyed Applies")
	body, err := marshal.Marshal(reqs[0])
	require.NoError(t, err)
	require.Contains(t, string(body), "customer:4")
	require.Contains(t, string(body), "USD/2 100007")
	require.Contains(t, string(res.out.Response), "postCommitVolumes")

	// The same values give the same request.
	res2 := runBlock(context.Background(), client, blocksByName["marketplace/deposit"], []uint64{53, 1_000_007})
	require.True(t, res2.out.OK)
	body2, err := marshal.Marshal(f.applied()[1])
	require.NoError(t, err)
	require.JSONEq(t, string(body), string(body2))

	// marketplace/revert picks among the observed non-reverted transactions.
	res3 := runBlock(context.Background(), client, blocksByName["marketplace/revert"], []uint64{1})
	require.True(t, res3.out.OK)
	require.Equal(t, 1, res3.used)
	require.Equal(t, uint64(6), f.applied()[2].GetUnsigned().GetRequests()[0].GetApply().GetAction().GetRevertTransaction().GetTransactionId())
}

func TestEveryBlockRunsWithinSixteenValues(t *testing.T) {
	t.Parallel()
	f := &blockFakeServer{balance: 100_000_000}
	client := bufconnClient(t, f)
	values := make([]uint64, 16)
	for i := range values {
		values[i] = uint64(i*7919 + 1)
	}
	for _, name := range expectedBlockNames {
		res := runBlock(context.Background(), client, blocksByName[name], values)
		require.True(t, res.out.OK || res.skipped, "%s: %+v", name, res.out)
		require.False(t, res.exhausted, name)
		require.LessOrEqual(t, res.used, 16, name)
	}
	// With no values at all every block still runs: draws return 0.
	for _, name := range expectedBlockNames {
		res := runBlock(context.Background(), client, blocksByName[name], nil)
		require.Equal(t, 0, res.used, name)
		// Only payroll/fund_clearing draws nothing (HEGEL-INVENTORY: W).
		require.Equal(t, name != "payroll/fund_clearing", res.exhausted, name)
	}
}

func TestBlockRunClassifiesErrors(t *testing.T) {
	t.Parallel()
	f := &blockFakeServer{balance: 0}
	client := bufconnClient(t, f)
	// Skip: no balance to purchase with.
	res := runBlock(context.Background(), client, blocksByName["marketplace/purchase"], []uint64{1, 2, 3})
	require.True(t, res.skipped)
	require.False(t, res.out.OK)
	require.Empty(t, f.applied())

	f.applyErr = status.Error(codes.FailedPrecondition, "already reverted")
	res = runBlock(context.Background(), client, blocksByName["marketplace/deposit"], []uint64{1, 2})
	require.False(t, res.out.OK)
	require.False(t, res.skipped)
	require.Equal(t, "FailedPrecondition", res.out.Code)

	// A panicking block is a non-local Unknown error.
	boom := &scenario.Block{Name: "x/boom", Run: func(context.Context, servicepb.BucketServiceClient, scenario.Random) (*servicepb.ApplyResponse, error) {
		panic("boom")
	}}
	res = runBlock(context.Background(), client, boom, nil)
	require.Equal(t, "Unknown", res.out.Code)
	require.False(t, res.out.Local)
}

func TestBlockSetupToleratesAlreadyExists(t *testing.T) {
	t.Parallel()
	f := &blockFakeServer{}
	client := bufconnClient(t, f)
	for _, g := range blockGroups {
		require.True(t, runBlockSetup(context.Background(), client, g).OK)
	}
	require.Len(t, f.applied(), 6)
	require.Empty(t, f.applied()[0].GetUnsigned().GetIdempotencyKey())

	f.applyErr = status.Error(codes.AlreadyExists, "exists")
	require.True(t, runBlockSetup(context.Background(), client, blockGroups["payroll"]).OK)
	f.applyErr = status.Error(codes.Unavailable, "no leader")
	out := runBlockSetup(context.Background(), client, blockGroups["payroll"])
	require.False(t, out.OK)
	require.Equal(t, "Unavailable", out.Code)
}

func TestBlockEndpoints(t *testing.T) {
	// Not parallel: s.mux() sets the process-wide extension server
	// (ext.go), which ext_test.go sets too (task 99: flaky when both ran
	// at once).
	f := &blockFakeServer{balance: 1_000_000}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	servicepb.RegisterBucketServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }() // Serve returns when the test stops the server
	t.Cleanup(srv.Stop)
	cs, err := newConns(lis.Addr().String())
	require.NoError(t, err)
	t.Cleanup(cs.close)
	s := &server{conns: cs}
	mux := s.mux()

	post := func(path, body string) (int, map[string]any) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)))
		var v map[string]any
		if rec.Code == http.StatusOK {
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &v))
		}

		return rec.Code, v
	}

	code, v := post("/block-setup", `{"opId":1,"group":"marketplace","timeoutMs":5000}`)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, true, v["ok"])
	require.Equal(t, boot, v["boot"])

	code, v = post("/block", `{"opId":2,"name":"marketplace/deposit","values":["53","1000007","18446744073709551615"],"timeoutMs":5000}`)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, true, v["ok"])
	require.Equal(t, false, v["skipped"])
	require.Equal(t, false, v["exhausted"])
	require.Equal(t, float64(2), v["used"])
	require.Contains(t, v["response"], "logs")

	code, v = post("/block", `{"opId":3,"name":"marketplace/deposit","values":[],"timeoutMs":5000}`)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, true, v["exhausted"])

	code, _ = post("/block", `{"opId":4,"name":"nope/nope","values":[]}`)
	require.Equal(t, http.StatusBadRequest, code)
	code, _ = post("/block", `{"opId":5,"name":"marketplace/deposit","values":["-1"]}`)
	require.Equal(t, http.StatusBadRequest, code)
	code, _ = post("/block-setup", `{"opId":6,"group":"nope"}`)
	require.Equal(t, http.StatusBadRequest, code)
}

// Every block, on both its skip and its success branches (balances 0, 1 and
// 100M) and over several value vectors (zeros, maxima, mixed), uses at most
// five values, so 16 pre-drawn values never run out.
func TestEveryBlockUsesAtMostFiveValues(t *testing.T) {
	t.Parallel()
	vectors := [][]uint64{make([]uint64, 16), make([]uint64, 16), make([]uint64, 16), make([]uint64, 16)}
	for i := range 16 {
		vectors[1][i] = ^uint64(0)
		vectors[2][i] = uint64(i*7919 + 1)
		vectors[3][i] = uint64(i) * 0x9e3779b97f4a7c15
	}
	for _, balance := range []int64{0, 1, 100_000_000} {
		client := bufconnClient(t, &blockFakeServer{balance: balance})
		for _, name := range expectedBlockNames {
			for vi, values := range vectors {
				res := runBlock(context.Background(), client, blocksByName[name], values)
				require.True(t, res.out.OK || res.skipped, "%s balance %d vector %d: %+v", name, balance, vi, res.out)
				require.False(t, res.exhausted, "%s balance %d vector %d", name, balance, vi)
				require.LessOrEqual(t, res.used, 5, "%s balance %d vector %d", name, balance, vi)
			}
		}
	}
}

// The reason of a business error reaches the driver, which classifies
// NotFound METADATA_NOT_FOUND as expected (rules/blocks.rs `classify`).
func TestBlockRunCarriesTheErrorReason(t *testing.T) {
	t.Parallel()
	st, err := status.New(codes.NotFound, "metadata not found").WithDetails(&errdetails.ErrorInfo{Reason: "METADATA_NOT_FOUND"})
	require.NoError(t, err)
	f := &blockFakeServer{balance: 1_000_000, applyErr: st.Err()}
	client := bufconnClient(t, f)
	res := runBlock(context.Background(), client, blocksByName["marketplace/metadata"], []uint64{0, 0, 0, 0, 0})
	require.False(t, res.out.OK)
	require.False(t, res.out.Local)
	require.Equal(t, "NotFound", res.out.Code)
	require.Equal(t, "METADATA_NOT_FOUND", res.out.Reason)
}
