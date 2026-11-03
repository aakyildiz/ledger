package main

import (
	"context"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// fakeBucket is a scripted BucketService on a local port.
type fakeBucket struct {
	servicepb.UnimplementedBucketServiceServer

	name string // reported in GetLedger responses, to see which node answered

	mu sync.Mutex
	// unavailable is the number of GetLedger calls still to fail with Unavailable.
	unavailable int
	// calls counts GetLedger calls.
	calls int
	// metadataBytes, when set, pads the GetLedger response to that size.
	metadataBytes int

	ledgers  []string
	accounts map[string][]string
	stats    map[string]*commonpb.LedgerStats
}

func (f *fakeBucket) GetLedger(_ context.Context, req *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.unavailable > 0 {
		f.unavailable--
		return nil, status.Error(codes.Unavailable, "scripted unavailable")
	}
	info := &commonpb.LedgerInfo{Name: f.name}
	if f.metadataBytes > 0 {
		info.Metadata = map[string]*commonpb.MetadataValue{
			"pad": {Type: &commonpb.MetadataValue_StringValue{StringValue: strings.Repeat("x", f.metadataBytes)}},
		}
	}

	return info, nil
}

// pageOf serves items in pages of the requested size, with a numeric cursor.
func pageOf(items []string, opts *commonpb.ListOptions) ([]string, string) {
	start := 0
	if c := opts.GetCursor(); c != "" {
		start, _ = strconv.Atoi(c)
	}
	size := int(opts.GetPageSize())
	if size <= 0 {
		size = 100
	}
	end := min(start+size, len(items))
	next := ""
	if end < len(items) {
		next = strconv.Itoa(end)
	}

	return items[start:end], next
}

func (f *fakeBucket) ListLedgers(req *servicepb.ListLedgersRequest, st grpc.ServerStreamingServer[commonpb.LedgerInfo]) error {
	page, next := pageOf(f.ledgers, req.GetOptions())
	for _, name := range page {
		if err := st.Send(&commonpb.LedgerInfo{Name: name, Id: uint32(slices.Index(f.ledgers, name) + 1)}); err != nil {
			return err
		}
	}
	if next != "" {
		st.SetTrailer(metadata.Pairs("x-next-cursor", next))
	}

	return nil
}

func (f *fakeBucket) ListAccounts(req *servicepb.ListAccountsRequest, st grpc.ServerStreamingServer[commonpb.Account]) error {
	accounts, ok := f.accounts[req.GetLedger()]
	if !ok {
		return status.Error(codes.NotFound, "no such ledger")
	}
	page, next := pageOf(accounts, req.GetOptions())
	for _, a := range page {
		if err := st.Send(&commonpb.Account{Address: a}); err != nil {
			return err
		}
	}
	if next != "" {
		st.SetTrailer(metadata.Pairs("x-next-cursor", next))
	}

	return nil
}

func (f *fakeBucket) GetLedgerStats(_ context.Context, req *servicepb.GetLedgerStatsRequest) (*commonpb.LedgerStats, error) {
	return f.stats[req.GetLedger()], nil
}

// startFake serves f on a local port and returns its address.
func startFake(t *testing.T, f *fakeBucket) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer(grpc.MaxSendMsgSize(128<<20), grpc.MaxRecvMsgSize(128<<20))
	servicepb.RegisterBucketServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }() // Serve returns when the test stops the server
	t.Cleanup(srv.Stop)

	return lis.Addr().String()
}
