package checkpointcheck_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	cc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/checkpointcheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Real-server conformance of the checkpoint validators (DESIGN §10, the
// bin/oracle integration pattern): a one-node in-process server, a bare
// Tracker fed the real outcomes, every read must validate.

func startServer(t *testing.T) (context.Context, servicepb.BucketServiceClient, clusterpb.ClusterServiceClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	lease := testserver.AllocateNodeLease()
	instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{NodeID: 1, ClusterID: "checkpointcheck", Ports: lease.Ports(), WalDir: t.TempDir(), DataDir: t.TempDir(), Output: io.Discard})
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

	return metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable"), client, cluster
}

func errOutcome(err error) oc.Outcome {
	st, ok := status.FromError(err)
	if !ok {
		return oc.Outcome{Unknown: "transport"}
	}
	out := oc.Outcome{Code: st.Code().String(), Message: st.Message()}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			out.Reason, out.Metadata = info.GetReason(), info.GetMetadata()
		}
	}

	return out
}

func unary[T proto.Message](resp T, err error) oc.Outcome {
	if err != nil {
		return errOutcome(err)
	}
	b, mErr := protojson.Marshal(resp)
	if mErr != nil {
		panic(mErr)
	}

	return oc.Outcome{OK: true, Response: b}
}

func stream[T any, P interface {
	*T
	proto.Message
}](s grpc.ServerStreamingClient[T], err error) oc.Outcome {
	if err != nil {
		return errOutcome(err)
	}
	out := oc.Outcome{OK: true, Complete: true}
	for {
		m, rErr := s.Recv()
		if errors.Is(rErr, io.EOF) {
			break
		}
		if rErr != nil {
			return errOutcome(rErr)
		}
		b, mErr := protojson.Marshal(P(m))
		if mErr != nil {
			panic(mErr)
		}
		out.Items = append(out.Items, b)
	}
	if next := s.Trailer().Get("x-next-cursor"); len(next) > 0 {
		out.NextCursor = next[0]
	}

	return out
}

func TestIntegration_CheckpointsAgainstRealServer(t *testing.T) {
	if testing.Short() {
		t.Skip("in-process server")
	}
	ctx, client, cluster := startServer(t)
	const prefix = "o-it-"
	tr := oc.NewTracker(oc.Config{RunID: "it", TrackedPrefix: prefix, QueryCheckpointLimit: 10})
	keys := 0
	write := func(reqs ...*servicepb.Request) (oc.Outcome, map[string]any) {
		t.Helper()
		keys++
		rec := oc.WriteRecord{Wid: fmt.Sprintf("w%d", keys), Key: fmt.Sprintf("%sk%d", prefix, keys), Role: oc.RoleOriginal, Family: "it", Requests: reqs}
		tk, _, predicted, ref := tr.BeginWrite(rec)
		require.Nil(t, ref)
		var out oc.Outcome
		require.Eventually(t, func() bool {
			out = unary(client.Apply(ctx, servicepb.UnsignedApplyRequest(rec.Key, reqs...)))

			return out.Definitive()
		}, 20*time.Second, 50*time.Millisecond)
		_, evs, ref := tr.EndWrite(tk, out)
		require.Nil(t, ref)
		require.Empty(t, testutil.Findings(evs), "%+v", out)

		return out, predicted
	}
	untracked := func(r *servicepb.Request) {
		t.Helper()
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", r))
		require.NoError(t, err)
	}
	hits := map[oc.EventID]int{}
	read := func(kind, arm string, req proto.Message, out oc.Outcome) []oc.Event {
		t.Helper()
		k, ok := oc.LookupReadKind(kind)
		require.True(t, ok)
		tk, snap, _ := tr.BeginRead(kind, "r")
		defer tr.FinishRead(tk)
		reqJSON, err := protojson.Marshal(req)
		require.NoError(t, err)
		require.False(t, out.Transient(false), "%s %s: transient %+v", kind, arm, out)
		evs := k.Validate(&oc.ReadCtx{T: tr, Ticket: tk, MaxTicket: tr.ReadFrontier(), Snapshot: snap, Kind: kind, Request: reqJSON, Outcome: out, Params: []byte(`{"arm":"` + arm + `"}`)})
		require.Empty(t, testutil.Findings(evs), "%s %s %s: outcome %+v", kind, arm, reqJSON, out)
		for _, e := range evs {
			if e.Kind != oc.EvRaw || e.Cond {
				hits[e.ID]++
			}
		}

		return evs
	}

	// Fresh cluster: empty registry, no schedule (checkpoint_setup's baseline).
	read(cc.KindCheckpointRegistry, "", &clusterpb.ListQueryCheckpointsRequest{}, unary(cluster.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})))
	read(cc.KindCheckpointSchedule, "", &clusterpb.GetQueryCheckpointScheduleRequest{}, unary(cluster.GetQueryCheckpointSchedule(ctx, &clusterpb.GetQueryCheckpointScheduleRequest{})))

	l1, l2 := prefix+"l1", prefix+"l2"
	untracked(testutil.CreateLedgerReq("default"))
	untracked(testutil.CreateLedgerReq("r-burst-it"))
	write(testutil.CreateLedgerReq(l1))
	write(testutil.TxReq(l1, "", "world", "t-0:1", "USD/2", 10))
	write(testutil.TxReq(l1, "", "t-0:1", "users:1", "USD/2", 4), testutil.AccountMetaReq(l1, "t-0:1", "k0", "v0"))
	out, predicted := write(testutil.CreateCheckpointReq())
	require.True(t, out.OK)
	resp, err := oc.DecodeApplyResponse(out.Response)
	require.NoError(t, err)
	id := resp.GetLogs()[0].GetPayload().GetCreatedQueryCheckpoint().GetCheckpointId()
	require.Equal(t, strconv.FormatUint(id, 10), predicted["checkpointId"], "the begin hook predicts the server's id")
	write(testutil.TxReq(l1, "", "world", "t-0:1", "USD/2", 7))
	write(testutil.CreateLedgerReq(l2))

	ro := func() *commonpb.ReadOptions { return &commonpb.ReadOptions{CheckpointId: id} }
	pGetLedger := &servicepb.GetLedgerRequest{Ledger: l1, Read: ro()}
	read(cc.KindPredictedCheckpointRead, "", pGetLedger, unary(client.GetLedger(ctx, pGetLedger)))

	for _, addr := range []string{"t-0:1", "users:1", "t-9:9"} {
		for _, collapse := range []bool{false, true} {
			r := &servicepb.GetAccountRequest{Ledger: l1, Address: addr, CheckpointId: id, CollapseColors: collapse}
			read(cc.KindCheckpointRead, cc.ArmGetAccount, r, unary(client.GetAccount(ctx, r)))
		}
	}
	for _, txID := range []uint64{1, 2, 3, 40} {
		r := &servicepb.GetTransactionRequest{Ledger: l1, TransactionId: txID, CheckpointId: id}
		read(cc.KindCheckpointRead, cc.ArmGetTransaction, r, unary(client.GetTransaction(ctx, r)))
	}
	stats := &servicepb.GetLedgerStatsRequest{Ledger: l1, CheckpointId: id}
	read(cc.KindCheckpointRead, cc.ArmGetLedgerStats, stats, unary(client.GetLedgerStats(ctx, stats)))
	for _, prefixes := range [][]string{nil, {"t-", "users:"}} {
		agg := &servicepb.AggregateVolumesRequest{Ledger: l1, CheckpointId: id, GroupByPrefixes: prefixes}
		read(cc.KindCheckpointRead, cc.ArmAggregateVolumes, agg, unary(client.AggregateVolumes(ctx, agg)))
	}
	frozen, ok := tr.FrozenCheckpoint(id)
	require.True(t, ok)
	maxSeq, _ := tr.FrozenCheckpointMaxSequence(id)
	var seqs []uint64
	for seq := range frozen.Logs() {
		seqs = append(seqs, seq)
	}
	slices.Sort(seqs)
	for _, seq := range append(seqs, maxSeq+1<<40) {
		r := &servicepb.GetLogRequest{Sequence: seq, CheckpointId: id}
		read(cc.KindCheckpointRead, cc.ArmGetLog, r, unary(client.GetLog(ctx, r)))
	}
	for _, size := range []uint32{1, 2, 10} {
		for _, reverse := range []bool{false, true} {
			for _, cursor := range []string{"", "t-0:1"} {
				r := &servicepb.ListAccountsRequest{Ledger: l1, Options: &commonpb.ListOptions{Read: ro(), PageSize: size, Reverse: reverse, Cursor: match.PageToken(cursor)}}
				read(cc.KindCheckpointRead, cc.ArmListAccounts, r, stream(client.ListAccounts(ctx, r)))
			}
			for _, cursor := range []string{"", "1"} {
				r := &servicepb.ListTransactionsRequest{Ledger: l1, Options: &commonpb.ListOptions{Read: ro(), PageSize: size, Reverse: reverse, Cursor: match.PageToken(cursor)}}
				read(cc.KindCheckpointRead, cc.ArmListTransactions, r, stream(client.ListTransactions(ctx, r)))
			}
			for _, cursor := range []string{"", prefix, l1} {
				r := &servicepb.ListLedgersRequest{Options: &commonpb.ListOptions{Read: ro(), PageSize: size, Reverse: reverse, Cursor: match.PageToken(cursor)}}
				read(cc.KindCheckpointRead, cc.ArmListLedgers, r, stream(client.ListLedgers(ctx, r)))
			}
		}
	}

	// Registry and schedule lifecycle.
	read(cc.KindCheckpointRegistry, "", &clusterpb.ListQueryCheckpointsRequest{}, unary(cluster.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})))
	write(setSchedule("@every 876000h"))
	read(cc.KindCheckpointSchedule, "", &clusterpb.GetQueryCheckpointScheduleRequest{}, unary(cluster.GetQueryCheckpointSchedule(ctx, &clusterpb.GetQueryCheckpointScheduleRequest{})))
	out, _ = write(setSchedule("invalid checkpoint cron"))
	require.Equal(t, "INVALID_CRON_EXPRESSION", out.Reason)
	out, _ = write(deleteCheckpoint(0))
	require.Equal(t, "CHECKPOINT_ID_REQUIRED", out.Reason)
	out, _ = write(deleteCheckpoint(tr.Model().NextQueryCheckpointID()))
	require.Equal(t, "CHECKPOINT_NOT_FOUND", out.Reason)
	write(&servicepb.Request{Type: &servicepb.Request_DeleteQueryCheckpointSchedule{DeleteQueryCheckpointSchedule: &servicepb.DeleteQueryCheckpointScheduleRequest{}}})

	// Delete: a deleted checkpoint's reads are NotFound (or the frozen data).
	write(deleteCheckpoint(id))
	r := &servicepb.GetAccountRequest{Ledger: l1, Address: "t-0:1", CheckpointId: id}
	read(cc.KindCheckpointRead, cc.ArmGetAccount, r, unary(client.GetAccount(ctx, r)))
	read(cc.KindCheckpointRegistry, "", &clusterpb.ListQueryCheckpointsRequest{}, unary(cluster.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})))

	// Fill the registry: the 11th live create hits the limit.
	for range 10 {
		write(testutil.CreateCheckpointReq())
	}
	out, _ = write(testutil.CreateCheckpointReq())
	require.Equal(t, "CHECKPOINT_LIMIT_REACHED", out.Reason)

	// Stage/freeze conformance of the limit rejection (the shared
	// TestRejectionStages model has no checkpoint limit, so this shape is
	// checked here): the model's freeze decision predicts whether the server
	// refuses a different body under the same key.
	limitKey := prefix + "k-limit"
	limitBulk := oracle.Bulk{Requests: []*servicepb.Request{testutil.CreateCheckpointReq()}, IdempotencyKey: limitKey}
	mres := tr.Model().Apply(limitBulk)
	require.Equal(t, "CHECKPOINT_LIMIT_REACHED", mres.Reason)
	first := unary(client.Apply(ctx, servicepb.UnsignedApplyRequest(limitKey, limitBulk.Requests...)))
	require.Equal(t, "CHECKPOINT_LIMIT_REACHED", first.Reason)
	replay := unary(client.Apply(ctx, servicepb.UnsignedApplyRequest(limitKey, limitBulk.Requests...)))
	require.Equal(t, "CHECKPOINT_LIMIT_REACHED", replay.Reason)
	other := unary(client.Apply(ctx, servicepb.UnsignedApplyRequest(limitKey, setSchedule("@every 900000h"))))
	modelFrozen := !oc.IsAdmissionStage(mres) && !oc.UncommittedReason(mres.Reason)
	require.Equal(t, modelFrozen, other.Reason == "IDEMPOTENCY_KEY_CONFLICT", "model frozen=%v, different body %+v", modelFrozen, other)
	read(cc.KindCheckpointRegistry, "", &clusterpb.ListQueryCheckpointsRequest{}, unary(cluster.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})))

	t.Logf("read coverage: %v", hits)
	for _, id := range []oc.EventID{cc.CovFrozenRead, cc.CovListing, cc.CovScheduleRead, cc.EvFrozenFleetListing} {
		require.Positive(t, hits[id], "%s", id)
	}
}
