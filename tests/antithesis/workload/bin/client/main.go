// Command client is the Antithesis client program for the hegel driver
// (tests/antithesis/hegel). It holds the gRPC connections to the ledger
// cluster and executes the RPCs the driver sends it; it makes no choice of its
// own (DESIGN §7).
//
// Routes (opRequest.route, routes.go): "lib" (default) is one shared
// connection configured as ledgerctl configures its own (protocol revision
// header and actions.GRPCRetryPolicy), round-robin over every node; "plain" is
// a second shared round-robin connection without a retry policy; "node" pins a
// direct connection to one node. Every connection accepts 64 MiB messages.
//
// It deliberately imports nothing from the workload's internal package (that
// package declares Antithesis assertions this program would never reach) and
// nothing from antithesis-sdk-go/lifecycle (deps_test.go).
//
// Protocol (HTTP on :8080):
//
//	GET  /health -> {"boot": "<id>"}            (does not touch the servers)
//	POST /op     <- {"opId": n, "service": "bucket"|"cluster"|"restore",
//	                 "method": "Apply", "request": <protojson>, "timeoutMs": n,
//	                 "maxItems": n, "route": "lib"|"plain", "node": "ledger-1:8888",
//	                 "consistency": "stale", "sign": {"keyId", "seed"}}
//	             -> {"boot", "opId", "outcome"}
//	POST /pubkey <- {"seed": "<b64 32 bytes>"} -> {"publicKey": "<b64>"}   (sign.go)
//	POST /digest (digest.go)
//	extension handlers registered with registerExt (ext.go)
//
// outcome is {"ok": true, "response": <protojson>} for a unary RPC,
// {"ok": true, "items": [<protojson>...], "complete": bool, "nextCursor"} for
// a server stream, or {"ok": false, "code": "Unavailable", "reason": "...",
// "message": "...", "metadata": {...}, "local": bool} for an error, plus
// "peer" (the node that answered) and "elapsedMs". Every completion is also
// sent to OUTCOME_ADDR over UDP as {"boot", "opId", "outcome"}, even if the
// HTTP request has already given up, so the driver learns late outcomes.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

var (
	boot = strconv.FormatInt(time.Now().UnixNano(), 10)

	marshal   = protojson.MarshalOptions{UseProtoNames: false, EmitUnpopulated: false}
	unmarshal = protojson.UnmarshalOptions{DiscardUnknown: false}

	services = map[string]protoreflect.FullName{
		"bucket":  servicepb.File_bucket_proto.Services().Get(0).FullName(),
		"cluster": clusterpb.File_cluster_proto.Services().Get(0).FullName(),
		// RestoreService runs on restore nodes only; calls must set node.
		"restore": restorepb.File_restore_proto.Services().Get(0).FullName(),
	}
)

type opRequest struct {
	OpID      uint64          `json:"opId"`
	Service   string          `json:"service"`
	Method    string          `json:"method"`
	Request   json.RawMessage `json:"request"`
	TimeoutMS int64           `json:"timeoutMs"`
	MaxItems  int             `json:"maxItems"`
	// Route selects the connection: "" or "lib" (production retry policy),
	// "plain" (no retry policy). Ignored when Node is set.
	Route string `json:"route"`
	// Node, when set (e.g. "ledger-1:8888"), sends the RPC to that node only,
	// over a direct connection without retries.
	Node string `json:"node"`
	// Consistency, when set, is sent as the x-consistency header ("stale"
	// reads the node's local state).
	Consistency string `json:"consistency"`
	// Sign, when set on a bucket Apply, sends request.unsigned signed (sign.go).
	Sign *signSpec `json:"sign,omitempty"`
}

type outcome struct {
	OK       bool              `json:"ok"`
	Response json.RawMessage   `json:"response,omitempty"`
	Items    []json.RawMessage `json:"items,omitempty"`
	Complete bool              `json:"complete,omitempty"`
	// NextCursor is the x-next-cursor trailer of a completed stream page.
	NextCursor string `json:"nextCursor,omitempty"`
	Code       string `json:"code,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Message    string `json:"message,omitempty"`
	// Metadata carries the ErrorInfo metadata of a business error.
	Metadata map[string]string `json:"metadata,omitempty"`
	// Local marks an error the client produced itself (dial failure,
	// marshalling, a client-side message-size limit): no server answered it.
	Local bool `json:"local,omitempty"`
	// Peer is the node address that answered, when known.
	Peer string `json:"peer,omitempty"`
	// ElapsedMS is the time the RPC took, in milliseconds.
	ElapsedMS int64 `json:"elapsedMs,omitempty"`
}

type event struct {
	Boot    string  `json:"boot"`
	OpID    uint64  `json:"opId"`
	Outcome outcome `json:"outcome"`
}

type server struct {
	conns    *conns
	udp      *net.UDPConn
	udpMu    sync.Mutex
	inFlight sync.WaitGroup
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("client boot=%s", boot)

	cs, err := newConns(os.Getenv("LEDGER_GRPC_ADDR"))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	s := &server{conns: cs, udp: dialOutcome(os.Getenv("OUTCOME_ADDR"))}

	// CLIENT_ADDR lets run_native.sh pick a free port (00d); compose keeps :8080.
	addr := os.Getenv("CLIENT_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	httpServer := &http.Server{Addr: addr, Handler: s.mux(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		<-sigs
		log.Printf("client shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(ctx); err != nil {
			log.Printf("shutdown: %v", err)
		}
	}()
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %v", err)
	}
	cs.close()
}

// mux mounts the core endpoints and every registered extension.
func (s *server) mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]string{"boot": boot})
	})
	mux.HandleFunc("POST /op", s.handleOp)
	mux.HandleFunc("POST /pubkey", handlePubkey)
	mux.HandleFunc("POST /digest", s.handleDigest)
	mountExts(mux, s)

	return mux
}

// dialOutcome resolves the outcome address, retrying once a second, in the
// background, so a missing driver never blocks startup.
func dialOutcome(addr string) *net.UDPConn {
	if addr == "" {
		return nil
	}
	for {
		raddr, err := net.ResolveUDPAddr("udp", addr)
		if err == nil {
			c, err := net.DialUDP("udp", nil, raddr)
			if err == nil {
				return c
			}
		}
		log.Printf("resolving OUTCOME_ADDR %s: %v", addr, err)
		time.Sleep(time.Second)
	}
}

// prepareOp validates an op request and builds its input message. Errors are
// the driver's (HTTP 400).
func prepareOp(req opRequest) (protoreflect.MethodDescriptor, proto.Message, error) {
	if !validRoute(req.Route) {
		return nil, nil, fmt.Errorf("unknown route %q", req.Route)
	}
	if req.Service == "restore" && req.Node == "" {
		return nil, nil, errors.New("restore calls must set node")
	}
	method, err := lookupMethod(req.Service, req.Method)
	if err != nil {
		return nil, nil, err
	}
	in, err := newMessage(method.Input())
	if err != nil {
		return nil, nil, err
	}
	if len(req.Request) > 0 {
		if err := unmarshal.Unmarshal(req.Request, in); err != nil {
			return nil, nil, fmt.Errorf("request: %w", err)
		}
	}
	if req.Sign != nil {
		apply, ok := in.(*servicepb.ApplyRequest)
		if !ok || req.Service != "bucket" {
			return nil, nil, errors.New("sign is only valid on bucket Apply")
		}
		if err := signApply(apply, *req.Sign); err != nil {
			return nil, nil, err
		}
	}

	return method, in, nil
}

func (s *server) handleOp(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var req opRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	method, in, err := prepareOp(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// The RPC runs on its own goroutine with its own deadline, so it completes
	// (and reports over UDP) even if the driver stops waiting.
	done := make(chan outcome, 1)
	s.inFlight.Add(1)
	go func() {
		defer s.inFlight.Done()
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout(req.TimeoutMS))
		defer cancel()
		out := s.callRoute(ctx, req.Route, req.Node, req.Consistency, method, in, req.MaxItems)
		s.report(req.OpID, req.Method, out)
		done <- out
	}()

	select {
	case out := <-done:
		writeJSON(w, event{Boot: boot, OpID: req.OpID, Outcome: out})
	case <-r.Context().Done():
	}
}

func opTimeout(ms int64) time.Duration {
	if ms <= 0 {
		return 10 * time.Second
	}

	return time.Duration(ms) * time.Millisecond
}

// callRoute sends one RPC through a route (or a pinned node), with the
// consistency header, and asserts its classification.
func (s *server) callRoute(ctx context.Context, route, node, consistency string, method protoreflect.MethodDescriptor, in proto.Message, maxItems int) outcome {
	fullMethod := fullMethodName(method)
	start := time.Now()
	conn, err := s.conns.pick(route, node)
	if err != nil {
		out := localOutcome(codes.Internal, "dial: "+err.Error())
		out.ElapsedMS = time.Since(start).Milliseconds()
		assertClassified(fullMethod, node, out)

		return out
	}
	if consistency != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-consistency", consistency)
	}
	out := call(ctx, conn, method, in, maxItems)
	out.ElapsedMS = time.Since(start).Milliseconds()
	assertClassified(fullMethod, node, out)

	return out
}

func fullMethodName(method protoreflect.MethodDescriptor) string {
	return fmt.Sprintf("/%s/%s", method.Parent().FullName(), method.Name())
}

// call sends one RPC and converts its result to an outcome (without
// elapsedMs, which the caller measures).
func call(ctx context.Context, conn *grpc.ClientConn, method protoreflect.MethodDescriptor, in proto.Message, maxItems int) outcome {
	fullMethod := fullMethodName(method)
	var p peer.Peer
	if !method.IsStreamingServer() {
		out, err := newMessage(method.Output())
		if err != nil {
			return localOutcome(codes.Internal, err.Error())
		}
		if err := conn.Invoke(ctx, fullMethod, in, out, grpc.Peer(&p)); err != nil {
			return withPeer(errOutcome(err), &p)
		}
		b, err := marshal.Marshal(out)
		if err != nil {
			return withPeer(localOutcome(codes.Internal, "marshal response: "+err.Error()), &p)
		}

		return withPeer(outcome{OK: true, Response: b}, &p)
	}

	desc := &grpc.StreamDesc{StreamName: string(method.Name()), ServerStreams: true}
	stream, err := conn.NewStream(ctx, desc, fullMethod, grpc.Peer(&p))
	if err != nil {
		return withPeer(errOutcome(err), &p)
	}
	if err := stream.SendMsg(in); err != nil && !errors.Is(err, io.EOF) {
		return withPeer(errOutcome(err), &p)
	}
	if err := stream.CloseSend(); err != nil {
		return withPeer(errOutcome(err), &p)
	}
	if maxItems <= 0 {
		maxItems = 1000
	}
	var items []json.RawMessage
	for {
		item, err := newMessage(method.Output())
		if err != nil {
			return withPeer(localOutcome(codes.Internal, err.Error()), &p)
		}
		err = stream.RecvMsg(item)
		if err == io.EOF {
			out := outcome{OK: true, Items: items, Complete: true}
			if next := stream.Trailer().Get("x-next-cursor"); len(next) > 0 {
				out.NextCursor = next[0]
			}

			return withPeer(out, &p)
		}
		if err != nil {
			return withPeer(errOutcome(err), &p)
		}
		b, err := marshal.Marshal(item)
		if err != nil {
			return withPeer(localOutcome(codes.Internal, "marshal item: "+err.Error()), &p)
		}
		items = append(items, b)
		if len(items) >= maxItems {
			return withPeer(outcome{OK: true, Items: items, Complete: false}, &p)
		}
	}
}

// withPeer records the answering node. grpc.Peer is filled once the call
// reached a transport; an error before that leaves it empty.
func withPeer(out outcome, p *peer.Peer) outcome {
	if p.Addr != nil {
		out.Peer = p.Addr.String()
	}

	return out
}

// report logs the completion and sends it to the driver over UDP. Large
// stream results are summarised on the UDP path; the HTTP response keeps them.
func (s *server) report(opID uint64, method string, out outcome) {
	if out.OK {
		log.Printf("op=%d method=%s ok items=%d peer=%s ms=%d", opID, method, len(out.Items), out.Peer, out.ElapsedMS)
	} else {
		log.Printf("op=%d method=%s error code=%s reason=%s local=%t peer=%s ms=%d msg=%q",
			opID, method, out.Code, out.Reason, out.Local, out.Peer, out.ElapsedMS, out.Message)
	}
	if s.udp == nil {
		return
	}
	b, err := json.Marshal(event{Boot: boot, OpID: opID, Outcome: out})
	if err != nil {
		log.Printf("op=%d marshal event: %v", opID, err)

		return
	}
	if len(b) > 60000 {
		small := out
		small.Items = nil
		small.Response = nil
		small.Message = "truncated"
		b, err = json.Marshal(event{Boot: boot, OpID: opID, Outcome: small})
		if err != nil {
			log.Printf("op=%d marshal event: %v", opID, err)

			return
		}
	}
	s.udpMu.Lock()
	// Best effort: a lost event leaves the operation unknown to the driver,
	// which the driver already allows.
	_, _ = s.udp.Write(b)
	s.udpMu.Unlock()
}

// localOutcome is an error the client produced itself.
func localOutcome(code codes.Code, msg string) outcome {
	return outcome{Code: code.String(), Message: msg, Local: true}
}

// Messages grpc-go uses for its own message-size checks
// (rpc_util.go: recv and send paths). The server's limits are 64 MiB like the
// client's, so a size error with these messages and no ErrorInfo is the
// client's own.
var localSizeMessages = []string{
	"grpc: received message larger than max",
	"grpc: received message after decompression larger than max",
	"grpc: trying to send message larger than max",
}

func errOutcome(err error) outcome {
	// A non-status error converts to codes.Unknown with its message, which is
	// what the outcome should say.
	st, _ := status.FromError(err)
	out := outcome{Code: st.Code().String(), Message: st.Message()}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			out.Reason = info.GetReason()
			out.Metadata = info.GetMetadata()
		}
	}
	if st.Code() == codes.ResourceExhausted && out.Reason == "" {
		for _, m := range localSizeMessages {
			if strings.HasPrefix(st.Message(), m) {
				out.Local = true
			}
		}
	}

	return out
}

func lookupMethod(service, method string) (protoreflect.MethodDescriptor, error) {
	name, ok := services[service]
	if !ok {
		return nil, fmt.Errorf("unknown service %q", service)
	}
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(name)
	if err != nil {
		return nil, err
	}
	m := d.(protoreflect.ServiceDescriptor).Methods().ByName(protoreflect.Name(method))
	if m == nil {
		return nil, fmt.Errorf("unknown method %s.%s", service, method)
	}
	return m, nil
}

func newMessage(d protoreflect.MessageDescriptor) (proto.Message, error) {
	t, err := protoregistry.GlobalTypes.FindMessageByName(d.FullName())
	if err != nil {
		return nil, err
	}
	return t.New().Interface(), nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// The driver gave up on the request; the UDP event carries the outcome.
		log.Printf("writing response: %v", err)
	}
}
