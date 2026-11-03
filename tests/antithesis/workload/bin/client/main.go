// Command client is the Antithesis client program for the hegel driver
// (tests/antithesis/hegel). It holds one shared gRPC connection to the ledger
// cluster, built exactly like the workload's (internal.NewConn: round-robin
// over every node, retry interceptors with backoff, classification), and
// executes the RPCs the driver sends it.
//
// Protocol (HTTP on :8080):
//
//	GET  /health -> {"boot": "<id>"}            (does not touch the servers)
//	POST /op     <- {"opId": n, "service": "bucket"|"cluster", "method": "Apply",
//	                 "request": <protojson>, "timeoutMs": n, "maxItems": n}
//	             -> {"boot", "opId", "outcome"}
//
// outcome is {"ok": true, "response": <protojson>} for a unary RPC,
// {"ok": true, "items": [<protojson>...], "complete": bool} for a server
// stream, or {"ok": false, "code": "Unavailable", "reason": "...",
// "message": "..."} for an error. Every completion is also sent to
// OUTCOME_ADDR over UDP as {"boot", "opId", "outcome"}, even if the HTTP
// request has already given up, so the driver learns late outcomes.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

var (
	boot = strconv.FormatInt(time.Now().UnixNano(), 10)

	marshal   = protojson.MarshalOptions{UseProtoNames: false, EmitUnpopulated: false}
	unmarshal = protojson.UnmarshalOptions{DiscardUnknown: false}

	services = map[string]protoreflect.FullName{
		"bucket":  servicepb.File_bucket_proto.Services().Get(0).FullName(),
		"cluster": clusterpb.File_cluster_proto.Services().Get(0).FullName(),
	}
)

type opRequest struct {
	OpID      uint64          `json:"opId"`
	Service   string          `json:"service"`
	Method    string          `json:"method"`
	Request   json.RawMessage `json:"request"`
	TimeoutMS int64           `json:"timeoutMs"`
	MaxItems  int             `json:"maxItems"`
	// Node, when set (e.g. "ledger-1:8888"), sends the RPC to that node only,
	// over a direct connection, instead of the shared round-robin client.
	Node string `json:"node"`
	// Consistency, when set, is sent as the x-consistency header ("stale"
	// reads the node's local state).
	Consistency string `json:"consistency"`
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
}

type event struct {
	Boot    string  `json:"boot"`
	OpID    uint64  `json:"opId"`
	Outcome outcome `json:"outcome"`
}

type server struct {
	conn     *grpc.ClientConn
	udp      *net.UDPConn
	udpMu    sync.Mutex
	inFlight sync.WaitGroup

	nodesMu sync.Mutex
	nodes   map[string]*grpc.ClientConn
}

// nodeConn returns a direct connection to one node, without retries, so a
// per-node read reports that node's own answer.
func (s *server) nodeConn(addr string) (*grpc.ClientConn, error) {
	s.nodesMu.Lock()
	defer s.nodesMu.Unlock()
	if c, ok := s.nodes[addr]; ok {
		return c, nil
	}
	c, err := grpc.NewClient(addr,
		grpcprotocol.ClientOption(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	s.nodes[addr] = c

	return c, nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("client boot=%s", boot)

	conn, err := internal.NewGRPCConn()
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	s := &server{conn: conn, udp: dialOutcome(os.Getenv("OUTCOME_ADDR")), nodes: map[string]*grpc.ClientConn{}}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]string{"boot": boot})
	})
	mux.HandleFunc("POST /op", s.handleOp)

	httpServer := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		<-sigs
		log.Printf("client shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(ctx)
	}()
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %v", err)
	}
	_ = conn.Close()
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
	method, err := lookupMethod(req.Service, req.Method)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	in, err := newMessage(method.Input())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.Request) > 0 {
		if err := unmarshal.Unmarshal(req.Request, in); err != nil {
			http.Error(w, "request: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	// The RPC runs on its own goroutine with its own deadline, so it completes
	// (and reports over UDP) even if the driver stops waiting.
	done := make(chan outcome, 1)
	s.inFlight.Add(1)
	go func() {
		defer s.inFlight.Done()
		timeout := time.Duration(req.TimeoutMS) * time.Millisecond
		if timeout <= 0 {
			timeout = 10 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if req.Consistency != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "x-consistency", req.Consistency)
		}
		conn := s.conn
		if req.Node != "" {
			c, err := s.nodeConn(req.Node)
			if err != nil {
				out := outcome{Code: "Internal", Message: err.Error()}
				s.report(req, out)
				done <- out
				return
			}
			conn = c
		}
		out := call(ctx, conn, method, in, req.MaxItems)
		s.report(req, out)
		done <- out
	}()

	select {
	case out := <-done:
		writeJSON(w, event{Boot: boot, OpID: req.OpID, Outcome: out})
	case <-r.Context().Done():
	}
}

func call(ctx context.Context, conn *grpc.ClientConn, method protoreflect.MethodDescriptor, in proto.Message, maxItems int) outcome {
	fullMethod := fmt.Sprintf("/%s/%s", method.Parent().FullName(), method.Name())
	if !method.IsStreamingServer() {
		out, err := newMessage(method.Output())
		if err != nil {
			return outcome{Code: "Internal", Message: err.Error()}
		}
		if err := conn.Invoke(ctx, fullMethod, in, out); err != nil {
			return errOutcome(err)
		}
		b, _ := marshal.Marshal(out)
		return outcome{OK: true, Response: b}
	}

	desc := &grpc.StreamDesc{StreamName: string(method.Name()), ServerStreams: true}
	stream, err := conn.NewStream(ctx, desc, fullMethod)
	if err != nil {
		return errOutcome(err)
	}
	if err := stream.SendMsg(in); err != nil {
		return errOutcome(err)
	}
	if err := stream.CloseSend(); err != nil {
		return errOutcome(err)
	}
	if maxItems <= 0 {
		maxItems = 1000
	}
	var items []json.RawMessage
	for {
		item, err := newMessage(method.Output())
		if err != nil {
			return outcome{Code: "Internal", Message: err.Error()}
		}
		err = stream.RecvMsg(item)
		if err == io.EOF {
			out := outcome{OK: true, Items: items, Complete: true}
			if next := stream.Trailer().Get("x-next-cursor"); len(next) > 0 {
				out.NextCursor = next[0]
			}

			return out
		}
		if err != nil {
			return errOutcome(err)
		}
		b, _ := marshal.Marshal(item)
		items = append(items, b)
		if len(items) >= maxItems {
			return outcome{OK: true, Items: items, Complete: false}
		}
	}
}

// report logs the completion and sends it to the driver over UDP. Large
// stream results are summarised on the UDP path; the HTTP response keeps them.
func (s *server) report(req opRequest, out outcome) {
	if out.OK {
		log.Printf("op=%d method=%s ok items=%d", req.OpID, req.Method, len(out.Items))
	} else {
		log.Printf("op=%d method=%s error code=%s reason=%s msg=%q", req.OpID, req.Method, out.Code, out.Reason, out.Message)
	}
	if s.udp == nil {
		return
	}
	b, _ := json.Marshal(event{Boot: boot, OpID: req.OpID, Outcome: out})
	if len(b) > 60000 {
		small := out
		small.Items = nil
		small.Response = nil
		small.Message = "truncated"
		b, _ = json.Marshal(event{Boot: boot, OpID: req.OpID, Outcome: small})
	}
	s.udpMu.Lock()
	_, _ = s.udp.Write(b)
	s.udpMu.Unlock()
}

func errOutcome(err error) outcome {
	st, _ := status.FromError(err)
	out := outcome{Code: st.Code().String(), Message: st.Message()}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			out.Reason = info.GetReason()
			out.Metadata = info.GetMetadata()
		}
	}
	if out.Reason == "" {
		out.Reason = internal.ErrorReason(err)
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
	_ = json.NewEncoder(w).Encode(v)
}
