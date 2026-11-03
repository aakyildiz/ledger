package main

// Scenario blocks (task 17, DESIGN §7 C7): the client runs pkg/scenario's
// business blocks for the hegel driver. Every choice a block makes comes from
// a list of values the driver drew with hegel (critic §3.7): no seed, no PRNG.
//
//	POST /block-setup <- {"opId", "group", "timeoutMs"}
//	                  -> {"boot", "opId", "ok", "code", "reason", "message"}
//	POST /block       <- {"opId", "name", "values": ["<u64>", …], "timeoutMs"}
//	                  -> {"boot", "opId", "ok", "skipped", "exhausted", "code",
//	                      "reason", "message", "local", "response", "used"}
//
// Both run through the lib route (the production retry policy), unkeyed, as
// the old scenario loop did (W/internal/block/block.go:77-157). Every RPC a
// block sends is classified (assertClassified), and every completion is
// reported over UDP like /op.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/scenario"
)

func init() {
	registerExt("POST /block-setup", handleBlockSetup)
	registerExt("POST /block", handleBlock)
}

// blockGroups are the six registered scenario groups, keyed by the scenario
// prefix of their block names (pkg/scenario, HEGEL-INVENTORY "Registered
// block checklist").
var blockGroups = func() map[string]*scenario.BlockGroup {
	m := map[string]*scenario.BlockGroup{}
	for _, g := range []*scenario.BlockGroup{
		scenario.MarketplaceBlocks(),
		scenario.GamingWalletBlocks(),
		scenario.LendingLifecycleBlocks(),
		scenario.SubscriptionBlocks(),
		scenario.MultiCurrencyBlocks(),
		scenario.MultiLedgerPayrollBlocks(),
	} {
		m[scenario.BlockScenario(g.Blocks[0].Name)] = g
	}

	return m
}()

// blocksByName indexes every block of blockGroups by its name.
var blocksByName = func() map[string]*scenario.Block {
	m := map[string]*scenario.Block{}
	for _, g := range blockGroups {
		for _, b := range g.Blocks {
			m[b.Name] = b
		}
	}

	return m
}()

// blockNames returns every registered block name, sorted.
func blockNames() []string {
	names := make([]string, 0, len(blocksByName))
	for n := range blocksByName {
		names = append(names, n)
	}
	sort.Strings(names)

	return names
}

// ---------------------------------------------------------------- random

// valueRandom is a scenario.Random backed by a list of pre-drawn values.
// Uint64 pops the next value; Intn and Int63n map one popped value with
// scenario.RandFunc's legacy one-draw modulo (pkg/scenario/block.go:23-40).
// Once the list is exhausted every draw returns 0 and exhausted is set.
type valueRandom struct {
	values    []uint64
	used      int
	exhausted bool
}

func newValueRandom(values []uint64) *valueRandom {
	return &valueRandom{values: values}
}

func (r *valueRandom) Uint64() uint64 {
	if r.used >= len(r.values) {
		r.exhausted = true

		return 0
	}
	v := r.values[r.used]
	r.used++

	return v
}

func (r *valueRandom) Intn(n int) int { return scenario.RandFunc(r.Uint64).Intn(n) }

func (r *valueRandom) Int63n(n int64) int64 { return scenario.RandFunc(r.Uint64).Int63n(n) }

// ---------------------------------------------------------------- classified connection

// classifyingConn sends every RPC through cc and asserts the classification
// of its result (DESIGN §7 C3), as /op does for its own RPCs. Blocks call the
// BucketService through it.
type classifyingConn struct {
	cc grpc.ClientConnInterface
}

func (c classifyingConn) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	err := c.cc.Invoke(ctx, method, args, reply, opts...)
	assertClassified(method, "", rpcOutcome(err))

	return err
}

func (c classifyingConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	st, err := c.cc.NewStream(ctx, desc, method, opts...)
	if err != nil {
		assertClassified(method, "", rpcOutcome(err))

		return nil, err
	}

	return &classifyingStream{ClientStream: st, method: method}, nil
}

// classifyingStream classifies the end of a server stream: io.EOF is a
// success, any other receive error is classified once.
type classifyingStream struct {
	grpc.ClientStream
	method string
	done   bool
}

func (s *classifyingStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err != nil && !s.done {
		s.done = true
		if errors.Is(err, io.EOF) {
			assertClassified(s.method, "", outcome{OK: true})
		} else {
			assertClassified(s.method, "", rpcOutcome(err))
		}
	}

	return err
}

// rpcOutcome converts an RPC result to an outcome (no response body).
func rpcOutcome(err error) outcome {
	if err == nil {
		return outcome{OK: true}
	}

	return errOutcome(err)
}

// ---------------------------------------------------------------- setup

type blockSetupRequest struct {
	OpID      uint64 `json:"opId"`
	Group     string `json:"group"`
	TimeoutMS int64  `json:"timeoutMs"`
}

type blockSetupResponse struct {
	Boot    string `json:"boot"`
	OpID    uint64 `json:"opId"`
	OK      bool   `json:"ok"`
	Code    string `json:"code,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
	Local   bool   `json:"local,omitempty"`
}

// runBlockSetup sends a group's Setup() actions in one unkeyed Apply,
// tolerating AlreadyExists (W/internal/block/block.go:77-110; the driver
// retries the rest).
func runBlockSetup(ctx context.Context, client servicepb.BucketServiceClient, g *scenario.BlockGroup) outcome {
	actions := g.Setup()
	if len(actions) == 0 {
		return outcome{OK: true}
	}
	_, err := scenario.ApplyActions(ctx, client, actions...)
	if err == nil || status.Code(err) == codes.AlreadyExists {
		return outcome{OK: true}
	}

	return errOutcome(err)
}

func handleBlockSetup(w http.ResponseWriter, r *http.Request) {
	s := extServer()
	var req blockSetupRequest
	if err := decodeBody(r, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	g, ok := blockGroups[req.Group]
	if !ok {
		http.Error(w, fmt.Sprintf("unknown block group %q", req.Group), http.StatusBadRequest)
		return
	}
	done := make(chan outcome, 1)
	s.inFlight.Add(1)
	go func() {
		defer s.inFlight.Done()
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout(req.TimeoutMS))
		defer cancel()
		start := time.Now()
		out := runBlockSetup(ctx, s.blockClient(), g)
		out.ElapsedMS = time.Since(start).Milliseconds()
		s.report(req.OpID, "BlockSetup:"+req.Group, out)
		done <- out
	}()
	select {
	case out := <-done:
		s.abortIfOwnClose(out)
		writeJSON(w, blockSetupResponse{
			Boot: boot, OpID: req.OpID, OK: out.OK,
			Code: out.Code, Reason: out.Reason, Message: out.Message, Local: out.Local,
		})
	case <-r.Context().Done():
	}
}

// ---------------------------------------------------------------- block

type blockRequest struct {
	OpID      uint64   `json:"opId"`
	Name      string   `json:"name"`
	Values    []string `json:"values"`
	TimeoutMS int64    `json:"timeoutMs"`
}

type blockResponse struct {
	Boot      string          `json:"boot"`
	OpID      uint64          `json:"opId"`
	OK        bool            `json:"ok"`
	Skipped   bool            `json:"skipped"`
	Exhausted bool            `json:"exhausted"`
	Code      string          `json:"code,omitempty"`
	Reason    string          `json:"reason,omitempty"`
	Message   string          `json:"message,omitempty"`
	Local     bool            `json:"local,omitempty"`
	Response  json.RawMessage `json:"response,omitempty"`
	Used      int             `json:"used"`
}

// blockResult is the outcome of one block run.
type blockResult struct {
	out       outcome
	skipped   bool
	exhausted bool
	used      int
}

// runBlock runs b with a value-list random source. A block panic (a bug in
// the block or in its use of the source) is reported as a non-local Unknown
// error, so the driver flags the block as failed.
func runBlock(ctx context.Context, client servicepb.BucketServiceClient, b *scenario.Block, values []uint64) (res blockResult) {
	rnd := newValueRandom(values)
	defer func() {
		res.used, res.exhausted = rnd.used, rnd.exhausted
		if p := recover(); p != nil {
			res.out = outcome{Code: codes.Unknown.String(), Message: fmt.Sprintf("block panicked: %v", p)}
			res.skipped = false
		}
	}()
	resp, err := b.Run(ctx, client, rnd)
	switch {
	case err == nil:
		res.out = outcome{OK: true}
		if resp != nil {
			body, merr := marshal.Marshal(resp)
			if merr != nil {
				res.out = localOutcome(codes.Internal, "marshal response: "+merr.Error())
			} else {
				res.out.Response = body
			}
		}
	case errors.Is(err, scenario.ErrSkip):
		res.skipped = true
		res.out = outcome{Message: err.Error()}
	default:
		res.out = errOutcome(err)
	}

	return res
}

func handleBlock(w http.ResponseWriter, r *http.Request) {
	s := extServer()
	var req blockRequest
	if err := decodeBody(r, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	b, ok := blocksByName[req.Name]
	if !ok {
		http.Error(w, fmt.Sprintf("unknown block %q", req.Name), http.StatusBadRequest)
		return
	}
	values, err := parseBlockValues(req.Values)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	done := make(chan blockResult, 1)
	s.inFlight.Add(1)
	go func() {
		defer s.inFlight.Done()
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout(req.TimeoutMS))
		defer cancel()
		start := time.Now()
		res := runBlock(ctx, s.blockClient(), b, values)
		res.out.ElapsedMS = time.Since(start).Milliseconds()
		s.report(req.OpID, "Block:"+req.Name, res.out)
		done <- res
	}()
	select {
	case res := <-done:
		s.abortIfOwnClose(res.out)
		writeJSON(w, blockResponse{
			Boot: boot, OpID: req.OpID, OK: res.out.OK, Skipped: res.skipped, Exhausted: res.exhausted,
			Code: res.out.Code, Reason: res.out.Reason, Message: res.out.Message, Local: res.out.Local,
			Response: res.out.Response, Used: res.used,
		})
	case <-r.Context().Done():
	}
}

// parseBlockValues parses the decimal u64 strings of a /block request.
func parseBlockValues(in []string) ([]uint64, error) {
	out := make([]uint64, len(in))
	for i, v := range in {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("values[%d]: %w", i, err)
		}
		out[i] = n
	}

	return out, nil
}

// blockClient is the BucketService over the lib route, with every RPC
// classified.
func (s *server) blockClient() servicepb.BucketServiceClient {
	return servicepb.NewBucketServiceClient(classifyingConn{cc: s.conns.lib})
}

func decodeBody(r *http.Request, v any) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, v); err != nil {
		log.Printf("bad extension request: %v", err)

		return err
	}

	return nil
}
