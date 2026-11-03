package main

import (
	"encoding/json"

	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
)

// Request bodies of the protocol (DESIGN §4). Field names and JSON types are
// fixed by tests/antithesis/hegel/protocol-fixtures/core (fixtures_test.go).

type setupReq struct {
	RunID                string `json:"runId"`
	TrackedPrefix        string `json:"trackedPrefix"`
	QueryCheckpointLimit string `json:"queryCheckpointLimit"`
	LiveTarget           int    `json:"liveTarget"`
}

type viewReq struct {
	Views []json.RawMessage `json:"views"`
}

type writeBeginReq struct {
	Wid            string            `json:"wid"`
	IdempotencyKey string            `json:"idempotencyKey"`
	Requests       []json.RawMessage `json:"requests"`
	TrackKey       bool              `json:"trackKey"`
	Role           string            `json:"role"`
	Of             string            `json:"of,omitempty"`
	Family         string            `json:"family"`
	Signed         bool              `json:"signed"`
	Hint           json.RawMessage   `json:"hint,omitempty"`
}

type via struct {
	OpID   uint64 `json:"opId"`
	Client int    `json:"client"`
	Route  string `json:"route"`
	Late   bool   `json:"late"`
}

type writeEndReq struct {
	Ticket   uint64     `json:"ticket"`
	Outcome  oc.Outcome `json:"outcome"`
	Attempts int        `json:"attempts"`
	Via      via        `json:"via"`
}

type writeLateReq struct {
	Ticket  uint64     `json:"ticket"`
	Outcome oc.Outcome `json:"outcome"`
	Via     via        `json:"via"`
}

type writeAbandonReq struct {
	Ticket   uint64 `json:"ticket"`
	Attempts int    `json:"attempts"`
}

type readBeginReq struct {
	Rid    string            `json:"rid"`
	Kind   string            `json:"kind"`
	Views  []json.RawMessage `json:"views"`
	Params json.RawMessage   `json:"params,omitempty"`
}

type readEndReq struct {
	Ticket  uint64          `json:"ticket"`
	Kind    string          `json:"kind"`
	Request json.RawMessage `json:"request"`
	Outcome oc.Outcome      `json:"outcome"`
	Route   oc.Route        `json:"route"`
	Hint    json.RawMessage `json:"hint,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type readAbandonReq struct {
	Ticket uint64 `json:"ticket"`
}

type quietBeginReq struct {
	Epoch uint64 `json:"epoch"`
}

type quietSectionReq struct {
	Epoch   uint64          `json:"epoch"`
	Section string          `json:"section"`
	Seq     uint64          `json:"seq"`
	Body    json.RawMessage `json:"body"`
}

type quietEndReq struct {
	Epoch uint64 `json:"epoch"`
}

// violation is one finding in a verdict.
type violation struct {
	ID      string         `json:"id"`
	Details map[string]any `json:"details"`
}

// verdict is the protocol's Verdict.
type verdict struct {
	OK         bool        `json:"ok"`
	Skipped    bool        `json:"skipped"`
	Violations []violation `json:"violations"`
}

func verdictOf(evs []oc.Event, skipped bool) verdict {
	v := verdict{OK: true, Skipped: skipped, Violations: []violation{}}
	for _, e := range evs {
		if !e.IsFinding() {
			continue
		}
		v.OK = false
		d := e.Details
		if d == nil {
			d = map[string]any{}
		}
		v.Violations = append(v.Violations, violation{ID: string(e.ID), Details: d})
	}

	return v
}

// coverageOf lists the ids of satisfied coverage events, deduplicated, in
// event order.
func coverageOf(evs []oc.Event) []string {
	out := []string{}
	seen := map[oc.EventID]bool{}
	for _, e := range evs {
		if e.IsFinding() || seen[e.ID] {
			continue
		}
		if e.Kind == oc.EvReachable || ((e.Kind == oc.EvSometimes || e.Kind == oc.EvRaw) && e.Cond) {
			seen[e.ID] = true
			out = append(out, string(e.ID))
		}
	}

	return out
}

// errorBody is the body of every non-200 answer.
func errorBody(ref *oc.Refusal) map[string]any {
	d := ref.Details
	if d == nil {
		d = map[string]any{}
	}

	return map[string]any{"error": map[string]any{"code": ref.Code, "message": ref.Message, "details": d}}
}

// decodeStrict decodes a JSON object body into v.
func decodeStrict(body []byte, v any) error {
	if len(body) == 0 {
		body = []byte("{}")
	}

	return json.Unmarshal(body, v)
}
