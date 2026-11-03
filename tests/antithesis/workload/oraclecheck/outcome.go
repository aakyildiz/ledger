package oraclecheck

//go:generate go run ./internal/genreasons -in ../../hegel/protocol-fixtures/core/definitive_reasons.txt -out outcome_reasons.go

import (
	"encoding/json"
	"strings"
)

// Outcome is the client outcome object (DESIGN §4.5), as bin/client writes it
// plus the driver's {"unknown": why} for no answer.
type Outcome struct {
	OK         bool
	Response   json.RawMessage
	Items      []json.RawMessage
	Complete   bool
	NextCursor string
	Code       string
	Reason     string
	Message    string
	Metadata   map[string]string
	Local      bool
	Unknown    string
	Peer       string
	ElapsedMs  int64
}

type outcomeJSON struct {
	OK         *bool             `json:"ok,omitempty"`
	Response   json.RawMessage   `json:"response,omitempty"`
	Items      []json.RawMessage `json:"items,omitempty"`
	Complete   bool              `json:"complete,omitempty"`
	NextCursor string            `json:"nextCursor,omitempty"`
	Code       string            `json:"code,omitempty"`
	Reason     string            `json:"reason,omitempty"`
	Message    string            `json:"message,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	Local      bool              `json:"local,omitempty"`
	Unknown    string            `json:"unknown,omitempty"`
	Peer       string            `json:"peer,omitempty"`
	ElapsedMs  int64             `json:"elapsedMs,omitempty"`
}

// MarshalJSON writes the outcome as the client does: "ok" is present on every
// answer, absent on the driver's "unknown" outcome; other empty fields are
// omitted.
func (o Outcome) MarshalJSON() ([]byte, error) {
	j := outcomeJSON{
		Response: o.Response, Items: o.Items, Complete: o.Complete, NextCursor: o.NextCursor,
		Code: o.Code, Reason: o.Reason, Message: o.Message, Metadata: o.Metadata, Local: o.Local,
		Unknown: o.Unknown, Peer: o.Peer, ElapsedMs: o.ElapsedMs,
	}
	if o.Unknown == "" {
		ok := o.OK
		j.OK = &ok
	}

	return json.Marshal(j)
}

// UnmarshalJSON reads a client outcome.
func (o *Outcome) UnmarshalJSON(b []byte) error {
	var j outcomeJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return err
	}
	*o = Outcome{
		Response: j.Response, Items: j.Items, Complete: j.Complete, NextCursor: j.NextCursor,
		Code: j.Code, Reason: j.Reason, Message: j.Message, Metadata: j.Metadata, Local: j.Local,
		Unknown: j.Unknown, Peer: j.Peer, ElapsedMs: j.ElapsedMs,
	}
	if j.OK != nil {
		o.OK = *j.OK
	}

	return nil
}

// IsDefinitiveReason reports whether reason ∈ D (DESIGN §4.5).
func IsDefinitiveReason(reason string) bool {
	return definitiveReasons[reason]
}

// DefinitiveReasons returns D, sorted.
func DefinitiveReasons() []string {
	return append([]string(nil), definitiveReasonList...)
}

// Definitive reports whether a write outcome is definitive (DESIGN §4.5): a
// success, or a non-local error whose reason is in D.
func (o Outcome) Definitive() bool {
	if o.Unknown != "" {
		return false
	}
	if o.OK {
		return true
	}

	return !o.Local && o.Reason != "" && IsDefinitiveReason(o.Reason)
}

// DefinitiveFrozen reports whether the outcome is definitive and its reason is
// not one the server leaves unfrozen (oracle.UncommittedReasons).
func (o Outcome) DefinitiveFrozen() bool {
	return o.Definitive() && (o.OK || !uncommittedReasons[o.Reason])
}

// Transient reports whether a read outcome belongs to the skip class (DESIGN
// §4.7): unknown, Unavailable without INDEX_BUILDING, DeadlineExceeded,
// Canceled, and, for kinds that send an Apply, Unauthenticated and
// PermissionDenied.
func (o Outcome) Transient(sendsWrites bool) bool {
	if o.Unknown != "" {
		return true
	}
	if o.OK {
		return false
	}
	switch o.Code {
	case "Unavailable":
		return o.Reason != "INDEX_BUILDING"
	case "DeadlineExceeded", "Canceled":
		return true
	case "Unauthenticated", "PermissionDenied":
		return sendsWrites
	}

	return false
}

// IsNotFound reports a gRPC NotFound answer.
func (o Outcome) IsNotFound() bool {
	return !o.OK && o.Unknown == "" && strings.EqualFold(o.Code, "NotFound")
}
