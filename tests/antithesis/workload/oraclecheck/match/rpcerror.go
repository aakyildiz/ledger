package match

import (
	"errors"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RPCError is the classification-relevant part of a gRPC outcome: the status
// code, the ErrorInfo reason and the ErrorInfo metadata. Matchers that took a
// gRPC error in the singleton model take an RPCError instead; callers build it
// from the client outcome (oraclecheck.Outcome's Code, Reason and Metadata).
//
// The zero value (Code == codes.OK) means "no error": the RPC succeeded.
type RPCError struct {
	Code     codes.Code
	Reason   string
	Metadata map[string]string
}

// NoError is the RPCError of a successful RPC.
var NoError = RPCError{}

// OK reports whether e describes a successful RPC.
func (e RPCError) OK() bool { return e.Code == codes.OK }

// HasReason reports whether e is an error carrying the ErrorInfo reason r
// (the model's internal.HasErrorReason).
func (e RPCError) HasReason(r string) bool { return !e.OK() && e.Reason == r }

var codeByName = func() map[string]codes.Code {
	m := map[string]codes.Code{}
	for c := codes.OK; c <= codes.Unauthenticated; c++ {
		m[c.String()] = c
	}

	return m
}()

// RPCErrorFrom builds an RPCError from the client outcome's fields: code is the
// gRPC code name as grpc's codes.Code.String() prints it ("InvalidArgument",
// "Unavailable", …; "" or "OK" mean success), reason the ErrorInfo reason and
// md the ErrorInfo metadata (md["index"] is the label RejectedIndexLabel reads).
// An unrecognised code name maps to codes.Unknown.
func RPCErrorFrom(code, reason string, md map[string]string) RPCError {
	if code == "" {
		return RPCError{Reason: reason, Metadata: md}
	}

	c, ok := codeByName[code]
	if !ok {
		c = codes.Unknown
	}

	return RPCError{Code: c, Reason: reason, Metadata: md}
}

// RPCErrorOf converts a gRPC error (as returned by a generated client) into an
// RPCError, reading the first ErrorInfo detail. nil → NoError. A non-status
// error maps to codes.Unknown with no reason.
func RPCErrorOf(err error) RPCError {
	if err == nil {
		return NoError
	}

	st, ok := status.FromError(err)
	if !ok {
		var se interface{ GRPCStatus() *status.Status }
		if !errors.As(err, &se) {
			return RPCError{Code: codes.Unknown}
		}
		st = se.GRPCStatus()
	}

	out := RPCError{Code: st.Code()}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			out.Reason = info.GetReason()
			out.Metadata = info.GetMetadata()

			break
		}
	}

	return out
}
