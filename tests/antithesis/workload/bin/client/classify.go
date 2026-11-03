package main

import (
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/codes"
)

// Reasons the classification looks at (commonpb.ErrorReason names).
const (
	reasonWritesBlockedDiskFull = "WRITES_BLOCKED_DISK_FULL"
	reasonExternalServiceError  = "EXTERNAL_SERVICE_ERROR"
)

// classified reports whether an RPC outcome belongs to the set the harness
// knows how to handle (DESIGN §7 C3). It ports the workload's IsClassified
// (W/internal/client.go:555-573, with IsTransient, IsCanceled and
// isBusinessError) and extends it with the codes the hegel harness can see
// legitimately: Unauthenticated and PermissionDenied (signing is exercised),
// and Unimplemented from a restore node (a restore-agent child that is not in
// restore mode does not serve RestoreService).
//
// Aborted, Internal and Unknown from the server are not classified: an
// occurrence means a silent server path the harness has no model for.
// Outcomes the client produced itself (local) are not server answers and are
// always classified.
func classified(out outcome, node string) bool {
	if out.OK || out.Local {
		return true
	}
	// IsExternalServiceError: transient under any code.
	if out.Reason == reasonExternalServiceError {
		return true
	}
	switch out.Code {
	case codes.Unavailable.String(),
		codes.DeadlineExceeded.String(),
		codes.Canceled.String(),
		codes.NotFound.String(),
		codes.AlreadyExists.String(),
		codes.InvalidArgument.String(),
		codes.FailedPrecondition.String(),
		codes.Unauthenticated.String(),
		codes.PermissionDenied.String():
		return true
	case codes.ResourceExhausted.String():
		return out.Reason == reasonWritesBlockedDiskFull
	case codes.Unimplemented.String():
		return strings.HasPrefix(node, "restore-")
	}

	return false
}

// assertClassified runs after every RPC the client sends, for /op, /digest
// and every extension. One literal, details vary.
func assertClassified(fullMethod, node string, out outcome) {
	assert.Always(classified(out, node), "client: every RPC error the client returns is classified", map[string]any{
		"method": fullMethod,
		"code":   out.Code,
		"reason": out.Reason,
		"node":   node,
	})
}
