package oraclecheck

import (
	"encoding/base64"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// BatchDumpLine renders the `[batch-dump]` line of one /write/end in the
// format of M/debug.go dumpBatch, which tests/oracle/cmd/replay parses:
// seq is the minimum committed log sequence on success, else 0; outcome is OK
// or the failure reason. The request is the ApplyRequest the driver sent
// (unsigned form, with the idempotency key).
func BatchDumpLine(ticket uint64, rec WriteRecord, out Outcome) string {
	outcome := "OK"
	var seq uint64
	switch {
	case out.OK:
		if resp, err := DecodeApplyResponse(out.Response); err == nil {
			seq = minLogSequence(resp.GetLogs())
		}
	case out.Reason != "":
		outcome = out.Reason
	default:
		outcome = "ERR:" + out.Code
	}

	b, err := servicepb.UnsignedApplyRequest(rec.Key, rec.Requests...).MarshalVT()
	if err != nil {
		return ""
	}

	return fmt.Sprintf("[batch-dump] ticket=%d seq=%d outcome=%s b64=%s", ticket, seq, outcome, base64.StdEncoding.EncodeToString(b))
}
