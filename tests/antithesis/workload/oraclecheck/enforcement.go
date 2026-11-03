package oraclecheck

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// requestedEnforcementMode returns the mode a request sets, through either
// request form, or nil (lifted from M/apply_surface.go).
func requestedEnforcementMode(req *servicepb.Request) *commonpb.ChartEnforcementMode {
	if setter := req.GetSetDefaultEnforcementMode(); setter != nil {
		mode := setter.GetEnforcementMode()

		return &mode
	}
	if setter := req.GetApply().GetAction().GetSetDefaultEnforcementMode(); setter != nil {
		mode := setter.GetEnforcementMode()

		return &mode
	}

	return nil
}
