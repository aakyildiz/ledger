// Command gen prints the definitive failure reasons of the hegel oracle
// protocol (DESIGN §4.5), one per line, sorted: every commonpb.ErrorReason
// name without the ERROR_REASON_ prefix whose domain.KindForReason is
// freezable (Validation, NotFound, AlreadyExists, Conflict, Precondition),
// minus NON_DEFINITIVE_OVERRIDES.
//
// Regenerate the fixture from the repository root:
//
//	go run ./tests/antithesis/hegel/protocol-fixtures/core/gen \
//	    > tests/antithesis/hegel/protocol-fixtures/core/definitive_reasons.txt
package main

import (
	"fmt"
	"sort"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// nonDefinitiveOverrides are freezable reasons that are nevertheless not a
// function of the request body and the replicated state. AUDIT_DISABLED is
// raised by admission from node-local audit-projection state.
var nonDefinitiveOverrides = map[string]bool{
	"AUDIT_DISABLED": true,
}

func main() {
	var reasons []string
	for v := range commonpb.ErrorReason_name {
		code := commonpb.ErrorReason(v)
		if !domain.IsFreezableFailure(domain.KindForReason(code)) {
			continue
		}
		name := domain.ReasonString(code)
		if nonDefinitiveOverrides[name] {
			continue
		}
		reasons = append(reasons, name)
	}
	sort.Strings(reasons)
	for _, r := range reasons {
		fmt.Println(r)
	}
}
