package main

import (
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
)

// nonDefinitiveOverrides: freezable reasons that are not a function of the
// body and the replicated state (DESIGN §4.5).
var nonDefinitiveOverrides = map[string]bool{"AUDIT_DISABLED": true}

// definitiveReasons regenerates D from the server's reason table.
func definitiveReasons() []string {
	var out []string
	for v := range commonpb.ErrorReason_name {
		code := commonpb.ErrorReason(v)
		if !domain.IsFreezableFailure(domain.KindForReason(code)) {
			continue
		}
		name := domain.ReasonString(code)
		if nonDefinitiveOverrides[name] {
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)

	return out
}

func readReasonList(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	var out []string
	for _, l := range strings.Split(string(b), "\n") {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
		}
	}

	return out
}

func TestDefinitiveReasons(t *testing.T) {
	t.Parallel()
	want := definitiveReasons()
	require.NotEmpty(t, want)
	require.Equal(t, want, readReasonList(t, fixtureDir+"/definitive_reasons.txt"),
		"regenerate the fixture: go run ./tests/antithesis/hegel/protocol-fixtures/core/gen")
	require.Equal(t, want, oc.DefinitiveReasons(), "run go generate in tests/antithesis/workload/oraclecheck")

	// The driver's include_str! copy must agree (mandatory since 00d).
	require.Equal(t, want, readReasonList(t, "../../../hegel/driver/src/definitive_reasons.txt"),
		"copy the fixture to tests/antithesis/hegel/driver/src/definitive_reasons.txt")
}
