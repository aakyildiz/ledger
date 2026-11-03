package main

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The client imports nothing from the workload's internal package (its
// assertions would be cataloged in the client image but never reached) and
// nothing from antithesis-sdk-go/lifecycle (only the driver reports setup
// complete).
func TestClientDeps(t *testing.T) {
	t.Parallel()
	out, err := exec.Command("go", "list", "-deps", ".").CombinedOutput()
	require.NoError(t, err, string(out))
	const internalPkg = "github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	for _, dep := range strings.Fields(string(out)) {
		require.False(t, dep == internalPkg || strings.HasPrefix(dep, internalPkg+"/"), "client imports %s", dep)
		require.NotContains(t, dep, "antithesis-sdk-go/lifecycle", "client imports %s", dep)
	}
	require.Contains(t, strings.Fields(string(out)), "github.com/antithesishq/antithesis-sdk-go/assert")
}
