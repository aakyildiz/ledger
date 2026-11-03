package main

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// DESIGN §5.1: the checker library imports no Antithesis SDK and nothing from
// workload/internal; the service imports no workload/internal and not the SDK
// lifecycle package (only the driver reports setup complete). Test files are
// excluded (go list -deps without -test).
//
// The root module's own packages (internal/storage/dal, reached through
// tests/oracle and internal/domain) import the SDK, so the transitive closure
// of oraclecheck necessarily contains it. The library rule is therefore
// checked as: no package of the workload module in the closure imports the
// SDK, and workload/internal is not in the closure at all.
func goListDeps(t *testing.T, pkgs ...string) []string {
	t.Helper()
	args := append([]string{"list", "-deps"}, pkgs...)
	out, err := exec.Command("go", args...).CombinedOutput()
	require.NoError(t, err, "%s", out)

	return strings.Fields(string(out))
}

const (
	workloadModule   = "github.com/formancehq/ledger/v3/tests/antithesis/workload"
	workloadInternal = workloadModule + "/internal"
)

func TestDeps_Library(t *testing.T) {
	t.Parallel()
	for _, dep := range goListDeps(t, "../../oraclecheck/...") {
		require.False(t, dep == workloadInternal || strings.HasPrefix(dep, workloadInternal+"/"), "oraclecheck imports %s", dep)
	}
	out, err := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}} {{join .Imports \" \"}}", "../../oraclecheck/...").CombinedOutput()
	require.NoError(t, err, "%s", out)
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || !strings.HasPrefix(fields[0], workloadModule) {
			continue
		}
		for _, imp := range fields[1:] {
			require.NotContains(t, imp, "antithesis-sdk-go", "%s imports the SDK", fields[0])
		}
	}
}

func TestDeps_Service(t *testing.T) {
	t.Parallel()
	for _, dep := range goListDeps(t, ".") {
		require.False(t, dep == workloadInternal || strings.HasPrefix(dep, workloadInternal+"/"), "bin/oracle imports %s", dep)
		require.NotContains(t, dep, "antithesis-sdk-go/lifecycle", "bin/oracle must not import the SDK lifecycle package")
	}
}
