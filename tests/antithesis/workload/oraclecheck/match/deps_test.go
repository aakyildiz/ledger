package match

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	workloadModule = "github.com/formancehq/ledger/v3/tests/antithesis/workload"
	oraclePackage  = "github.com/formancehq/ledger/v3/tests/oracle"
	sdkModule      = "github.com/antithesishq/antithesis-sdk-go"
)

func goListDeps(t *testing.T, format string, pkgs ...string) []string {
	t.Helper()

	args := append([]string{"list", "-deps", "-f", format}, pkgs...)
	out, err := exec.Command("go", args...).CombinedOutput()
	require.NoError(t, err, string(out))

	return strings.Split(strings.TrimSpace(string(out)), "\n")
}

// TestNoSDKImport pins the package's purity (DESIGN §5.1): match and
// matchtest return booleans and verdict strings only, so neither they nor
// anything they pull in beyond the oracle model may link the Antithesis SDK,
// and they never import the old workload's internal packages or 00a's
// oraclecheck.
//
// The oracle model itself (tests/oracle) reaches antithesis-sdk-go/assert
// through the server's internal/storage/dal, which every user of O/ inherits;
// that path is outside this package's control, so the SDK packages in match's
// closure must be exactly ones O/ already brings, and no package outside the
// root module's own dependency closure of O/ may import the SDK.
func TestNoSDKImport(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go tool not on PATH")
	}

	oracleDeps := map[string]bool{}
	for _, dep := range goListDeps(t, "{{.ImportPath}}", oraclePackage) {
		oracleDeps[dep] = true
	}

	for _, line := range goListDeps(t, "{{.ImportPath}}|{{join .Imports \",\"}}", ".", "./matchtest") {
		pkg, imports, _ := strings.Cut(line, "|")

		require.False(t, strings.HasPrefix(pkg, workloadModule+"/internal"), "match must not import %s", pkg)
		require.NotEqual(t, workloadModule+"/oraclecheck", pkg, "match must not import oraclecheck (00a)")
		require.False(t, strings.HasPrefix(pkg, workloadModule+"/bin"), "match must not import %s", pkg)

		if strings.HasPrefix(pkg, sdkModule) {
			require.True(t, oracleDeps[pkg], "%s is linked by something other than the oracle model", pkg)
		}

		for _, imp := range strings.Split(imports, ",") {
			if strings.HasPrefix(imp, sdkModule) {
				require.True(t, oracleDeps[pkg], "%s imports the Antithesis SDK and is not part of the oracle model's closure", pkg)
			}
		}
	}
}
