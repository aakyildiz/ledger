package auditcheck

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
)

// Fixtures shared with the Rust driver (HG/protocol-fixtures/10).
var fixtureDir = filepath.Join("..", "..", "..", "hegel", "protocol-fixtures", "10")

type tokenCase struct {
	Key   string `json:"key"`
	Token string `json:"token"`
}

// pageTokenKeys are the keys audit_list builds tokens for (decimal audit
// sequences and the malformed-cursor menu) plus keys exercising
// encoding/json's escaping.
var pageTokenKeys = []string{
	"1", "4", "18446744073709551615",
	"not-a-cursor", "-1", "0x10", " 12", "12 ", "1.0", "18446744073709551616",
	`quote"back\slash`, "a<b&c>", "tab\tnew\nline", "ctl\x01", "é",
}

// The Rust page_token must equal Go's pagecursor encoding: this test owns the
// fixture (UPDATE_FIXTURES=1 rewrites it), the Rust test compares against it.
func TestPageTokenFixture(t *testing.T) {
	t.Parallel()
	var want struct {
		Cases []tokenCase `json:"cases"`
	}
	for _, k := range pageTokenKeys {
		want.Cases = append(want.Cases, tokenCase{Key: k, Token: match.PageToken(k)})
	}
	path := filepath.Join(fixtureDir, "page_tokens.json")
	if os.Getenv("UPDATE_FIXTURES") != "" {
		require.NoError(t, os.MkdirAll(fixtureDir, 0o755))
		b, err := json.MarshalIndent(want, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, append(b, '\n'), 0o644))
	}
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var got struct {
		Cases []tokenCase `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Equal(t, want, got)
}

var probeNames = map[string]Probe{
	"none": ProbeNone, "seq-in-or": ProbeSeqInOr, "not": ProbeNot, "unspecified-field": ProbeUnspecifiedField,
	"type-mismatch": ProbeTypeMismatch, "key-param": ProbeKeyParam, "nul-prefix": ProbeNulPrefix,
}

// Every builder shape of audit_list (fixture written by the Rust test
// filter_fixtures) unmarshals with strict protojson, and the oracle classifies
// it as the probe the driver built it to draw.
func TestFilterFixtures_ClientAccepts(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(filepath.Join(fixtureDir, "filters.json"))
	require.NoError(t, err)
	var fx struct {
		Cases []struct {
			Name   string          `json:"name"`
			Filter json.RawMessage `json:"filter"`
			Probe  string          `json:"probe"`
		} `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &fx))
	require.NotEmpty(t, fx.Cases)
	strict := protojson.UnmarshalOptions{DiscardUnknown: false}
	for _, c := range fx.Cases {
		f := &commonpb.QueryFilter{}
		require.NoError(t, strict.Unmarshal(c.Filter, f), "%s: %s", c.Name, c.Filter)
		want, known := probeNames[c.Probe]
		require.True(t, known, c.Probe)
		require.Equal(t, want, ClassifyProbe(f), "%s: %s", c.Name, match.DescribeFilter(f))
	}
}
