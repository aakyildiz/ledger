package querycheck

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

// Fixtures shared with the Rust driver (HG/protocol-fixtures/06).
var fixtureDir = filepath.Join("..", "..", "..", "hegel", "protocol-fixtures", "06")

type tokenCase struct {
	Key   string `json:"key"`
	Token string `json:"token"`
}

// pageTokenKeys are the resume keys the list rules build tokens for (pool
// addresses, decimal ids, the malformed-cursor menu) plus keys exercising
// encoding/json's escaping.
var pageTokenKeys = []string{
	"t-0:1", "t-24:99", "users:31", "world", "1", "256", "18446744073709551615",
	"not-a-cursor", "-1", "0x10", " 12", "12 ", "1.0", "18446744073709551616",
	`quote"back\slash`, "a<b&c>", "tab\tnew\nline", "ctl\x01", "é", "line sep",
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
	for _, c := range got.Cases {
		require.Equal(t, c.Key, CursorKey(c.Token))
	}
}

type filterCase struct {
	Name    string          `json:"name"`
	Target  string          `json:"target"`
	Filter  json.RawMessage `json:"filter"`
	Refused *bool           `json:"refused"`
}

// Every filter the Rust builders and generators produce (fixtures written by
// the Rust test filter_fixtures) unmarshals with the client's protojson
// options; the builder cases also pin whether the oracle classifies them as
// refused.
func TestFilterFixtures_ClientAccepts(t *testing.T) {
	t.Parallel()
	strict := protojson.UnmarshalOptions{DiscardUnknown: false}
	for _, file := range []string{"builders.json", "generated.json"} {
		raw, err := os.ReadFile(filepath.Join(fixtureDir, "filters", file))
		require.NoError(t, err, file)
		var fx struct {
			Cases []filterCase `json:"cases"`
		}
		require.NoError(t, json.Unmarshal(raw, &fx), file)
		require.NotEmpty(t, fx.Cases, file)
		for i, c := range fx.Cases {
			f := &commonpb.QueryFilter{}
			require.NoError(t, strict.Unmarshal(c.Filter, f), "%s #%d %s: %s", file, i, c.Name, c.Filter)
			target := commonpb.QueryTarget(commonpb.QueryTarget_value["QUERY_TARGET_"+c.Target])
			rejected := match.ClassifyRejectedFilter(f, target).Rejected
			if c.Refused != nil {
				require.Equal(t, *c.Refused, rejected, "%s: %s", c.Name, match.DescribeFilter(f))
			}
		}
	}
}
