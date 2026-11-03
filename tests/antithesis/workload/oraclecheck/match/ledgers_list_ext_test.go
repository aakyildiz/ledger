package match_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match/matchtest"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// DESIGN §3.2 fixture: tracked ledgers o-r-l1..l3 (o-r-l4 deleted), foreign
// names on both sides of the tracked range, as the shared listing holds them.
const listPrefix = "o-r-"

var (
	foreignBefore = []string{"billing", "default", "dept-a"}
	foreignAfter  = []string{"r-ns-x", "treasury"}
	allForeign    = append(append([]string{}, foreignBefore...), foreignAfter...)
)

func listingFleet(t *testing.T) oracle.GlobalState {
	t.Helper()

	var reqs []*servicepb.Request
	for _, name := range []string{"o-r-l1", "o-r-l2", "o-r-l3", "o-r-l4"} {
		reqs = append(reqs, &servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: name}}})
	}

	reqs = append(reqs,
		actions.SaveLedgerMetadataAction("o-r-l2", map[string]string{"owner": "treasury"}),
		actions.DeleteLedgerAction("o-r-l4"),
	)

	res := oracle.NewGlobalState().Apply(oracle.Bulk{Requests: reqs})
	require.True(t, res.OK, res.Reason)

	return res.State
}

// rows renders a hand-built page: tracked names as the model's ledger info,
// foreign names as bare rows.
func rows(gs oracle.GlobalState, names ...string) []*commonpb.LedgerInfo {
	out := make([]*commonpb.LedgerInfo, 0, len(names))
	for _, name := range names {
		if info := matchtest.RenderLedgerInfo(gs, name); info != nil {
			out = append(out, info)

			continue
		}

		out = append(out, &commonpb.LedgerInfo{Name: name, Id: 1, CreatedAt: &commonpb.Timestamp{Data: 1}})
	}

	return out
}

func listReq(cursor string, size uint32, reverse bool) match.ListLedgersReq {
	return match.ListLedgersReq{Cursor: match.PageToken(cursor), PageSize: size, Reverse: reverse}
}

// Every page of every traversal the server can serve over the shared fleet
// passes: page sizes from 1 (pages of foreign rows only, pages straddling each
// boundary) to the whole listing, both directions, resumed through the
// published token.
func TestLedgerListingMatches_ServerTraversalsPass(t *testing.T) {
	t.Parallel()
	gs := listingFleet(t)

	for _, reverse := range []bool{false, true} {
		for size := uint32(0); size <= 9; size++ {
			req := match.ListLedgersReq{PageSize: size, Reverse: reverse}
			for pages := 0; ; pages++ {
				require.Less(t, pages, 20)

				page, token := matchtest.RenderLedgerPage(gs, listPrefix, allForeign, req)
				ok, why := match.LedgerListingMatches(gs, listPrefix, req, page, token)
				require.True(t, ok, "reverse=%v size=%d cursor=%q: %s", reverse, size, req.Cursor, why)

				if token == "" {
					break
				}

				req.Cursor = token
			}
		}
	}
}

func TestLedgerListingMatches_DesignCases(t *testing.T) {
	t.Parallel()
	gs := listingFleet(t)

	cases := []struct {
		name   string
		req    match.ListLedgersReq
		page   []string
		next   string
		ok     bool
		reason string
	}{
		// Foreign names before and after the tracked range.
		{name: "foreign before the range, then the whole range, then foreign after", req: listReq("", 10, false),
			page: []string{"billing", "default", "dept-a", "o-r-l1", "o-r-l2", "o-r-l3", "r-ns-x", "treasury"}, ok: true},
		{name: "reverse: foreign after the range (lexically above) comes first", req: listReq("", 10, true),
			page: []string{"treasury", "r-ns-x", "o-r-l3", "o-r-l2", "o-r-l1", "dept-a", "default", "billing"}, ok: true},
		// Pages straddling each boundary, both directions.
		{name: "straddles the lower boundary forward", req: listReq("default", 3, false),
			page: []string{"dept-a", "o-r-l1", "o-r-l2"}, next: "t", ok: true},
		{name: "straddles the upper boundary forward", req: listReq("o-r-l2", 3, false),
			page: []string{"o-r-l3", "r-ns-x", "treasury"}, ok: true},
		{name: "straddles the upper boundary in reverse", req: listReq("treasury", 3, true),
			page: []string{"r-ns-x", "o-r-l3", "o-r-l2"}, next: "t", ok: true},
		{name: "straddles the lower boundary in reverse", req: listReq("o-r-l2", 3, true),
			page: []string{"o-r-l1", "dept-a", "default"}, next: "t", ok: true},
		// A full page of foreign rows.
		{name: "a full page of foreign rows before the range", req: listReq("", 2, false),
			page: []string{"billing", "default"}, next: "t", ok: true},
		{name: "a full page of foreign rows past the range once it is exhausted", req: listReq("o-r-l3", 2, false),
			page: []string{"r-ns-x", "treasury"}, next: "t", ok: true},
		{name: "a full page of foreign rows past the range before it is listed", req: listReq("", 2, false),
			page: []string{"r-ns-x", "treasury"}, next: "t", ok: false, reason: "rule 3 (lexical): tracked ledgers remain"},
		{name: "reverse: a full page of foreign rows below the range while tracked remain", req: listReq("treasury", 2, true),
			page: []string{"dept-a", "default"}, next: "t", ok: false, reason: "rule 3"},
		// A short page that omits a tracked ledger.
		{name: "short page omits a tracked ledger in the middle", req: listReq("", 10, false),
			page: []string{"billing", "default", "dept-a", "o-r-l1", "o-r-l3", "r-ns-x", "treasury"}, ok: false, reason: "rule 2"},
		{name: "short page ends before the tracked range", req: listReq("", 10, false),
			page: []string{"billing", "default", "dept-a"}, ok: false, reason: "rule 5"},
		{name: "short page omits the last tracked ledger", req: listReq("o-r-l1", 10, false),
			page: []string{"o-r-l2"}, ok: false, reason: "rule 5"},
		// A foreign row between two tracked rows.
		{name: "foreign row between two tracked rows", req: listReq("", 10, false),
			page: []string{"o-r-l1", "r-ns-x", "o-r-l2", "o-r-l3"}, ok: false, reason: "rule 3"},
		{name: "foreign row below the range between two tracked rows", req: listReq("", 10, false),
			page: []string{"o-r-l1", "dept-a", "o-r-l2", "o-r-l3"}, ok: false, reason: "rule 3"},
		{name: "foreign row after a tracked row while tracked remain (full page)", req: listReq("dept-a", 2, false),
			page: []string{"o-r-l1", "r-ns-x"}, next: "t", ok: false, reason: "rule 3"},
		// Rule 2: prefix of the window, model rows.
		{name: "a deleted tracked ledger is served", req: listReq("o-r-l3", 10, false),
			page: []string{"o-r-l4", "r-ns-x", "treasury"}, ok: false, reason: "rule 2"},
		{name: "tracked rows out of order", req: listReq("", 10, false),
			page: []string{"o-r-l2", "o-r-l1", "o-r-l3"}, ok: false, reason: "rule 2"},
		{name: "a tracked row before the cursor", req: listReq("o-r-l1", 10, false),
			page: []string{"o-r-l1", "o-r-l2", "o-r-l3"}, ok: false, reason: "rule 2"},
		// Rules 4 and 5: page length and cursor legality by fullness.
		{name: "full page, strict prefix, with a token", req: listReq("dept-a", 2, false),
			page: []string{"o-r-l1", "o-r-l2"}, next: "t", ok: true},
		{name: "full page, strict prefix, no token", req: listReq("dept-a", 2, false),
			page: []string{"o-r-l1", "o-r-l2"}, ok: false, reason: "rule 5"},
		{name: "full page, window exhausted, no token", req: listReq("o-r-l1", 2, false),
			page: []string{"o-r-l2", "o-r-l3"}, ok: true},
		{name: "full page, window exhausted, a token", req: listReq("o-r-l1", 2, false),
			page: []string{"o-r-l2", "o-r-l3"}, next: "t", ok: true},
		{name: "short page with a token", req: listReq("o-r-l1", 10, false),
			page: []string{"o-r-l2", "o-r-l3"}, next: "t", ok: false, reason: "rule 5"},
		{name: "page longer than the effective size", req: listReq("", 2, false),
			page: []string{"billing", "default", "dept-a"}, next: "t", ok: false, reason: "rule 4"},
		{name: "page size 0 is the server default", req: listReq("", 0, false),
			page: []string{"billing", "default", "dept-a", "o-r-l1", "o-r-l2", "o-r-l3", "r-ns-x", "treasury"}, ok: true},
		{name: "page length is never compared with the tracked count", req: listReq("", 4, false),
			page: []string{"billing", "default", "dept-a", "o-r-l1"}, next: "t", ok: true},
		{name: "an empty listing past the end", req: listReq("treasury", 10, false), page: nil, ok: true},
		{name: "an empty page while tracked remain", req: listReq("", 10, false), page: nil, ok: false, reason: "rule 5"},
	}

	for _, c := range cases {
		ok, why := match.LedgerListingMatches(gs, listPrefix, c.req, rows(gs, c.page...), c.next)
		require.Equal(t, c.ok, ok, "%s (%s): %s", c.name, c.reason, why)
		if !ok {
			require.NotEmpty(t, why, c.name)
		}
	}
}

// A served tracked row must be the model's whole ledger info; a foreign row's
// content is never compared.
func TestLedgerListingMatches_RowContent(t *testing.T) {
	t.Parallel()
	gs := listingFleet(t)

	page := rows(gs, "dept-a", "o-r-l1", "o-r-l2", "o-r-l3", "r-ns-x")
	req := listReq("default", 10, false)

	ok, why := match.LedgerListingMatches(gs, listPrefix, req, page, "")
	require.True(t, ok, why)

	page[0].Metadata = map[string]*commonpb.MetadataValue{"anything": {Type: &commonpb.MetadataValue_StringValue{StringValue: "x"}}}
	ok, why = match.LedgerListingMatches(gs, listPrefix, req, page, "")
	require.True(t, ok, "foreign row content is not the oracle's: %s", why)

	page[2].Metadata = nil // o-r-l2 carries owner=treasury in the model
	ok, _ = match.LedgerListingMatches(gs, listPrefix, req, page, "")
	require.False(t, ok, "a tracked row must match the model's ledger info")
}

func TestLedgerListingMatches_RefusesUnsupportedCursors(t *testing.T) {
	t.Parallel()
	gs := listingFleet(t)

	ok, why := match.LedgerListingMatches(gs, listPrefix, match.ListLedgersReq{Cursor: "%%%", PageSize: 10}, nil, "")
	require.False(t, ok)
	require.Contains(t, why, "cursor")
}
