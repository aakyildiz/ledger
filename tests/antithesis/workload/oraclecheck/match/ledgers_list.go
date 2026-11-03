package match

import (
	"fmt"
	"slices"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/pkg/pagecursor"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// LedgerWindow is the page ListLedgers must serve: the base's live ledgers in
// name order — reversed when reverse — past the exclusive cursor and capped at
// pageSize, with the verdict on whether a further ledger is waiting.
func LedgerWindow(fleet []string, cursor string, pageSize int, reverse bool) ([]string, CursorMore) {
	window := slices.Sorted(slices.Values(fleet))

	if reverse {
		slices.Reverse(window)
	}

	if cursor != "" {
		kept := window[:0]
		for _, name := range window {
			if reverse && name >= cursor || !reverse && name <= cursor {
				continue
			}

			kept = append(kept, name)
		}

		window = kept
	}

	if len(window) > pageSize {
		return window[:pageSize], CursorRequired
	}

	return window, CursorForbidden
}

// LastLedgerKey is the cursor a ledger listing implies: the name of its last
// row, empty for a page that showed none.
func LastLedgerKey(names []string) string {
	if len(names) == 0 {
		return ""
	}

	return names[len(names)-1]
}

// LedgerWindowMatches reports whether names and next are the window base's live
// fleet gives this cursor, page size and direction.
func LedgerWindowMatches(base oracle.GlobalState, names []string, cursor string, pageSize int, reverse bool, next string) bool {
	want, more := LedgerWindow(base.LiveLedgers(), cursor, pageSize, reverse)

	return slices.Equal(names, want) && NextCursorLegal(next, more, LastLedgerKey(names), len(names), pageSize)
}

// ListLedgersReq is the part of a ListLedgers request the listing matcher
// reads: options.cursor (the page token exactly as sent; "" for the first
// page), options.pageSize as requested (before EffectivePageSize) and
// options.reverse.
type ListLedgersReq struct {
	Cursor   string
	PageSize uint32
	Reverse  bool
}

// TrackedLedgerWindow is W of DESIGN §3.2: base's live ledgers whose name has
// the tracked prefix, strictly after cursorKey (a ledger name; "" = from the
// start) in the requested direction, in that order. It is not capped.
func TrackedLedgerWindow(base oracle.GlobalState, prefix, cursorKey string, reverse bool) []string {
	var tracked []string
	for _, name := range base.LiveLedgers() {
		if strings.HasPrefix(name, prefix) {
			tracked = append(tracked, name)
		}
	}

	window, _ := LedgerWindow(tracked, cursorKey, len(tracked)+1, reverse)

	return window
}

// LedgerListingMatches is the one ListLedgers matcher (DESIGN §3.2). The
// listing is shared with ledgers the oracle does not track (scenario ledgers,
// "default", r-* ledgers), which sort on both sides of the tracked range, so
// only the tracked rows are compared with the model and the foreign rows are
// tolerated where the server can put them. With P = EffectivePageSize of the
// requested size, T = the served rows with the tracked prefix (served order)
// and W = TrackedLedgerWindow (the live tracked ledgers strictly after the
// cursor, by direction):
//
//  1. T and W are computed as above;
//  2. T must be a prefix of W, and every row of T must be base's ledger info
//     (LedgerInfoMatches);
//  3. every foreign row lies outside the tracked range — before the first or
//     after the last tracked row of the page, in served order; a foreign row
//     between two tracked rows is a violation, and a foreign row after a
//     tracked row means the tracked range is exhausted, so T must equal W.
//     Because every name between two tracked names carries the tracked prefix,
//     a foreign name is lexically either below the whole range or above it;
//     "after the range, by direction" is read the same way for a page with no
//     tracked row (a foreign row past the range implies T == W), and a foreign
//     row on the wrong side of a tracked row is the same violation;
//  4. len(page) ≤ P; the page length is never compared with len(T);
//  5. len(page) < P (the server says the listing ended): T must equal W and a
//     nextCursor is a violation; len(page) == P: T may be a strict prefix of
//     W, a nextCursor is legal, and an absent nextCursor is legal only if
//     T == W.
//
// nextCursor is the x-next-cursor trailer as served (token or decoded key —
// only its presence is judged). why names the first broken rule; it is ""
// when ok. Callers pass the candidate base (task 04) or the frozen checkpoint
// snapshot (task 09). Structural row checks (LedgerInfoStructureViolation,
// duplicate names, reused ids, LedgerIdentity) are separate.
func LedgerListingMatches(base oracle.GlobalState, prefix string, req ListLedgersReq, page []*commonpb.LedgerInfo, nextCursor string) (ok bool, why string) {
	cur, err := pagecursor.Decode(req.Cursor)
	if err != nil || cur.Back {
		return false, fmt.Sprintf("request cursor %q is not a forward ledger cursor", req.Cursor)
	}

	pageSize := EffectivePageSize(int(req.PageSize))
	want := TrackedLedgerWindow(base, prefix, cur.Key, req.Reverse)

	// beyond reports whether a foreign name sorts past the whole tracked range
	// in the listing's direction.
	beyond := func(name string) bool {
		if req.Reverse {
			return name < prefix
		}

		return name > prefix
	}

	var (
		tracked       []*commonpb.LedgerInfo
		foreignAfter  bool // a foreign row follows a tracked row, or sorts past the range
		lastTrackedAt = -1
	)

	for i, info := range page {
		name := info.GetName()
		if strings.HasPrefix(name, prefix) {
			if foreignAfter {
				return false, fmt.Sprintf("tracked row %q follows a foreign row past the tracked range", name)
			}

			tracked = append(tracked, info)
			lastTrackedAt = i

			continue
		}

		if lastTrackedAt >= 0 && !beyond(name) {
			return false, fmt.Sprintf("foreign row %q below the tracked range follows tracked row %q", name, page[lastTrackedAt].GetName())
		}

		if lastTrackedAt >= 0 || beyond(name) {
			foreignAfter = true
		}
	}

	if len(tracked) > len(want) {
		return false, fmt.Sprintf("served %d tracked rows, the model's window holds %d (%s)", len(tracked), len(want), strings.Join(want, ","))
	}

	for i, info := range tracked {
		if info.GetName() != want[i] {
			return false, fmt.Sprintf("tracked row %d is %q, the model's window has %q (%s)", i, info.GetName(), want[i], strings.Join(want, ","))
		}

		if !LedgerInfoMatches(base, info) {
			return false, fmt.Sprintf("tracked row %q is not the model's ledger info", info.GetName())
		}
	}

	exhausted := len(tracked) == len(want)
	if foreignAfter && !exhausted {
		return false, fmt.Sprintf("a foreign row lies past the tracked range but tracked ledgers remain: served %d of %s", len(tracked), strings.Join(want, ","))
	}

	if len(page) > pageSize {
		return false, fmt.Sprintf("page holds %d rows, more than the effective page size %d", len(page), pageSize)
	}

	if len(page) < pageSize {
		if !exhausted {
			return false, fmt.Sprintf("short page (%d < %d) ended the listing before the tracked window: served %d of %s", len(page), pageSize, len(tracked), strings.Join(want, ","))
		}

		if nextCursor != "" {
			return false, fmt.Sprintf("short page (%d < %d) published a next cursor", len(page), pageSize)
		}

		return true, ""
	}

	if nextCursor == "" && !exhausted {
		return false, fmt.Sprintf("full page without a next cursor ended the listing before the tracked window: served %d of %s", len(tracked), strings.Join(want, ","))
	}

	return true, ""
}
