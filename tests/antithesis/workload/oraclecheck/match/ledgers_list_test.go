package match

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLedgerWindow_OrdersSkipsAndTruncates(t *testing.T) {
	t.Parallel()

	fleet := []string{"l-c", "l-a", "l-d", "l-b"}

	window, more := LedgerWindow(fleet, "", 10, false)
	require.Equal(t, []string{"l-a", "l-b", "l-c", "l-d"}, window, "the listing is name-ordered whatever order the fleet was created in")
	require.Equal(t, CursorForbidden, more)

	window, more = LedgerWindow(fleet, "", 10, true)
	require.Equal(t, []string{"l-d", "l-c", "l-b", "l-a"}, window)
	require.Equal(t, CursorForbidden, more)

	// Resume is exclusive and runs in the iteration's own direction.
	window, _ = LedgerWindow(fleet, "l-b", 10, false)
	require.Equal(t, []string{"l-c", "l-d"}, window)

	window, _ = LedgerWindow(fleet, "l-b", 10, true)
	require.Equal(t, []string{"l-a"}, window)

	// A name the fleet does not hold still bounds the scan.
	window, _ = LedgerWindow(fleet, "l-bb", 10, false)
	require.Equal(t, []string{"l-c", "l-d"}, window)

	window, more = LedgerWindow(fleet, "", 2, false)
	require.Equal(t, []string{"l-a", "l-b"}, window)
	require.Equal(t, CursorRequired, more, "two ledgers are still waiting")

	window, more = LedgerWindow(fleet, "l-b", 2, false)
	require.Equal(t, []string{"l-c", "l-d"}, window)
	require.Equal(t, CursorForbidden, more, "the page ended on the last ledger")

	window, more = LedgerWindow(fleet, "l-d", 2, false)
	require.Empty(t, window)
	require.Equal(t, CursorForbidden, more)
}

func TestLastLedgerKey_IsTheLastNameServed(t *testing.T) {
	t.Parallel()

	require.Equal(t, "l-b", LastLedgerKey([]string{"l-a", "l-b"}))
	require.Empty(t, LastLedgerKey(nil))
}
