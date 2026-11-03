package match

import (
	"testing"

	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/stretchr/testify/require"
)

func TestNextCursorLegal_PresenceFollowsTheModelsVerdict(t *testing.T) {
	t.Parallel()

	// A full page of two, so the structural half is satisfied throughout.
	const rows, pageSize = 2, 2

	// A row is waiting, so the handler's peek fired and it owes a token naming
	// the last row it actually sent.
	require.True(t, NextCursorLegal("acc:b", CursorRequired, "acc:b", rows, pageSize))
	require.False(t, NextCursorLegal("", CursorRequired, "acc:b", rows, pageSize), "a truncated page must hand back a token")
	require.False(t, NextCursorLegal("acc:c", CursorRequired, "acc:b", rows, pageSize), "the token names the last row sent, not the peeked one")

	// Nothing is waiting, so a token would promise a page that does not exist.
	require.True(t, NextCursorLegal("", CursorForbidden, "acc:b", rows, pageSize))
	require.False(t, NextCursorLegal("acc:b", CursorForbidden, "acc:b", rows, pageSize))

	// The leftover rows hinge on a stamp the model has not learned, so both
	// answers are legal — but the value still is not free.
	require.True(t, NextCursorLegal("", CursorEither, "acc:b", rows, pageSize))
	require.True(t, NextCursorLegal("acc:b", CursorEither, "acc:b", rows, pageSize))
	require.False(t, NextCursorLegal("acc:a", CursorEither, "acc:b", rows, pageSize))
}

// The peek cannot fire before a full page has been sent, so a short page carries
// no token whatever the model knows — the one direction that holds even where
// the model cannot enumerate the universe it is reading.
func TestNextCursorLegal_OnlyAFullPageCarriesAToken(t *testing.T) {
	t.Parallel()

	require.False(t, NextCursorLegal("acc:a", CursorEither, "acc:a", 1, 2), "a short page cannot have peeked")
	require.False(t, NextCursorLegal("acc:a", CursorRequired, "acc:a", 1, 2))
	require.True(t, NextCursorLegal("", CursorEither, "acc:a", 1, 2))

	// A page that showed no row has nothing a token could be derived from.
	require.False(t, NextCursorLegal("acc:a", CursorEither, "", 0, 2))
	require.True(t, NextCursorLegal("", CursorForbidden, "", 0, 2))
}

// The driver mirrors the server's page-size bounds rather than importing the
// controller into its own binary, so the mirror is pinned against the real
// clamp here — in the test binary, where linking the controller costs nothing.
func TestEffectivePageSize_MirrorsTheServerClamp(t *testing.T) {
	t.Parallel()

	require.EqualValues(t, ctrl.DefaultPageSize, ServerDefaultPageSize)
	require.EqualValues(t, ctrl.MaxPageSize, ServerMaxPageSize)

	for _, requested := range []int{0, 1, 2, 50, 99, 100, 999, 1000, 1001, 5000} {
		require.EqualValues(t, ctrl.ClampPageSize(uint32(requested)), EffectivePageSize(requested),
			"page size %d", requested)
	}
}

// The three regimes of the server's clamp, spelled out (DESIGN §6.8: page-size
// draws include 0 and sizes above 1000, and validators compare against this).
func TestEffectivePageSize_DefaultInRangeAndClamp(t *testing.T) {
	t.Parallel()

	require.Equal(t, 100, EffectivePageSize(0), "0 asks for the server default")

	for _, inRange := range []int{1, 2, 3, 5, 10, 50, 100, 999, 1000} {
		require.Equal(t, inRange, EffectivePageSize(inRange))
	}

	for _, above := range []int{1001, 4096, 1 << 31} {
		require.Equal(t, 1000, EffectivePageSize(above), "%d clamps to the maximum", above)
	}
}
