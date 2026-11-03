package match

import "github.com/formancehq/ledger/v3/pkg/pagecursor"

// CursorMore is the model's verdict on whether a row follows the page a token
// accompanies.
type CursorMore uint8

const (
	// CursorForbidden: the page exhausted the rows its base holds.
	CursorForbidden CursorMore = iota
	// CursorRequired: a row the base requires is waiting past the page.
	CursorRequired
	// CursorEither: the only rows left hinge on a server stamp the model has
	// not learned, so serving them or stopping are both legal.
	CursorEither
)

// NextCursorKey decodes a published next token to its key. A token that does
// not decode to a forward position comes back prefixed with
// undecodableCursor, so it matches no key a page can end on.
func NextCursorKey(token string) string {
	c, err := pagecursor.Decode(token)
	if err != nil || c.Back || c.Key == "" {
		return undecodableCursor + token
	}

	return c.Key
}

const undecodableCursor = "\x00undecodable:"

// NextCursorLegal reports whether the token published alongside a page is the
// one that page's own rows imply.
//
// The handler peeks one row past the page and publishes a token only when the
// peek fires, so a token is a claim that another row exists — and the token is
// the LAST SENT row's key, not the peeked row's, because resume is exclusive
// and naming the peek would skip it.
//
// rows and pageSize carry the structural half, which holds whatever the model
// knows: the peek cannot fire before a full page has been sent, so a page the
// size limit did not fill carries no token. A follower that routed the read
// forwards the leader's token instead of deriving its own, but only after
// relaying the leader's whole page, which is full for the same reason — the
// leader caps at its own maximum and advertises the remainder through the
// trailer (adapter/grpc/cursor.go, upstreamPeekCursor).
func NextCursorLegal(next string, more CursorMore, lastKey string, rows, pageSize int) bool {
	if next == "" {
		return more != CursorRequired
	}

	if rows != pageSize {
		return false
	}

	return more != CursorForbidden && next == lastKey
}

// NextCursorOf is the key a list page's x-next-cursor trailer names, as the
// matchers' next argument expects it: "" when the server published no token,
// else NextCursorKey(token).
func NextCursorOf(token string) string {
	if token == "" {
		return ""
	}

	return NextCursorKey(token)
}
