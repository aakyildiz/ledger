package matchtest

import "github.com/formancehq/ledger/v3/pkg/pagecursor"

// pagecursorDecode decodes a forward ListLedgers token to its key.
func pagecursorDecode(token string) (string, error) {
	c, err := pagecursor.Decode(token)
	if err != nil {
		return "", err
	}

	if c.Back {
		return "", pagecursor.ErrInvalid
	}

	return c.Key, nil
}
