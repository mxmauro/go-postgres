package postgres

import (
	"strings"
)

// -----------------------------------------------------------------------------

func newError(wrappedErr error, text string) *Error {
	e := &Error{
		message: text,
		err:     wrappedErr,
	}
	return e
}

func encodeDSN(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}
