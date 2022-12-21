package postgres

import (
	"errors"
	"net"
)

// -----------------------------------------------------------------------------

// Error is the error type usually returned by us.
type Error struct {
	message string
	err     error // Err is the underlying error that occurred during the operation.
}

// NoRowsError is the error we return if the query does not return any row.
type NoRowsError struct {
}

// -----------------------------------------------------------------------------

// Unwrap returns the underlying error.
func (e *Error) Unwrap() error {
	return e.err
}

// Error returns a string representation of the error.
func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	s := e.message
	if e.err != nil {
		s += " [err=" + e.err.Error() + "]"
	}
	return s
}

func (e *NoRowsError) Error() string {
	return "no rows in result set"
}

// -----------------------------------------------------------------------------

// IsDatabaseError returns true if the given error object is a database error.
func IsDatabaseError(err error) bool {
	var e *Error

	return errors.As(err, &e)
}

// IsNoRowsError returns true if the given error is the result of returning an empty result set.
func IsNoRowsError(err error) bool {
	var e *NoRowsError

	return errors.As(err, &e)
}

// IsNetworkError returns true if the error is related to a network issue.
func IsNetworkError(err error) bool {
	if err != nil {
		var ne net.Error
		var netOpErr *net.OpError
		var netDnsErr *net.DNSError

		if errors.As(err, &ne) || errors.As(err, &netOpErr) || errors.As(err, &netDnsErr) {
			return true
		}
	}
	return false
}
