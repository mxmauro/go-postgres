package postgres

import (
	"errors"
	"net"
	"strings"
)

// -----------------------------------------------------------------------------

// Error is the error type usually returned by us.
type Error struct {
	message string
	err     error // Err is the underlying error that occurred during the operation.
	Details *ErrorDetails
}

type ErrorDetails struct {
	Severity       string
	Code           string
	Message        string
	Detail         string
	Hint           string
	Position       int32
	Where          string
	SchemaName     string
	TableName      string
	ColumnName     string
	DataTypeName   string
	ConstraintName string
	File           string
	Line           int32
	Routine        string
}

// NoRowsError is the error we return if the query does not return any row.
type NoRowsError struct {
}

// -----------------------------------------------------------------------------

// Unwrap returns the underlying error.
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

// Error returns a string representation of the error.
func (e *Error) Error() string {
	if e == nil {
		return ""
	}

	sb := strings.Builder{}
	_, _ = sb.WriteString(e.message)
	if e.err != nil {
		_, _ = sb.WriteString(" [err=" + e.err.Error() + "]")
	}
	if e.Details != nil {
		_, _ = sb.WriteString(" [code=" + e.Details.Code + "]")
	}
	return sb.String()
}

func (e *Error) IsDuplicateKeyError() bool {
	if e != nil && e.Details != nil {
		if e.Details.Code == "23505" {
			return true
		}
	}
	return false
}

func (e *Error) IsConstraintViolationError() bool {
	if e != nil && e.Details != nil {
		switch e.Details.Code {
		case "23000":
			return true
		case "23502":
			return true
		case "23503":
			return true
		case "23514":
			return true
		case "23P01":
			return true
		}
	}
	return false
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

func IsDuplicateKeyError(err error) bool {
	var e *Error

	if errors.As(err, &e) {
		return e.IsDuplicateKeyError()
	}
	return false
}

func IsConstraintViolationError(err error) bool {
	var e *Error

	if errors.As(err, &e) {
		return e.IsConstraintViolationError()
	}
	return false
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
