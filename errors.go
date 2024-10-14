package postgres

import (
	"errors"
	"strings"
)

// -----------------------------------------------------------------------------

type ErrorType int

const (
	ErrorTypeNone                ErrorType = iota
	ErrorTypeConnection          ErrorType = iota
	ErrorTypePostgresGeneric     ErrorType = iota
	ErrorTypeDuplicateKey        ErrorType = iota
	ErrorTypeConstraintViolation ErrorType = iota
	ErrorTypeTxSerialization     ErrorType = iota
	ErrorTypeNoRows              ErrorType = 10000
)

// -----------------------------------------------------------------------------

// Error is the error type usually returned by us.
type Error struct {
	message string
	err     error // Err is the underlying error that occurred during the operation.
	Details *ErrorDetails
	Type    ErrorType
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
		return "<nil>"
	}

	sb := strings.Builder{}
	if len(e.message) > 0 {
		_, _ = sb.WriteString(e.message)
		if e.err != nil {
			_, _ = sb.WriteString(" [err=" + e.err.Error() + "]")
		}
	} else if e.err != nil {
		_, _ = sb.WriteString(e.err.Error())
	} else {
		_, _ = sb.WriteString("<nil>")
	}
	if e.Details != nil {
		_, _ = sb.WriteString(" [code=" + e.Details.Code + "]")
	}
	return sb.String()
}

func (e *NoRowsError) Error() string {
	return "no rows in result set"
}

// -----------------------------------------------------------------------------

// TypeOfError returns the type of error.
func TypeOfError(err error) ErrorType {
	var e *Error
	var nre *NoRowsError

	if errors.As(err, &e) {
		return e.Type
	}
	if errors.As(err, &nre) {
		return ErrorTypeNoRows
	}
	return ErrorTypeNone
}

// IsNoRowsError returns true if the given error is the result of returning an empty result set.
func IsNoRowsError(err error) bool {
	var e *NoRowsError

	return errors.As(err, &e)
}
