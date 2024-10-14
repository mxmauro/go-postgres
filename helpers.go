// See the LICENSE file for license details.

package postgres

import (
	"errors"
	"net"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// -----------------------------------------------------------------------------

func newError(wrappedErr error, message string) error {
	var e *Error
	var pgErr *pgconn.PgError
	var ne net.Error
	var netOpErr *net.OpError
	var netDnsErr *net.DNSError

	if wrappedErr == nil {
		return nil
	}

	// Is it already our error?
	if errors.As(wrappedErr, &e) {
		return e
	}

	// Is it a no rows in result error?
	if errors.Is(wrappedErr, pgx.ErrNoRows) {
		return errNoRows
	}

	// Is it a connection/network issue?
	if errors.As(wrappedErr, &ne) || errors.As(wrappedErr, &netOpErr) || errors.As(wrappedErr, &netDnsErr) {
		e = &Error{
			message: message,
			err:     wrappedErr,
			Type:    ErrorTypeConnection,
		}

		// Done
		return e
	}

	// Is it a postgres error?
	if !errors.As(wrappedErr, &pgErr) {
		// No. Return the original error
		return wrappedErr
	}

	// Create our error wrapper
	e = &Error{
		message: message,
		err:     wrappedErr,
		Details: &ErrorDetails{
			Severity:       pgErr.Severity,
			Code:           pgErr.Code,
			Message:        pgErr.Message,
			Detail:         pgErr.Detail,
			Hint:           pgErr.Hint,
			Position:       pgErr.Position,
			Where:          pgErr.Where,
			SchemaName:     pgErr.SchemaName,
			TableName:      pgErr.TableName,
			ColumnName:     pgErr.ColumnName,
			DataTypeName:   pgErr.DataTypeName,
			ConstraintName: pgErr.ConstraintName,
			File:           pgErr.File,
			Line:           pgErr.Line,
			Routine:        pgErr.Routine,
		},
	}

	switch pgErr.Code {
	case "23000":
		fallthrough
	case "23502":
		fallthrough
	case "23503":
		fallthrough
	case "23514":
		fallthrough
	case "23P01":
		e.Type = ErrorTypeConstraintViolation

	case "23505":
		e.Type = ErrorTypeDuplicateKey

	case "40001":
		e.Type = ErrorTypeTxSerialization

	default:
		e.Type = ErrorTypePostgresGeneric
	}

	// Done
	return e
}

func encodeDSN(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

func quoteIdentifier(s string) string {
	return "\"" + strings.ReplaceAll(s, "\"", "\"\"") + "\""
}
