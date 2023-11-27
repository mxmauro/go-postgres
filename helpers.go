package postgres

import (
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// -----------------------------------------------------------------------------

func newError(wrappedErr error, text string) *Error {
	var e *Error
	var pgErr *pgconn.PgError

	if errors.As(wrappedErr, &e) {
		return e
	}
	e = &Error{
		message: text,
		err:     wrappedErr,
	}

	if errors.As(wrappedErr, &pgErr) {
		e.Details = &ErrorDetails{
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
		}
	}

	// Done
	return e
}

func encodeDSN(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}
