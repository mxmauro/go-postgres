package postgres

import (
	"github.com/jackc/pgx/v5"
)

// -----------------------------------------------------------------------------

// Row defines a returned record.
type Row interface {
	// Scan saves the content of the current row in the destination variables.
	Scan(dest ...interface{}) error
}

// -----------------------------------------------------------------------------

type rowGetter struct {
	db  *Database
	row pgx.Row
}

func (r *rowGetter) Scan(dest ...interface{}) error {
	err := r.row.Scan(dest...)
	if err != nil {
		err = newError(err, "unable to scan row")
	}
	return r.db.processError(err)
}
