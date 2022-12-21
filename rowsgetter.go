package postgres

import (
	"github.com/jackc/pgx/v5"
)

// -----------------------------------------------------------------------------

type RowGetter interface {
	Get(dest ...interface{}) error
}

// -----------------------------------------------------------------------------

type rowsGetter struct {
	db   *Database
	rows pgx.Rows
}

func (r rowsGetter) Get(dest ...interface{}) error {
	err := r.rows.Scan(dest...)
	return r.db.processError(err)
}
