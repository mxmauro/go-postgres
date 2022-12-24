package postgres

import (
	"github.com/jackc/pgx/v5"
)

// -----------------------------------------------------------------------------

type Row interface {
	Scan(dest ...interface{}) error
}

// -----------------------------------------------------------------------------

type rowGetter struct {
	db  *Database
	row pgx.Row
}

func (r rowGetter) Scan(dest ...interface{}) error {
	err := r.row.Scan(dest...)
	return r.db.processError(err)
}
