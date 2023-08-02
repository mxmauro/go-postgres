package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// -----------------------------------------------------------------------------

// ScanRowsCallback defines a callback that is called on each row returned by the executed query.
type ScanRowsCallback = func(ctx context.Context, row Row) (bool, error)

// Rows defines a set of returned records.
type Rows interface {
	// Do calls the provided callback for each row returned by the executed query.
	Do(callback ScanRowsCallback) error
}

// -----------------------------------------------------------------------------

type rowsGetter struct {
	ctx  context.Context
	db   *Database
	rows pgx.Rows
	err  error
}

func (r rowsGetter) Do(callback ScanRowsCallback) error {
	if r.err == nil {
		// Scan returned rows
		for r.rows.Next() {
			cont, err := callback(r.ctx, r)
			if err != nil {
				r.err = err
				break
			}
			if !cont {
				break
			}
		}
		r.rows.Close()
	}

	// Done
	return r.db.processError(r.err)
}

func (r rowsGetter) Scan(dest ...interface{}) error {
	err := r.rows.Scan(dest...)
	return r.db.processError(err)
}
