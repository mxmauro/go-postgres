// See the LICENSE file for license details.

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

type rowGetter struct {
	db  *Database
	row pgx.Row
}

// -----------------------------------------------------------------------------

func (r *rowGetter) Scan(dest ...interface{}) error {
	err := r.row.Scan(dest...)
	return r.db.handleError(newError(err, "unable to scan row"))
}
