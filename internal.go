package postgres

import (
	"errors"

	"github.com/jackc/pgx/v5"
)

// -----------------------------------------------------------------------------

var errNoRows = &NoRowsError{}

// -----------------------------------------------------------------------------

func (db *Database) processError(err error) error {
	isNoRows := false
	if errors.Is(err, pgx.ErrNoRows) {
		err = errNoRows
		isNoRows = true
	}

	// Only deal with fatal database errors. Cancellation, timeouts and empty result sets are not considered fatal.
	db.err.mutex.Lock()
	defer db.err.mutex.Unlock()

	if err != nil && (!isNoRows) && IsDatabaseError(err) {
		if db.err.last == nil {
			db.err.last = err
			if db.err.handler != nil {
				db.err.handler(err)
			}
		}
	} else {
		if db.err.last != nil {
			db.err.last = nil
			if db.err.handler != nil {
				db.err.handler(nil)
			}
		}
	}

	// Done
	return err
}
