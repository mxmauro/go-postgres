package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// -----------------------------------------------------------------------------

var errNoRows = &NoRowsError{}

// -----------------------------------------------------------------------------

// Gets a connection from the pool and initiates a transaction.
func (db *Database) getTx(ctx context.Context) (pgx.Tx, error) {
	tx, err := db.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted, //pgx.Serializable,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.NotDeferrable,
	})
	if err != nil {
		return nil, newError(err, "unable to start db transaction")
	}

	//Done
	return tx, nil
}

func (db *Database) processError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		err = errNoRows
	}

	// Only deal with fatal database errors. Cancellation, timeouts and empty result sets are not considered fatal.
	db.err.mutex.Lock()
	defer db.err.mutex.Unlock()

	if err != nil && IsDatabaseError(err) && err != errNoRows {
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
