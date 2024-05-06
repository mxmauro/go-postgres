package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// -----------------------------------------------------------------------------

// Tx encloses a transaction object.
type Tx struct {
	db *Database
	tx pgx.Tx
}

// -----------------------------------------------------------------------------

// DB returns the underlying database driver.
func (tx *Tx) DB() *Database {
	return tx.db
}

// Exec executes an SQL statement within the transaction.
func (tx *Tx) Exec(ctx context.Context, sql string, args ...interface{}) (int64, error) {
	affectedRows := int64(0)
	ct, err := tx.tx.Exec(ctx, sql, args...)
	if err == nil {
		affectedRows = ct.RowsAffected()
	}
	return affectedRows, tx.db.processError(err)
}

// QueryRow executes a SQL query within the transaction.
func (tx *Tx) QueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	return &rowGetter{
		db:  tx.db,
		row: tx.tx.QueryRow(ctx, sql, args...),
	}
}

// QueryRows executes a SQL query within the transaction.
func (tx *Tx) QueryRows(ctx context.Context, sql string, args ...interface{}) Rows {
	rows, err := tx.tx.Query(ctx, sql, args...)
	return &rowsGetter{
		db:   tx.db,
		ctx:  ctx,
		rows: rows,
		err:  err,
	}
}

// Copy executes a SQL copy query within the transaction.
func (tx *Tx) Copy(ctx context.Context, tableName string, columnNames []string, callback CopyCallback) (int64, error) {
	n, err := tx.tx.CopyFrom(
		ctx,
		pgx.Identifier{tableName},
		columnNames,
		&copyWithCallback{
			ctx:      ctx,
			callback: callback,
		},
	)

	// Done
	return n, tx.db.processError(err)
}

// WithinTx executes a callback function within the context of a nested transaction.
func (tx *Tx) WithinTx(ctx context.Context, cb WithinTxCallback) error {
	innerTx, err := tx.tx.Begin(ctx)
	if err == nil {
		err = cb(ctx, Tx{
			db: tx.db,
			tx: innerTx,
		})
		if err == nil {
			err = innerTx.Commit(ctx)
			if err != nil {
				err = newError(err, "unable to commit db transaction")
			}
		}
		if err != nil {
			_ = innerTx.Rollback(context.Background()) // Using context.Background() on purpose
		}
	} else {
		err = newError(err, "unable to start transaction")
	}
	return tx.db.processError(err)
}
