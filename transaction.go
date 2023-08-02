package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// -----------------------------------------------------------------------------

// Tx encloses a transation object.
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
	ct, err := tx.tx.Exec(ctx, sql, args...)
	return ct.RowsAffected(), tx.db.processError(err)
}

// QueryRow executes a SQL query within the transaction.
func (tx *Tx) QueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	return rowGetter{
		db:  tx.db,
		row: tx.tx.QueryRow(ctx, sql, args...),
	}
}

// QueryRows executes a SQL query within the transaction.
func (tx *Tx) QueryRows(ctx context.Context, sql string, args ...interface{}) Rows {
	rows, err := tx.tx.Query(ctx, sql, args...)
	return rowsGetter{
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
		copyWithCallback{
			ctx:      ctx,
			callback: callback,
		},
	)

	// Done
	return n, tx.db.processError(err)
}
