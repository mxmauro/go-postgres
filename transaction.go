package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// -----------------------------------------------------------------------------

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
func (tx *Tx) Exec(ctx context.Context, params QueryParams) (int64, error) {
	ct, err := tx.tx.Exec(ctx, params.sql, params.args...)
	return ct.RowsAffected(), tx.db.processError(err)
}

// QueryRow executes a SQL query within the transaction.
func (tx *Tx) QueryRow(ctx context.Context, params QueryParams, dest ...interface{}) error {
	row := tx.tx.QueryRow(ctx, params.sql, params.args...)
	err := row.Scan(dest)
	return tx.db.processError(err)
}

// QueryRows executes a SQL query within the transaction.
func (tx *Tx) QueryRows(ctx context.Context, params QueryParams, callback QueryRowsCallback) error {
	rows, err := tx.tx.Query(ctx, params.sql, params.args...)
	if err != nil {
		// Scan returned rows
		rg := rowsGetter{
			db:   tx.db,
			rows: rows,
		}
		for rows.Next() {
			var cont bool

			cont, err = callback(ctx, rg)
			if err != nil || (!cont) {
				break
			}
		}
		rows.Close()
	}

	// Done
	return tx.db.processError(err)
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
