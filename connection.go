package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// -----------------------------------------------------------------------------

// Conn encloses a single connection object.
type Conn struct {
	db   *Database
	conn *pgxpool.Conn
}

// -----------------------------------------------------------------------------

// DB returns the underlying database driver.
func (c *Conn) DB() *Database {
	return c.db
}

// Exec executes an SQL statement within the single connection.
func (c *Conn) Exec(ctx context.Context, sql string, args ...interface{}) (int64, error) {
	affectedRows := int64(0)
	ct, err := c.conn.Exec(ctx, sql, args...)
	if err == nil {
		affectedRows = ct.RowsAffected()
	} else {
		err = newError(err, "")
	}
	return affectedRows, c.db.handleError(err)
}

// QueryRow executes a SQL query within the single connection.
func (c *Conn) QueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	return &rowGetter{
		db:  c.db,
		row: c.conn.QueryRow(ctx, sql, args...),
	}
}

// QueryRows executes a SQL query within the single connection.
func (c *Conn) QueryRows(ctx context.Context, sql string, args ...interface{}) Rows {
	rows, err := c.conn.Query(ctx, sql, args...)
	return &rowsGetter{
		db:   c.db,
		ctx:  ctx,
		rows: rows,
		err:  newError(err, "unable to run query"),
	}
}

// Copy executes a SQL copy query within the single connection.
func (c *Conn) Copy(ctx context.Context, tableName string, columnNames []string, callback CopyCallback) (int64, error) {
	n, err := c.conn.CopyFrom(
		ctx,
		pgx.Identifier{tableName},
		columnNames,
		&copyWithCallback{
			ctx: ctx,
			cb:  callback,
		},
	)

	// Done
	return n, c.db.handleError(newError(err, "unable to execute command"))
}

// WithinTx executes a callback function within the context of a single connection.
func (c *Conn) WithinTx(ctx context.Context, cb WithinTxCallback, opts ...WithinTxOptions) error {
	txOpts := pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.NotDeferrable,
	}
	if len(opts) > 0 {
		if opts[0].ReadOnly {
			txOpts.AccessMode = pgx.ReadOnly
		}
		if opts[0].RepeatableRead {
			txOpts.IsoLevel = pgx.RepeatableRead
		}
	}

	innerTx, err := c.conn.BeginTx(ctx, txOpts)
	if err == nil {
		err = cb(ctx, Tx{
			db: c.db,
			tx: innerTx,
		})
		if err == nil {
			err = innerTx.Commit(ctx)
			if err != nil {
				err = newError(err, "unable to commit db transaction")
			}
		} else {
			err = newError(err, "callback returned failure")
		}
		if err != nil {
			_ = innerTx.Rollback(context.Background()) // Using context.Background() on purpose
		}
	} else {
		err = newError(err, "unable to start transaction")
	}
	return c.db.handleError(err)
}
