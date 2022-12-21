package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// -----------------------------------------------------------------------------

type WithinTxCallback = func(ctx context.Context, tx Tx) error
type QueryRowsCallback = func(ctx context.Context, rows RowGetter) (bool, error)
type CopyCallback func(ctx context.Context, idx int) ([]interface{}, error)

// -----------------------------------------------------------------------------

type Database struct {
	pool *pgxpool.Pool
	err  struct {
		mutex   sync.Mutex
		handler ErrorHandler
		last    error
	}
}

type Options struct {
	Host     string `json:"host"`
	Port     uint16 `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Name     string `json:"name"`
	MaxConns int32  `json:"maxConns"`
	SSLMode  SSLMode
}

type ErrorHandler func(err error)

type SSLMode int

const (
	SSLModeAllow SSLMode = iota
	SSLModeRequired
	SSLModeDisable
)

// -----------------------------------------------------------------------------

// New creates a new postgresql database driver.
func New(ctx context.Context, opts Options) (*Database, error) {
	var sslMode string

	// Create database object
	db := Database{}
	db.err.mutex = sync.Mutex{}

	// Setup basic configuration options
	switch opts.SSLMode {
	case SSLModeDisable:
		sslMode = "disable"
	case SSLModeAllow:
		sslMode = "prefer"
	case SSLModeRequired:
		sslMode = "require"
	default:
		return nil, errors.New("invalid SSL mode")
	}

	connString := fmt.Sprintf(
		"host='%s' port=%d user='%s' password='%s' dbname='%s' sslmode=%s",
		encodeDSN(opts.Host), opts.Port, encodeDSN(opts.User), encodeDSN(opts.Password), encodeDSN(opts.Name),
		sslMode,
	)

	// Create PGX pool configuration. Usage of ParseConfig is mandatory :(
	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		db.Close()
		return nil, errors.New("unable to parse connection string")
	}

	// Override some settings
	poolConfig.MaxConns = opts.MaxConns
	if opts.MaxConns <= 0 {
		poolConfig.MaxConns = 32
	}
	poolConfig.MaxConnIdleTime = 10 * time.Minute
	poolConfig.HealthCheckPeriod = time.Minute
	poolConfig.MaxConnLifetime = 1 * time.Hour
	poolConfig.MaxConnLifetimeJitter = time.Minute

	// Create the database connection pool
	db.pool, err = pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		db.Close()
		return nil, errors.New("unable to initialize database connection pool")
	}

	// Done
	return &db, nil
}

// Close shutdown the connection pool
func (db *Database) Close() {
	if db.pool != nil {
		db.pool.Close()
		db.pool = nil
	}
	db.SetEventHandler(nil)
}

// SetEventHandler sets a new error handler callback
func (db *Database) SetEventHandler(handler ErrorHandler) {
	db.err.mutex.Lock()
	defer db.err.mutex.Unlock()

	db.err.handler = handler
}

// Exec executes an SQL statement on a new connection
func (db *Database) Exec(ctx context.Context, params QueryParams) (int64, error) {
	ct, err := db.pool.Exec(ctx, params.sql, params.args...)
	return ct.RowsAffected(), db.processError(err)
}

// QueryRow executes a SQL query on a new connection
//
// NOTES:
// ~~~~~
//  1. Most of the commonly used types in Postgres can be mapped to standard Golang type including
//     time.Time for timestamps (except time with tz which is not supported)
//  2. When reading JSON/JSONB fields, the underlying library (PGX) tries to unmarshall it into the
//     destination variable. In order to just retrieve the json string, add the `::text` suffix to
//     the field in the query.
//  3. To avoid overflows on high uint64 values, store them in NUMERIC(24,0) fields.
//  4. For time-only fields, date is set to Jan 1, 2000 by PGX in time.Time variables.
func (db *Database) QueryRow(ctx context.Context, params QueryParams, dest ...interface{}) error {
	row := db.pool.QueryRow(ctx, params.sql, params.args...)
	err := row.Scan(dest...)
	return db.processError(err)
}

// QueryRows executes a SQL query on a new connection
func (db *Database) QueryRows(ctx context.Context, params QueryParams, callback QueryRowsCallback) error {
	rows, err := db.pool.Query(ctx, params.sql, params.args...)
	if err == nil {
		// Scan returned rows
		rg := rowsGetter{
			db:   db,
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
	return db.processError(err)
}

// Copy executes a SQL copy query within the transaction.
func (db *Database) Copy(ctx context.Context, tableName string, columnNames []string, callback CopyCallback) (int64, error) {
	n, err := db.pool.CopyFrom(
		ctx,
		pgx.Identifier{tableName},
		columnNames,
		copyWithCallback{
			ctx:      ctx,
			callback: callback,
		},
	)

	// Done
	return n, db.processError(err)
}

// WithinTx executes a callback function within the context of a transaction
func (db *Database) WithinTx(ctx context.Context, callback WithinTxCallback) error {
	tx, err := db.getTx(ctx)
	if err == nil {
		err = callback(ctx, Tx{
			db: db,
			tx: tx,
		})
		if err == nil {
			err = tx.Commit(ctx)
			if err != nil {
				err = newError(err, "unable to commit db transaction")
			}
		}
		if err != nil {
			_ = tx.Rollback(context.Background()) // Using context.Background() on purpose
		}
	}
	return db.processError(err)
}
