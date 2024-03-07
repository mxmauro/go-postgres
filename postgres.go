package postgres

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// -----------------------------------------------------------------------------

// WithinTxCallback defines a callback called in the context of the initiated transaction.
type WithinTxCallback = func(ctx context.Context, tx Tx) error

// CopyCallback defines a callback that is called for each record being copied to the database
type CopyCallback func(ctx context.Context, idx int) ([]interface{}, error)

// -----------------------------------------------------------------------------

// Database represents a PostgreSQL database accessor.
type Database struct {
	pool *pgxpool.Pool
	err  struct {
		mutex   sync.Mutex
		handler ErrorHandler
		last    error
	}
}

// Options defines the database connection options.
type Options struct {
	Host     string `json:"host"`
	Port     uint16 `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Name     string `json:"name"`
	MaxConns int32  `json:"maxConns"`
	SSLMode  SSLMode
}

// ErrorHandler defines a custom error handler.
type ErrorHandler func(err error)

// SSLMode states if secure communication with the server is optional or mandatory.
type SSLMode int

const (
	SSLModeAllow SSLMode = iota
	SSLModeRequired
	SSLModeDisable
)

// -----------------------------------------------------------------------------

// New creates a new postgresql database driver.
func New(ctx context.Context, opts Options) (*Database, error) {
	// Validate options
	if len(opts.Host) == 0 {
		return nil, errors.New("invalid host")
	}
	if len(opts.User) == 0 {
		return nil, errors.New("invalid user name")
	}
	if len(opts.Name) == 0 {
		return nil, errors.New("invalid database name")
	}
	sslMode := "disable"
	switch opts.SSLMode {
	case SSLModeDisable:
	case SSLModeAllow:
		sslMode = "prefer"
	case SSLModeRequired:
		sslMode = "require"
	default:
		return nil, errors.New("invalid SSL mode")
	}

	// Create database object
	db := Database{}
	db.err.mutex = sync.Mutex{}

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

// NewFromURL creates a new postgresql database driver from an URL
func NewFromURL(ctx context.Context, rawUrl string) (*Database, error) {
	opts := Options{}

	u, err := url.ParseRequestURI(rawUrl)
	if err != nil {
		return nil, errors.New("invalid url provided")
	}

	// Check schema
	if u.Scheme != "pg" && u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return nil, errors.New("invalid url schema")
	}

	// Check host name and port
	opts.Host = u.Hostname()
	if len(opts.Host) == 0 {
		return nil, errors.New("invalid host")
	}
	s := u.Port()
	if len(s) == 0 {
		opts.Port = 5432
	} else {
		val, err2 := strconv.Atoi(s)
		if err2 != nil || val < 1 || val > 65535 {
			return nil, errors.New("invalid port")
		}
		opts.Port = uint16(val)
	}

	// Check user and password
	if u.User == nil {
		return nil, errors.New("invalid user name")
	}
	opts.User = u.User.Username()
	if len(opts.User) == 0 {
		return nil, errors.New("invalid user name")
	}

	// Check database name
	if len(u.Path) < 1 || (!strings.HasPrefix(u.Path, "/")) || strings.Index(u.Path[1:], "/") >= 0 {
		return nil, errors.New("invalid database name")
	}
	opts.Name = u.Path[1:]

	// Check ssl mode
	opts.SSLMode = SSLModeDisable
	switch u.Query().Get("sslmode") {
	case "allow":
		opts.SSLMode = SSLModeAllow

	case "required":
		opts.SSLMode = SSLModeRequired

	case "disabled":
		fallthrough
	case "":

	default:
		return nil, errors.New("invalid SSL mode")
	}

	// Check max connections count
	s = u.Query().Get("maxconn")
	if len(s) > 0 {
		val, err2 := strconv.Atoi(s)
		if err2 != nil || val < 0 {
			return nil, errors.New("invalid max connections count")
		}
		opts.MaxConns = int32(val)
	}

	// Create
	return New(ctx, opts)
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
func (db *Database) Exec(ctx context.Context, sql string, args ...interface{}) (int64, error) {
	ct, err := db.pool.Exec(ctx, sql, args...)
	if err != nil {
		err = newError(err, "unable to execute command")
	}
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
func (db *Database) QueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	return &rowGetter{
		db:  db,
		row: db.pool.QueryRow(ctx, sql, args...),
	}
}

// QueryRows executes a SQL query on a new connection
func (db *Database) QueryRows(ctx context.Context, sql string, args ...interface{}) Rows {
	rows, err := db.pool.Query(ctx, sql, args...)
	if err != nil {
		err = newError(err, "unable to scan row")
	}
	return &rowsGetter{
		db:   db,
		ctx:  ctx,
		rows: rows,
		err:  err,
	}
}

// Copy executes a SQL copy query within the transaction.
func (db *Database) Copy(ctx context.Context, tableName string, columnNames []string, callback CopyCallback) (int64, error) {
	n, err := db.pool.CopyFrom(
		ctx,
		pgx.Identifier{tableName},
		columnNames,
		&copyWithCallback{
			ctx:      ctx,
			callback: callback,
		},
	)
	if err != nil {
		err = newError(err, "unable to execute command")
	}

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
