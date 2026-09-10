package db

import (
	"context"
	"database/sql"
	"time"

	"github.com/rancher/steve/pkg/sqlcache/db/logging"
)

// TxClient is an interface over a subset of sql.Tx methods
// rationale 1: explicitly forbid direct access to Commit and Rollback functionality
// as that is exclusively dealt with by WithTransaction in ../db
// rationale 2: allow mocking
type TxClient interface {
	Exec(query string, args ...any) (sql.Result, error)

	// Query runs a one-shot query on the transaction's own connection.
	//
	// Prefer this over Client.Prepare + Client.QueryForRows for any read
	// issued while a write transaction is open. A *sql.Stmt prepared on the
	// pool records every connection it was prepared on, and Stmt.finalClose
	// takes driverConn.Lock on each of them - but rows.Close returns that
	// connection to the pool *before* running finalClose. A writer that grabs
	// the just-freed connection holds its mutex for the whole of
	// BEGIN IMMEDIATE, so the transaction holding the SQLite write lock ends
	// up waiting on a writer that is waiting on that same write lock. Nothing
	// breaks the cycle except busy_timeout.
	//
	// Query goes through sql.Tx.QueryContext, which reuses the transaction's
	// already-checked-out connection and never creates a *sql.Stmt, so there
	// is nothing to finalClose and no pooled connection to lock.
	//
	// The returned Rows must be closed before the transaction commits.
	Query(ctx context.Context, query string, args ...any) (Rows, error)

	Stmt(stmt Stmt) Stmt
}

// Tx represents the methods used from sql.Tx
type Tx interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	Stmt(stmt *sql.Stmt) *sql.Stmt
	Commit() error
	Rollback() error
}

// txClient is the main implementation of TxClient, delegates to sql.Tx
// other implementations exist for testing purposes
type txClient struct {
	tx          Tx
	queryLogger logging.QueryLogger
}

type TxClientOption func(*txClient)

func NewTxClient(tx Tx, opts ...TxClientOption) TxClient {
	c := &txClient{tx: tx, queryLogger: &logging.NoopQueryLogger{}}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c txClient) Exec(query string, args ...any) (sql.Result, error) {
	defer c.queryLogger.Log(time.Now(), query, args)
	res, err := c.tx.Exec(query, args...)
	if err != nil {
		err = &QueryError{
			QueryString: query,
			Err:         err,
		}
	}
	return res, err
}

func (c txClient) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	defer c.queryLogger.Log(time.Now(), query, args)
	r, err := c.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, &QueryError{
			QueryString: query,
			Err:         err,
		}
	}
	return rows{Rows: r, queryString: query}, nil
}

func (c txClient) Stmt(s Stmt) Stmt {
	return &stmt{
		queryLogger: c.queryLogger,
		Stmt:        c.tx.Stmt(s.SQLStmt()),
		queryString: s.GetQueryString(),
	}
}

func WithQueryLogger(logger logging.QueryLogger) TxClientOption {
	return func(c *txClient) {
		c.queryLogger = logger
	}
}
