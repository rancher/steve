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

	// Query reads on the transaction's own connection; use it instead of
	// Client.Prepare + QueryForRows inside a write transaction, which deadlocks.
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
