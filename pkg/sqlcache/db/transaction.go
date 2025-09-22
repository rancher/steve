package db

import (
	"database/sql"
)

// TxClient is an interface over a subset of sql.Tx methods
// rationale 1: explicitly forbid direct access to Commit and Rollback functionality
// as that is exclusively dealt with by WithTransaction in ../db
// rationale 2: allow mocking
type TxClient interface {
	Exec(query string, args ...any) (sql.Result, error)
	Stmt(stmt Stmt) Stmt
}

// txClient is the main implementation of TxClient, delegates to sql.Tx
// other implementations exist for testing purposes
type txClient struct {
	tx *sql.Tx
}

func NewTxClient(tx *sql.Tx) TxClient {
	return &txClient{tx: tx}
}

func (c txClient) Exec(query string, args ...any) (sql.Result, error) {
	return c.tx.Exec(query, args...)
}

func (c txClient) Stmt(s Stmt) Stmt {
	return &stmt{
		Stmt:        c.tx.Stmt(s.SQLStmt()),
		queryString: s.GetQueryString(),
	}
}
