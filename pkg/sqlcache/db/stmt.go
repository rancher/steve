package db

import (
	"context"
	"database/sql"
)

// Stmt is an interface over a subset of sql.Stmt methods
// rationale: allow mocking
type Stmt interface {
	Exec(args ...any) (sql.Result, error)
	Query(args ...any) (*sql.Rows, error)
	QueryContext(ctx context.Context, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, args ...any) *sql.Row
	Close() error

	// SQLStmt unwraps the original sql.Stmt
	SQLStmt() *sql.Stmt

	// GetQueryString returns the original text used to prepare this statement
	GetQueryString() string
}

type stmt struct {
	*sql.Stmt
	queryString string
}

func (s *stmt) SQLStmt() *sql.Stmt {
	return s.Stmt
}

func (s *stmt) GetQueryString() string {
	return s.queryString
}
