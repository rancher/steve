package db

import (
	"context"
	"database/sql"
)

type Row struct {
	*sql.Row
	queryString string
}

func (r Row) Err() error {
	if err := r.Row.Err(); err != nil {
		return &QueryError{QueryString: r.queryString, Err: err}
	}
	return nil
}

type rows struct {
	*sql.Rows
	queryString string
}

func (r rows) Err() error {
	if err := r.Rows.Err(); err != nil {
		return &QueryError{QueryString: r.queryString, Err: err}
	}
	return nil
}

type stmt struct {
	*sql.Stmt
	queryString string
}

func (s *stmt) Exec(args ...any) (sql.Result, error) {
	res, err := s.Stmt.Exec(args...)
	if err != nil {
		err = &QueryError{
			QueryString: s.queryString,
			Err:         err,
		}
	}
	return res, err
}

func (s *stmt) QueryContext(ctx context.Context, args ...any) (Rows, error) {
	res, err := s.Stmt.QueryContext(ctx, args...)
	if err != nil {
		return res, &QueryError{
			QueryString: s.queryString,
			Err:         err,
		}
	}
	return rows{Rows: res, queryString: s.queryString}, nil
}

func (s *stmt) QueryRowContext(ctx context.Context, args ...any) Row {
	return Row{Row: s.Stmt.QueryRowContext(ctx, args...), queryString: s.queryString}
}

func (s *stmt) Close() error {
	if err := s.Stmt.Close(); err != nil {
		return &QueryError{QueryString: s.queryString, Err: err}
	}
	return nil
}

func (s *stmt) SQLStmt() *sql.Stmt {
	return s.Stmt
}

func (s *stmt) GetQueryString() string {
	return s.queryString
}
