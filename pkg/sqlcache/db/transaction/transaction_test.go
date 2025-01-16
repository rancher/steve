package transaction

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

//go:generate mockgen --build_flags=--mod=mod -package transaction -destination ./transaction_mocks_test.go github.com/rancher/lasso/pkg/cache/sql/db/transaction Stmt,SQLTx

func TestNewClient(t *testing.T) {
	tx := NewMockSQLTx(gomock.NewController(t))
	c := NewClient(tx)
	assert.Equal(t, tx, c.sqlTx)
}

func TestCommit(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	tests = append(tests, testCase{description: "Commit() with no errors returned from sql TX should return no error", test: func(t *testing.T) {
		tx := NewMockSQLTx(gomock.NewController(t))
		tx.EXPECT().Commit().Return(nil)
		c := &Client{
			sqlTx: tx,
		}
		err := c.Commit()
		assert.Nil(t, err)
	}})
	tests = append(tests, testCase{description: "Commit() with error from sql TX commit() should return error", test: func(t *testing.T) {
		tx := NewMockSQLTx(gomock.NewController(t))
		tx.EXPECT().Commit().Return(fmt.Errorf("error"))
		c := &Client{
			sqlTx: tx,
		}
		err := c.Commit()
		assert.NotNil(t, err)
	}})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestExec(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	tests = append(tests, testCase{description: "Exec() with no errors returned from sql TX should return no error", test: func(t *testing.T) {
		tx := NewMockSQLTx(gomock.NewController(t))
		stmtStr := "some statement %s"
		arg := 5
		// should be passed same statement and arg that was passed to parent function
		tx.EXPECT().Exec(stmtStr, arg).Return(nil, nil)
		c := &Client{
			sqlTx: tx,
		}
		err := c.Exec(stmtStr, arg)
		assert.Nil(t, err)
	}})
	tests = append(tests, testCase{description: "Exec() with error returned from sql TX Exec() and Rollback() error should return an error", test: func(t *testing.T) {
		tx := NewMockSQLTx(gomock.NewController(t))
		stmtStr := "some statement %s"
		arg := 5
		// should be passed same statement and arg that was passed to parent function
		tx.EXPECT().Exec(stmtStr, arg).Return(nil, fmt.Errorf("error"))
		tx.EXPECT().Rollback().Return(nil)
		c := &Client{
			sqlTx: tx,
		}
		err := c.Exec(stmtStr, arg)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "Exec() with error returned from sql TX Exec() and Rollback() error should return an error", test: func(t *testing.T) {
		tx := NewMockSQLTx(gomock.NewController(t))
		stmtStr := "some statement %s"
		arg := 5
		// should be passed same statement and arg that was passed to parent function
		tx.EXPECT().Exec(stmtStr, arg).Return(nil, fmt.Errorf("error"))
		tx.EXPECT().Rollback().Return(fmt.Errorf("error"))
		c := &Client{
			sqlTx: tx,
		}
		err := c.Exec(stmtStr, arg)
		assert.NotNil(t, err)
	}})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestStmt(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	tests = append(tests, testCase{description: "Exec() with no errors returned from sql TX should return no error", test: func(t *testing.T) {
		tx := NewMockSQLTx(gomock.NewController(t))
		stmt := &sql.Stmt{}
		var returnedTXStmt *sql.Stmt
		// should be passed same statement and arg that was passed to parent function
		tx.EXPECT().Stmt(stmt).Return(returnedTXStmt)
		c := &Client{
			sqlTx: tx,
		}
		returnedStmt := c.Stmt(stmt)
		// whatever tx returned should be returned here. Nil was used because none of sql.Stmt's fields are exported so its simpler to test nil as it
		// won't be equal to an empty struct
		assert.Equal(t, returnedTXStmt, returnedStmt)
	}})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestStmtExec(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	tests = append(tests, testCase{description: "StmtExec with no errors returned from Stmt should return no error", test: func(t *testing.T) {
		tx := NewMockSQLTx(gomock.NewController(t))
		stmt := NewMockStmt(gomock.NewController(t))
		arg := "something"
		// should be passed same arg that was passed to parent function
		stmt.EXPECT().Exec(arg).Return(nil, nil)
		c := &Client{
			sqlTx: tx,
		}
		err := c.StmtExec(stmt, arg)
		assert.Nil(t, err)
	}})
	tests = append(tests, testCase{description: "StmtExec with error returned from Stmt Exec and no Tx Rollback() error should return error", test: func(t *testing.T) {
		tx := NewMockSQLTx(gomock.NewController(t))
		stmt := NewMockStmt(gomock.NewController(t))
		arg := "something"
		// should be passed same arg that was passed to parent function
		stmt.EXPECT().Exec(arg).Return(nil, fmt.Errorf("error"))
		tx.EXPECT().Rollback().Return(nil)
		c := &Client{
			sqlTx: tx,
		}
		err := c.StmtExec(stmt, arg)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "StmtExec with error returned from Stmt Exec and Tx Rollback() error should return error", test: func(t *testing.T) {
		tx := NewMockSQLTx(gomock.NewController(t))
		stmt := NewMockStmt(gomock.NewController(t))
		arg := "something"
		// should be passed same arg that was passed to parent function
		stmt.EXPECT().Exec(arg).Return(nil, fmt.Errorf("error"))
		tx.EXPECT().Rollback().Return(fmt.Errorf("error2"))
		c := &Client{
			sqlTx: tx,
		}
		err := c.StmtExec(stmt, arg)
		assert.NotNil(t, err)
	}})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}
