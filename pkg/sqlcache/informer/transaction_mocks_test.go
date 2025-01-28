// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rancher/steve/pkg/sqlcache/db/transaction (interfaces: Stmt,Client)
//
// Generated by this command:
//
//	mockgen --build_flags=--mod=mod -package informer -destination ./transaction_mocks_test.go -mock_names Client=MockTXClient github.com/rancher/steve/pkg/sqlcache/db/transaction Stmt,Client
//

// Package informer is a generated GoMock package.
package informer

import (
	context "context"
	sql "database/sql"
	reflect "reflect"

	transaction "github.com/rancher/steve/pkg/sqlcache/db/transaction"
	gomock "go.uber.org/mock/gomock"
)

// MockStmt is a mock of Stmt interface.
type MockStmt struct {
	ctrl     *gomock.Controller
	recorder *MockStmtMockRecorder
}

// MockStmtMockRecorder is the mock recorder for MockStmt.
type MockStmtMockRecorder struct {
	mock *MockStmt
}

// NewMockStmt creates a new mock instance.
func NewMockStmt(ctrl *gomock.Controller) *MockStmt {
	mock := &MockStmt{ctrl: ctrl}
	mock.recorder = &MockStmtMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockStmt) EXPECT() *MockStmtMockRecorder {
	return m.recorder
}

// Exec mocks base method.
func (m *MockStmt) Exec(arg0 ...any) (sql.Result, error) {
	m.ctrl.T.Helper()
	varargs := []any{}
	for _, a := range arg0 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Exec", varargs...)
	ret0, _ := ret[0].(sql.Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Exec indicates an expected call of Exec.
func (mr *MockStmtMockRecorder) Exec(arg0 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Exec", reflect.TypeOf((*MockStmt)(nil).Exec), arg0...)
}

// Query mocks base method.
func (m *MockStmt) Query(arg0 ...any) (*sql.Rows, error) {
	m.ctrl.T.Helper()
	varargs := []any{}
	for _, a := range arg0 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Query", varargs...)
	ret0, _ := ret[0].(*sql.Rows)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Query indicates an expected call of Query.
func (mr *MockStmtMockRecorder) Query(arg0 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Query", reflect.TypeOf((*MockStmt)(nil).Query), arg0...)
}

// QueryContext mocks base method.
func (m *MockStmt) QueryContext(arg0 context.Context, arg1 ...any) (*sql.Rows, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0}
	for _, a := range arg1 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryContext", varargs...)
	ret0, _ := ret[0].(*sql.Rows)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryContext indicates an expected call of QueryContext.
func (mr *MockStmtMockRecorder) QueryContext(arg0 any, arg1 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0}, arg1...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryContext", reflect.TypeOf((*MockStmt)(nil).QueryContext), varargs...)
}

// MockTXClient is a mock of Client interface.
type MockTXClient struct {
	ctrl     *gomock.Controller
	recorder *MockTXClientMockRecorder
}

// MockTXClientMockRecorder is the mock recorder for MockTXClient.
type MockTXClientMockRecorder struct {
	mock *MockTXClient
}

// NewMockTXClient creates a new mock instance.
func NewMockTXClient(ctrl *gomock.Controller) *MockTXClient {
	mock := &MockTXClient{ctrl: ctrl}
	mock.recorder = &MockTXClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTXClient) EXPECT() *MockTXClientMockRecorder {
	return m.recorder
}

// Cancel mocks base method.
func (m *MockTXClient) Cancel() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Cancel")
	ret0, _ := ret[0].(error)
	return ret0
}

// Cancel indicates an expected call of Cancel.
func (mr *MockTXClientMockRecorder) Cancel() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Cancel", reflect.TypeOf((*MockTXClient)(nil).Cancel))
}

// Commit mocks base method.
func (m *MockTXClient) Commit() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Commit")
	ret0, _ := ret[0].(error)
	return ret0
}

// Commit indicates an expected call of Commit.
func (mr *MockTXClientMockRecorder) Commit() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Commit", reflect.TypeOf((*MockTXClient)(nil).Commit))
}

// Exec mocks base method.
func (m *MockTXClient) Exec(arg0 string, arg1 ...any) error {
	m.ctrl.T.Helper()
	varargs := []any{arg0}
	for _, a := range arg1 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Exec", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// Exec indicates an expected call of Exec.
func (mr *MockTXClientMockRecorder) Exec(arg0 any, arg1 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0}, arg1...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Exec", reflect.TypeOf((*MockTXClient)(nil).Exec), varargs...)
}

// Stmt mocks base method.
func (m *MockTXClient) Stmt(arg0 *sql.Stmt) transaction.Stmt {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stmt", arg0)
	ret0, _ := ret[0].(transaction.Stmt)
	return ret0
}

// Stmt indicates an expected call of Stmt.
func (mr *MockTXClientMockRecorder) Stmt(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stmt", reflect.TypeOf((*MockTXClient)(nil).Stmt), arg0)
}

// StmtExec mocks base method.
func (m *MockTXClient) StmtExec(arg0 transaction.Stmt, arg1 ...any) error {
	m.ctrl.T.Helper()
	varargs := []any{arg0}
	for _, a := range arg1 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "StmtExec", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// StmtExec indicates an expected call of StmtExec.
func (mr *MockTXClientMockRecorder) StmtExec(arg0 any, arg1 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0}, arg1...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StmtExec", reflect.TypeOf((*MockTXClient)(nil).StmtExec), varargs...)
}
