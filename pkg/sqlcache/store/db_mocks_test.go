// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rancher/lasso/pkg/cache/sql/db (interfaces: TXClient,Rows)
//
// Generated by this command:
//
//	mockgen --build_flags=--mod=mod -package store -destination ./db_mocks_test.go github.com/rancher/lasso/pkg/cache/sql/db TXClient,Rows
//

// Package store is a generated GoMock package.
package store

import (
	sql "database/sql"
	reflect "reflect"

	transaction "github.com/rancher/lasso/pkg/cache/sql/db/transaction"
	gomock "go.uber.org/mock/gomock"
)

// MockTXClient is a mock of TXClient interface.
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

// MockRows is a mock of Rows interface.
type MockRows struct {
	ctrl     *gomock.Controller
	recorder *MockRowsMockRecorder
}

// MockRowsMockRecorder is the mock recorder for MockRows.
type MockRowsMockRecorder struct {
	mock *MockRows
}

// NewMockRows creates a new mock instance.
func NewMockRows(ctrl *gomock.Controller) *MockRows {
	mock := &MockRows{ctrl: ctrl}
	mock.recorder = &MockRowsMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRows) EXPECT() *MockRowsMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockRows) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockRowsMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockRows)(nil).Close))
}

// Err mocks base method.
func (m *MockRows) Err() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Err")
	ret0, _ := ret[0].(error)
	return ret0
}

// Err indicates an expected call of Err.
func (mr *MockRowsMockRecorder) Err() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Err", reflect.TypeOf((*MockRows)(nil).Err))
}

// Next mocks base method.
func (m *MockRows) Next() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Next")
	ret0, _ := ret[0].(bool)
	return ret0
}

// Next indicates an expected call of Next.
func (mr *MockRowsMockRecorder) Next() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Next", reflect.TypeOf((*MockRows)(nil).Next))
}

// Scan mocks base method.
func (m *MockRows) Scan(arg0 ...any) error {
	m.ctrl.T.Helper()
	varargs := []any{}
	for _, a := range arg0 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Scan", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// Scan indicates an expected call of Scan.
func (mr *MockRowsMockRecorder) Scan(arg0 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Scan", reflect.TypeOf((*MockRows)(nil).Scan), arg0...)
}
