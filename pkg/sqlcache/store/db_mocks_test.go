// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rancher/steve/pkg/sqlcache/db (interfaces: Rows,Client)
//
// Generated by this command:
//
//	mockgen --build_flags=--mod=mod -package store -destination ./db_mocks_test.go github.com/rancher/steve/pkg/sqlcache/db Rows,Client
//

// Package store is a generated GoMock package.
package store

import (
	context "context"
	sql "database/sql"
	reflect "reflect"

	db "github.com/rancher/steve/pkg/sqlcache/db"
	transaction "github.com/rancher/steve/pkg/sqlcache/db/transaction"
	gomock "go.uber.org/mock/gomock"
)

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

// MockClient is a mock of Client interface.
type MockClient struct {
	ctrl     *gomock.Controller
	recorder *MockClientMockRecorder
}

// MockClientMockRecorder is the mock recorder for MockClient.
type MockClientMockRecorder struct {
	mock *MockClient
}

// NewMockClient creates a new mock instance.
func NewMockClient(ctrl *gomock.Controller) *MockClient {
	mock := &MockClient{ctrl: ctrl}
	mock.recorder = &MockClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClient) EXPECT() *MockClientMockRecorder {
	return m.recorder
}

// BeginTx mocks base method.
func (m *MockClient) BeginTx(arg0 context.Context, arg1 bool) (transaction.Client, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BeginTx", arg0, arg1)
	ret0, _ := ret[0].(transaction.Client)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BeginTx indicates an expected call of BeginTx.
func (mr *MockClientMockRecorder) BeginTx(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BeginTx", reflect.TypeOf((*MockClient)(nil).BeginTx), arg0, arg1)
}

// CloseStmt mocks base method.
func (m *MockClient) CloseStmt(arg0 db.Closable) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CloseStmt", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// CloseStmt indicates an expected call of CloseStmt.
func (mr *MockClientMockRecorder) CloseStmt(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CloseStmt", reflect.TypeOf((*MockClient)(nil).CloseStmt), arg0)
}

// NewConnection mocks base method.
func (m *MockClient) NewConnection() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewConnection")
	ret0, _ := ret[0].(error)
	return ret0
}

// NewConnection indicates an expected call of NewConnection.
func (mr *MockClientMockRecorder) NewConnection() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewConnection", reflect.TypeOf((*MockClient)(nil).NewConnection))
}

// Prepare mocks base method.
func (m *MockClient) Prepare(arg0 string) *sql.Stmt {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Prepare", arg0)
	ret0, _ := ret[0].(*sql.Stmt)
	return ret0
}

// Prepare indicates an expected call of Prepare.
func (mr *MockClientMockRecorder) Prepare(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Prepare", reflect.TypeOf((*MockClient)(nil).Prepare), arg0)
}

// QueryForRows mocks base method.
func (m *MockClient) QueryForRows(arg0 context.Context, arg1 transaction.Stmt, arg2 ...any) (*sql.Rows, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryForRows", varargs...)
	ret0, _ := ret[0].(*sql.Rows)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryForRows indicates an expected call of QueryForRows.
func (mr *MockClientMockRecorder) QueryForRows(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryForRows", reflect.TypeOf((*MockClient)(nil).QueryForRows), varargs...)
}

// ReadInt mocks base method.
func (m *MockClient) ReadInt(arg0 db.Rows) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadInt", arg0)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReadInt indicates an expected call of ReadInt.
func (mr *MockClientMockRecorder) ReadInt(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadInt", reflect.TypeOf((*MockClient)(nil).ReadInt), arg0)
}

// ReadObjects mocks base method.
func (m *MockClient) ReadObjects(arg0 db.Rows, arg1 reflect.Type, arg2 bool) ([]any, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadObjects", arg0, arg1, arg2)
	ret0, _ := ret[0].([]any)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReadObjects indicates an expected call of ReadObjects.
func (mr *MockClientMockRecorder) ReadObjects(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadObjects", reflect.TypeOf((*MockClient)(nil).ReadObjects), arg0, arg1, arg2)
}

// ReadStrings mocks base method.
func (m *MockClient) ReadStrings(arg0 db.Rows) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadStrings", arg0)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReadStrings indicates an expected call of ReadStrings.
func (mr *MockClientMockRecorder) ReadStrings(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadStrings", reflect.TypeOf((*MockClient)(nil).ReadStrings), arg0)
}

// Upsert mocks base method.
func (m *MockClient) Upsert(arg0 transaction.Client, arg1 *sql.Stmt, arg2 string, arg3 any, arg4 bool) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Upsert", arg0, arg1, arg2, arg3, arg4)
	ret0, _ := ret[0].(error)
	return ret0
}

// Upsert indicates an expected call of Upsert.
func (mr *MockClientMockRecorder) Upsert(arg0, arg1, arg2, arg3, arg4 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Upsert", reflect.TypeOf((*MockClient)(nil).Upsert), arg0, arg1, arg2, arg3, arg4)
}
