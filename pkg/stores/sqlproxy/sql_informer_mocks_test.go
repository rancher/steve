// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rancher/steve/pkg/sqlcache/informer (interfaces: ByOptionsLister)
//
// Generated by this command:
//
//	mockgen --build_flags=--mod=mod -package sqlproxy -destination ./sql_informer_mocks_test.go github.com/rancher/steve/pkg/sqlcache/informer ByOptionsLister
//

// Package sqlproxy is a generated GoMock package.
package sqlproxy

import (
	context "context"
	reflect "reflect"

	informer "github.com/rancher/steve/pkg/sqlcache/informer"
	partition "github.com/rancher/steve/pkg/sqlcache/partition"
	gomock "go.uber.org/mock/gomock"
	unstructured "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// MockByOptionsLister is a mock of ByOptionsLister interface.
type MockByOptionsLister struct {
	ctrl     *gomock.Controller
	recorder *MockByOptionsListerMockRecorder
}

// MockByOptionsListerMockRecorder is the mock recorder for MockByOptionsLister.
type MockByOptionsListerMockRecorder struct {
	mock *MockByOptionsLister
}

// NewMockByOptionsLister creates a new mock instance.
func NewMockByOptionsLister(ctrl *gomock.Controller) *MockByOptionsLister {
	mock := &MockByOptionsLister{ctrl: ctrl}
	mock.recorder = &MockByOptionsListerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockByOptionsLister) EXPECT() *MockByOptionsListerMockRecorder {
	return m.recorder
}

// ListByOptions mocks base method.
func (m *MockByOptionsLister) ListByOptions(arg0 context.Context, arg1 informer.ListOptions, arg2 []partition.Partition, arg3 string) (*unstructured.UnstructuredList, int, string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListByOptions", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(*unstructured.UnstructuredList)
	ret1, _ := ret[1].(int)
	ret2, _ := ret[2].(string)
	ret3, _ := ret[3].(error)
	return ret0, ret1, ret2, ret3
}

// ListByOptions indicates an expected call of ListByOptions.
func (mr *MockByOptionsListerMockRecorder) ListByOptions(arg0, arg1, arg2, arg3 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListByOptions", reflect.TypeOf((*MockByOptionsLister)(nil).ListByOptions), arg0, arg1, arg2, arg3)
}
