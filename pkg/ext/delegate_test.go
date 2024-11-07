package ext

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	gomock "go.uber.org/mock/gomock"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
)

func TestDelegate_Watch(t *testing.T) {
	type input struct {
		ctx             context.Context
		internaloptions *metainternalversion.ListOptions
	}

	type output struct {
		watch watch.Interface
		err   error
	}

	type testCase struct {
		name                    string
		input                   input
		expected                output
		storeSetup              func(*MockStore[*TestType, *TestTypeList])
		simulateConvertionError bool
		wantedErr               bool
	}

	tests := []testCase{
		{
			name: "missing user in context",
			input: input{
				ctx:             context.TODO(),
				internaloptions: &metainternalversion.ListOptions{},
			},
			expected: output{
				err: errMissingUserInfo,
			},
			wantedErr:  true,
			storeSetup: func(ms *MockStore[*TestType, *TestTypeList]) {},
		},
		{
			name: "convert list error",
			input: input{
				ctx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				internaloptions: &metainternalversion.ListOptions{},
			},
			simulateConvertionError: true,
			wantedErr:               true,
			storeSetup:              func(ms *MockStore[*TestType, *TestTypeList]) {},
		},
	}

	for _, tt := range tests {
		scheme := runtime.NewScheme()
		addToSchemeTest(scheme)

		if !tt.simulateConvertionError {
			scheme.AddConversionFunc(&metainternalversion.ListOptions{}, &metav1.ListOptions{}, convert_internalversion_ListOptions_to_v1_ListOptions)
		}

		gvk := schema.GroupVersionKind{Group: "ext.cattle.io", Version: "v1", Kind: "TestType"}
		gvr := schema.GroupVersionResource{Group: "ext.cattle.io", Version: "v1", Resource: "testtypes"}

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStore := NewMockStore[*TestType, *TestTypeList](ctrl)

		deleg4te := &delegate[*TestType, *TestTypeList]{
			scheme: scheme,
			t:      &TestType{},
			tList:  &TestTypeList{},
			gvk:    gvk,
			gvr:    gvr,
			store:  mockStore,
		}

		watch, err := deleg4te.Watch(tt.input.ctx, tt.input.internaloptions)
		if tt.wantedErr {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, watch, tt.expected.watch)
		}
	}
}

func TestDelegate_Update(t *testing.T) {
	type input struct {
		parentCtx        context.Context
		name             string
		objInfo          rest.UpdatedObjectInfo
		createValidation rest.ValidateObjectFunc
		updateValidation rest.ValidateObjectUpdateFunc
		forceAllowCreate bool
		options          *metav1.UpdateOptions
	}

	type output struct {
		obj     runtime.Object
		created bool
		err     error
	}

	type testCase struct {
		name    string
		setup   func(*MockUpdatedObjectInfo, *MockStore[*TestType, *TestTypeList])
		input   input
		expect  output
		wantErr bool
	}

	tests := []testCase{
		{
			name: "working case",
			input: input{
				parentCtx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				name: "test-user",
				// objInfo is created in the for loop
				forceAllowCreate: false,
				options:          &metav1.UpdateOptions{},
			},
			expect: output{
				obj:     &TestType{},
				created: false,
				err:     nil,
			},
			setup: func(objInfo *MockUpdatedObjectInfo, store *MockStore[*TestType, *TestTypeList]) {
				objInfo.EXPECT().UpdatedObject(gomock.Any(), gomock.Any()).Return(&TestType{}, nil)
				store.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&TestType{}, nil)
				store.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).Return(&TestType{}, nil)
			},
			wantErr: false,
		},
		{
			name: "missing user in context",
			input: input{
				parentCtx: context.TODO(),
			},
			setup: func(muoi *MockUpdatedObjectInfo, ms *MockStore[*TestType, *TestTypeList]) {},
			expect: output{
				obj:     nil,
				created: false,
				err:     errMissingUserInfo,
			},
			wantErr: true,
		},
		{
			name: "get failed - other error",
			input: input{
				parentCtx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				name: "test-user",
			},
			expect: output{
				obj:     nil,
				created: false,
				err:     assert.AnError,
			},
			setup: func(objInfo *MockUpdatedObjectInfo, store *MockStore[*TestType, *TestTypeList]) {
				store.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, assert.AnError)
			},
			wantErr: true,
		},
		{
			name: "get failed - not found - updated object error",
			input: input{
				parentCtx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				name: "test-user",
			},
			expect: output{
				obj:     nil,
				created: false,
				err:     assert.AnError,
			},
			setup: func(objInfo *MockUpdatedObjectInfo, store *MockStore[*TestType, *TestTypeList]) {
				store.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil,
					&apierrors.StatusError{
						ErrStatus: metav1.Status{
							Code:   http.StatusNotFound,
							Reason: metav1.StatusReasonNotFound,
						},
					},
				)
				objInfo.EXPECT().UpdatedObject(gomock.Any(), gomock.Any()).Return(nil, assert.AnError)
			},
			wantErr: true,
		},
		{
			name: "get failed - not found - create succeeded",
			input: input{
				parentCtx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				name:             "test-user",
				createValidation: func(ctx context.Context, obj runtime.Object) error { return nil },
			},
			expect: output{
				obj:     &TestType{},
				created: true,
				err:     nil,
			},
			setup: func(objInfo *MockUpdatedObjectInfo, store *MockStore[*TestType, *TestTypeList]) {
				store.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil,
					&apierrors.StatusError{
						ErrStatus: metav1.Status{
							Code:   http.StatusNotFound,
							Reason: metav1.StatusReasonNotFound,
						},
					},
				)
				objInfo.EXPECT().UpdatedObject(gomock.Any(), gomock.Any()).Return(&TestType{}, nil)
				store.EXPECT().Create(gomock.Any(), gomock.Any(), gomock.Any()).Return(&TestType{}, nil)
			},
			wantErr: false,
		},
		{
			name: "get failed - not found - create validation error",
			input: input{
				parentCtx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				name: "test-user",
				createValidation: func(ctx context.Context, obj runtime.Object) error {
					return assert.AnError
				},
			},
			expect: output{
				obj:     nil,
				created: false,
				err:     assert.AnError,
			},
			setup: func(objInfo *MockUpdatedObjectInfo, store *MockStore[*TestType, *TestTypeList]) {
				store.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil,
					&apierrors.StatusError{
						ErrStatus: metav1.Status{
							Code:   http.StatusNotFound,
							Reason: metav1.StatusReasonNotFound,
						},
					},
				)
				objInfo.EXPECT().UpdatedObject(gomock.Any(), gomock.Any()).Return(&TestType{}, nil)
			},
			wantErr: true,
		},
		{
			name: "get failed - not found - type error",
			input: input{
				parentCtx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				name: "test-user",
				createValidation: func(ctx context.Context, obj runtime.Object) error {
					return nil
				},
			},
			expect: output{
				obj:     nil,
				created: false,
				err:     assert.AnError,
			},
			setup: func(objInfo *MockUpdatedObjectInfo, store *MockStore[*TestType, *TestTypeList]) {
				store.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil,
					&apierrors.StatusError{
						ErrStatus: metav1.Status{
							Code:   http.StatusNotFound,
							Reason: metav1.StatusReasonNotFound,
						},
					},
				)
				objInfo.EXPECT().UpdatedObject(gomock.Any(), gomock.Any()).Return(&runtime.Unknown{}, nil)

			},
			wantErr: true,
		},
		{
			name: "get failed - not found - store create error",
			input: input{
				parentCtx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				name: "test-user",
				createValidation: func(ctx context.Context, obj runtime.Object) error {
					return nil
				},
			},
			expect: output{
				obj:     nil,
				created: false,
				err:     assert.AnError,
			},
			setup: func(objInfo *MockUpdatedObjectInfo, store *MockStore[*TestType, *TestTypeList]) {
				store.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil,
					&apierrors.StatusError{
						ErrStatus: metav1.Status{
							Code:   http.StatusNotFound,
							Reason: metav1.StatusReasonNotFound,
						},
					},
				)
				store.EXPECT().Create(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, assert.AnError)
				objInfo.EXPECT().UpdatedObject(gomock.Any(), gomock.Any()).Return(&TestType{}, nil)

			},
			wantErr: true,
		},
		{
			name: "get worked - updated object error",
			input: input{
				parentCtx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				name: "test-user",
				createValidation: func(ctx context.Context, obj runtime.Object) error {
					return nil
				},
			},
			expect: output{
				obj:     nil,
				created: false,
				err:     assert.AnError,
			},
			setup: func(objInfo *MockUpdatedObjectInfo, store *MockStore[*TestType, *TestTypeList]) {
				store.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&TestType{}, nil)
				objInfo.EXPECT().UpdatedObject(gomock.Any(), gomock.Any()).Return(nil, assert.AnError)

			},
			wantErr: true,
		},
		{
			name: "get worked - type error",
			input: input{
				parentCtx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				name: "test-user",
				createValidation: func(ctx context.Context, obj runtime.Object) error {
					return nil
				},
			},
			expect: output{
				obj:     nil,
				created: false,
				err:     assert.AnError,
			},
			setup: func(objInfo *MockUpdatedObjectInfo, store *MockStore[*TestType, *TestTypeList]) {
				store.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&TestType{}, nil)
				objInfo.EXPECT().UpdatedObject(gomock.Any(), gomock.Any()).Return(&runtime.Unknown{}, nil)

			},
			wantErr: true,
		},
		{
			name: "get worked - update validation error",
			input: input{
				parentCtx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				name: "test-user",
				createValidation: func(ctx context.Context, obj runtime.Object) error {
					return nil
				},
				updateValidation: func(ctx context.Context, obj, old runtime.Object) error {
					return assert.AnError
				},
			},
			expect: output{
				obj:     nil,
				created: false,
				err:     assert.AnError,
			},
			setup: func(objInfo *MockUpdatedObjectInfo, store *MockStore[*TestType, *TestTypeList]) {
				store.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&TestType{}, nil)
				objInfo.EXPECT().UpdatedObject(gomock.Any(), gomock.Any()).Return(&TestType{}, nil)

			},
			wantErr: true,
		},
		{
			name: "get worked - store update error",
			input: input{
				parentCtx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				name: "test-user",
				createValidation: func(ctx context.Context, obj runtime.Object) error {
					return nil
				},
			},
			expect: output{
				obj:     nil,
				created: false,
				err:     assert.AnError,
			},
			setup: func(objInfo *MockUpdatedObjectInfo, store *MockStore[*TestType, *TestTypeList]) {
				store.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&TestType{}, nil)
				objInfo.EXPECT().UpdatedObject(gomock.Any(), gomock.Any()).Return(&TestType{}, nil)
				store.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, assert.AnError)
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			addToSchemeTest(scheme)

			gvk := schema.GroupVersionKind{Group: "ext.cattle.io", Version: "v1", Kind: "TestType"}
			gvr := schema.GroupVersionResource{Group: "ext.cattle.io", Version: "v1", Resource: "testtypes"}

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := NewMockStore[*TestType, *TestTypeList](ctrl)
			mockObjInfo := NewMockUpdatedObjectInfo(ctrl)
			tt.input.objInfo = mockObjInfo
			tt.setup(mockObjInfo, mockStore)

			deleg4te := &delegate[*TestType, *TestTypeList]{
				scheme: scheme,
				t:      &TestType{},
				tList:  &TestTypeList{},
				gvk:    gvk,
				gvr:    gvr,
				store:  mockStore,
			}

			obj, created, err := deleg4te.Update(tt.input.parentCtx, tt.input.name, tt.input.objInfo, tt.input.createValidation, tt.input.updateValidation, tt.input.forceAllowCreate, tt.input.options)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expect.obj, obj)
				assert.Equal(t, tt.expect.created, created)
			}
		})
	}

}

func TestDelegate_Create(t *testing.T) {
	type input struct {
		ctx              context.Context
		obj              runtime.Object
		createValidation rest.ValidateObjectFunc
		options          *metav1.CreateOptions
	}

	type output struct {
		createResult runtime.Object
		err          error
	}

	type testCase struct {
		name       string
		storeSetup func(*MockStore[*TestType, *TestTypeList])
		wantErr    bool
		input      input
		expect     output
	}

	tests := []testCase{
		{
			name: "working case",
			storeSetup: func(ms *MockStore[*TestType, *TestTypeList]) {
				ms.EXPECT().Create(gomock.Any(), gomock.Any(), gomock.Any()).Return(&TestType{}, nil)
			},
			input: input{
				ctx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				obj:     &TestType{},
				options: &metav1.CreateOptions{},
			},
			expect: output{
				createResult: &TestType{},
				err:          nil,
			},
			wantErr: false,
		},
		{
			name:       "missing user in the context",
			storeSetup: func(ms *MockStore[*TestType, *TestTypeList]) {},
			input: input{
				ctx:     context.Background(),
				obj:     &TestType{},
				options: &metav1.CreateOptions{},
			},
			expect: output{
				createResult: nil,
				err:          errMissingUserInfo,
			},
			wantErr: true,
		},
		{
			name: "store create error",
			storeSetup: func(ms *MockStore[*TestType, *TestTypeList]) {
				ms.EXPECT().Create(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, assert.AnError)
			},
			input: input{
				ctx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				obj:     &TestType{},
				options: &metav1.CreateOptions{},
			},
			expect: output{
				createResult: nil,
				err:          assert.AnError,
			},
			wantErr: true,
		},
		{
			name:       "wrong type error",
			storeSetup: func(ms *MockStore[*TestType, *TestTypeList]) {},
			input: input{
				ctx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				obj:     &runtime.Unknown{},
				options: &metav1.CreateOptions{},
			},
			expect: output{
				createResult: nil,
				err:          assert.AnError,
			},
			wantErr: true,
		},
		{
			name:       "create validation error",
			storeSetup: func(ms *MockStore[*TestType, *TestTypeList]) {},
			input: input{
				ctx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				obj: &TestType{},
				createValidation: func(ctx context.Context, obj runtime.Object) error {
					return assert.AnError
				},
				options: &metav1.CreateOptions{},
			},
			expect: output{
				createResult: &TestType{},
				err:          assert.AnError,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			addToSchemeTest(scheme)

			gvk := schema.GroupVersionKind{Group: "ext.cattle.io", Version: "v1", Kind: "TestType"}
			gvr := schema.GroupVersionResource{Group: "ext.cattle.io", Version: "v1", Resource: "testtypes"}

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := NewMockStore[*TestType, *TestTypeList](ctrl)
			tt.storeSetup(mockStore)

			deleg4te := &delegate[*TestType, *TestTypeList]{
				scheme: scheme,
				t:      &TestType{},
				tList:  &TestTypeList{},
				gvk:    gvk,
				gvr:    gvr,
				store:  mockStore,
			}

			result, err := deleg4te.Create(tt.input.ctx, tt.input.obj, tt.input.createValidation, tt.input.options)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expect.createResult, result)
			}
		})
	}
}

func TestDelegate_Delete(t *testing.T) {
	type input struct {
		ctx              context.Context
		name             string
		deleteValidation rest.ValidateObjectFunc
		options          *metav1.DeleteOptions
	}

	type output struct {
		deleteResult runtime.Object
		completed    bool
		err          error
	}

	type testCase struct {
		name       string
		storeSetup func(*MockStore[*TestType, *TestTypeList])
		wantErr    bool
		input      input
		expect     output
	}

	tests := []testCase{
		{
			name: "working case",
			storeSetup: func(ms *MockStore[*TestType, *TestTypeList]) {
				ms.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&TestType{}, nil)
				ms.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			},
			input: input{
				ctx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				name:    "test-object",
				options: &metav1.DeleteOptions{},
			},
			expect: output{
				deleteResult: &TestType{},
				completed:    true,
				err:          nil,
			},
			wantErr: false,
		},
		{
			name:       "missing user in the context",
			storeSetup: func(ms *MockStore[*TestType, *TestTypeList]) {},
			input: input{
				ctx:     context.Background(),
				name:    "test-object",
				options: &metav1.DeleteOptions{},
			},
			expect: output{
				deleteResult: nil,
				completed:    false,
				err:          errMissingUserInfo,
			},
			wantErr: true,
		},
		{
			name: "store get error",
			storeSetup: func(ms *MockStore[*TestType, *TestTypeList]) {
				ms.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, assert.AnError)
			},
			input: input{
				ctx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				name:    "test-object",
				options: &metav1.DeleteOptions{},
			},
			expect: output{
				deleteResult: nil,
				completed:    false,
				err:          assert.AnError,
			},
			wantErr: true,
		},
		{
			name: "store delete error",
			storeSetup: func(ms *MockStore[*TestType, *TestTypeList]) {
				ms.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&TestType{}, nil)
				ms.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(assert.AnError)
			},
			input: input{
				ctx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				name:    "test-object",
				options: &metav1.DeleteOptions{},
			},
			expect: output{
				deleteResult: &TestType{},
				completed:    true,
				err:          assert.AnError,
			},
			wantErr: true,
		},
		{
			name: "delete validation error",
			storeSetup: func(ms *MockStore[*TestType, *TestTypeList]) {
				ms.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&TestType{}, nil)
			},
			input: input{
				ctx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				name:             "test-object",
				deleteValidation: func(ctx context.Context, obj runtime.Object) error { return assert.AnError },
				options:          &metav1.DeleteOptions{},
			},
			expect: output{
				deleteResult: nil,
				completed:    false,
				err:          assert.AnError,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			addToSchemeTest(scheme)

			gvk := schema.GroupVersionKind{Group: "ext.cattle.io", Version: "v1", Kind: "TestType"}
			gvr := schema.GroupVersionResource{Group: "ext.cattle.io", Version: "v1", Resource: "testtypes"}

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := NewMockStore[*TestType, *TestTypeList](ctrl)
			tt.storeSetup(mockStore)

			deleg4te := &delegate[*TestType, *TestTypeList]{
				scheme: scheme,
				t:      &TestType{},
				tList:  &TestTypeList{},
				gvk:    gvk,
				gvr:    gvr,
				store:  mockStore,
			}

			result, completed, err := deleg4te.Delete(tt.input.ctx, tt.input.name, tt.input.deleteValidation, tt.input.options)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expect.deleteResult, result)
				assert.Equal(t, tt.expect.completed, completed)
			}
		})
	}
}

func TestDelegate_Get(t *testing.T) {
	type input struct {
		ctx     context.Context
		name    string
		options *metav1.GetOptions
	}

	type output struct {
		getResult runtime.Object
		err       error
	}

	type testCase struct {
		name       string
		storeSetup func(*MockStore[*TestType, *TestTypeList])
		wantErr    bool
		input      input
		expect     output
	}

	tests := []testCase{
		{
			name: "working case",
			storeSetup: func(ms *MockStore[*TestType, *TestTypeList]) {
				ms.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&TestType{}, nil)
			},
			input: input{
				ctx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				name:    "test-object",
				options: &metav1.GetOptions{},
			},
			expect: output{
				getResult: &TestType{},
				err:       nil,
			},
			wantErr: false,
		},
		{
			name:       "missing user in the context",
			storeSetup: func(ms *MockStore[*TestType, *TestTypeList]) {},
			input: input{
				ctx:     context.Background(),
				name:    "testing-obj",
				options: &metav1.GetOptions{},
			},
			expect: output{
				getResult: nil,
				err:       errMissingUserInfo,
			},
			wantErr: true,
		},
		{
			name: "store get error",
			storeSetup: func(ms *MockStore[*TestType, *TestTypeList]) {
				ms.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, assert.AnError)
			},
			input: input{
				ctx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				name:    "test-object",
				options: &metav1.GetOptions{},
			},
			expect: output{
				getResult: &TestType{},
				err:       assert.AnError,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			addToSchemeTest(scheme)

			gvk := schema.GroupVersionKind{Group: "ext.cattle.io", Version: "v1", Kind: "TestType"}
			gvr := schema.GroupVersionResource{Group: "ext.cattle.io", Version: "v1", Resource: "testtypes"}

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := NewMockStore[*TestType, *TestTypeList](ctrl)
			tt.storeSetup(mockStore)

			deleg4te := &delegate[*TestType, *TestTypeList]{
				scheme: scheme,
				t:      &TestType{},
				tList:  &TestTypeList{},
				gvk:    gvk,
				gvr:    gvr,
				store:  mockStore,
			}

			result, err := deleg4te.Get(tt.input.ctx, tt.input.name, tt.input.options)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expect.getResult, result)
			}

		})
	}
}

func TestDelegate_List(t *testing.T) {
	type input struct {
		ctx         context.Context
		listOptions *metainternalversion.ListOptions
	}

	type output struct {
		listResult runtime.Object
		err        error
	}

	type testCase struct {
		name                 string
		storeSetup           func(*MockStore[*TestType, *TestTypeList])
		simulateConvertError bool
		wantErr              bool
		input                input
		expect               output
	}

	tests := []testCase{
		{
			name: "working case, for completion reasons",
			input: input{
				ctx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				listOptions: &metainternalversion.ListOptions{},
			},
			storeSetup: func(ms *MockStore[*TestType, *TestTypeList]) {
				ms.EXPECT().List(gomock.Any(), gomock.Any()).Return(&TestTypeList{}, nil)
			},
			wantErr: false,
			expect: output{
				listResult: &TestTypeList{},
				err:        nil,
			},
		},
		{
			name: "missing user in the context",
			input: input{
				ctx:         context.Background(),
				listOptions: &metainternalversion.ListOptions{},
			},
			storeSetup: func(mockStore *MockStore[*TestType, *TestTypeList]) {},
			wantErr:    true,
			expect: output{
				listResult: nil,
				err:        errMissingUserInfo,
			},
		},
		{
			name: "convertListOptions error",
			input: input{
				ctx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				listOptions: &metainternalversion.ListOptions{},
			},
			storeSetup:           func(mockStore *MockStore[*TestType, *TestTypeList]) {},
			simulateConvertError: true,
			wantErr:              true,
			expect: output{
				listResult: nil,
				err:        assert.AnError,
			},
		},
		{
			name: "error returned by store",
			input: input{
				ctx: request.WithUser(context.Background(), &user.DefaultInfo{
					Name: "test-user",
				}),
				listOptions: &metainternalversion.ListOptions{},
			},
			storeSetup: func(mockStore *MockStore[*TestType, *TestTypeList]) {
				mockStore.EXPECT().List(gomock.Any(), gomock.Any()).Return(nil, assert.AnError)
			},
			wantErr: true,
			expect: output{
				listResult: nil,
				err:        assert.AnError,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			addToSchemeTest(scheme)

			if !tt.simulateConvertError {
				scheme.AddConversionFunc(&metainternalversion.ListOptions{}, &metav1.ListOptions{}, convert_internalversion_ListOptions_to_v1_ListOptions)
			}

			gvk := schema.GroupVersionKind{Group: "ext.cattle.io", Version: "v1", Kind: "TestType"}
			gvr := schema.GroupVersionResource{Group: "ext.cattle.io", Version: "v1", Resource: "testtypes"}

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := NewMockStore[*TestType, *TestTypeList](ctrl)
			tt.storeSetup(mockStore)

			deleg4te := &delegate[*TestType, *TestTypeList]{
				scheme: scheme,
				t:      &TestType{},
				tList:  &TestTypeList{},
				gvk:    gvk,
				gvr:    gvr,
				store:  mockStore,
			}

			result, err := deleg4te.List(tt.input.ctx, tt.input.listOptions)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expect.listResult, result)
			}
		})
	}
}

func convert_internalversion_ListOptions_to_v1_ListOptions(in, out interface{}, s conversion.Scope) error {
	i, ok := in.(*metainternalversion.ListOptions)
	if !ok {
		return errors.New("cannot convert in param into internalversion.ListOptions")
	}
	o, ok := out.(*metav1.ListOptions)
	if !ok {
		return errors.New("cannot convert out param into metav1.ListOptions")
	}
	if i.LabelSelector != nil {
		o.LabelSelector = i.LabelSelector.String()
	}
	if i.FieldSelector != nil {
		o.FieldSelector = i.FieldSelector.String()
	}
	o.Watch = i.Watch
	o.ResourceVersion = i.ResourceVersion
	o.TimeoutSeconds = i.TimeoutSeconds
	return nil
}
