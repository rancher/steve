package ext

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestConvertListOptions(t *testing.T) {
	internal := &metainternalversion.ListOptions{
		ResourceVersion: "foo",
		Watch:           true,
	}
	expected := &metav1.ListOptions{
		ResourceVersion: "foo",
		Watch:           true,
	}
	got, err := ConvertListOptions(internal)
	assert.NoError(t, err)
	assert.Equal(t, expected, got)
}

func TestConvertError(t *testing.T) {
	tests := []struct {
		name   string
		input  error
		output error
	}{
		{
			name: "api status error",
			input: &apierrors.StatusError{
				ErrStatus: metav1.Status{
					Code:   http.StatusNotFound,
					Reason: metav1.StatusReasonNotFound,
				},
			},
			output: &apierrors.StatusError{
				ErrStatus: metav1.Status{
					Code:   http.StatusNotFound,
					Reason: metav1.StatusReasonNotFound,
				},
			},
		},
		{
			name:  "generic error",
			input: assert.AnError,
			output: &apierrors.StatusError{ErrStatus: metav1.Status{
				Status: metav1.StatusFailure,
				Code:   http.StatusInternalServerError,
				Reason: metav1.StatusReasonInternalError,
				Details: &metav1.StatusDetails{
					Causes: []metav1.StatusCause{{Message: assert.AnError.Error()}},
				},
				Message: fmt.Sprintf("Internal error occurred: %v", assert.AnError),
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.output, convertError(tt.input))
		})
	}
}

type fakeUpdatedObjectInfo struct {
	obj runtime.Object
}

func (f *fakeUpdatedObjectInfo) UpdatedObject(ctx context.Context, oldObj runtime.Object) (runtime.Object, error) {
	return f.obj, nil
}

func (f *fakeUpdatedObjectInfo) Preconditions() *metav1.Preconditions {
	return nil
}

func TestCreateOrUpdate(t *testing.T) {
	t.Run("create options are respected", func(t *testing.T) {
		var createValidationCalled bool
		createValidation := func(ctx context.Context, obj runtime.Object) error {
			createValidationCalled = true
			return nil
		}

		var updateValidationCalled bool
		updateValidation := func(ctx context.Context, obj runtime.Object, oldObj runtime.Object) error {
			updateValidationCalled = true
			return nil
		}

		forceAllowCreate := false

		options := &metav1.UpdateOptions{
			DryRun:          []string{"All"},
			FieldManager:    "test",
			FieldValidation: metav1.FieldValidationStrict,
		}

		getFn := func(ctx context.Context, name string, opts *metav1.GetOptions) (*TestType, error) {
			return nil, apierrors.NewNotFound(testTypeGVR.GroupResource(), name)
		}

		var createFnCalled bool
		createFn := func(ctx context.Context, obj *TestType, opts *metav1.CreateOptions) (*TestType, error) {
			createFnCalled = true

			require.NotNil(t, opts)
			assert.Equal(t, options.DryRun, opts.DryRun)
			assert.Equal(t, options.FieldManager, opts.FieldManager)
			assert.Equal(t, options.FieldValidation, opts.FieldValidation)

			return obj.DeepCopy(), nil
		}

		var updateFnCalled bool
		updateFn := func(ctx context.Context, obj *TestType, opts *metav1.UpdateOptions) (*TestType, error) {
			updateFnCalled = true
			return obj.DeepCopy(), nil
		}

		objInfo := &fakeUpdatedObjectInfo{
			obj: &TestType{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
			},
		}

		obj, isCreated, err := CreateOrUpdate(context.Background(), "foo", objInfo, createValidation, updateValidation, forceAllowCreate, options, getFn, createFn, updateFn)
		require.NoError(t, err)
		assert.NotNil(t, obj)
		assert.True(t, isCreated)

		assert.True(t, createValidationCalled)
		assert.False(t, updateValidationCalled)
		assert.True(t, createFnCalled)
		assert.False(t, updateFnCalled)
	})
}
