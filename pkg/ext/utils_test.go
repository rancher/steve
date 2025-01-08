package ext

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
