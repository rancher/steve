package ext

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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
			output := convertError(tt.input)
			assert.Equal(t, tt.output, output)
		})
	}

}
