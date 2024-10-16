package events_test

import (
	"testing"

	"github.com/rancher/steve/pkg/resources/virtual/events"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestTransformEvents(t *testing.T) {
	tests := []struct {
		name       string
		input      any
		wantOutput any
		wantError  bool
	}{
		{
			name: "fix event fields",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "/v1",
					"kind":       "Event",
					"metadata": map[string]interface{}{
						"name":      "gregsFarm",
						"namespace": "gregsNamespace",
					},
					"id":   "eventTest1id",
					"type": "Gorniplatz",
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "/v1",
					"kind":       "Event",
					"metadata": map[string]interface{}{
						"name":      "gregsFarm",
						"namespace": "gregsNamespace",
					},
					"id":    "eventTest1id",
					"type":  "Gorniplatz",
					"_type": "Gorniplatz",
				},
			},
		},
		{
			name: "don't fix non-default-group event fields",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "palau.io/v1",
					"kind":       "Event",
					"metadata": map[string]interface{}{
						"name":      "gregsFarm",
						"namespace": "gregsNamespace",
					},
					"id":   "eventTest1id",
					"type": "Gorniplatz",
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "palau.io/v1",
					"kind":       "Event",
					"metadata": map[string]interface{}{
						"name":      "gregsFarm",
						"namespace": "gregsNamespace",
					},
					"id":   "eventTest1id",
					"type": "Gorniplatz",
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output interface{}
			var err error
			raw, ok := test.input.(*unstructured.Unstructured)
			if ok && raw.GetKind() == "Event" && raw.GetAPIVersion() == "/v1" {
				output, err = events.TransformEventObject(raw)
			} else {
				output = raw
				err = nil
			}
			require.Equal(t, test.wantOutput, output)
			if test.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
