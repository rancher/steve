package pods_test

import (
	"testing"

	"github.com/rancher/steve/pkg/resources/virtual/pods"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestTransformPods(t *testing.T) {
	tests := []struct {
		name       string
		input      any
		wantOutput any
		wantError  bool
	}{
		{
			name: "fix pod fields",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "/v1",
					"kind":       "Pod",
					"metadata": map[string]interface{}{
						"name":      "gregsFarm",
						"namespace": "gregsNamespace",
						"state": map[string]interface{}{
							"name": "succeeded",
						},
						"fields": []interface{}{
							"1",
							"1",
							"Completed",
						},
					},
					"id":   "podTest1id",
					"type": "Gorniplatz",
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "/v1",
					"kind":       "Pod",
					"metadata": map[string]interface{}{
						"name":      "gregsFarm",
						"namespace": "gregsNamespace",
						"state": map[string]interface{}{
							"name": "completed",
						},
						"fields": []interface{}{
							"1",
							"1",
							"Completed",
						},
					},
					"id":   "podTest1id",
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
			if ok && raw.GetKind() == "Pod" && raw.GetAPIVersion() == "/v1" {
				output, err = pods.TransformPodObject(raw)
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
