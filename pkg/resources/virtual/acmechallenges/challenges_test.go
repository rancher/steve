package acmechallenges

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type challengeTestType struct {
	name       string
	input      any
	wantOutput any
	wantError  bool
}

func TestTransformChallenge(t *testing.T) {
	conversions := map[string]string{
		"valid":      "active",
		"ready":      "in-progress",
		"processing": "in-progress",
		"invalid":    "error",
		"errored":    "error",
		"denied":     "denied",
		"expired":    "expired",
		"expiring":   "expiring",
		"pending":    "pending",
	}
	isError := map[string]bool{"invalid": true, "errored": true, "denied": true}
	for k, v := range conversions {
		objName := "joansFarm" + k
		id := "joansNamespace/" + objName
		test := challengeTestType{
			name: "fix certificate field " + objName,
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "acme.cert-manager.io/v1",
					"kind":       "Challenge",
					"metadata": map[string]interface{}{
						"name":      objName,
						"namespace": "joansNamespace",
					},
					"status": map[string]interface{}{
						"state": k,
					},
					"id": id,
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "acme.cert-manager.io/v1",
					"kind":       "Challenge",
					"metadata": map[string]interface{}{
						"name":      objName,
						"namespace": "joansNamespace",
						"state": map[string]interface{}{
							"name": v,
						},
					},
					"status": map[string]interface{}{
						"state": k,
					},
					"id": id,
				},
			},
		}
		if isError[k] {
			test.wantOutput.(*unstructured.Unstructured).Object["metadata"].(map[string]interface{})["state"].(map[string]interface{})["error"] = true
		}
		t.Run(test.name, func(t *testing.T) {
			var output interface{}
			var err error
			raw, ok := test.input.(*unstructured.Unstructured)
			if ok && raw.GetKind() == "Challenge" && raw.GetAPIVersion() == "acme.cert-manager.io/v1" {
				output, err = TransformChallenge(raw)
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

func TestTransformInProgressChallenge(t *testing.T) {
	objName := "pendingProcessing"
	id := "joansNamespace/" + objName
	test := challengeTestType{
		name: "fix certificate field " + objName,
		input: &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "acme.cert-manager.io/v1",
				"kind":       "Challenge",
				"metadata": map[string]interface{}{
					"name":      objName,
					"namespace": "joansNamespace",
					"state":     map[string]interface{}{},
				},
				"status": map[string]interface{}{
					"state":      "pending",
					"processing": true,
				},
				"id": id,
			},
		},
		wantOutput: &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "acme.cert-manager.io/v1",
				"kind":       "Challenge",
				"metadata": map[string]interface{}{
					"name":      objName,
					"namespace": "joansNamespace",
					"state": map[string]interface{}{
						"name": "in-progress",
					},
				},
				"status": map[string]interface{}{
					"state":      "pending",
					"processing": true,
				},
				"id": id,
			},
		},
	}
	t.Run(test.name, func(t *testing.T) {
		var output interface{}
		var err error
		raw, ok := test.input.(*unstructured.Unstructured)
		if ok && raw.GetKind() == "Challenge" && raw.GetAPIVersion() == "acme.cert-manager.io/v1" {
			output, err = TransformChallenge(raw)
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
