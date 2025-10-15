package clusters

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestTransformManagedCluster(t *testing.T) {
	tests := []struct {
		name       string
		input      *unstructured.Unstructured
		wantOutput *unstructured.Unstructured
		wantError  bool
	}{
		{
			name: "a non-ready cluster",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"id":   1,
					"type": "management.cattle.io.cluster",
					"metadata": map[string]interface{}{
						"name": "c-m-boris",
					},
					"spec": map[string]interface{}{
						"displayName": "boris",
						"internal":    false,
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:16Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "BackingNamespaceCreated",
							},
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:16Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "DefaultProjectCreated",
							},
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:16Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "SystemProjectCreated",
							},
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:17Z",
								"status":         "False",
								"transitioning":  false,
								"type":           "Ready",
							},
						},
					},
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"id":   1,
					"type": "management.cattle.io.cluster",
					"metadata": map[string]interface{}{
						"name": "c-m-boris",
					},
					"spec": map[string]interface{}{
						"displayName": "boris",
						"internal":    false,
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:16Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "BackingNamespaceCreated",
							},
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:16Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "DefaultProjectCreated",
							},
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:16Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "SystemProjectCreated",
							},
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:17Z",
								"status":         "False",
								"transitioning":  false,
								"type":           "Ready",
							},
						},
						"connected": false,
					},
				},
			},
		},
		{
			name: "the local cluster",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"id":   2,
					"type": "management.cattle.io.cluster",
					"metadata": map[string]interface{}{
						"name": "local",
					},
					"spec": map[string]interface{}{
						"displayName": "local",
						"internal":    true,
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:41:37Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "BackingNamespaceCreated",
							},
							map[string]interface{}{

								"error":          false,
								"lastUpdateTime": "2025-01-10T22:41:37Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "DefaultProjectCreated",
							},
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "",
								"status":         "True",
								"transitioning":  false,
								"type":           "Ready",
							},
						},
					},
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"id":   2,
					"type": "management.cattle.io.cluster",
					"metadata": map[string]interface{}{
						"name": "local",
					},
					"spec": map[string]interface{}{
						"displayName": "local",
						"internal":    true,
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{

								"error":          false,
								"lastUpdateTime": "2025-01-10T22:41:37Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "BackingNamespaceCreated",
							},
							map[string]interface{}{

								"error":          false,
								"lastUpdateTime": "2025-01-10T22:41:37Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "DefaultProjectCreated",
							},
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "",
								"status":         "True",
								"transitioning":  false,
								"type":           "Ready",
							},
						},
						"connected": true,
					},
				},
			},
		},
		{
			name: "a ready non-local cluster",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"id":   3,
					"type": "management.cattle.io.cluster",
					"metadata": map[string]interface{}{
						"name": "c-m-natasha",
					},
					"spec": map[string]interface{}{
						"displayName": "c-m-natasha",
						"internal":    false,
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{

								"error":          false,
								"lastUpdateTime": "2025-01-10T22:41:37Z",
								"status":         "Ready",
								"transitioning":  false,
								"type":           "BackingNamespaceCreated",
							},
							map[string]interface{}{

								"error":          false,
								"lastUpdateTime": "2025-01-10T22:41:37Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "DefaultProjectCreated",
							},
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "",
								"status":         "True",
								"transitioning":  false,
								"type":           "Ready",
							},
						},
					},
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"id":   3,
					"type": "management.cattle.io.cluster",
					"metadata": map[string]interface{}{
						"name": "c-m-natasha",
					},
					"spec": map[string]interface{}{
						"displayName": "c-m-natasha",
						"internal":    false,
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:41:37Z",
								"status":         "Ready",
								"transitioning":  false,
								"type":           "BackingNamespaceCreated",
							},
							map[string]interface{}{

								"error":          false,
								"lastUpdateTime": "2025-01-10T22:41:37Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "DefaultProjectCreated",
							},
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "",
								"status":         "True",
								"transitioning":  false,
								"type":           "Ready",
							},
						},
						"connected": true,
					},
				},
			},
		},
		{
			name: "the 'K' in memory fields gets transformed",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"id":   4,
					"type": "management.cattle.io.cluster",
					"metadata": map[string]interface{}{
						"name": "c-m-yodie",
					},
					"spec": map[string]interface{}{
						"displayName": "yodie",
						"internal":    false,
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:17Z",
								"status":         "False",
								"transitioning":  false,
								"type":           "Ready",
							},
						},
						"allocatable": map[string]interface{}{
							"cpu":    "1",
							"memory": "12K",
							"pods":   "1",
						},
						"requested": map[string]interface{}{
							"cpu":    "2",
							"memory": "12Ki",
							"pods":   "2",
						},
					},
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"id":   4,
					"type": "management.cattle.io.cluster",
					"metadata": map[string]interface{}{
						"name": "c-m-yodie",
					},
					"spec": map[string]interface{}{
						"displayName": "yodie",
						"internal":    false,
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:17Z",
								"status":         "False",
								"transitioning":  false,
								"type":           "Ready",
							},
						},
						"connected": false,
						"allocatable": map[string]interface{}{
							"cpu":       "1",
							"memoryRaw": float64(12000),
							"memory":    "12K",
							"pods":      "1",
						},
						"requested": map[string]interface{}{
							"cpu":       "2",
							"memory":    "12Ki",
							"memoryRaw": float64(12288),
							"pods":      "2",
						},
					},
				},
			},
		},
		{
			name: "the 'M' in memory fields gets transformed",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"id":   5,
					"type": "management.cattle.io.cluster",
					"metadata": map[string]interface{}{
						"name": "c-m-homer",
					},
					"spec": map[string]interface{}{
						"displayName": "homer",
						"internal":    false,
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:17Z",
								"status":         "False",
								"transitioning":  false,
								"type":           "Ready",
							},
						},
						"allocatable": map[string]interface{}{
							"cpu":    "1",
							"memory": "12M",
							"pods":   "1",
						},
						"requested": map[string]interface{}{
							"cpu":    "2",
							"memory": "12Mi",
							"pods":   "2",
						},
					},
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"id":   5,
					"type": "management.cattle.io.cluster",
					"metadata": map[string]interface{}{
						"name": "c-m-homer",
					},
					"spec": map[string]interface{}{
						"displayName": "homer",
						"internal":    false,
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:17Z",
								"status":         "False",
								"transitioning":  false,
								"type":           "Ready",
							},
						},
						"connected": false,
						"allocatable": map[string]interface{}{
							"cpu":       "1",
							"memoryRaw": float64(12000000),
							"memory":    "12M",
							"pods":      "1",
						},
						"requested": map[string]interface{}{
							"cpu":       "2",
							"memoryRaw": float64(12582912),
							"memory":    "12Mi",
							"pods":      "2",
						},
					},
				},
			},
		},
		{
			name: "the 'G' in memory fields gets transformed",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"id":   6,
					"type": "management.cattle.io.cluster",
					"metadata": map[string]interface{}{
						"name": "c-m-gortz",
					},
					"spec": map[string]interface{}{
						"displayName": "gortz",
						"internal":    false,
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:17Z",
								"status":         "False",
								"transitioning":  false,
								"type":           "Ready",
							},
						},
						"allocatable": map[string]interface{}{
							"cpu":    "1",
							"memory": "12G",
							"pods":   "1",
						},
						"requested": map[string]interface{}{
							"cpu":    "2",
							"memory": "12Gi",
							"pods":   "2",
						},
					},
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"id":   6,
					"type": "management.cattle.io.cluster",
					"metadata": map[string]interface{}{
						"name": "c-m-gortz",
					},
					"spec": map[string]interface{}{
						"displayName": "gortz",
						"internal":    false,
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:17Z",
								"status":         "False",
								"transitioning":  false,
								"type":           "Ready",
							},
						},
						"connected": false,
						"allocatable": map[string]interface{}{
							"cpu":       "1",
							"memoryRaw": float64(12000000000),
							"memory":    "12G",
							"pods":      "1",
						},
						"requested": map[string]interface{}{
							"cpu":       "2",
							"memoryRaw": float64(12884901888),
							"memory":    "12Gi",
							"pods":      "2",
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TransformManagedCluster(tt.input)
			if tt.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantOutput, got)
			}
			if (err != nil) != tt.wantError {
				t.Errorf("TransformManagedCluster() error = %v, wantErr %v", err, tt.wantError)
				return
			}
			if !reflect.DeepEqual(got, tt.wantOutput) {
				t.Errorf("TransformManagedCluster() got = %v, want %v", got, tt.wantOutput)
			}
		})
	}
}
