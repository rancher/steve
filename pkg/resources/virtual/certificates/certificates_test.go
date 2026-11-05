package certificates

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type certTestType struct {
	name           string
	input          any
	wantOutput     any
	wantError      bool
	errorSubstring string
}

func TestTransformCertificate(t *testing.T) {
	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)
	tomorrow := now.AddDate(0, 0, 1)
	tests := []certTestType{
		{
			name: "no conditions",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "cert-manager.io/v1",
					"kind":       "Certificate",
					"metadata": map[string]interface{}{
						"name":      "no conditions",
						"namespace": "fredasFarmNamespace",
					},
					"status": map[string]interface{}{},
					"id":     "fredasFarmNamespace/no conditions",
				},
			},
			wantError:      true,
			errorSubstring: "failed to find status.conditions block in certificate \"no conditions\"",
		},
		{
			name: "cert about to expire",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "cert-manager.io/v1",
					"kind":       "Certificate",
					"metadata": map[string]interface{}{
						"name":      "isGood",
						"namespace": "wallysFarmNamespace",
						"state": map[string]interface{}{
							"name":  "valid",
							"error": false,
						},
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"type":   "Ready",
								"status": "True",
							},
						},
						"notAfter":    tomorrow.Format(time.RFC3339),
						"notBefore":   yesterday.Format(time.RFC3339),
						"renewalTime": tomorrow.Format(time.RFC3339),
					},
					"id": "wallysFarmNamespace/isGood",
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "cert-manager.io/v1",
					"kind":       "Certificate",
					"metadata": map[string]interface{}{
						"name":      "isGood",
						"namespace": "wallysFarmNamespace",
						"state": map[string]interface{}{
							"name":  "active",
							"error": false,
						},
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"type":   "Ready",
								"status": "True",
							},
						},
						"notAfter":    tomorrow.Format(time.RFC3339),
						"notBefore":   yesterday.Format(time.RFC3339),
						"renewalTime": tomorrow.Format(time.RFC3339),
					},
					"id": "wallysFarmNamespace/isGood",
				},
			},
		},
		{
			name: "condition says ready is false",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "cert-manager.io/v1",
					"kind":       "Certificate",
					"metadata": map[string]interface{}{
						"name":      "saysReadyIsFalse",
						"namespace": "wallysFarmNamespace",
						"state": map[string]interface{}{
							"name":  "valid",
							"error": false,
						},
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"reason":        "Ready is 'false'",
								"ready":         false,
								"status":        "True",
								"transitioning": false,
								"type":          "Ready",
							},
						},
					},
					"id": "wallysFarmNamespace/saysReadyIsFalse",
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "cert-manager.io/v1",
					"kind":       "Certificate",
					"metadata": map[string]interface{}{
						"name":      "saysReadyIsFalse",
						"namespace": "wallysFarmNamespace",
						"state": map[string]interface{}{
							"name":  "error",
							"error": false,
						},
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"reason":        "Ready is 'false'",
								"ready":         false,
								"status":        "True",
								"transitioning": false,
								"type":          "Ready",
							},
						},
					},
					"id": "wallysFarmNamespace/saysReadyIsFalse",
				},
			},
		},
		{
			name: "failedIssuanceAttempts > 0 => error",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "cert-manager.io/v1",
					"kind":       "Certificate",
					"metadata": map[string]interface{}{
						"name":      "severalFailedIssuanceAttempts",
						"namespace": "carlsFarmNamespace",
						"state": map[string]interface{}{
							"name":  "valid",
							"error": false,
						},
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"reason":                 "Ready is 'false'",
								"failedIssuanceAttempts": int64(3),
								"status":                 "True",
								"transitioning":          false,
								"type":                   "Ready",
							},
						},
					},
					"id": "carlsFarmNamespace/severalFailedIssuanceAttempts",
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "cert-manager.io/v1",
					"kind":       "Certificate",
					"metadata": map[string]interface{}{
						"name":      "severalFailedIssuanceAttempts",
						"namespace": "carlsFarmNamespace",
						"state": map[string]interface{}{
							"name":  "error",
							"error": false,
						},
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"reason":                 "Ready is 'false'",
								"failedIssuanceAttempts": int64(3),
								"status":                 "True",
								"transitioning":          false,
								"type":                   "Ready",
							},
						},
					},
					"id": "carlsFarmNamespace/severalFailedIssuanceAttempts",
				},
			},
		},
		{
			name: "issuing message => in-progress",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "cert-manager.io/v1",
					"kind":       "Certificate",
					"metadata": map[string]interface{}{
						"name":      "isGood",
						"namespace": "joonsFarmNamespace",
						"state": map[string]interface{}{
							"name":  "valid",
							"error": false,
						},
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":         false,
								"message":       "Issuing certificate as Secret does not exist",
								"reason":        "DoesNotExist",
								"status":        "True",
								"transitioning": false,
								"type":          "Ready",
							},
						},
					},
					"id": "joonsFarmNamespace/isGood",
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "cert-manager.io/v1",
					"kind":       "Certificate",
					"metadata": map[string]interface{}{
						"name":      "isGood",
						"namespace": "joonsFarmNamespace",
						"state": map[string]interface{}{
							"name":  "in-progress",
							"error": false,
						},
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":         false,
								"message":       "Issuing certificate as Secret does not exist",
								"reason":        "DoesNotExist",
								"status":        "True",
								"transitioning": false,
								"type":          "Ready",
							},
						},
					},
					"id": "joonsFarmNamespace/isGood",
				},
			},
		},
		{
			name: "is good, no timestamps",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "cert-manager.io/v1",
					"kind":       "Certificate",
					"metadata": map[string]interface{}{
						"name":      "isGood",
						"namespace": "joonsFarmNamespace",
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":         false,
								"message":       "Ready",
								"status":        "True",
								"transitioning": false,
								"type":          "Ready", // this is a lie
							},
						},
					},
					"id": "joonsFarmNamespace/isGood",
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "cert-manager.io/v1",
					"kind":       "Certificate",
					"metadata": map[string]interface{}{
						"name":      "isGood",
						"namespace": "joonsFarmNamespace",
						"state": map[string]interface{}{
							"name":  "active",
							"error": false,
						},
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":         false,
								"message":       "Ready",
								"status":        "True",
								"transitioning": false,
								"type":          "Ready", // this is a lie
							},
						},
					},
					"id": "joonsFarmNamespace/isGood",
				},
			},
		},
		{
			name: "has expired",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "cert-manager.io/v1",
					"kind":       "Certificate",
					"metadata": map[string]interface{}{
						"name":      "hasExprired",
						"namespace": "beakersFarmNamespace",
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":         false,
								"message":       "Ready",
								"status":        "True",
								"transitioning": false,
								"type":          "Ready", // this is a lie
							},
						},
						"notAfter": yesterday.Format(time.RFC3339),
					},
					"id": "beakersFarmNamespace/hasExprired",
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "cert-manager.io/v1",
					"kind":       "Certificate",
					"metadata": map[string]interface{}{
						"name":      "hasExprired",
						"namespace": "beakersFarmNamespace",
						"state": map[string]interface{}{
							"name":  "expired",
							"error": true,
						},
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":         false,
								"message":       "Ready",
								"status":        "True",
								"transitioning": false,
								"type":          "Ready", // this is a lie
							},
						},
						"notAfter": yesterday.Format(time.RFC3339),
					},
					"id": "beakersFarmNamespace/hasExprired",
				},
			},
		},
		{
			name: "is about to expire",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "cert-manager.io/v1",
					"kind":       "Certificate",
					"metadata": map[string]interface{}{
						"name":      "willExpire",
						"namespace": "beakersFarmNamespace",
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":         false,
								"message":       "Ready",
								"status":        "True",
								"transitioning": false,
								"type":          "Ready", // this is a lie
							},
						},
						"renewalTime": yesterday.Format(time.RFC3339),
						"notAfter":    tomorrow.Format(time.RFC3339),
					},
					"id": "beakersFarmNamespace/willExpire",
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "cert-manager.io/v1",
					"kind":       "Certificate",
					"metadata": map[string]interface{}{
						"name":      "willExpire",
						"namespace": "beakersFarmNamespace",
						"state": map[string]interface{}{
							"name":  "expiring",
							"error": false,
						},
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":         false,
								"message":       "Ready",
								"status":        "True",
								"transitioning": false,
								"type":          "Ready", // this is a lie
							},
						},
						"renewalTime": yesterday.Format(time.RFC3339),
						"notAfter":    tomorrow.Format(time.RFC3339),
					},
					"id": "beakersFarmNamespace/willExpire",
				},
			},
		},
		{
			name: "looks good, but lastCondition.status is false => error",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "cert-manager.io/v1",
					"kind":       "Certificate",
					"metadata": map[string]interface{}{
						"name":      "falseStatus",
						"namespace": "oliviasPoolNamespace",
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":         false,
								"message":       "Ready",
								"status":        "False",
								"transitioning": false,
								"type":          "Ready", // this is a lie
							},
						},
					},
					"id": "oliviasPoolNamespace/falseStatus",
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "cert-manager.io/v1",
					"kind":       "Certificate",
					"metadata": map[string]interface{}{
						"name":      "falseStatus",
						"namespace": "oliviasPoolNamespace",
						"state": map[string]interface{}{
							"name":  "error",
							"error": true,
						},
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":         false,
								"message":       "Ready",
								"status":        "False",
								"transitioning": false,
								"type":          "Ready", // this is a lie
							},
						},
					},
					"id": "oliviasPoolNamespace/falseStatus",
				},
			},
		},
		{
			name: "looks good, but lastCondition.status is unknown => pending",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "cert-manager.io/v1",
					"kind":       "Certificate",
					"metadata": map[string]interface{}{
						"name":      "unknownStatus",
						"namespace": "geraldsHouseNamespace",
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":         false,
								"message":       "Ready",
								"status":        "Unknown",
								"transitioning": false,
								"type":          "Ready", // this is a lie
							},
						},
					},
					"id": "geraldsHouseNamespace/unknownStatus",
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "cert-manager.io/v1",
					"kind":       "Certificate",
					"metadata": map[string]interface{}{
						"name":      "unknownStatus",
						"namespace": "geraldsHouseNamespace",
						"state": map[string]interface{}{
							"name":  "pending",
							"error": false,
						},
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":         false,
								"message":       "Ready",
								"status":        "Unknown",
								"transitioning": false,
								"type":          "Ready", // this is a lie
							},
						},
					},
					"id": "geraldsHouseNamespace/unknownStatus",
				},
			},
		},
		{
			name: "looks good, but lastCondition.status is missing => pending",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "cert-manager.io/v1",
					"kind":       "Certificate",
					"metadata": map[string]interface{}{
						"name":      "missingStatus",
						"namespace": "paisPlateNamespace",
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":         false,
								"message":       "Ready",
								"transitioning": false,
								"type":          "Ready", // this is a lie
							},
						},
					},
					"id": "paisPlateNamespace/missingStatus",
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "cert-manager.io/v1",
					"kind":       "Certificate",
					"metadata": map[string]interface{}{
						"name":      "missingStatus",
						"namespace": "paisPlateNamespace",
						"state": map[string]interface{}{
							"name":  "pending",
							"error": false,
						},
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":         false,
								"message":       "Ready",
								"transitioning": false,
								"type":          "Ready", // this is a lie
							},
						},
					},
					"id": "paisPlateNamespace/missingStatus",
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output interface{}
			var err error
			raw, ok := test.input.(*unstructured.Unstructured)
			if ok && raw.GetKind() == "Certificate" && raw.GetAPIVersion() == "cert-manager.io/v1" {
				output, err = TransformCertificate(raw)
			} else {
				output = raw
				err = nil
			}
			if test.wantError {
				require.Error(t, err)
				if test.errorSubstring != "" {
					require.Contains(t, err.Error(), test.errorSubstring)
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, test.wantOutput, output)
			}
		})
	}
}
