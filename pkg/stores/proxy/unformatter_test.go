package proxy

import (
	"testing"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/stretchr/testify/assert"
)

func Test_unformat(t *testing.T) {
	tests := []struct {
		name string
		obj  types.APIObject
		want types.APIObject
	}{
		{
			name: "noop",
			obj: types.APIObject{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"name": "noop",
					},
				},
			},
			want: types.APIObject{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"name": "noop",
					},
				},
			},
		},
		{
			name: "remove fields",
			obj: types.APIObject{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"name": "foo",
						"fields": []string{
							"name",
							"address",
							"phonenumber",
						},
						"relationships": []map[string]interface{}{
							{
								"toId": "bar",
								"rel":  "uses",
							},
						},
						"state": map[string]interface{}{
							"error": false,
						},
					},
					"status": map[string]interface{}{
						"conditions": []map[string]interface{}{
							{
								"type":           "Ready",
								"status":         "True",
								"lastUpdateTime": "a minute ago",
								"transitioning":  false,
								"error":          false,
							},
							{
								"type":           "Initialized",
								"status":         "True",
								"lastUpdateTime": "yesterday",
								"transitioning":  false,
								"error":          false,
							},
						},
					},
				},
			},
			want: types.APIObject{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"name": "foo",
					},
					"status": map[string]interface{}{
						"conditions": []map[string]interface{}{
							{
								"type":   "Ready",
								"status": "True",
							},
							{
								"type":   "Initialized",
								"status": "True",
							},
						},
					},
				},
			},
		},
		{
			name: "unrecognized object",
			obj: types.APIObject{
				Object: "object",
			},
			want: types.APIObject{
				Object: "object",
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := unformat(test.obj)
			assert.Equal(t, test.want, got)
		})
	}
}
