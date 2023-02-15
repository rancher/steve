package partition_alpha

import (
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/steve/pkg/stores/partition/listprocessor"
	"github.com/rancher/wrangler/pkg/schemas"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/sets"
	"testing"
)

func TestAll(t *testing.T) {
	tests := []struct {
		name           string
		apiOp          *types.APIRequest
		id             string
		schema         *types.APISchema
		wantPartitions []listprocessor.Partition
	}{
		{
			name:  "all passthrough",
			apiOp: &types.APIRequest{},
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"access": accesscontrol.AccessListByVerb{
							"list": accesscontrol.AccessList{
								accesscontrol.Access{
									Namespace:    "*",
									ResourceName: "*",
								},
							},
						},
					},
				},
			},
			wantPartitions: passthroughPartitions,
		},
		{
			name:  "global access for global request",
			apiOp: &types.APIRequest{},
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"access": accesscontrol.AccessListByVerb{
							"list": accesscontrol.AccessList{
								accesscontrol.Access{
									Namespace:    "*",
									ResourceName: "r1",
								},
								accesscontrol.Access{
									Namespace:    "*",
									ResourceName: "r2",
								},
							},
						},
					},
				},
			},
			wantPartitions: []listprocessor.Partition{
				{
					Names: sets.NewString("r1", "r2"),
				},
			},
		},
		{
			name:  "namespace access for global request",
			apiOp: &types.APIRequest{},
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"namespaced": true,
						"access": accesscontrol.AccessListByVerb{
							"list": accesscontrol.AccessList{
								accesscontrol.Access{
									Namespace:    "n1",
									ResourceName: "*",
								},
								accesscontrol.Access{
									Namespace:    "n2",
									ResourceName: "*",
								},
							},
						},
					},
				},
			},
			wantPartitions: []listprocessor.Partition{
				{
					Namespace: "n1",
					All:       true,
				},
				{
					Namespace: "n2",
					All:       true,
				},
			},
		},
		{
			name: "namespace access for namespaced request",
			apiOp: &types.APIRequest{
				Namespace: "n1",
			},
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"namespaced": true,
						"access": accesscontrol.AccessListByVerb{
							"list": accesscontrol.AccessList{
								accesscontrol.Access{
									Namespace:    "n1",
									ResourceName: "*",
								},
							},
						},
					},
				},
			},
			wantPartitions: passthroughPartitions,
		},
		{
			// we still get a partition even if there is no access to it, it will be rejected by the API server later
			name: "namespace access for invalid namespaced request",
			apiOp: &types.APIRequest{
				Namespace: "n2",
			},
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"namespaced": true,
						"access": accesscontrol.AccessListByVerb{
							"list": accesscontrol.AccessList{
								accesscontrol.Access{
									Namespace:    "n1",
									ResourceName: "*",
								},
							},
						},
					},
				},
			},
			wantPartitions: []listprocessor.Partition{
				{
					Namespace: "n2",
				},
			},
		},
		{
			name:  "by names access for global request",
			apiOp: &types.APIRequest{},
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"namespaced": true,
						"access": accesscontrol.AccessListByVerb{
							"list": accesscontrol.AccessList{
								accesscontrol.Access{
									Namespace:    "n1",
									ResourceName: "r1",
								},
								accesscontrol.Access{
									Namespace:    "n1",
									ResourceName: "r2",
								},
								accesscontrol.Access{
									Namespace:    "n2",
									ResourceName: "r1",
								},
							},
						},
					},
				},
			},
			wantPartitions: []listprocessor.Partition{
				{
					Namespace: "n1",
					Names:     sets.NewString("r1", "r2"),
				},
				{
					Namespace: "n2",
					Names:     sets.NewString("r1"),
				},
			},
		},
		{
			name: "by names access for namespaced request",
			apiOp: &types.APIRequest{
				Namespace: "n1",
			},
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"namespaced": true,
						"access": accesscontrol.AccessListByVerb{
							"list": accesscontrol.AccessList{
								accesscontrol.Access{
									Namespace:    "n1",
									ResourceName: "r1",
								},
								accesscontrol.Access{
									Namespace:    "n2",
									ResourceName: "r1",
								},
							},
						},
					},
				},
			},
			wantPartitions: []listprocessor.Partition{
				{
					Namespace: "n1",
					Names:     sets.NewString("r1"),
				},
			},
		},
		{
			name:  "by id",
			apiOp: &types.APIRequest{},
			id:    "n1/r1",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
				},
			},
			wantPartitions: []listprocessor.Partition{
				{
					Namespace: "n1",
					Names:     sets.NewString("r1"),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			partitioner := rbacPartitioner{}
			verb := "list"
			gotPartitions, gotErr := partitioner.All(test.apiOp, test.schema, verb, test.id)
			assert.Nil(t, gotErr)
			assert.Equal(t, test.wantPartitions, gotPartitions)
		})
	}
}
