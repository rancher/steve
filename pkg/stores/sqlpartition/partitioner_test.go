package sqlpartition

import (
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/lasso/pkg/cache/sql/partition"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/wrangler/v3/pkg/schemas"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/sets"
)

func TestVerbList(t *testing.T) {
	tests := []struct {
		name           string
		apiOp          *types.APIRequest
		id             string
		schema         *types.APISchema
		wantPartitions []partition.Partition
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
			wantPartitions: []partition.Partition{
				{
					Names: sets.New[string]("r1", "r2"),
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
			wantPartitions: []partition.Partition{
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
			wantPartitions: []partition.Partition{
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
			wantPartitions: []partition.Partition{
				{
					Namespace: "n1",
					Names:     sets.New[string]("r1", "r2"),
				},
				{
					Namespace: "n2",
					Names:     sets.New[string]("r1"),
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
			wantPartitions: []partition.Partition{
				{
					Namespace: "n1",
					Names:     sets.New[string]("r1"),
				},
			},
		},
		{
			name:  "by id fully unauthorized",
			apiOp: &types.APIRequest{},
			id:    "n1/r1",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
				},
			},
			wantPartitions: nil,
		},
		{
			name:  "by id missing namespace",
			apiOp: &types.APIRequest{},
			id:    "n1/r1",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"namespaced": true,
						"access": accesscontrol.AccessListByVerb{
							"list": accesscontrol.AccessList{
								accesscontrol.Access{
									Namespace:    "n2",
									ResourceName: "r2",
								},
							},
						},
					},
				},
			},
			wantPartitions: nil,
		},
		{
			name:  "by id missing resource",
			apiOp: &types.APIRequest{},
			id:    "n1/r1",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"namespaced": true,
						"access": accesscontrol.AccessListByVerb{
							"list": accesscontrol.AccessList{
								accesscontrol.Access{
									Namespace:    "n1",
									ResourceName: "r2",
								},
							},
						},
					},
				},
			},
			wantPartitions: nil,
		},
		{
			name:  "by id authorized by name",
			apiOp: &types.APIRequest{},
			id:    "n1/r1",
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
							},
						},
					},
				},
			},
			wantPartitions: []partition.Partition{
				{
					Namespace: "n1",
					Names:     sets.New[string]("r1"),
				},
			},
		},
		{
			name: "by id authorized by namespace",
			apiOp: &types.APIRequest{
				Namespace: "n1",
			},
			id: "n1/r1",
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
			wantPartitions: []partition.Partition{
				{
					Namespace: "n1",
					All:       false,
					Names:     sets.New[string]("r1"),
				},
			},
		},
		{
			name: "by namespaced id authorized by name",
			apiOp: &types.APIRequest{
				Namespace: "n1",
			},
			id: "r1",
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
							},
						},
					},
				},
			},
			wantPartitions: []partition.Partition{
				{
					Namespace: "n1",
					All:       false,
					Names:     sets.New[string]("r1"),
				},
			},
		},
		{
			name:  "by id ignores unrequested resources",
			apiOp: &types.APIRequest{},
			id:    "n1/r1",
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
							},
						},
					},
				},
			},
			wantPartitions: []partition.Partition{
				{
					Namespace: "n1",
					Names:     sets.New[string]("r1"),
				},
			},
		},
		// Note: this is deprecated fallback behavior. When we remove the behavior,
		// rewrite this test to expect an error instead.
		{
			name: "by id prefers id embedded namespace",
			apiOp: &types.APIRequest{
				Namespace: "n2",
			},
			id: "n1/r1",
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
									ResourceName: "r2",
								},
							},
						},
					},
				},
			},
			wantPartitions: []partition.Partition{
				{
					Namespace: "n1",
					Names:     sets.New[string]("r1"),
				},
			},
		},
		{
			name:  "cluster scoped id unauthorized",
			apiOp: &types.APIRequest{},
			id:    "c1",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
				},
			},
			wantPartitions: nil,
		},
		{
			name:  "cluster scoped id authorized by name",
			apiOp: &types.APIRequest{},
			id:    "c1",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"namespaced": false,
						"access": accesscontrol.AccessListByVerb{
							"list": accesscontrol.AccessList{
								accesscontrol.Access{
									Namespace:    "*",
									ResourceName: "c1",
								},
							},
						},
					},
				},
			},
			wantPartitions: []partition.Partition{
				{
					Namespace: "",
					Names:     sets.New[string]("c1"),
				},
			},
		},
		{
			name:  "cluster scoped id authorized globally",
			apiOp: &types.APIRequest{},
			id:    "c1",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"namespaced": false,
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
			wantPartitions: []partition.Partition{
				{
					Namespace: "",
					Names:     sets.New[string]("c1"),
				},
			},
		},
		{
			name:  "cluster scoped id ignores unrequested resources",
			apiOp: &types.APIRequest{},
			id:    "c1",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"namespaced": false,
						"access": accesscontrol.AccessListByVerb{
							"list": accesscontrol.AccessList{
								accesscontrol.Access{
									Namespace:    "*",
									ResourceName: "c1",
								},
								accesscontrol.Access{
									Namespace:    "*",
									ResourceName: "c2",
								},
							},
						},
					},
				},
			},
			wantPartitions: []partition.Partition{
				{
					Namespace: "",
					Names:     sets.New[string]("c1"),
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

func TestVerbWatch(t *testing.T) {
	tests := []struct {
		name           string
		apiOp          *types.APIRequest
		id             string
		schema         *types.APISchema
		wantPartitions []partition.Partition
	}{
		{
			name:  "by id fully unauthorized",
			apiOp: &types.APIRequest{},
			id:    "n1/r1",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
				},
			},
			wantPartitions: nil,
		},
		{
			name:  "by id missing namespace",
			apiOp: &types.APIRequest{},
			id:    "n1/r1",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"namespaced": true,
						"access": accesscontrol.AccessListByVerb{
							"watch": accesscontrol.AccessList{
								accesscontrol.Access{
									Namespace:    "n2",
									ResourceName: "r2",
								},
							},
						},
					},
				},
			},
			wantPartitions: nil,
		},
		{
			name:  "by id missing resource",
			apiOp: &types.APIRequest{},
			id:    "n1/r1",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"namespaced": true,
						"access": accesscontrol.AccessListByVerb{
							"watch": accesscontrol.AccessList{
								accesscontrol.Access{
									Namespace:    "n1",
									ResourceName: "r2",
								},
							},
						},
					},
				},
			},
			wantPartitions: nil,
		},
		{
			name:  "by id authorized by name",
			apiOp: &types.APIRequest{},
			id:    "n1/r1",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"namespaced": true,
						"access": accesscontrol.AccessListByVerb{
							"watch": accesscontrol.AccessList{
								accesscontrol.Access{
									Namespace:    "n1",
									ResourceName: "r1",
								},
							},
						},
					},
				},
			},
			wantPartitions: []partition.Partition{
				{
					Namespace: "n1",
					Names:     sets.New[string]("r1"),
				},
			},
		},
		{
			name: "by id authorized by namespace",
			apiOp: &types.APIRequest{
				Namespace: "n1",
			},
			id: "n1/r1",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"namespaced": true,
						"access": accesscontrol.AccessListByVerb{
							"watch": accesscontrol.AccessList{
								accesscontrol.Access{
									Namespace:    "n1",
									ResourceName: "*",
								},
							},
						},
					},
				},
			},
			wantPartitions: []partition.Partition{
				{
					Namespace: "n1",
					All:       false,
					Names:     sets.New[string]("r1"),
				},
			},
		},
		{
			name: "by namespaced id authorized by name",
			apiOp: &types.APIRequest{
				Namespace: "n1",
			},
			id: "r1",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"namespaced": true,
						"access": accesscontrol.AccessListByVerb{
							"watch": accesscontrol.AccessList{
								accesscontrol.Access{
									Namespace:    "n1",
									ResourceName: "r1",
								},
							},
						},
					},
				},
			},
			wantPartitions: []partition.Partition{
				{
					Namespace: "n1",
					All:       false,
					Names:     sets.New[string]("r1"),
				},
			},
		},
		{
			name:  "by id ignores unrequested resources",
			apiOp: &types.APIRequest{},
			id:    "n1/r1",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"namespaced": true,
						"access": accesscontrol.AccessListByVerb{
							"watch": accesscontrol.AccessList{
								accesscontrol.Access{
									Namespace:    "n1",
									ResourceName: "r1",
								},
								accesscontrol.Access{
									Namespace:    "n1",
									ResourceName: "r2",
								},
							},
						},
					},
				},
			},
			wantPartitions: []partition.Partition{
				{
					Namespace: "n1",
					Names:     sets.New[string]("r1"),
				},
			},
		},
		// Note: this is deprecated fallback behavior. When we remove the behavior,
		// rewrite this test to expect an error instead.
		{
			name: "by id prefers id embedded namespace",
			apiOp: &types.APIRequest{
				Namespace: "n2",
			},
			id: "n1/r1",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"namespaced": true,
						"access": accesscontrol.AccessListByVerb{
							"watch": accesscontrol.AccessList{
								accesscontrol.Access{
									Namespace:    "n1",
									ResourceName: "r1",
								},
								accesscontrol.Access{
									Namespace:    "n2",
									ResourceName: "r2",
								},
							},
						},
					},
				},
			},
			wantPartitions: []partition.Partition{
				{
					Namespace: "n1",
					Names:     sets.New[string]("r1"),
				},
			},
		},
		{
			name:  "cluster scoped id unauthorized",
			apiOp: &types.APIRequest{},
			id:    "c1",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
				},
			},
			wantPartitions: nil,
		},
		{
			name:  "cluster scoped id authorized by name",
			apiOp: &types.APIRequest{},
			id:    "c1",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"namespaced": false,
						"access": accesscontrol.AccessListByVerb{
							"watch": accesscontrol.AccessList{
								accesscontrol.Access{
									Namespace:    "*",
									ResourceName: "c1",
								},
							},
						},
					},
				},
			},
			wantPartitions: []partition.Partition{
				{
					Namespace: "",
					Names:     sets.New[string]("c1"),
				},
			},
		},
		{
			name:  "cluster scoped id authorized globally",
			apiOp: &types.APIRequest{},
			id:    "c1",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"namespaced": false,
						"access": accesscontrol.AccessListByVerb{
							"watch": accesscontrol.AccessList{
								accesscontrol.Access{
									Namespace:    "*",
									ResourceName: "*",
								},
							},
						},
					},
				},
			},
			wantPartitions: []partition.Partition{
				{
					Namespace: "",
					Names:     sets.New[string]("c1"),
				},
			},
		},
		{
			name:  "cluster scoped id ignores unrequested resources",
			apiOp: &types.APIRequest{},
			id:    "c1",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "foo",
					Attributes: map[string]interface{}{
						"namespaced": false,
						"access": accesscontrol.AccessListByVerb{
							"watch": accesscontrol.AccessList{
								accesscontrol.Access{
									Namespace:    "*",
									ResourceName: "c1",
								},
								accesscontrol.Access{
									Namespace:    "*",
									ResourceName: "c2",
								},
							},
						},
					},
				},
			},
			wantPartitions: []partition.Partition{
				{
					Namespace: "",
					Names:     sets.New[string]("c1"),
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			partitioner := rbacPartitioner{}
			verb := "watch"
			gotPartitions, gotErr := partitioner.All(test.apiOp, test.schema, verb, test.id)
			assert.Nil(t, gotErr)
			assert.Equal(t, test.wantPartitions, gotPartitions)
		})
	}
}

func TestStore(t *testing.T) {
	expectedStore := NewMockUnstructuredStore(gomock.NewController(t))
	rp := rbacPartitioner{
		proxyStore: expectedStore,
	}
	store := rp.Store()
	assert.Equal(t, expectedStore, store)
}
