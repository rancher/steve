package partition

import (
	"fmt"
	"sort"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/steve/pkg/attributes"
	"github.com/rancher/steve/pkg/stores/partition/listprocessor"
	"github.com/rancher/steve/pkg/stores/proxy"
	"github.com/rancher/wrangler/pkg/kv"
	"k8s.io/apimachinery/pkg/util/sets"
)

var (
	passthroughPartitions = []listprocessor.Partition{
		{Passthrough: true},
	}
)

// rbacPartitioner is an implementation of the partition.Partitioner interface.
type rbacPartitioner struct {
	proxyStore *proxy.Store
}

// Lookup returns the default passthrough partition which is used only for retrieving single resources.
// Listing or watching resources require custom partitions.
func (p *rbacPartitioner) Lookup(apiOp *types.APIRequest, schema *types.APISchema, verb, id string) (listprocessor.Partition, error) {
	switch verb {
	case "create":
		fallthrough
	case "get":
		fallthrough
	case "update":
		fallthrough
	case "delete":
		return passthroughPartitions[0], nil
	default:
		return listprocessor.Partition{}, fmt.Errorf("partition list: invalid verb %s", verb)
	}
}

// All returns a slice of partitions applicable to the API schema and the user's access level.
// For watching individual resources or for blanket access permissions, it returns the passthrough partition.
// For more granular permissions, it returns a slice of partitions matching an allowed namespace or resource names.
func (p *rbacPartitioner) All(apiOp *types.APIRequest, schema *types.APISchema, verb, id string) ([]listprocessor.Partition, error) {
	switch verb {
	case "list":
		fallthrough
	case "watch":
		if id != "" {
			ns, name := kv.RSplit(id, "/")
			return []listprocessor.Partition{
				{
					Namespace:   ns,
					All:         false,
					Passthrough: false,
					Names:       sets.NewString(name),
				},
			}, nil
		}
		partitions, passthrough := isPassthrough(apiOp, schema, verb)
		if passthrough {
			return passthroughPartitions, nil
		}
		sort.Slice(partitions, func(i, j int) bool {
			return partitions[i].Namespace < partitions[j].Namespace
		})
		return partitions, nil
	default:
		return nil, fmt.Errorf("parition all: invalid verb %s", verb)
	}
}

// Store returns an UnstructuredStore suited to listing and watching resources by partition.
func (p *rbacPartitioner) Store() UnstructuredStore {
	return p.proxyStore
}

// isPassthrough determines whether a request can be passed through directly to the underlying store
// or if the results need to be partitioned by namespace and name based on the requester's access.
func isPassthrough(apiOp *types.APIRequest, schema *types.APISchema, verb string) ([]listprocessor.Partition, bool) {
	accessListByVerb, _ := attributes.Access(schema).(accesscontrol.AccessListByVerb)
	if accessListByVerb.All(verb) {
		return nil, true
	}

	resources := accessListByVerb.Granted(verb)
	if apiOp.Namespace != "" {
		if resources[apiOp.Namespace].All {
			return nil, true
		}
		return []listprocessor.Partition{
			{
				Namespace: apiOp.Namespace,
				Names:     resources[apiOp.Namespace].Names,
			},
		}, false
	}

	var result []listprocessor.Partition

	if attributes.Namespaced(schema) {
		for k, v := range resources {
			result = append(result, listprocessor.Partition{
				Namespace: k,
				All:       v.All,
				Names:     v.Names,
			})
		}
	} else {
		for _, v := range resources {
			result = append(result, listprocessor.Partition{
				All:   v.All,
				Names: v.Names,
			})
		}
	}

	return result, false
}
