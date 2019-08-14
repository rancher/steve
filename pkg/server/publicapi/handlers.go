package publicapi

import (
	"github.com/gorilla/mux"
	"github.com/rancher/naok/pkg/attributes"
	"github.com/rancher/naok/pkg/resources/schema"
	"github.com/rancher/norman/pkg/types"
	runtimeschema "k8s.io/apimachinery/pkg/runtime/schema"
)

func k8sAPI(sf schema.Factory, apiOp *types.APIRequest) {
	vars := mux.Vars(apiOp.Request)
	group := vars["group"]
	if group == "core" {
		group = ""
	}

	apiOp.Name = vars["name"]
	apiOp.Type = sf.ByGVR(runtimeschema.GroupVersionResource{
		Version:  vars["version"],
		Group:    group,
		Resource: vars["resource"],
	})

	nOrN := vars["nameorns"]
	if nOrN != "" {
		schema := apiOp.Schemas.Schema(apiOp.Type)
		if attributes.Namespaced(schema) {
			vars["namespace"] = nOrN
		} else {
			vars["name"] = nOrN
		}
	}

	if namespace := vars["namespace"]; namespace != "" {
		apiOp.Namespaces = []string{namespace}
	}
}
