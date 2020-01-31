package publicapi

import (
	"github.com/gorilla/mux"
	"github.com/rancher/norman/v2/pkg/types"
	"github.com/rancher/steve/pkg/attributes"
	"github.com/rancher/steve/pkg/resources/schema"
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

func apiRoot(sf schema.Factory, apiOp *types.APIRequest) {
	apiOp.Type = "apiRoot"
}
