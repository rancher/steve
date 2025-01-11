package clusters

import (
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TransformManagedClusters does special-case handling on <management.cattle.io v3 Cluster>s
// create a new virtual `status.ready` booean field that looks for `type = "Ready"` in any
// of the status.conditions records.

func TransformManagedCluster(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	conditions, ok, err := unstructured.NestedFieldNoCopy(obj.Object, "status", "conditions")
	if err != nil {
		//logrus.Errorf("Failed to find status.conditions in cluster %s: %v", obj.GetName(), obj.Object)
		return obj, err
	}
	if !ok {
		return obj, fmt.Errorf("failed to find status.conditions block in cluster %s", obj.GetName())
	}
	readyStatus := false
	for _, condition := range conditions.([]map[string]interface{}) {
		if condition["type"] == "Ready" {
			readyStatus = true
			break
		}
	}
	err = unstructured.SetNestedField(obj.Object, readyStatus, "status", "ready")
	return obj, err
}
