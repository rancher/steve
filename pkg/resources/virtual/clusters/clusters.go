package clusters

import (
	"fmt"

	"github.com/sirupsen/logrus"
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
	connectedStatus := false
	conditionsAsArray, ok := conditions.([]interface{})
	if !ok {
		return obj, fmt.Errorf("failed to parse status.conditions as array")
	}
	for i, condition := range conditionsAsArray {
		conditionMap, ok := condition.(map[string]interface{})
		if !ok {
			logrus.Errorf("Failed to process condition %v (%d) as a map", condition, i)
			return obj, fmt.Errorf("failed to parse a condition as a map")
		}
		if conditionMap["type"] == "Ready" && conditionMap["status"] == "True" {
			connectedStatus = true
			break
		}
	}
	err = unstructured.SetNestedField(obj.Object, connectedStatus, "status", "connected")
	if err != nil {
		return obj, err
	}
	name := obj.GetName()
	err = unstructured.SetNestedField(obj.Object, name == "local", "spec", "internal")
	return obj, err
}
