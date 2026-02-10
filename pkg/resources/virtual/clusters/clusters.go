package clusters

import (
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TransformManagedClusters does special-case handling on <management.cattle.io v3 Cluster>s:
// creates a new virtual `status.connected` boolean field that looks for `type = "Ready"` in any
// of the status.conditions records.
//
// It also converts the annotated status.requested.memory and status.allocatable.memory fields into
// their underlying byte values.

func TransformManagedCluster(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	obj, err := transformManagedClusterConditions(obj)
	if err != nil {
		return obj, err
	}

	obj, err = transformManagedClusterFields(obj)
	if err != nil {
		return obj, err
	}

	return transformManagedClusterQuantities(obj)
}

// Provisioned Cluster priorities
// provider:
// given in md.annotations[ui.rancher/provider]: 1
// given in spec.rkeConfig.machinePools[0].machineConfigRef.kind: 4 (without trailing "Config")

// k8sVersion: if given, has priority 1 (MCIC has priority 2). Lowest priority wins

func TransformProvisionedCluster(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	clusterFields := make(map[string]interface{})
	providerPriority := int64(-1)
	annotations := obj.GetAnnotations()
	provider, ok := annotations["provider.cattle.io"]
	if ok {
		clusterFields["provider"] = provider
		providerPriority = int64(1)
		clusterFields["providerPriority"] = providerPriority
	} else {
		pools, found, err := unstructured.NestedSlice(obj.Object, "spec", "rkeConfig", "machinePools")
		if err != nil {
			return obj, err
		}
		if found && len(pools) > 0 {
			pools0Map, ok := (pools[0]).(map[string]any)
			if ok {
				machineConfigRefRaw, ok := pools0Map["machineConfigRef"]
				if ok {
					machineConfigRef, ok := machineConfigRefRaw.(map[string]interface{})
					if ok {
						kindRaw, ok := machineConfigRef["kind"]
						if ok {
							kind, ok := kindRaw.(string)
							if ok {
								if strings.HasSuffix(strings.ToLower(kind), "config") {
									clusterFields["provider"] = kind[0 : len(kind)-len("config")]
									clusterFields["providerPriority"] = int64(4)
								}
							}
						}
					}
				}
			}
		}
	}
	spec, found, err := unstructured.NestedMap(obj.Object, "spec")
	if err != nil || !found {
		// Doesn't make sense if this fails, but allow for it anyway
		return obj, err
	}
	_, ok = spec["rkeConfig"]
	if ok {
		gitVersion, found, err := unstructured.NestedString(obj.Object, "status", "version", "gitVersion")
		if found && err == nil {
			clusterFields["k8sVersion"] = gitVersion
			clusterFields["k8sVersionPriority"] = int64(1)
		} else {
			k8sVersion, found, err := unstructured.NestedString(spec, "kubernetesVersion")
			if found && err == nil {
				clusterFields["k8sVersion"] = k8sVersion
				clusterFields["k8sVersionPriority"] = int64(1)
			}
		}
	}
	if len(clusterFields) > 0 {
		err = unstructured.SetNestedField(obj.Object, clusterFields, "metadata", "clusterFields")
	}
	return obj, err
}

func transformManagedClusterConditions(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	conditions, ok, err := unstructured.NestedFieldNoCopy(obj.Object, "status", "conditions")
	if err != nil {
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
	for _, condition := range conditionsAsArray {
		conditionMap, ok := condition.(map[string]interface{})
		if !ok {
			return obj, fmt.Errorf("failed to parse a condition (type %t) as a map", condition)
		}
		if conditionMap["type"] == "Ready" && conditionMap["status"] == "True" {
			connectedStatus = true
			break
		}
	}
	err = unstructured.SetNestedField(obj.Object, connectedStatus, "status", "connected")
	return obj, err
}

// Try to determine the provider and k8s version fields from the MCIC
// Each assigned field is assigned a priority.
// The same field might be assigned a value in the MCIC's corresponding PCIC
// We want to use the field with the lowest priority, as determined by the
// conversions here and in transformProvisionedClusterFields
//
// priorities:
// provider:
// hardwired to harvester: 2
// given in `spec.internal`, `status.provider` & status.driver -> 3
//
// k8s versions determined here are priority 2
// k8s versions determined in a PCIC will have priority 1
// lowest priority will "win"
func transformManagedClusterFields(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	labels := obj.GetLabels()
	provider, ok := labels["provider.cattle.io"]
	clusterFields := make(map[string]interface{})
	spec, specFound, err := unstructured.NestedMap(obj.Object, "spec")
	if err != nil {
		return obj, err
	}
	var priority int64 = -1
	if ok && provider == "harvester" {
		clusterFields["provider"] = provider
		priority = 2
		clusterFields["providerPriority"] = priority
	} else if specFound {
		isInternal, ok := spec["internal"].(bool)
		if ok && !isInternal {
			status, statusFound, err := unstructured.NestedMap(obj.Object, "status")
			if err == nil && statusFound {
				statusProvider, providerOk := status["provider"].(string)
				statusDriver, driverOk := status["driver"].(string)
				if providerOk && driverOk && statusProvider == statusDriver {
					clusterFields["provider"] = "imported"
					priority = 3
					clusterFields["providerPriority"] = priority
				}
			}
			if priority == -1 {
				for k, v := range spec {
					if strings.HasSuffix(k, "Config") {
						configBlock, ok := v.(map[string]interface{})
						if ok {
							importedRaw, ok := configBlock["imported"]
							if ok {
								imported, ok := importedRaw.(bool)
								if ok && imported {
									clusterFields["provider"] = "imported"
									priority = 3
									clusterFields["providerPriority"] = priority
									break
								}
							}
						}
					}
				}
			}
			if priority == -1 {
				driver, found, err := unstructured.NestedFieldNoCopy(obj.Object, "status", "driver")
				haveDriver := false
				if err == nil {
					if found {
						haveDriver = driver != nil
					}
				}
				if !haveDriver {
					clusterFields["provider"] = "imported"
					priority = 3
					clusterFields["providerPriority"] = priority
				}
			}
		}
	}
	statusK8sVersion, statusK8sVersionFound, err := unstructured.NestedString(obj.Object, "status", "version", "gitVersion")
	if err != nil {
		return obj, err
	}
	priority = -1
	if statusK8sVersionFound {
		clusterFields["k8sVersion"] = statusK8sVersion
		priority = 2
		clusterFields["k8sVersionPriority"] = priority
	} else if specFound {
		for k, v := range spec {
			if strings.HasSuffix(k, "Config") {
				configBlock, ok := v.(map[string]interface{})
				if ok {
					k8sVersionRaw, ok := configBlock["kubernetesVersion"]
					if ok {
						ks8Version, ok := k8sVersionRaw.(string)
						if ok {
							clusterFields["k8sVersion"] = ks8Version
							priority = 2
							clusterFields["k8sVersionPriority"] = priority
							break
						}
					}
				}
			}
		}
	}
	if priority == -1 {
		clusterFields["k8sVersion"] = "-"
		clusterFields["k8sVersionPriority"] = int64(2)
	}
	err = unstructured.SetNestedField(obj.Object, clusterFields, "metadata", "clusterFields")
	return obj, err
}

func transformManagedClusterQuantities(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	for _, statusName := range []string{"requested", "allocatable"} {
		mapx, ok, err := unstructured.NestedMap(obj.Object, "status", statusName)
		if ok && err == nil {
			mem, ok := mapx["memory"]
			madeChange := false
			if ok {
				quantity, err := resource.ParseQuantity(mem.(string))
				if err == nil {
					mapx["memoryRaw"] = quantity.AsApproximateFloat64()
					madeChange = true
				} else {
					logrus.Errorf("Failed to parse memory quantity <%v>: %v", mem, err)
				}
			}
			cpu, ok := mapx["cpu"]
			if ok {
				quantity, err := resource.ParseQuantity(cpu.(string))
				if err == nil {
					mapx["cpuRaw"] = quantity.AsApproximateFloat64()
					madeChange = true
				} else {
					logrus.Errorf("Failed to parse cpu quantity <%v>: %v", cpu, err)
				}
			}
			if madeChange {
				unstructured.SetNestedMap(obj.Object, mapx, "status", statusName)
			}
		}
	}
	return obj, nil
}
