package clusters

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TransformManagedClusters does special-case handling on <management.cattle.io v3 Cluster>s:
// creates a new virtual `status.connected` boolean field that looks for `type = "Ready"` in any
// of the status.conditions records.
//
// It also converts the annotated status.requested.memory and status.allocatable.memory fields into
// their underlying byte values.

func TransformManagedCluster(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
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

	for _, statusName := range []string{"requested", "allocatable"} {
		mapx, ok, err := unstructured.NestedMap(obj.Object, "status", statusName)
		if ok && err == nil {
			mem, ok := mapx["memory"]
			if ok {
				memoryAsBytes, err := fixMemory(mem.(string))
				if err != nil {
					return obj, err
				}
				mapx["memoryRaw"] = memoryAsBytes
				unstructured.SetNestedMap(obj.Object, mapx, "status", statusName)
			}
		}
	}
	return obj, nil
}

func fixMemory(memory string) (float64, error) {
	rx := `^([0-9]+)(\w{0,2})$`
	ptn := regexp.MustCompile(rx)
	m := ptn.FindStringSubmatch(memory)
	if m == nil || len(m) != 3 {
		return 0, fmt.Errorf("couldn't parse '%s' as a numeric value", memory)
	}
	tbl := map[string]int{
		"B": 0,
		"K": 1,
		"M": 2,
		"G": 3,
		"T": 4,
		"E": 5,
	}
	size, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, fmt.Errorf("couldn't parse '%s' as a numeric value: %w", memory, err)
	}
	factor := 0
	base := 1000
	var finalError error
	if len(m[2]) > 0 {
		var ok bool
		factor, ok = tbl[strings.ToUpper(m[2][0:1])]
		if !ok {
			factor = 0
		}
		if len(m[2]) > 2 {
			finalError = fmt.Errorf("numeric value '%s' has an unrecognized suffix '%s'", memory, m[2])
		} else if len(m[2]) == 2 {
			if strings.ToUpper(m[2][1:2]) == "I" {
				base = 1024
			} else {
				finalError = fmt.Errorf("numeric value '%s' has an unrecognized suffix '%s'", memory, m[2])
			}
		}
	}
	return float64(size) * math.Pow(float64(base), float64(factor)), finalError
}
