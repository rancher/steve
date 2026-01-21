package restarts

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"time"

	rescommon "github.com/rancher/steve/pkg/resources/common"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var Now = time.Now

// Pattern: "4 (3h38m ago)" or just "0"
var restartsPattern = regexp.MustCompile(`^(\d+)(?:\s+\((.+?)\s+ago\))?$`)

type Converter struct {
	GVK     schema.GroupVersionKind
	Columns []rescommon.ColumnDefinition
}

func (c *Converter) Transform(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	fields, got, err := unstructured.NestedSlice(obj.Object, "metadata", "fields")
	if err != nil || !got {
		return obj, err
	}

	updated := false

	for _, col := range c.Columns {
		if !c.isRestartsColumn(col) {
			continue
		}

		index := rescommon.GetIndexValueFromString(col.Field)
		if index == -1 {
			continue
		}

		if index >= len(fields) || fields[index] == nil {
			continue
		}

		value, ok := fields[index].(string)
		if !ok {
			continue
		}

		// Parse "4 (3h38m ago)" -> [4, timestamp_ms]
		jsonArray, err := parseRestarts(value)
		if err != nil {
			logrus.Errorf("failed to parse restarts value %q: %v", value, err)
			continue
		}

		// Convert json.RawMessage to []interface{} for unstructured
		var arrayValue []interface{}
		if err := json.Unmarshal(jsonArray, &arrayValue); err != nil {
			logrus.Errorf("failed to unmarshal restarts json: %v", err)
			continue
		}

		fields[index] = arrayValue
		updated = true
	}

	if updated {
		if err := unstructured.SetNestedSlice(obj.Object, fields, "metadata", "fields"); err != nil {
			return obj, err
		}
	}

	return obj, nil
}

func (c *Converter) isRestartsColumn(col rescommon.ColumnDefinition) bool {
	return col.Name == "Restarts" && c.GVK.Kind == "Pod" && c.GVK.Group == "" && c.GVK.Version == "v1"
}

func parseRestarts(value string) (json.RawMessage, error) {
	matches := restartsPattern.FindStringSubmatch(value)
	if matches == nil {
		return nil, fmt.Errorf("invalid format: %q", value)
	}

	count, _ := strconv.Atoi(matches[1])

	var timestamp interface{} = nil
	if matches[2] != "" {
		// Parse duration like "3h38m"
		dur, err := rescommon.ParseHumanReadableDuration(matches[2])
		if err == nil {
			timestamp = Now().Add(-dur).UnixMilli()
		}
	}

	result := []interface{}{count, timestamp}
	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}

	return json.RawMessage(jsonBytes), nil
}
