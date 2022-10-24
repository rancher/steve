// Package listprocessor contains methods for filtering, sorting, and paginating lists of objects.
package listprocessor

import (
	"strconv"
	"strings"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/wrangler/pkg/data"
	"github.com/rancher/wrangler/pkg/data/convert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	defaultLimit  = 100000
	continueParam = "continue"
	limitParam    = "limit"
	filterParam   = "filter"
)

// ListOptions represents the query parameters that may be included in a list request.
type ListOptions struct {
	ChunkSize int
	Resume    string
	Filters   []Filter
}

// Filter represents a field to filter by.
// A subfield in an object is represented in a request query using . notation, e.g. 'metadata.name'.
// The subfield is internally represented as a slice, e.g. [metadata, name].
type Filter struct {
	field []string
	match string
}

// ParseQuery parses the query params of a request and returns a ListOptions.
func ParseQuery(apiOp *types.APIRequest) *ListOptions {
	chunkSize := getLimit(apiOp)
	q := apiOp.Request.URL.Query()
	cont := q.Get(continueParam)
	filterParams := q[filterParam]
	filterOpts := []Filter{}
	for _, filters := range filterParams {
		filter := strings.Split(filters, "=")
		if len(filter) != 2 {
			continue
		}
		filterOpts = append(filterOpts, Filter{field: strings.Split(filter[0], "."), match: filter[1]})
	}
	return &ListOptions{
		ChunkSize: chunkSize,
		Resume:    cont,
		Filters:   filterOpts,
	}
}

// getLimit extracts the limit parameter from the request or sets a default of 100000.
// Since a default is always set, this implies that clients must always be
// aware that the list may be incomplete.
func getLimit(apiOp *types.APIRequest) int {
	limitString := apiOp.Request.URL.Query().Get(limitParam)
	limit, err := strconv.Atoi(limitString)
	if err != nil {
		limit = 0
	}
	if limit <= 0 {
		limit = defaultLimit
	}
	return limit
}

// FilterList accepts a channel of unstructured objects and a slice of filters and returns the filtered list.
// Filters are ANDed together.
func FilterList(list <-chan []unstructured.Unstructured, filters []Filter) []unstructured.Unstructured {
	result := []unstructured.Unstructured{}
	for items := range list {
		for _, item := range items {
			if len(filters) == 0 {
				result = append(result, item)
				continue
			}
			if matchesAll(item.Object, filters) {
				result = append(result, item)
			}
		}
	}
	return result
}

func matchesOne(obj map[string]interface{}, filter Filter) bool {
	var objValue interface{}
	var ok bool
	subField := []string{}
	for !ok && len(filter.field) > 0 {
		objValue, ok = data.GetValue(obj, filter.field...)
		if !ok {
			subField = append(subField, filter.field[len(filter.field)-1])
			filter.field = filter.field[:len(filter.field)-1]
		}
	}
	if !ok {
		return false
	}
	switch typedVal := objValue.(type) {
	case string, int, bool:
		if len(subField) > 0 {
			return false
		}
		stringVal := convert.ToString(typedVal)
		if strings.Contains(stringVal, filter.match) {
			return true
		}
	case []interface{}:
		filter = Filter{field: subField, match: filter.match}
		if matchesAny(typedVal, filter) {
			return true
		}
	}
	return false
}

func matchesAny(obj []interface{}, filter Filter) bool {
	for _, v := range obj {
		switch typedItem := v.(type) {
		case string, int, bool:
			stringVal := convert.ToString(typedItem)
			if strings.Contains(stringVal, filter.match) {
				return true
			}
		case map[string]interface{}:
			if matchesOne(typedItem, filter) {
				return true
			}
		case []interface{}:
			if matchesAny(typedItem, filter) {
				return true
			}
		}
	}
	return false
}

func matchesAll(obj map[string]interface{}, filters []Filter) bool {
	for _, f := range filters {
		if !matchesOne(obj, f) {
			return false
		}
	}
	return true
}
