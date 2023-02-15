// Package listprocessor contains methods for filtering, sorting, and paginating lists of objects.
package listprocessor

import (
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"

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
	sortParam     = "sort"
	pageSizeParam = "pagesize"
	pageParam     = "page"
	revisionParam = "revision"
)

// ListOptions represents the query parameters that may be included in a list request.
type ListOptions struct {
	ChunkSize  int
	Resume     string
	Filters    []Filter
	Sort       Sort
	Pagination Pagination
	Revision   string
}

// Partition represents filtering of a request's results by namespace or a list of resource names
type Partition struct {
	Namespace   string
	All         bool
	Passthrough bool
	Names       sets.String
}

// Filter represents a field to filter by.
// A subfield in an object is represented in a request query using . notation, e.g. 'metadata.name'.
// The subfield is internally represented as a slice, e.g. [metadata, name].
type Filter struct {
	Field []string
	Match string
}

// String returns the filter as a query string.
func (f Filter) String() string {
	field := strings.Join(f.Field, ".")
	return field + "=" + f.Match
}

// SortOrder represents whether the list should be ascending or descending.
type SortOrder int

const (
	// ASC stands for ascending order.
	ASC SortOrder = iota
	// DESC stands for descending (reverse) order.
	DESC
)

// Sort represents the criteria to sort on.
// The subfield to sort by is represented in a request query using . notation, e.g. 'metadata.name'.
// The subfield is internally represented as a slice, e.g. [metadata, name].
// The order is represented by prefixing the sort key by '-', e.g. sort=-metadata.name.
type Sort struct {
	PrimaryField   []string
	SecondaryField []string
	PrimaryOrder   SortOrder
	SecondaryOrder SortOrder
}

// String returns the sort parameters as a query string.
func (s Sort) String() string {
	field := ""
	if s.PrimaryOrder == DESC {
		field = "-" + field
	}
	field += strings.Join(s.PrimaryField, ".")
	if len(s.SecondaryField) > 0 {
		field += ","
		if s.SecondaryOrder == DESC {
			field += "-"
		}
		field += strings.Join(s.SecondaryField, ".")
	}
	return field
}

// Pagination represents how to return paginated results.
type Pagination struct {
	PageSize int
	Page     int
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
		filterOpts = append(filterOpts, Filter{Field: strings.Split(filter[0], "."), Match: filter[1]})
	}
	// sort the filter fields so they can be used as a cache key in the store
	sort.Slice(filterOpts, func(i, j int) bool {
		fieldI := strings.Join(filterOpts[i].Field, ".")
		fieldJ := strings.Join(filterOpts[j].Field, ".")
		return fieldI < fieldJ
	})
	sortOpts := Sort{}
	sortKeys := q.Get(sortParam)
	if sortKeys != "" {
		sortParts := strings.SplitN(sortKeys, ",", 2)
		primaryField := sortParts[0]
		if primaryField != "" && primaryField[0] == '-' {
			sortOpts.PrimaryOrder = DESC
			primaryField = primaryField[1:]
		}
		if primaryField != "" {
			sortOpts.PrimaryField = strings.Split(primaryField, ".")
		}
		if len(sortParts) > 1 {
			secondaryField := sortParts[1]
			if secondaryField != "" && secondaryField[0] == '-' {
				sortOpts.SecondaryOrder = DESC
				secondaryField = secondaryField[1:]
			}
			if secondaryField != "" {
				sortOpts.SecondaryField = strings.Split(secondaryField, ".")
			}
		}
	}
	var err error
	pagination := Pagination{}
	pagination.PageSize, err = strconv.Atoi(q.Get(pageSizeParam))
	if err != nil {
		pagination.PageSize = 0
	}
	pagination.Page, err = strconv.Atoi(q.Get(pageParam))
	if err != nil {
		pagination.Page = 1
	}
	revision := q.Get(revisionParam)
	return &ListOptions{
		ChunkSize:  chunkSize,
		Resume:     cont,
		Filters:    filterOpts,
		Sort:       sortOpts,
		Pagination: pagination,
		Revision:   revision,
	}
}

// getLimit extracts the limit parameter from the request or sets a default of 100000.
// The default limit can be explicitly disabled by setting it to zero or negative.
// If the default is accepted, clients must be aware that the list may be incomplete, and use the "continue" token to get the next chunk of results.
func getLimit(apiOp *types.APIRequest) int {
	limitString := apiOp.Request.URL.Query().Get(limitParam)
	limit, err := strconv.Atoi(limitString)
	if err != nil {
		limit = defaultLimit
	}
	return limit
}

// ToList accepts a channel of unstructured objects and returns its contents as a list.
func ToList(list <-chan []unstructured.Unstructured) []unstructured.Unstructured {
	result := []unstructured.Unstructured{}
	for items := range list {
		for _, item := range items {
			result = append(result, item)
			continue
		}
	}
	return result
}

func matchesOne(obj map[string]interface{}, filter Filter) bool {
	var objValue interface{}
	var ok bool
	subField := []string{}
	for !ok && len(filter.Field) > 0 {
		objValue, ok = data.GetValue(obj, filter.Field...)
		if !ok {
			subField = append(subField, filter.Field[len(filter.Field)-1])
			filter.Field = filter.Field[:len(filter.Field)-1]
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
		if strings.Contains(stringVal, filter.Match) {
			return true
		}
	case []interface{}:
		filter = Filter{Field: subField, Match: filter.Match}
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
			if strings.Contains(stringVal, filter.Match) {
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