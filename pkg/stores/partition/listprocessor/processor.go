// Package listprocessor contains methods for filtering, sorting, and paginating lists of objects.
package listprocessor

import (
	"sort"
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

// Filter represents a field to filter by.
// A subfield in an object is represented in a request query using . notation, e.g. 'metadata.name'.
// The subfield is internally represented as a slice, e.g. [metadata, name].
type Filter struct {
	field []string
	match string
}

// String returns the filter as a query string.
func (f Filter) String() string {
	field := strings.Join(f.field, ".")
	return field + "=" + f.match
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
	field []string
	order SortOrder
}

// String returns the sort parameters as a query string.
func (s Sort) String() string {
	field := strings.Join(s.field, ".")
	if s.order == DESC {
		field = "-" + field
	}
	return field
}

// Pagination represents how to return paginated results.
type Pagination struct {
	pageSize int
	page     int
}

// PageSize returns the integer page size.
func (p Pagination) PageSize() int {
	return p.pageSize
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
	// sort the filter fields so they can be used as a cache key in the store
	sort.Slice(filterOpts, func(i, j int) bool {
		fieldI := strings.Join(filterOpts[i].field, ".")
		fieldJ := strings.Join(filterOpts[j].field, ".")
		return fieldI < fieldJ
	})
	sortOpts := Sort{}
	sortKey := q.Get(sortParam)
	if sortKey != "" && sortKey[0] == '-' {
		sortOpts.order = DESC
		sortKey = sortKey[1:]
	}
	if sortKey != "" {
		sortOpts.field = strings.Split(sortKey, ".")
	}
	var err error
	pagination := Pagination{}
	pagination.pageSize, err = strconv.Atoi(q.Get(pageSizeParam))
	if err != nil {
		pagination.pageSize = 0
	}
	pagination.page, err = strconv.Atoi(q.Get(pageParam))
	if err != nil {
		pagination.page = 1
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

// SortList sorts the slice by the provided sort criteria.
func SortList(list []unstructured.Unstructured, s Sort) []unstructured.Unstructured {
	if len(s.field) == 0 {
		return list
	}
	sort.Slice(list, func(i, j int) bool {
		iField := convert.ToString(data.GetValueN(list[i].Object, s.field...))
		jField := convert.ToString(data.GetValueN(list[j].Object, s.field...))
		if s.order == ASC {
			return iField < jField
		}
		return jField < iField
	})
	return list
}

// PaginateList returns a subset of the result based on the pagination criteria as well as the total number of pages the caller can expect.
func PaginateList(list []unstructured.Unstructured, p Pagination) ([]unstructured.Unstructured, int) {
	if p.pageSize <= 0 {
		return list, 0
	}
	page := p.page - 1
	if p.page < 1 {
		page = 0
	}
	pages := len(list) / p.pageSize
	if len(list)%p.pageSize != 0 {
		pages++
	}
	offset := p.pageSize * page
	if offset > len(list) {
		return []unstructured.Unstructured{}, pages
	}
	if offset+p.pageSize > len(list) {
		return list[offset:], pages
	}
	return list[offset : offset+p.pageSize], pages
}
