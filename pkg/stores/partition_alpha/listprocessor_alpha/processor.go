// Package listprocessor contains methods for filtering, sorting, and paginating lists of objects.
package listprocessor_alpha

import (
	"regexp"
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/wrangler/pkg/data"
	"github.com/rancher/wrangler/pkg/data/convert"
	corecontrollers "github.com/rancher/wrangler/pkg/generated/controllers/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	defaultLimit            = 100000
	continueParam           = "continue"
	limitParam              = "limit"
	filterParam             = "filter"
	sortParam               = "sort"
	pageSizeParam           = "pagesize"
	pageParam               = "page"
	revisionParam           = "revision"
	projectsOrNamespacesVar = "projectsornamespaces"
	projectIDFieldLabel     = "field.cattle.io/projectId"

	orOp  = ","
	notOp = "!"
)

var opReg = regexp.MustCompile(`[!]?=`)

type op string

const (
	eq    op = ""
	notEq op = "!="
)

// ListOptions represents the query parameters that may be included in a list request.
type ListOptions struct {
	ChunkSize            int
	Resume               string
	Filters              []OrFilter
	Sort                 Sort
	Pagination           Pagination
	ProjectsOrNamespaces ProjectsOrNamespacesFilter
}

// Partition represents filtering of a request's results by namespace or a list of resource names
type Partition struct {
	Namespace   string
	All         bool
	Passthrough bool
	Names       sets.String
}

// TODO: make sure Op filter is supported by vai
// Filter represents a field to filter by.
// A subfield in an object is represented in a request query using . notation, e.g. 'metadata.name'.
// The subfield is internally represented as a slice, e.g. [metadata, name].
type Filter struct {
	Field []string
	Match string
	Op    op
}

// String returns the filter as a query string.
func (f Filter) String() string {
	field := strings.Join(f.Field, ".")
	return field + "=" + f.Match
}

// OrFilter represents a set of possible fields to filter by, where an item may match any filter in the set to be included in the result.
type OrFilter struct {
	filters []Filter
}

// String returns the filter as a query string.
func (f OrFilter) String() string {
	var fields strings.Builder
	for i, field := range f.filters {
		fields.WriteString(strings.Join(field.Field, "."))
		fields.WriteByte('=')
		fields.WriteString(field.Match)
		if i < len(f.filters)-1 {
			fields.WriteByte(',')
		}
	}
	return fields.String()
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

type ProjectsOrNamespacesFilter struct {
	filter map[string]struct{}
	op     op
}

// ParseQuery parses the query params of a request and returns a ListOptions.
func ParseQuery(apiOp *types.APIRequest) *ListOptions {
	opts := ListOptions{}

	opts.ChunkSize = getLimit(apiOp)

	q := apiOp.Request.URL.Query()
	cont := q.Get(continueParam)
	opts.Resume = cont

	filterParams := q[filterParam]
	filterOpts := []OrFilter{}
	for _, filters := range filterParams {
		orFilters := strings.Split(filters, orOp)
		orFilter := OrFilter{}
		for _, filter := range orFilters {
			var op op
			if strings.Contains(filter, "!=") {
				op = "!="
			}
			filter := opReg.Split(filter, -1)
			if len(filter) != 2 {
				continue
			}
			orFilter.filters = append(orFilter.filters, Filter{Field: strings.Split(filter[0], "."), Match: filter[1], Op: op})
		}
		filterOpts = append(filterOpts, orFilter)
	}
	// sort the filter fields so they can be used as a cache key in the store
	for _, orFilter := range filterOpts {
		sort.Slice(orFilter.filters, func(i, j int) bool {
			fieldI := strings.Join(orFilter.filters[i].Field, ".")
			fieldJ := strings.Join(orFilter.filters[j].Field, ".")
			return fieldI < fieldJ
		})
	}
	sort.Slice(filterOpts, func(i, j int) bool {
		var fieldI, fieldJ strings.Builder
		for _, f := range filterOpts[i].filters {
			fieldI.WriteString(strings.Join(f.Field, "."))
		}
		for _, f := range filterOpts[j].filters {
			fieldJ.WriteString(strings.Join(f.Field, "."))
		}
		return fieldI.String() < fieldJ.String()
	})
	opts.Filters = filterOpts

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
	opts.Sort = sortOpts

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
	opts.Pagination = pagination

	projectsOptions := ProjectsOrNamespacesFilter{}
	var op op
	projectsOrNamespaces := q.Get(projectsOrNamespacesVar)
	if projectsOrNamespaces == "" {
		projectsOrNamespaces = q.Get(projectsOrNamespacesVar + notOp)
		if projectsOrNamespaces != "" {
			op = notEq
		}
	}
	if projectsOrNamespaces != "" {
		projectsOptions.filter = make(map[string]struct{})
		for _, pn := range strings.Split(projectsOrNamespaces, ",") {
			projectsOptions.filter[pn] = struct{}{}
		}
		projectsOptions.op = op
		opts.ProjectsOrNamespaces = projectsOptions
	}
	return &opts
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
		filter = Filter{Field: subField, Match: filter.Match, Op: filter.Op}
		if matchesOneInList(typedVal, filter) {
			return true
		}
	}
	return false
}

func matchesOneInList(obj []interface{}, filter Filter) bool {
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
			if matchesOneInList(typedItem, filter) {
				return true
			}
		}
	}
	return false
}

func matchesAny(obj map[string]interface{}, filter OrFilter) bool {
	for _, f := range filter.filters {
		matches := matchesOne(obj, f)
		if (matches && f.Op == eq) || (!matches && f.Op == notEq) {
			return true
		}
	}
	return false
}

func matchesAll(obj map[string]interface{}, filters []OrFilter) bool {
	for _, f := range filters {
		if !matchesAny(obj, f) {
			return false
		}
	}
	return true
}

// TODO: add support for project filter via vai
func FilterByProjectsAndNamespaces(list []unstructured.Unstructured, projectsOrNamespaces ProjectsOrNamespacesFilter, namespaceCache corecontrollers.NamespaceCache) []unstructured.Unstructured {
	if len(projectsOrNamespaces.filter) == 0 {
		return list
	}
	result := []unstructured.Unstructured{}
	for _, obj := range list {
		namespaceName := obj.GetNamespace()
		if namespaceName == "" {
			continue
		}
		namespace, err := namespaceCache.Get(namespaceName)
		if namespace == nil || err != nil {
			continue
		}
		projectLabel, _ := namespace.GetLabels()[projectIDFieldLabel]
		_, matchesProject := projectsOrNamespaces.filter[projectLabel]
		_, matchesNamespace := projectsOrNamespaces.filter[namespaceName]
		matches := matchesProject || matchesNamespace
		if projectsOrNamespaces.op == eq && matches {
			result = append(result, obj)
		}
		if projectsOrNamespaces.op == notEq && !matches {
			result = append(result, obj)
		}
	}
	return result
}
