// Package listprocessor contains methods for filtering, sorting, and paginating lists of objects.
package listprocessor_alpha

import (
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/lasso/pkg/cache/sql"
	"github.com/rancher/lasso/pkg/cache/sql/partition"
	"github.com/rancher/wrangler/pkg/data"
	"github.com/rancher/wrangler/pkg/data/convert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
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

// ListOptions represents the query parameters that may be included in a list request.
type ListOptions struct {
	ChunkSize  int
	Resume     string
	Filters    []sql.OrFilter
	Sort       sql.Sort
	Pagination sql.Pagination
}

type Informer interface {
	cache.SharedIndexInformer
	// ListByOptions returns objects according to the specified list options and partitions
	// see ListOptionIndexer.ListByOptions
	ListByOptions(lo *sql.ListOptions, partitions []partition.Partition, namespace string) (*unstructured.UnstructuredList, string, error)
}

// ParseQuery parses the query params of a request and returns a ListOptions.
func ParseQuery(apiOp *types.APIRequest, namespaceCache Informer) (*sql.ListOptions, error) {
	opts := sql.ListOptions{}

	opts.ChunkSize = getLimit(apiOp)

	q := apiOp.Request.URL.Query()
	cont := q.Get(continueParam)
	opts.Resume = cont

	filterParams := q[filterParam]
	filterOpts := []sql.OrFilter{}
	for _, filters := range filterParams {
		orFilters := strings.Split(filters, orOp)
		orFilter := sql.OrFilter{}
		for _, filter := range orFilters {
			var op sql.Op
			if strings.Contains(filter, "!=") {
				op = "!="
			}
			filter := opReg.Split(filter, -1)
			if len(filter) != 2 {
				continue
			}
			orFilter.Filters = append(orFilter.Filters, sql.Filter{Field: strings.Split(filter[0], "."), Match: filter[1], Op: op})
		}
		filterOpts = append(filterOpts, orFilter)
	}
	// sort the filter fields so they can be used as a cache key in the store
	for _, orFilter := range filterOpts {
		sort.Slice(orFilter.Filters, func(i, j int) bool {
			fieldI := strings.Join(orFilter.Filters[i].Field, ".")
			fieldJ := strings.Join(orFilter.Filters[j].Field, ".")
			return fieldI < fieldJ
		})
	}
	sort.Slice(filterOpts, func(i, j int) bool {
		var fieldI, fieldJ strings.Builder
		for _, f := range filterOpts[i].Filters {
			fieldI.WriteString(strings.Join(f.Field, "."))
		}
		for _, f := range filterOpts[j].Filters {
			fieldJ.WriteString(strings.Join(f.Field, "."))
		}
		return fieldI.String() < fieldJ.String()
	})
	opts.Filters = filterOpts

	sortOpts := sql.Sort{}
	sortKeys := q.Get(sortParam)
	if sortKeys != "" {
		sortParts := strings.SplitN(sortKeys, ",", 2)
		primaryField := sortParts[0]
		if primaryField != "" && primaryField[0] == '-' {
			sortOpts.PrimaryOrder = sql.DESC
			primaryField = primaryField[1:]
		}
		if primaryField != "" {
			sortOpts.PrimaryField = strings.Split(primaryField, ".")
		}
		if len(sortParts) > 1 {
			secondaryField := sortParts[1]
			if secondaryField != "" && secondaryField[0] == '-' {
				sortOpts.SecondaryOrder = sql.DESC
				secondaryField = secondaryField[1:]
			}
			if secondaryField != "" {
				sortOpts.SecondaryField = strings.Split(secondaryField, ".")
			}
		}
	}
	opts.Sort = sortOpts

	var err error
	pagination := sql.Pagination{}
	pagination.PageSize, err = strconv.Atoi(q.Get(pageSizeParam))
	if err != nil {
		pagination.PageSize = 0
	}
	pagination.Page, err = strconv.Atoi(q.Get(pageParam))
	if err != nil {
		pagination.Page = 1
	}
	opts.Pagination = pagination

	var op sql.Op
	projectsOrNamespaces := q.Get(projectsOrNamespacesVar)
	if projectsOrNamespaces == "" {
		projectsOrNamespaces = q.Get(projectsOrNamespacesVar + notOp)
		if projectsOrNamespaces != "" {
			op = sql.NotEq
		}
	}
	if projectsOrNamespaces != "" {
		projOrNSFilters, err := parseNamespaceOrProjectFilters(projectsOrNamespaces, op, namespaceCache)
		if err != nil {
			return nil, err
		}
		if op == sql.NotEq {
			for _, filter := range projOrNSFilters {
				opts.Filters = append(opts.Filters, sql.OrFilter{Filters: []sql.Filter{filter}})
			}
		} else {
			opts.Filters = append(opts.Filters, sql.OrFilter{Filters: projOrNSFilters})
		}
	}
	return &opts, nil
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

func matchesOne(obj map[string]interface{}, filter sql.Filter) bool {
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
		filter = sql.Filter{Field: subField, Match: filter.Match, Op: filter.Op}
		if matchesOneInList(typedVal, filter) {
			return true
		}
	}
	return false
}

func matchesOneInList(obj []interface{}, filter sql.Filter) bool {
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

func matchesAny(obj map[string]interface{}, filter sql.OrFilter) bool {
	for _, f := range filter.Filters {
		matches := matchesOne(obj, f)
		if (matches && f.Op == sql.Eq) || (!matches && f.Op == sql.NotEq) {
			return true
		}
	}
	return false
}

func matchesAll(obj map[string]interface{}, filters []sql.OrFilter) bool {
	for _, f := range filters {
		if !matchesAny(obj, f) {
			return false
		}
	}
	return true
}

func parseNamespaceOrProjectFilters(projOrNS string, op sql.Op, namespaceInformer Informer) ([]sql.Filter, error) {
	var filters []sql.Filter
	for _, pn := range strings.Split(projOrNS, ",") {
		uList, _, err := namespaceInformer.ListByOptions(&sql.ListOptions{
			Filters: []sql.OrFilter{
				{
					Filters: []sql.Filter{
						{
							Field: []string{"metadata", "name"},
							Match: pn,
							Op:    sql.Eq,
						},
						{
							Field: []string{"metadata", "labels", "field.cattle.io/projectId"},
							Match: pn,
							Op:    sql.Eq,
						},
					},
				},
			},
		}, []partition.Partition{{Passthrough: true}}, "")
		if err != nil {
			return filters, err
		}
		for _, item := range uList.Items {
			filters = append(filters, sql.Filter{
				Field: []string{"metadata", "namespace"},
				Match: item.GetName(),
				Op:    op,
			})
		}
		continue
	}
	return filters, nil
}
