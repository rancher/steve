// Package listprocessor contains methods for filtering, sorting, and paginating lists of objects.
package listprocessor_alpha

import (
	"context"
	"fmt"
	"github.com/rancher/apiserver/pkg/apierror"
	"github.com/rancher/wrangler/pkg/schemas/validation"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/lasso/pkg/cache/sql/informer"
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
	Filters    []informer.OrFilter
	Sort       informer.Sort
	Pagination informer.Pagination
}

type Informer interface {
	cache.SharedIndexInformer
	// ListByOptions returns objects according to the specified list options and partitions
	// see ListOptionIndexer.ListByOptions
	ListByOptions(ctx context.Context, lo informer.ListOptions, partitions []partition.Partition, namespace string) (*unstructured.UnstructuredList, string, error)
}

// ParseQuery parses the query params of a request and returns a ListOptions.
func ParseQuery(apiOp *types.APIRequest, namespaceCache Informer) (informer.ListOptions, error) {
	opts := informer.ListOptions{}

	opts.ChunkSize = getLimit(apiOp)

	q := apiOp.Request.URL.Query()
	cont := q.Get(continueParam)
	opts.Resume = cont

	filterParams := q[filterParam]
	filterOpts := []informer.OrFilter{}
	for _, filters := range filterParams {
		orFilters := strings.Split(filters, orOp)
		orFilter := informer.OrFilter{}
		for _, filter := range orFilters {
			var op informer.Op
			if strings.Contains(filter, "!=") {
				op = "!="
			}
			filter := opReg.Split(filter, -1)
			if len(filter) != 2 {
				continue
			}
			usePartialMatch := !(strings.HasPrefix(filter[1], `'`) && strings.HasSuffix(filter[1], `'`))
			orFilter.Filters = append(orFilter.Filters, informer.Filter{Field: strings.Split(filter[0], "."), Match: filter[1], Op: op, Partial: usePartialMatch})
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

	sortOpts := informer.Sort{}
	sortKeys := q.Get(sortParam)
	if sortKeys != "" {
		sortParts := strings.SplitN(sortKeys, ",", 2)
		primaryField := sortParts[0]
		if primaryField != "" && primaryField[0] == '-' {
			sortOpts.PrimaryOrder = informer.DESC
			primaryField = primaryField[1:]
		}
		if primaryField != "" {
			sortOpts.PrimaryField = strings.Split(primaryField, ".")
		}
		if len(sortParts) > 1 {
			secondaryField := sortParts[1]
			if secondaryField != "" && secondaryField[0] == '-' {
				sortOpts.SecondaryOrder = informer.DESC
				secondaryField = secondaryField[1:]
			}
			if secondaryField != "" {
				sortOpts.SecondaryField = strings.Split(secondaryField, ".")
			}
		}
	}
	opts.Sort = sortOpts

	var err error
	pagination := informer.Pagination{}
	pagination.PageSize, err = strconv.Atoi(q.Get(pageSizeParam))
	if err != nil {
		pagination.PageSize = 0
	}
	pagination.Page, err = strconv.Atoi(q.Get(pageParam))
	if err != nil {
		pagination.Page = 1
	}
	opts.Pagination = pagination

	var op informer.Op
	projectsOrNamespaces := q.Get(projectsOrNamespacesVar)
	if projectsOrNamespaces == "" {
		projectsOrNamespaces = q.Get(projectsOrNamespacesVar + notOp)
		if projectsOrNamespaces != "" {
			op = informer.NotEq
		}
	}
	if projectsOrNamespaces != "" {
		projOrNSFilters, err := parseNamespaceOrProjectFilters(apiOp.Context(), projectsOrNamespaces, op, namespaceCache)
		if err != nil {
			return opts, err
		}
		if projOrNSFilters == nil {
			return opts, apierror.NewAPIError(validation.NotFound, fmt.Sprintf("could not find any namespacess named [%s] or namespaces belonging to project named [%s]", projOrNSFilters, projOrNSFilters))
		}
		if op == informer.NotEq {
			for _, filter := range projOrNSFilters {
				opts.Filters = append(opts.Filters, informer.OrFilter{Filters: []informer.Filter{filter}})
			}
		} else {
			opts.Filters = append(opts.Filters, informer.OrFilter{Filters: projOrNSFilters})
		}
	}
	/* TODO: come back to this
	if len(opts.Filters) == 1 && opts.Filters[0].Filters[0] == nil {
		fmt.Println("debug")
	}*/
	return opts, nil
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

func matchesOne(obj map[string]interface{}, filter informer.Filter) bool {
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
		filter = informer.Filter{Field: subField, Match: filter.Match, Op: filter.Op}
		if matchesOneInList(typedVal, filter) {
			return true
		}
	}
	return false
}

func matchesOneInList(obj []interface{}, filter informer.Filter) bool {
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

func matchesAny(obj map[string]interface{}, filter informer.OrFilter) bool {
	for _, f := range filter.Filters {
		matches := matchesOne(obj, f)
		if (matches && f.Op == informer.Eq) || (!matches && f.Op == informer.NotEq) {
			return true
		}
	}
	return false
}

func matchesAll(obj map[string]interface{}, filters []informer.OrFilter) bool {
	for _, f := range filters {
		if !matchesAny(obj, f) {
			return false
		}
	}
	return true
}

func parseNamespaceOrProjectFilters(ctx context.Context, projOrNS string, op informer.Op, namespaceInformer Informer) ([]informer.Filter, error) {
	var filters []informer.Filter
	for _, pn := range strings.Split(projOrNS, ",") {
		uList, _, err := namespaceInformer.ListByOptions(ctx, informer.ListOptions{
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field: []string{"metadata", "name"},
							Match: pn,
							Op:    informer.Eq,
						},
						{
							Field: []string{"metadata", "labels[field.cattle.io/projectId]"},
							Match: pn,
							Op:    informer.Eq,
						},
					},
				},
			},
		}, []partition.Partition{{Passthrough: true}}, "")
		if err != nil {
			return filters, err
		}
		for _, item := range uList.Items {
			filters = append(filters, informer.Filter{
				Field:   []string{"metadata", "namespace"},
				Match:   item.GetName(),
				Op:      op,
				Partial: false,
			})
		}
		continue
	}

	return filters, nil
}
