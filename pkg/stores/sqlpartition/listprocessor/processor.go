// Package listprocessor contains methods for filtering, sorting, and paginating lists of objects.
package listprocessor

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/rancher/apiserver/pkg/apierror"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/sqlcache/informer"
	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/rancher/steve/pkg/stores/queryhelper"
	"github.com/rancher/wrangler/v3/pkg/schemas/validation"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/selection"
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
var labelsRegex = regexp.MustCompile(`^(metadata).(labels)\[(.+)\]$`)

// ListOptions represents the query parameters that may be included in a list request.
type ListOptions struct {
	ChunkSize  int
	Resume     string
	Filters    []informer.OrFilter
	Sort       informer.Sort
	Pagination informer.Pagination
}

type Cache interface {
	// ListByOptions returns objects according to the specified list options and partitions.
	// Specifically:
	//   - an unstructured list of resources belonging to any of the specified partitions
	//   - the total number of resources (returned list might be a subset depending on pagination options in lo)
	//   - a continue token, if there are more pages after the returned one
	//   - an error instead of all of the above if anything went wrong
	ListByOptions(ctx context.Context, lo informer.ListOptions, partitions []partition.Partition, namespace string) (*unstructured.UnstructuredList, int, string, error)
}

func k8sOpToRancherOp(k8sOp selection.Operator) (informer.Op, error) {
	h := map[selection.Operator]informer.Op{
		selection.Equals:       informer.Eq,
		selection.DoubleEquals: informer.Eq,
		selection.NotEquals:    informer.NotEq,
		selection.In:           informer.In,
		selection.NotIn:        informer.NotIn,
		selection.Exists:       informer.Exists,
		selection.DoesNotExist: informer.NotExists,
	}
	v, ok := h[k8sOp]
	if ok {
		return v, nil
	}
	return "", fmt.Errorf("unknown k8sOp: %s", k8sOp)
}

func isQuotedStringTarget(values []string) bool {
	if len(values) != 1 || len(values[0]) == 0 {
		return false
	}
	s1 := values[0][0:1]
	if !strings.Contains(`"'`, s1) {
		return false
	}
	return strings.HasSuffix(values[0], s1)
}

// k8sRequirementToOrFilter - convert one k8s Requirement to a list of Filter's:

func k8sRequirementToOrFilter(requirement queryparser.Requirement) (informer.Filter, error) {
	values := requirement.Values()
	queryFields := splitQuery(requirement.Key())
	op, err := k8sOpToRancherOp(requirement.Operator())
	if err != nil {
		return informer.Filter{}, err
	}
	usePartialMatch := true
	if isQuotedStringTarget(values) {
		usePartialMatch = false
		// Strip off the quotes
		values[0] = values[0][1 : len(values[0])-1]
	}
	return informer.Filter{
		Field:   queryFields,
		Matches: values,
		Op:      op,
		Partial: usePartialMatch,
	}, nil
}

// ParseQuery parses the query params of a request and returns a ListOptions.
func ParseQuery(apiOp *types.APIRequest, namespaceCache Cache) (informer.ListOptions, error) {
	opts := informer.ListOptions{}

	opts.ChunkSize = getLimit(apiOp)

	q := apiOp.Request.URL.Query()
	cont := q.Get(continueParam)
	opts.Resume = cont

	filterParams := q[filterParam]
	filterOpts := []informer.OrFilter{}
	for _, filters := range filterParams {
		requirements, err := queryparser.ParseToRequirements(filters)
		if err != nil {
			return informer.ListOptions{}, err
		}
		orFilter := informer.OrFilter{}
		for _, requirement := range requirements {
			filter, err := k8sRequirementToOrFilter(requirement)
			if err != nil {
				return opts, err
			}
			orFilter.Filters = append(orFilter.Filters, filter)
		}
		filterOpts = append(filterOpts, orFilter)
	}
	opts.Filters = filterOpts

	sortOpts := informer.Sort{}
	sortKeys := q.Get(sortParam)
	if sortKeys != "" {
		sortParts := strings.Split(sortKeys, ",")
		for _, sortPart := range sortParts {
			field := sortPart
			if len(field) > 0 {
				sortOrder := informer.ASC
				if field[0] == '-' {
					sortOrder = informer.DESC
					field = field[1:]
				}
				if len(field) > 0 {
					sortOpts.Fields = append(sortOpts.Fields, queryhelper.SafeSplit(field))
					sortOpts.Orders = append(sortOpts.Orders, sortOrder)
				}
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

	op := informer.Eq
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
			return opts, apierror.NewAPIError(validation.NotFound, fmt.Sprintf("could not find any namespacess named [%s] or namespaces belonging to project named [%s]", projectsOrNamespaces, projectsOrNamespaces))
		}
		if op == informer.NotEq {
			for _, filter := range projOrNSFilters {
				opts.Filters = append(opts.Filters, informer.OrFilter{Filters: []informer.Filter{filter}})
			}
		} else {
			opts.Filters = append(opts.Filters, informer.OrFilter{Filters: projOrNSFilters})
		}
	}

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

func splitQuery(query string) []string {
	m := labelsRegex.FindStringSubmatch(query)
	if m != nil && len(m) >= 4 {
		return m[1:]
	}
	return strings.Split(query, ".")
}

func parseNamespaceOrProjectFilters(ctx context.Context, projOrNS string, op informer.Op, namespaceInformer Cache) ([]informer.Filter, error) {
	var filters []informer.Filter
	for _, pn := range strings.Split(projOrNS, ",") {
		uList, _, _, err := namespaceInformer.ListByOptions(ctx, informer.ListOptions{
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"metadata", "name"},
							Matches: []string{pn},
							Op:      informer.Eq,
						},
						{
							Field:   []string{"metadata", "labels[field.cattle.io/projectId]"},
							Matches: []string{pn},
							Op:      informer.Eq,
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
				Matches: []string{item.GetName()},
				Op:      op,
				Partial: false,
			})
		}
		continue
	}

	return filters, nil
}
