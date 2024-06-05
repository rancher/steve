package listprocessor

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/lasso/pkg/cache/sql/informer"
	"github.com/rancher/lasso/pkg/cache/sql/partition"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

//go:generate mockgen --build_flags=--mod=mod -package listprocessor -destination ./proxy_mocks_test.go github.com/rancher/steve/pkg/stores/sqlproxy Cache

func TestParseQuery(t *testing.T) {
	type testCase struct {
		description  string
		setupNSCache func() Cache
		nsc          Cache
		req          *types.APIRequest
		expectedLO   informer.ListOptions
		errExpected  bool
	}
	var tests []testCase
	tests = append(tests, testCase{
		description: "ParseQuery() with no errors returned should returned no errors. Should have proper defaults set.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: ""},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters:   make([]informer.OrFilter, 0),
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
		setupNSCache: func() Cache {
			return nil
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with no errors returned should returned no errors. If projectsornamespaces is not empty" +
			" and nsc returns namespaces, they should be included as filters.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "projectsornamespaces=somethin"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"metadata", "namespace"},
							Match:   "ns1",
							Op:      "",
							Partial: false,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
		setupNSCache: func() Cache {
			list := &unstructured.UnstructuredList{
				Items: []unstructured.Unstructured{
					{
						Object: map[string]interface{}{
							"metadata": map[string]interface{}{
								"name": "ns1",
							},
						},
					},
				},
			}
			nsc := NewMockCache(gomock.NewController(t))
			nsc.EXPECT().ListByOptions(context.Background(), informer.ListOptions{
				Filters: []informer.OrFilter{
					{
						Filters: []informer.Filter{
							{
								Field: []string{"metadata", "name"},
								Match: "somethin",
								Op:    informer.Eq,
							},
							{
								Field: []string{"metadata", "labels[field.cattle.io/projectId]"},
								Match: "somethin",
								Op:    informer.Eq,
							},
						},
					},
				},
			}, []partition.Partition{{Passthrough: true}}, "").Return(list, "", nil)
			return nsc
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with a namespace informer error returned should return an error.",
		req: &types.APIRequest{
			Request: &http.Request{
				// namespace informer is only used if projectsornamespace param is given
				URL: &url.URL{RawQuery: "projectsornamespaces=somethin"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"metadata", "namespace"},
							Match:   "ns1",
							Op:      "",
							Partial: false,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
		errExpected: true,
		setupNSCache: func() Cache {
			nsi := NewMockCache(gomock.NewController(t))
			nsi.EXPECT().ListByOptions(context.Background(), informer.ListOptions{
				Filters: []informer.OrFilter{
					{
						Filters: []informer.Filter{
							{
								Field: []string{"metadata", "name"},
								Match: "somethin",
								Op:    informer.Eq,
							},
							{
								Field: []string{"metadata", "labels[field.cattle.io/projectId]"},
								Match: "somethin",
								Op:    informer.Eq,
							},
						},
					},
				},
			}, []partition.Partition{{Passthrough: true}}, "").Return(nil, "", fmt.Errorf("error"))
			return nsi
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with no errors returned should returned no errors. If projectsornamespaces is not empty" +
			" and nsc does not return namespaces, an error should be returned.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "projectsornamespaces=somethin"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"metadata", "namespace"},
							Match:   "ns1",
							Op:      "",
							Partial: false,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
		errExpected: true,
		setupNSCache: func() Cache {
			list := &unstructured.UnstructuredList{
				Items: []unstructured.Unstructured{},
			}
			nsi := NewMockCache(gomock.NewController(t))
			nsi.EXPECT().ListByOptions(context.Background(), informer.ListOptions{
				Filters: []informer.OrFilter{
					{
						Filters: []informer.Filter{
							{
								Field: []string{"metadata", "name"},
								Match: "somethin",
								Op:    informer.Eq,
							},
							{
								Field: []string{"metadata", "labels[field.cattle.io/projectId]"},
								Match: "somethin",
								Op:    informer.Eq,
							},
						},
					},
				},
			}, []partition.Partition{{Passthrough: true}}, "").Return(list, "", nil)
			return nsi
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with filter param set should include filter with partial set to true in list options.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=a=c"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"a"},
							Match:   "c",
							Op:      "",
							Partial: true,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
		setupNSCache: func() Cache {
			return nil
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with filter param set, with value in single quotes, should include filter with partial set to false in list options.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=a='c'"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"a"},
							Match:   "c",
							Op:      "",
							Partial: false,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
		setupNSCache: func() Cache {
			return nil
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with multiple filter params, should include multiple or filters.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=a=c&filter=b=d"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"a"},
							Match:   "c",
							Op:      "",
							Partial: true,
						},
					},
				},
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"b"},
							Match:   "d",
							Op:      "",
							Partial: true,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
		setupNSCache: func() Cache {
			return nil
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with a filter param with a comma separate value, should include a single or filter with" +
			" multiple filters.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=a=c,b=d"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"a"},
							Match:   "c",
							Op:      "",
							Partial: true,
						},
						{
							Field:   []string{"b"},
							Match:   "d",
							Op:      "",
							Partial: true,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
		setupNSCache: func() Cache {
			return nil
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with no errors returned should returned no errors. If one sort param is given, primary field" +
			" sort option should be set",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "sort=metadata.name"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Sort: informer.Sort{
				PrimaryField: []string{"metadata", "name"},
			},
			Filters: make([]informer.OrFilter, 0),
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
		setupNSCache: func() Cache {
			return nil
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with no errors returned should returned no errors. If one sort param is given primary field " +
			"and hyphen prefix for field value, sort option should be set with descending order.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "sort=-metadata.name"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Sort: informer.Sort{
				PrimaryField: []string{"metadata", "name"},
				PrimaryOrder: informer.DESC,
			},
			Filters: make([]informer.OrFilter, 0),
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
		setupNSCache: func() Cache {
			return nil
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with no errors returned should returned no errors. If two sort params are given, sort " +
			"options with primary field and secondary field should be set.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "sort=-metadata.name,spec.something"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Sort: informer.Sort{
				PrimaryField:   []string{"metadata", "name"},
				PrimaryOrder:   informer.DESC,
				SecondaryField: []string{"spec", "something"},
				SecondaryOrder: informer.ASC,
			},
			Filters: make([]informer.OrFilter, 0),
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
		setupNSCache: func() Cache {
			return nil
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with no errors returned should returned no errors. If continue params is given, resume" +
			" should be set with assigned value.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "continue=5"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Resume:    "5",
			Filters:   make([]informer.OrFilter, 0),
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
		setupNSCache: func() Cache {
			return nil
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with no errors returned should returned no errors. If continue param is given, resume" +
			" should be set with assigned value.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "continue=5"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Resume:    "5",
			Filters:   make([]informer.OrFilter, 0),
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
		setupNSCache: func() Cache {
			return nil
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with no errors returned should returned no errors. If limit param is given, chunksize" +
			" should be set with assigned value.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "limit=3"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: 3,
			Filters:   make([]informer.OrFilter, 0),
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
		setupNSCache: func() Cache {
			return nil
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with no errors returned should returned no errors. If page param is given, page" +
			" should be set with assigned value.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "page=3"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters:   make([]informer.OrFilter, 0),
			Pagination: informer.Pagination{
				Page: 3,
			},
		},
		setupNSCache: func() Cache {
			return nil
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with no errors returned should returned no errors. If pagesize param is given, pageSize" +
			" should be set with assigned value.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "pagesize=20"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters:   make([]informer.OrFilter, 0),
			Pagination: informer.Pagination{
				PageSize: 20,
				Page:     1,
			},
		},
		setupNSCache: func() Cache {
			return nil
		},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			test.nsc = test.setupNSCache()
			lo, err := ParseQuery(test.req, test.nsc)
			if test.errExpected {
				assert.NotNil(t, err)
				return
			}
			assert.Equal(t, test.expectedLO, lo)
		})
	}
}
