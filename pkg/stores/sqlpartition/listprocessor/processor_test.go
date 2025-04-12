package listprocessor

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/sqlcache/informer"
	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
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
		errorText    string
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
							Matches: []string{"ns1"},
							Op:      informer.Eq,
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
			nsc.EXPECT().ListByOptions(context.Background(), &informer.ListOptions{
				Filters: []informer.OrFilter{
					{
						Filters: []informer.Filter{
							{
								Field:   []string{"metadata", "name"},
								Matches: []string{"somethin"},
								Op:      informer.Eq,
							},
							{
								Field:   []string{"metadata", "labels", "field.cattle.io/projectId"},
								Matches: []string{"somethin"},
								Op:      informer.Eq,
							},
						},
					},
				},
			}, []partition.Partition{{Passthrough: true}}, "").Return(list, len(list.Items), "", nil)
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
							Matches: []string{"ns1"},
							Op:      informer.Eq,
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
			nsi.EXPECT().ListByOptions(context.Background(), &informer.ListOptions{
				Filters: []informer.OrFilter{
					{
						Filters: []informer.Filter{
							{
								Field:   []string{"metadata", "name"},
								Matches: []string{"somethin"},
								Op:      informer.Eq,
							},
							{
								Field:   []string{"metadata", "labels", "field.cattle.io/projectId"},
								Matches: []string{"somethin"},
								Op:      informer.Eq,
							},
						},
					},
				},
			}, []partition.Partition{{Passthrough: true}}, "").Return(nil, 0, "", fmt.Errorf("error"))
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
							Matches: []string{"ns1"},
							Op:      informer.Eq,
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
			nsi.EXPECT().ListByOptions(context.Background(), &informer.ListOptions{
				Filters: []informer.OrFilter{
					{
						Filters: []informer.Filter{
							{
								Field:   []string{"metadata", "name"},
								Matches: []string{"somethin"},
								Op:      informer.Eq,
							},
							{
								Field:   []string{"metadata", "labels", "field.cattle.io/projectId"},
								Matches: []string{"somethin"},
								Op:      informer.Eq,
							},
						},
					},
				},
			}, []partition.Partition{{Passthrough: true}}, "").Return(list, len(list.Items), "", nil)
			return nsi
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with filter param set should include filter with partial set to true in list options.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=a~c"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"a"},
							Matches: []string{"c"},
							Op:      informer.Eq,
							Partial: true,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with filter param set, should include filter with partial set to false in list options.",
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
							Matches: []string{"c"},
							Op:      informer.Eq,
							Partial: false,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with filter param set, with value in double quotes should return an error.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: `filter=a1="c1"`},
			},
		},
		errExpected: true,
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with a labels filter param should create a labels-specific filter.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=metadata.labels[grover.example.com/fish]~heads"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"metadata", "labels", "grover.example.com/fish"},
							Matches: []string{"heads"},
							Op:      informer.Eq,
							Partial: true,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with an annotations filter param should split it correctly.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=metadata.annotations[chumley.example.com/fish]=seals"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"metadata", "annotations", "chumley.example.com/fish"},
							Matches: []string{"seals"},
							Op:      informer.Eq,
							Partial: false,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with a numeric filter index should split it correctly.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=metadata.fields[3]<5"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"metadata", "fields", "3"},
							Matches: []string{"5"},
							Op:      informer.Lt,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with a labels filter param should create a labels-specific filter.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=metadata.labels[grover.example.com/fish]~heads"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"metadata", "labels", "grover.example.com/fish"},
							Matches: []string{"heads"},
							Op:      informer.Eq,
							Partial: true,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
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
							Matches: []string{"c"},
							Op:      informer.Eq,
							Partial: false,
						},
					},
				},
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"b"},
							Matches: []string{"d"},
							Op:      informer.Eq,
							Partial: false,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
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
							Matches: []string{"c"},
							Op:      informer.Eq,
							Partial: false,
						},
					},
				},
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"b"},
							Matches: []string{"d"},
							Op:      informer.Eq,
							Partial: false,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() should handle comma-separated standard and labels filters.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=beer=pabst,metadata.labels[beer2.io/ale] ~schlitz"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"beer"},
							Matches: []string{"pabst"},
							Op:      informer.Eq,
							Partial: false,
						},
						{
							Field:   []string{"metadata", "labels", "beer2.io/ale"},
							Matches: []string{"schlitz"},
							Op:      informer.Eq,
							Partial: true,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() should handle simple dot-separated label filters.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=beer=natty-bo,metadata.labels.beer3~rainier"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"beer"},
							Matches: []string{"natty-bo"},
							Op:      informer.Eq,
							Partial: false,
						},
						{
							Field:   []string{"metadata", "labels", "beer3"},
							Matches: []string{"rainier"},
							Op:      informer.Eq,
							Partial: true,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() should handle 'in' and 'IN' with one arg",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=a1In in (x1),a2In IN (x2)"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"a1In"},
							Matches: []string{"x1"},
							Op:      informer.In,
							Partial: false,
						},
						{
							Field:   []string{"a2In"},
							Matches: []string{"x2"},
							Op:      informer.In,
							Partial: false,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() should handle 'in' with multiple args",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=a2In in (x2a, x2b)"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"a2In"},
							Matches: []string{"x2a", "x2b"},
							Op:      informer.In,
							Partial: false,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() should handle 'notin' and 'NOTIN' with one arg",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=a1NotIn notin (x1),a2NotIn NOTIN (x2)"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"a1NotIn"},
							Matches: []string{"x1"},
							Op:      informer.NotIn,
							Partial: false,
						},
						{
							Field:   []string{"a2NotIn"},
							Matches: []string{"x2"},
							Op:      informer.NotIn,
							Partial: false,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() should handle 'in' with multiple args",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=a3NotIn in (x3a, x3b)"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"a3NotIn"},
							Matches: []string{"x3a", "x3b"},
							Op:      informer.In,
							Partial: false,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() should handle 'in' and 'notin' in mixed case",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=a4In iN (x4a),a4NotIn nOtIn (x4b)"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"a4In"},
							Matches: []string{"x4a"},
							Op:      informer.In,
							Partial: false,
						},
						{
							Field:   []string{"a4NotIn"},
							Matches: []string{"x4b"},
							Op:      informer.NotIn,
							Partial: false,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() should complain on non-label exists tests",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=a5In1,!a5In2, ! a5In3"},
			},
		},
		errExpected: true,
		errorText:   "unable to parse requirement: existence tests are valid only for labels; not valid for field 'a5In1'",
	})
	tests = append(tests, testCase{
		description: "ParseQuery() should allow label exists tests",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=metadata.labels.a5In1,!metadata.labels.a5In2, ! metadata.labels.a5In3"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"metadata", "labels", "a5In1"},
							Op:      informer.Exists,
							Matches: []string{},
							Partial: false,
						},
						{
							Field:   []string{"metadata", "labels", "a5In2"},
							Op:      informer.NotExists,
							Matches: []string{},
							Partial: false,
						},
						{
							Field:   []string{"metadata", "labels", "a5In3"},
							Op:      informer.NotExists,
							Matches: []string{},
							Partial: false,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() should handle numeric comparisons",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=a<1,b>2"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"a"},
							Op:      informer.Lt,
							Matches: []string{"1"},
							Partial: false,
						},
						{
							Field:   []string{"b"},
							Op:      informer.Gt,
							Matches: []string{"2"},
							Partial: false,
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with no errors returned should returned no errors. It should sort on the one given" +
			" sort option should be set",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "sort=metadata.name"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			SortList: informer.SortList{
				SortDirectives: []informer.Sort{
					{
						Fields: []string{"metadata", "name"},
						Order:  informer.ASC,
					},
				},
			},
			Filters: make([]informer.OrFilter, 0),
			Pagination: informer.Pagination{
				Page: 1,
			},
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
			SortList: informer.SortList{
				SortDirectives: []informer.Sort{
					{
						Fields: []string{"metadata", "name"},
						Order:  informer.DESC,
					},
				},
			},
			Filters: make([]informer.OrFilter, 0),
			Pagination: informer.Pagination{
				Page: 1,
			},
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
			SortList: informer.SortList{
				SortDirectives: []informer.Sort{
					{
						Fields: []string{"metadata", "name"},
						Order:  informer.DESC,
					},
					{
						Fields: []string{"spec", "something"},
						Order:  informer.ASC,
					},
				},
			},
			Filters: make([]informer.OrFilter, 0),
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
	})

	tests = append(tests, testCase{
		description: "sorting can parse bracketed field names correctly",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "sort=-metadata.labels[beef.cattle.io/snort],metadata.labels.steer,metadata.labels[bossie.cattle.io/moo],spec.something"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			SortList: informer.SortList{
				SortDirectives: []informer.Sort{
					{
						Fields: []string{"metadata", "labels", "beef.cattle.io/snort"},
						Order:  informer.DESC,
					},
					{
						Fields: []string{"metadata", "labels", "steer"},
						Order:  informer.ASC,
					},
					{
						Fields: []string{"metadata", "labels", "bossie.cattle.io/moo"},
						Order:  informer.ASC,
					},
					{
						Fields: []string{"spec", "something"},
						Order:  informer.ASC,
					},
				},
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
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			if test.setupNSCache == nil {
				test.nsc = nil
			} else {
				test.nsc = test.setupNSCache()
			}
			lo, err := ParseQuery(test.req, test.nsc)
			if test.errExpected {
				require.NotNil(t, err)
				if test.errorText != "" {
					assert.Contains(t, test.errorText, err.Error())
				}
				return
			} else {
				assert.Nil(t, err)
			}
			assert.Equal(t, test.expectedLO, lo)
		})
	}
}

// Focus on the indirect accessors
func TestParseIndirectQuery(t *testing.T) {
	type testCase struct {
		description  string
		setupNSCache func() Cache
		nsc          Cache
		req          *types.APIRequest
		expectedLO   informer.ListOptions
		errExpected  bool
		errorText    string
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
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with an indirect accessor should create an indirect object.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=metadata.labels[grover.example.com/fish] => [things.cattle.io][boils][metadata.name][spec.carousel] ~heads"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:          []string{"metadata", "labels", "grover.example.com/fish"},
							Matches:        []string{"heads"},
							Op:             informer.Eq,
							Partial:        true,
							IsIndirect:     true,
							IndirectFields: []string{"things.cattle.io", "boils", "metadata.name", "spec.carousel"},
						},
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with an indirect accessor and other things should create an indirect object.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=metadata.fields[3]<5,metadata.labels[grover.example.com/fish] => [things.cattle.io][boils][metadata.name][spec.carousel] ~heads&sort=-metadata.name&filter=b>2"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			Filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"metadata", "fields", "3"},
							Matches: []string{"5"},
							Op:      informer.Lt,
						},
						{
							Field:          []string{"metadata", "labels", "grover.example.com/fish"},
							Matches:        []string{"heads"},
							Op:             informer.Eq,
							Partial:        true,
							IsIndirect:     true,
							IndirectFields: []string{"things.cattle.io", "boils", "metadata.name", "spec.carousel"},
						},
					},
				},
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"b"},
							Matches: []string{"2"},
							Op:      informer.Gt,
						},
					},
				},
			},
			SortList: informer.SortList{
				SortDirectives: []informer.Sort{
					{
						Fields: []string{"metadata", "name"},
						Order:  informer.DESC,
					},
				},
			},
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			if test.setupNSCache == nil {
				test.nsc = nil
			} else {
				test.nsc = test.setupNSCache()
			}
			lo, err := ParseQuery(test.req, test.nsc)
			if test.errExpected {
				require.NotNil(t, err)
				if test.errorText != "" {
					assert.Contains(t, test.errorText, err.Error())
				}
				return
			} else {
				assert.Nil(t, err)
			}
			assert.Equal(t, test.expectedLO, lo)
		})
	}
}

func TestParseSortDirective(t *testing.T) {
	type testCase struct {
		description  string
		setupNSCache func() Cache
		nsc          Cache
		req          *types.APIRequest
		expectedLO   informer.ListOptions
		errExpected  bool
		errorText    string
	}
	var tests []testCase
	tests = append(tests, testCase{
		description: "ParseQuery() with no errors returned should returned no errors. It should sort on the one given" +
			" sort option should be set",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "sort=metadata.name"},
			},
		},
		expectedLO: informer.ListOptions{
			ChunkSize: defaultLimit,
			SortList: informer.SortList{
				SortDirectives: []informer.Sort{
					{
						Fields: []string{"metadata", "name"},
						Order:  informer.ASC,
					},
				},
			},
			Filters: make([]informer.OrFilter, 0),
			Pagination: informer.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "A sort parameter shouldn't have a not-exist operator",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "sort=!metadata.name"},
			},
		},
		errExpected: true,
		errorText:   "sort directive !metadata.name can't contain operator (!)",
	})
	tests = append(tests, testCase{
		description: "A sort parameter shouldn't have a binary operator",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "sort=metadata.name=fish"},
			},
		},
		errExpected: true,
		errorText:   "sort directive metadata.name=fish can't contain operator (=)",
	})
	tests = append(tests, testCase{
		description: "A sort parameter shouldn't have a binary operator (not =)",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "sort=metadata.name=> 4"},
			},
		},
		errExpected: true,
		errorText:   "found '4', expected: a sequence of bracketed identifiers",
	})
	tests = append(tests, testCase{
		description: "A sort parameter shouldn't have a binary operator with other sort params",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "sort=metadata.fish,metadata.name=> 4,metadata.cows"},
			},
		},
		errExpected: true,
		errorText:   "found '4', expected: a sequence of bracketed identifiers",
	})
	tests = append(tests, testCase{
		description: "Handle a missing sort parameter - empty string",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "sort="},
			},
		},
		errExpected: true,
		errorText:   "invalid sort key: <empty string>",
	})
	tests = append(tests, testCase{
		description: "Handle a missing sort parameter - no lh arg for an operator",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "sort=in (a, b, c)"},
			},
		},
		errExpected: true,
		errorText:   "found unexpected token '(' in sort parameter",
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			if test.setupNSCache == nil {
				test.nsc = nil
			} else {
				test.nsc = test.setupNSCache()
			}
			lo, err := ParseQuery(test.req, test.nsc)
			if test.errExpected {
				require.NotNil(t, err)
				if test.errorText != "" {
					assert.Contains(t, test.errorText, err.Error())
				}
				return
			} else {
				assert.Nil(t, err)
			}
			assert.Equal(t, test.expectedLO, lo)
		})
	}
}
