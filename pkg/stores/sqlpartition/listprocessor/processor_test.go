package listprocessor

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
	"github.com/stretchr/testify/assert"
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
		expectedLO   sqltypes.ListOptions
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
		expectedLO: sqltypes.ListOptions{
			Filters: make([]sqltypes.OrFilter, 0),
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"metadata", "namespace"},
							Matches: []string{"ns1"},
							Op:      sqltypes.Eq,
							Partial: false,
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{
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
			nsc.EXPECT().ListByOptions(context.Background(), &sqltypes.ListOptions{
				Filters: []sqltypes.OrFilter{
					{
						Filters: []sqltypes.Filter{
							{
								Field:   []string{"metadata", "name"},
								Matches: []string{"somethin"},
								Op:      sqltypes.Eq,
							},
							{
								Field:   []string{"metadata", "labels", "field.cattle.io/projectId"},
								Matches: []string{"somethin"},
								Op:      sqltypes.Eq,
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
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"metadata", "namespace"},
							Matches: []string{"ns1"},
							Op:      sqltypes.Eq,
							Partial: false,
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{
				Page: 1,
			},
		},
		errExpected: true,
		setupNSCache: func() Cache {
			nsi := NewMockCache(gomock.NewController(t))
			nsi.EXPECT().ListByOptions(context.Background(), &sqltypes.ListOptions{
				Filters: []sqltypes.OrFilter{
					{
						Filters: []sqltypes.Filter{
							{
								Field:   []string{"metadata", "name"},
								Matches: []string{"somethin"},
								Op:      sqltypes.Eq,
							},
							{
								Field:   []string{"metadata", "labels", "field.cattle.io/projectId"},
								Matches: []string{"somethin"},
								Op:      sqltypes.Eq,
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
			" and nsc does not return namespaces, it should return an empty filter array",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "projectsornamespaces=somethin"},
			},
		},
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{},
			Pagination: sqltypes.Pagination{
				Page: 1,
			},
		},
		errExpected: true,
		setupNSCache: func() Cache {
			list := &unstructured.UnstructuredList{
				Items: []unstructured.Unstructured{},
			}
			nsi := NewMockCache(gomock.NewController(t))
			nsi.EXPECT().ListByOptions(context.Background(), &sqltypes.ListOptions{
				Filters: []sqltypes.OrFilter{
					{
						Filters: []sqltypes.Filter{
							{
								Field:   []string{"metadata", "name"},
								Matches: []string{"somethin"},
								Op:      sqltypes.Eq,
							},
							{
								Field:   []string{"metadata", "labels", "field.cattle.io/projectId"},
								Matches: []string{"somethin"},
								Op:      sqltypes.Eq,
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
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"a"},
							Matches: []string{"c"},
							Op:      sqltypes.Eq,
							Partial: true,
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"a"},
							Matches: []string{"c"},
							Op:      sqltypes.Eq,
							Partial: false,
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with filter param set, with value in double quotes.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: `filter=a1="c1"`},
			},
		},
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"a1"},
							Matches: []string{"c1"},
							Op:      sqltypes.Eq,
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with filter param set, with value in single quotes.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=a1b='c1b'"},
			},
		},
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"a1b"},
							Matches: []string{"c1b"},
							Op:      sqltypes.Eq,
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"metadata", "labels", "grover.example.com/fish"},
							Matches: []string{"heads"},
							Op:      sqltypes.Eq,
							Partial: true,
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"metadata", "annotations", "chumley.example.com/fish"},
							Matches: []string{"seals"},
							Op:      sqltypes.Eq,
							Partial: false,
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"metadata", "fields", "3"},
							Matches: []string{"5"},
							Op:      sqltypes.Lt,
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"metadata", "labels", "grover.example.com/fish"},
							Matches: []string{"heads"},
							Op:      sqltypes.Eq,
							Partial: true,
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"a"},
							Matches: []string{"c"},
							Op:      sqltypes.Eq,
							Partial: false,
						},
					},
				},
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"b"},
							Matches: []string{"d"},
							Op:      sqltypes.Eq,
							Partial: false,
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"a"},
							Matches: []string{"c"},
							Op:      sqltypes.Eq,
							Partial: false,
						},
					},
				},
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"b"},
							Matches: []string{"d"},
							Op:      sqltypes.Eq,
							Partial: false,
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"beer"},
							Matches: []string{"pabst"},
							Op:      sqltypes.Eq,
							Partial: false,
						},
						{
							Field:   []string{"metadata", "labels", "beer2.io/ale"},
							Matches: []string{"schlitz"},
							Op:      sqltypes.Eq,
							Partial: true,
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"beer"},
							Matches: []string{"natty-bo"},
							Op:      sqltypes.Eq,
							Partial: false,
						},
						{
							Field:   []string{"metadata", "labels", "beer3"},
							Matches: []string{"rainier"},
							Op:      sqltypes.Eq,
							Partial: true,
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"a1In"},
							Matches: []string{"x1"},
							Op:      sqltypes.In,
							Partial: false,
						},
						{
							Field:   []string{"a2In"},
							Matches: []string{"x2"},
							Op:      sqltypes.In,
							Partial: false,
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"a2In"},
							Matches: []string{"x2a", "x2b"},
							Op:      sqltypes.In,
							Partial: false,
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"a1NotIn"},
							Matches: []string{"x1"},
							Op:      sqltypes.NotIn,
							Partial: false,
						},
						{
							Field:   []string{"a2NotIn"},
							Matches: []string{"x2"},
							Op:      sqltypes.NotIn,
							Partial: false,
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"a3NotIn"},
							Matches: []string{"x3a", "x3b"},
							Op:      sqltypes.In,
							Partial: false,
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"a4In"},
							Matches: []string{"x4a"},
							Op:      sqltypes.In,
							Partial: false,
						},
						{
							Field:   []string{"a4NotIn"},
							Matches: []string{"x4b"},
							Op:      sqltypes.NotIn,
							Partial: false,
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"metadata", "labels", "a5In1"},
							Op:      sqltypes.Exists,
							Matches: []string{},
							Partial: false,
						},
						{
							Field:   []string{"metadata", "labels", "a5In2"},
							Op:      sqltypes.NotExists,
							Matches: []string{},
							Partial: false,
						},
						{
							Field:   []string{"metadata", "labels", "a5In3"},
							Op:      sqltypes.NotExists,
							Matches: []string{},
							Partial: false,
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"a"},
							Op:      sqltypes.Lt,
							Matches: []string{"1"},
							Partial: false,
						},
						{
							Field:   []string{"b"},
							Op:      sqltypes.Gt,
							Matches: []string{"2"},
							Partial: false,
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "name"},
						Order:  sqltypes.ASC,
					},
				},
			},
			Filters: make([]sqltypes.OrFilter, 0),
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "name"},
						Order:  sqltypes.DESC,
					},
				},
			},
			Filters: make([]sqltypes.OrFilter, 0),
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "name"},
						Order:  sqltypes.DESC,
					},
					{
						Fields: []string{"spec", "something"},
						Order:  sqltypes.ASC,
					},
				},
			},
			Filters: make([]sqltypes.OrFilter, 0),
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "labels", "beef.cattle.io/snort"},
						Order:  sqltypes.DESC,
					},
					{
						Fields: []string{"metadata", "labels", "steer"},
						Order:  sqltypes.ASC,
					},
					{
						Fields: []string{"metadata", "labels", "bossie.cattle.io/moo"},
						Order:  sqltypes.ASC,
					},
					{
						Fields: []string{"spec", "something"},
						Order:  sqltypes.ASC,
					},
				},
			},
			Filters: make([]sqltypes.OrFilter, 0),
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			Filters: make([]sqltypes.OrFilter, 0),
			Pagination: sqltypes.Pagination{
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
		expectedLO: sqltypes.ListOptions{
			Filters: make([]sqltypes.OrFilter, 0),
			Pagination: sqltypes.Pagination{
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
				assert.NotNil(t, err)
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
