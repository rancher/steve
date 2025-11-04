package listprocessor

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:generate go tool -modfile ../../../../gotools/mockgen/go.mod mockgen --build_flags=--mod=mod -package listprocessor -destination ./proxy_mocks_test.go github.com/rancher/steve/pkg/stores/sqlproxy Cache

func TestParseQuery(t *testing.T) {
	type testCase struct {
		description string
		req         *types.APIRequest
		gvKind      string // This is to distinguish Namespace projectornamespaces from others
		expectedLO  sqltypes.ListOptions
		errExpected bool
		errorText   string
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
		description: "ParseQuery() with only projectsornamespaces should return a project/ns filter.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "projectsornamespaces=somethin"},
			},
		},
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{},
			ProjectsOrNamespaces: sqltypes.OrFilter{
				Filters: []sqltypes.Filter{
					{
						Field:   []string{"metadata", "name"},
						Matches: []string{"somethin"},
						Op:      sqltypes.In,
					},
					{
						Field:   []string{"metadata", "labels", "field.cattle.io/projectId"},
						Matches: []string{"somethin"},
						Op:      sqltypes.In,
					},
				},
			},
			Pagination: sqltypes.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with only projectsornamespaces on a namespace GVK should return a standard filter.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "projectsornamespaces=elm&filter=metadata.name~beech"},
			},
		},
		gvKind: "Namespace",
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"metadata", "name"},
							Matches: []string{"beech"},
							Op:      sqltypes.Eq,
							Partial: true,
						},
					},
				},
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"metadata", "name"},
							Matches: []string{"elm"},
							Op:      sqltypes.In,
						},
						{
							Field:   []string{"metadata", "labels", "field.cattle.io/projectId"},
							Matches: []string{"elm"},
							Op:      sqltypes.In,
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
		description: "ParseQuery() with only a negative projectsornamespaces should return a project/ns filter.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "projectsornamespaces!=somethin"},
			},
		},
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{},
			ProjectsOrNamespaces: sqltypes.OrFilter{
				Filters: []sqltypes.Filter{
					{
						Field:   []string{"metadata", "name"},
						Matches: []string{"somethin"},
						Op:      sqltypes.NotIn,
					},
					{
						Field:   []string{"metadata", "labels", "field.cattle.io/projectId"},
						Matches: []string{"somethin"},
						Op:      sqltypes.NotIn,
					},
				},
			},
			Pagination: sqltypes.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with only negative projectsornamespaces on a namespace GVK should return a standard filter.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "projectsornamespaces!=elm&filter=metadata.name~beech"},
			},
		},
		gvKind: "Namespace",
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"metadata", "name"},
							Matches: []string{"beech"},
							Op:      sqltypes.Eq,
							Partial: true,
						},
					},
				},
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"metadata", "name"},
							Matches: []string{"elm"},
							Op:      sqltypes.NotIn,
						},
					},
				},
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"metadata", "labels", "field.cattle.io/projectId"},
							Matches: []string{"elm"},
							Op:      sqltypes.NotIn,
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
		description: "ParseQuery() with revision query param",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "revision=3400"},
			},
		},
		expectedLO: sqltypes.ListOptions{
			Revision: "3400",
			Filters:  []sqltypes.OrFilter{},
			Pagination: sqltypes.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() with wrong revision query param",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "revision=invalid"},
			},
		},
		expectedLO: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{},
			Pagination: sqltypes.Pagination{
				Page: 1,
			},
		},
		errExpected: true,
		errorText:   "invalid revision query param 400: value invalid for revision query param is not valid",
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
		description: "ParseQuery() should sort on the specified sort option",
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
		description: "ParseQuery() sort option should sort in descending order when a sort param has a hyphen prefix.",
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
		description: "ParseQuery() with no errors: If two sort params are given, recognize ASC/DESC and map ip(field) to SortAsIP:true.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "sort=-metadata.name,ip(spec.something)"},
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
						Fields:   []string{"spec", "something"},
						Order:    sqltypes.ASC,
						SortAsIP: true,
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
	})
	tests = append(tests, testCase{
		description: "ParseQuery() should honor page params.",
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
		description: "ParseQuery() should honor pagesize param",
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
	tests = append(tests, testCase{
		description: "ParseQuery() should process summary parameter",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "summary=metadata.state.name,metadata.labels.status,metadata.labels[rye/rye/rocco],spec.containers.image[2]"},
			},
		},
		expectedLO: sqltypes.ListOptions{
			SummaryFieldList: sqltypes.SummaryFieldList{
				[]string{"metadata", "state", "name"},
				[]string{"metadata", "labels", "status"},
				[]string{"metadata", "labels", "rye/rye/rocco"},
				[]string{"spec", "containers", "image", "2"},
			},
			Filters: make([]sqltypes.OrFilter, 0),
			Pagination: sqltypes.Pagination{
				Page: 1,
			},
		},
	})
	tests = append(tests, testCase{
		description: "ParseQuery() should complain when more than one summary is given",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "summary=metadata.state.name&summary=metadata.namespace"},
			},
		},
		errExpected: true,
		errorText:   "got 2 summary parameters, at most 1 is allowed",
	})
	tests = append(tests, testCase{
		description: "ParseQuery() should complain when summary is given with a filter",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "filter=metadata.name=a&summary=metadata.namespace"},
			},
		},
		errExpected: true,
		errorText:   "unable to parse requirement: summary parameters can't appear with other parameters (filter)",
	})
	tests = append(tests, testCase{
		description: "ParseQuery() should complain when summary is given with a sort param",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "sort=metadata.name=a&summary=metadata.namespace"},
			},
		},
		errExpected: true,
		errorText:   "unable to parse requirement: summary parameters can't appear with other parameters (sort)",
	})
	tests = append(tests, testCase{
		description: "ParseQuery() should complain when summary is given with a page param",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "summary=metadata.namespace&page=5"},
			},
		},
		errExpected: true,
		errorText:   "unable to parse requirement: summary parameters can't appear with other parameters (page)",
	})
	tests = append(tests, testCase{
		description: "ParseQuery() should complain when summary is given with a pagesize param",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "pagesize=6&summary=metadata.namespace"},
			},
		},
		errExpected: true,
		errorText:   "unable to parse requirement: summary parameters can't appear with other parameters (pagesize)",
	})
	tests = append(tests, testCase{
		description: "ParseQuery() should complain when summary is given with no fields to summarize",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "summary="},
			},
		},
		errExpected: true,
		errorText:   "unable to parse requirement: empty summary parameter doesn't make sense",
	})
	tests = append(tests, testCase{
		description: "ParseQuery() should complain when summary is given with only commas",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "summary=,,,,"},
			},
		},
		errExpected: true,
		errorText:   "unable to parse requirement: empty summary parameter doesn't make sense",
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			lo, err := ParseQuery(test.req, test.gvKind)
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
