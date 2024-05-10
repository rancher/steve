package listprocessor_alpha

import (
	"github.com/golang/mock/gomock"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/lasso/pkg/cache/sql/informer"
	"github.com/rancher/lasso/pkg/cache/sql/partition"
	"net/http"
	"net/url"
	"testing"
)

//go:generate mockgen --build_flags=--mod=mod -package listprocessor_alpha -destination ./proxy_mocks_test.go github.com/rancher/steve/pkg/stores/proxy_alpha Informer

func TestParseQuery(t *testing.T) {
	type testCase struct {
		description     string
		setupNSInformer func() Informer
		nsi             Informer
		req             *types.APIRequest
		expectedLO      informer.ListOptions
	}
	var tests []testCase
	tests = append(tests, testCase{
		description: "List() with no errors returned should returned no errors. Should have empty reivsion, count " +
			"should match number of items in list, and id should include namespace (if applicable) and name, separated" +
			" by a '/'.",
		req: &types.APIRequest{
			Request: &http.Request{
				URL: &url.URL{RawQuery: "projectsornamespaces=somethin"},
			},
		},
		setupNSInformer: func() Informer {
			nsi := NewMockInformer(gomock.NewController(t))
			nsi.ListByOptions(nil, informer.ListOptions{
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
			}, []partition.Partition{{Passthrough: true}}, "")
			return nsi
		},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			test.nsi = test.setupNSInformer()
			ParseQuery(test.req, test.nsi)
		})
	}
}
