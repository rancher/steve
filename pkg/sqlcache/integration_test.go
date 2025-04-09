package sql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/rancher/steve/pkg/sqlcache/informer"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	"github.com/rancher/steve/pkg/sqlcache/partition"
)

const testNamespace = "sql-test"

var defaultPartition = partition.Partition{
	All: true,
}

type IntegrationSuite struct {
	suite.Suite
	testEnv   envtest.Environment
	clientset kubernetes.Clientset
	restCfg   rest.Config
}

func (i *IntegrationSuite) SetupSuite() {
	i.testEnv = envtest.Environment{}
	restCfg, err := i.testEnv.Start()
	i.Require().NoError(err, "error when starting env test - this is likely because setup-envtest wasn't done. Check the README for more information")
	i.restCfg = *restCfg
	clientset, err := kubernetes.NewForConfig(restCfg)
	i.Require().NoError(err)
	i.clientset = *clientset
	testNs := v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: testNamespace,
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	_, err = i.clientset.CoreV1().Namespaces().Create(ctx, &testNs, metav1.CreateOptions{})
	i.Require().NoError(err)
}

func (i *IntegrationSuite) TearDownSuite() {
	err := i.testEnv.Stop()
	i.Require().NoError(err)
}

func (i *IntegrationSuite) TestSQLCacheFilters() {
	fields := [][]string{{"metadata", "annotations", "somekey"}}
	require := i.Require()
	configMapWithAnnotations := func(name string, annotations map[string]string) v1.ConfigMap {
		return v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Namespace:   testNamespace,
				Annotations: annotations,
			},
		}
	}
	createConfigMaps := func(configMaps ...v1.ConfigMap) {
		for _, configMap := range configMaps {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			configMapClient := i.clientset.CoreV1().ConfigMaps(testNamespace)
			_, err := configMapClient.Create(ctx, &configMap, metav1.CreateOptions{})
			require.NoError(err)
			// avoiding defer in a for loop
			cancel()
		}
	}

	// we create some configmaps before the cache starts and some after so that we can test the initial list and
	// subsequent watches to make sure both work
	// matches the filter for somekey == somevalue
	matches := configMapWithAnnotations("matches-filter", map[string]string{"somekey": "somevalue"})
	// partial match for somekey == somevalue (different suffix)
	partialMatches := configMapWithAnnotations("partial-matches", map[string]string{"somekey": "somevaluehere"})
	specialCharacterMatch := configMapWithAnnotations("special-character-matches", map[string]string{"somekey": "c%%l_value"})
	backSlashCharacterMatch := configMapWithAnnotations("backslash-character-matches", map[string]string{"somekey": `my\windows\path`})
	createConfigMaps(matches, partialMatches, specialCharacterMatch, backSlashCharacterMatch)

	cache, cacheFactory, err := i.createCacheAndFactory(fields, nil)
	require.NoError(err)
	defer cacheFactory.Reset()

	// doesn't match the filter for somekey == somevalue
	notMatches := configMapWithAnnotations("not-matches-filter", map[string]string{"somekey": "notequal"})
	// has no annotations, shouldn't match any filter
	missing := configMapWithAnnotations("missing", nil)
	createConfigMaps(notMatches, missing)

	configMapNames := []string{matches.Name, partialMatches.Name, notMatches.Name, missing.Name, specialCharacterMatch.Name, backSlashCharacterMatch.Name}
	err = i.waitForCacheReady(configMapNames, testNamespace, cache)
	require.NoError(err)

	orFiltersForFilters := func(filters ...informer.Filter) []informer.OrFilter {
		return []informer.OrFilter{
			{
				Filters: filters,
			},
		}
	}
	tests := []struct {
		name      string
		filters   []informer.OrFilter
		wantNames []string
	}{
		{
			name: "matches filter",
			filters: orFiltersForFilters(informer.Filter{
				Field:   []string{"metadata", "annotations", "somekey"},
				Matches: []string{"somevalue"},
				Op:      informer.Eq,
				Partial: false,
			}),
			wantNames: []string{"matches-filter"},
		},
		{
			name: "partial matches filter",
			filters: orFiltersForFilters(informer.Filter{
				Field:   []string{"metadata", "annotations", "somekey"},
				Matches: []string{"somevalue"},
				Op:      informer.Eq,
				Partial: true,
			}),
			wantNames: []string{"matches-filter", "partial-matches"},
		},
		{
			name: "no matches for filter with underscore as it is interpreted literally",
			filters: orFiltersForFilters(informer.Filter{
				Field:   []string{"metadata", "annotations", "somekey"},
				Matches: []string{"somevalu_"},
				Op:      informer.Eq,
				Partial: true,
			}),
			wantNames: nil,
		},
		{
			name: "no matches for filter with percent sign as it is interpreted literally",
			filters: orFiltersForFilters(informer.Filter{
				Field:   []string{"metadata", "annotations", "somekey"},
				Matches: []string{"somevalu%"},
				Op:      informer.Eq,
				Partial: true,
			}),
			wantNames: nil,
		},
		{
			name: "match with special characters",
			filters: orFiltersForFilters(informer.Filter{
				Field:   []string{"metadata", "annotations", "somekey"},
				Matches: []string{"c%%l_value"},
				Op:      informer.Eq,
				Partial: true,
			}),
			wantNames: []string{"special-character-matches"},
		},
		{
			name: "match with literal backslash character",
			filters: orFiltersForFilters(informer.Filter{
				Field:   []string{"metadata", "annotations", "somekey"},
				Matches: []string{`my\windows\path`},
				Op:      informer.Eq,
				Partial: true,
			}),
			wantNames: []string{"backslash-character-matches"},
		},
		{
			name: "not eq filter",
			filters: orFiltersForFilters(informer.Filter{
				Field:   []string{"metadata", "annotations", "somekey"},
				Matches: []string{"somevalue"},
				Op:      informer.NotEq,
				Partial: false,
			}),
			wantNames: []string{"partial-matches", "not-matches-filter", "missing", "special-character-matches", "backslash-character-matches"},
		},
		{
			name: "partial not eq filter",
			filters: orFiltersForFilters(informer.Filter{
				Field:   []string{"metadata", "annotations", "somekey"},
				Matches: []string{"somevalue"},
				Op:      informer.NotEq,
				Partial: true,
			}),
			wantNames: []string{"not-matches-filter", "missing", "special-character-matches", "backslash-character-matches"},
		},
		{
			name: "multiple or filters match",
			filters: orFiltersForFilters(
				informer.Filter{
					Field:   []string{"metadata", "annotations", "somekey"},
					Matches: []string{"somevalue"},
					Op:      informer.Eq,
					Partial: true,
				},
				informer.Filter{
					Field:   []string{"metadata", "annotations", "somekey"},
					Matches: []string{"notequal"},
					Op:      informer.Eq,
					Partial: false,
				},
			),
			wantNames: []string{"matches-filter", "partial-matches", "not-matches-filter"},
		},
		{
			name: "or filters on different fields",
			filters: orFiltersForFilters(
				informer.Filter{
					Field:   []string{"metadata", "annotations", "somekey"},
					Matches: []string{"somevalue"},
					Op:      informer.Eq,
					Partial: true,
				},
				informer.Filter{
					Field:   []string{`metadata`, `name`},
					Matches: []string{"missing"},
					Op:      informer.Eq,
					Partial: false,
				},
			),
			wantNames: []string{"matches-filter", "partial-matches", "missing"},
		},
		{
			name: "and filters, both must match",
			filters: []informer.OrFilter{
				{
					Filters: []informer.Filter{
						{
							Field:   []string{"metadata", "annotations", "somekey"},
							Matches: []string{"somevalue"},
							Op:      informer.Eq,
							Partial: true,
						},
					},
				},
				{
					Filters: []informer.Filter{
						{
							Field:   []string{`metadata`, `name`},
							Matches: []string{"matches-filter"},
							Op:      informer.Eq,
							Partial: false,
						},
					},
				},
			},
			wantNames: []string{"matches-filter"},
		},
		{
			name: "no matches",
			filters: orFiltersForFilters(
				informer.Filter{
					Field:   []string{"metadata", "annotations", "somekey"},
					Matches: []string{"valueNotRepresented"},
					Op:      informer.Eq,
					Partial: false,
				},
			),
			wantNames: []string{},
		},
	}

	for _, test := range tests {
		test := test
		i.Run(test.name, func() {
			options := informer.ListOptions{
				Filters: test.filters,
			}
			partitions := []partition.Partition{defaultPartition}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()
			cfgMaps, total, continueToken, err := cache.ListByOptions(ctx, &options, partitions, testNamespace)
			i.Require().NoError(err)
			// since there's no additional pages, the continue token should be empty
			i.Require().Equal("", continueToken)
			i.Require().NotNil(cfgMaps)
			// assert instead of require so that we can see the full evaluation of # of resources returned
			i.Assert().Equal(len(test.wantNames), total)
			i.Assert().Len(cfgMaps.Items, len(test.wantNames))
			requireNames := sets.Set[string]{}
			requireNames.Insert(test.wantNames...)
			gotNames := sets.Set[string]{}
			for _, configMap := range cfgMaps.Items {
				gotNames.Insert(configMap.GetName())
			}
			i.Require().True(requireNames.Equal(gotNames), "wanted %v, got %v", requireNames, gotNames)
		})
	}
}

func (i *IntegrationSuite) createCacheAndFactory(fields [][]string, transformFunc cache.TransformFunc) (*factory.Cache, *factory.CacheFactory, error) {
	cacheFactory, err := factory.NewCacheFactory()
	if err != nil {
		return nil, nil, fmt.Errorf("unable to make factory: %w", err)
	}
	dynamicClient, err := dynamic.NewForConfig(&i.restCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to make dynamicClient: %w", err)
	}
	configMapGVK := schema.GroupVersionKind{
		Group:   "",
		Version: "v1",
		Kind:    "ConfigMap",
	}
	configMapGVR := schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "configmaps",
	}
	dynamicResource := dynamicClient.Resource(configMapGVR).Namespace(testNamespace)
	cache, err := cacheFactory.CacheFor(context.Background(), fields, transformFunc, dynamicResource, configMapGVK, true, true)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to make cache: %w", err)
	}
	return &cache, cacheFactory, nil
}

func (i *IntegrationSuite) waitForCacheReady(readyResourceNames []string, namespace string, cache *factory.Cache) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	return wait.PollUntilContextCancel(ctx, time.Millisecond*100, true, func(ctx context.Context) (done bool, err error) {
		var options informer.ListOptions
		partitions := []partition.Partition{defaultPartition}
		cacheCtx, cacheCancel := context.WithTimeout(ctx, time.Second*5)
		defer cacheCancel()
		currentResources, total, _, err := cache.ListByOptions(cacheCtx, &options, partitions, namespace)
		if err != nil {
			// note that we don't return the error since that would stop the polling
			return false, nil
		}
		if total != len(readyResourceNames) {
			return false, nil
		}
		wantNames := sets.Set[string]{}
		wantNames.Insert(readyResourceNames...)
		gotNames := sets.Set[string]{}
		for _, current := range currentResources.Items {
			name := current.GetName()
			if !wantNames.Has(name) {
				return true, fmt.Errorf("got resource %s which wasn't expected", name)
			}
			gotNames.Insert(name)
		}
		return wantNames.Equal(gotNames), nil
	})
}

func TestIntegrationSuite(t *testing.T) {
	suite.Run(t, new(IntegrationSuite))
}
