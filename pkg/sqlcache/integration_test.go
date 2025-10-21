package sql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/steve/pkg/client"
	"github.com/rancher/steve/pkg/clustercache"
	schemacontroller "github.com/rancher/steve/pkg/controllers/schema"
	"github.com/rancher/steve/pkg/resources"
	"github.com/rancher/steve/pkg/resources/common"
	"github.com/rancher/steve/pkg/schema"
	"github.com/rancher/steve/pkg/schema/definitions"
	"github.com/rancher/steve/pkg/server"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/rancher/steve/pkg/sqlcache/schematracker"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
	"github.com/rancher/steve/pkg/stores/sqlproxy"
	"github.com/rancher/steve/pkg/summarycache"
	"github.com/stretchr/testify/suite"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

const testNamespace = "sql-test"

var defaultPartition = partition.Partition{
	All: true,
}

type IntegrationSuite struct {
	suite.Suite
	testEnv   envtest.Environment
	clientset kubernetes.Clientset
	restCfg   *rest.Config
}

func (i *IntegrationSuite) SetupSuite() {
	i.testEnv = envtest.Environment{}
	restCfg, err := i.testEnv.Start()
	i.Require().NoError(err, "error when starting env test - this is likely because setup-envtest wasn't done. Check the README for more information")
	i.restCfg = restCfg
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
	fields := [][]string{{"id"}, {"metadata", "annotations", "somekey"}}
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
	defer func() {
		cacheFactory.DoneWithCache(cache)
		cacheFactory.Stop(cache.GVK())
	}()

	// doesn't match the filter for somekey == somevalue
	notMatches := configMapWithAnnotations("not-matches-filter", map[string]string{"somekey": "notequal"})
	// has no annotations, shouldn't match any filter
	missing := configMapWithAnnotations("missing", nil)
	createConfigMaps(notMatches, missing)

	configMapNames := []string{matches.Name, partialMatches.Name, notMatches.Name, missing.Name, specialCharacterMatch.Name, backSlashCharacterMatch.Name}
	err = i.waitForCacheReady(configMapNames, testNamespace, cache)
	require.NoError(err)

	orFiltersForFilters := func(filters ...sqltypes.Filter) []sqltypes.OrFilter {
		return []sqltypes.OrFilter{
			{
				Filters: filters,
			},
		}
	}
	tests := []struct {
		name      string
		filters   []sqltypes.OrFilter
		wantNames []string
	}{
		{
			name: "matches filter",
			filters: orFiltersForFilters(sqltypes.Filter{
				Field:   []string{"metadata", "annotations", "somekey"},
				Matches: []string{"somevalue"},
				Op:      sqltypes.Eq,
				Partial: false,
			}),
			wantNames: []string{"matches-filter"},
		},
		{
			name: "partial matches filter",
			filters: orFiltersForFilters(sqltypes.Filter{
				Field:   []string{"metadata", "annotations", "somekey"},
				Matches: []string{"somevalue"},
				Op:      sqltypes.Eq,
				Partial: true,
			}),
			wantNames: []string{"matches-filter", "partial-matches"},
		},
		{
			name: "no matches for filter with underscore as it is interpreted literally",
			filters: orFiltersForFilters(sqltypes.Filter{
				Field:   []string{"metadata", "annotations", "somekey"},
				Matches: []string{"somevalu_"},
				Op:      sqltypes.Eq,
				Partial: true,
			}),
			wantNames: nil,
		},
		{
			name: "no matches for filter with percent sign as it is interpreted literally",
			filters: orFiltersForFilters(sqltypes.Filter{
				Field:   []string{"metadata", "annotations", "somekey"},
				Matches: []string{"somevalu%"},
				Op:      sqltypes.Eq,
				Partial: true,
			}),
			wantNames: nil,
		},
		{
			name: "match with special characters",
			filters: orFiltersForFilters(sqltypes.Filter{
				Field:   []string{"metadata", "annotations", "somekey"},
				Matches: []string{"c%%l_value"},
				Op:      sqltypes.Eq,
				Partial: true,
			}),
			wantNames: []string{"special-character-matches"},
		},
		{
			name: "match with literal backslash character",
			filters: orFiltersForFilters(sqltypes.Filter{
				Field:   []string{"metadata", "annotations", "somekey"},
				Matches: []string{`my\windows\path`},
				Op:      sqltypes.Eq,
				Partial: true,
			}),
			wantNames: []string{"backslash-character-matches"},
		},
		{
			name: "not eq filter",
			filters: orFiltersForFilters(sqltypes.Filter{
				Field:   []string{"metadata", "annotations", "somekey"},
				Matches: []string{"somevalue"},
				Op:      sqltypes.NotEq,
				Partial: false,
			}),
			wantNames: []string{"partial-matches", "not-matches-filter", "missing", "special-character-matches", "backslash-character-matches"},
		},
		{
			name: "partial not eq filter",
			filters: orFiltersForFilters(sqltypes.Filter{
				Field:   []string{"metadata", "annotations", "somekey"},
				Matches: []string{"somevalue"},
				Op:      sqltypes.NotEq,
				Partial: true,
			}),
			wantNames: []string{"not-matches-filter", "missing", "special-character-matches", "backslash-character-matches"},
		},
		{
			name: "multiple or filters match",
			filters: orFiltersForFilters(
				sqltypes.Filter{
					Field:   []string{"metadata", "annotations", "somekey"},
					Matches: []string{"somevalue"},
					Op:      sqltypes.Eq,
					Partial: true,
				},
				sqltypes.Filter{
					Field:   []string{"metadata", "annotations", "somekey"},
					Matches: []string{"notequal"},
					Op:      sqltypes.Eq,
					Partial: false,
				},
			),
			wantNames: []string{"matches-filter", "partial-matches", "not-matches-filter"},
		},
		{
			name: "or filters on different fields",
			filters: orFiltersForFilters(
				sqltypes.Filter{
					Field:   []string{"metadata", "annotations", "somekey"},
					Matches: []string{"somevalue"},
					Op:      sqltypes.Eq,
					Partial: true,
				},
				sqltypes.Filter{
					Field:   []string{`metadata`, `name`},
					Matches: []string{"missing"},
					Op:      sqltypes.Eq,
					Partial: false,
				},
			),
			wantNames: []string{"matches-filter", "partial-matches", "missing"},
		},
		{
			name: "and filters, both must match",
			filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"metadata", "annotations", "somekey"},
							Matches: []string{"somevalue"},
							Op:      sqltypes.Eq,
							Partial: true,
						},
					},
				},
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{`metadata`, `name`},
							Matches: []string{"matches-filter"},
							Op:      sqltypes.Eq,
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
				sqltypes.Filter{
					Field:   []string{"metadata", "annotations", "somekey"},
					Matches: []string{"valueNotRepresented"},
					Op:      sqltypes.Eq,
					Partial: false,
				},
			),
			wantNames: []string{},
		},
	}

	for _, test := range tests {
		test := test
		i.Run(test.name, func() {
			options := sqltypes.ListOptions{
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
	cacheFactory, err := factory.NewCacheFactory(factory.CacheFactoryOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("unable to make factory: %w", err)
	}
	dynamicClient, err := dynamic.NewForConfig(i.restCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to make dynamicClient: %w", err)
	}
	configMapGVK := k8sschema.GroupVersionKind{
		Group:   "",
		Version: "v1",
		Kind:    "ConfigMap",
	}
	configMapGVR := k8sschema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "configmaps",
	}
	dynamicResource := dynamicClient.Resource(configMapGVR).Namespace(testNamespace)
	typeGuidance := map[string]string{}
	cache, err := cacheFactory.CacheFor(context.Background(), fields, nil, nil, transformFunc, dynamicResource, configMapGVK, typeGuidance, true, true)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to make cache: %w", err)
	}
	return cache, cacheFactory, nil
}

func (i *IntegrationSuite) waitForCacheReady(readyResourceNames []string, namespace string, cache *factory.Cache) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	return wait.PollUntilContextCancel(ctx, time.Millisecond*100, true, func(ctx context.Context) (done bool, err error) {
		var options sqltypes.ListOptions
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

func (i *IntegrationSuite) TestProxyStore() {
	ctx, cancel := context.WithCancel(i.T().Context())
	defer cancel()

	require := i.Require()
	cols, err := common.NewDynamicColumns(i.restCfg)
	require.NoError(err)

	cf, err := client.NewFactory(i.restCfg, false)
	require.NoError(err)

	baseSchemas := types.EmptyAPISchemas()

	ccache := clustercache.NewClusterCache(ctx, cf.AdminDynamicClient())

	ctrl, err := server.NewController(i.restCfg, nil)
	require.NoError(err)

	asl := accesscontrol.NewAccessStore(ctx, true, ctrl.RBAC)
	sf := schema.NewCollection(ctx, baseSchemas, asl)

	err = resources.DefaultSchemas(ctx, baseSchemas, ccache, cf, sf, "")
	require.NoError(err)

	definitions.Register(ctx, baseSchemas, ctrl.K8s.Discovery(),
		ctrl.CRD.CustomResourceDefinition(), ctrl.API.APIService())

	summaryCache := summarycache.New(sf, ccache)
	summaryCache.Start(ctx)

	cacheFactory, err := factory.NewCacheFactoryWithContext(ctx, factory.CacheFactoryOptions{})
	require.NoError(err)

	proxyStore, err := sqlproxy.NewProxyStore(ctx, cols, cf, summaryCache, summaryCache, cacheFactory, true)
	require.NoError(err)
	require.NotNil(proxyStore)

	sqlSchemaTracker := schematracker.NewSchemaTracker(proxyStore)

	onSchemasHandler := func(schemas *schema.Collection) error {
		var retErr error

		err := ccache.OnSchemas(schemas)
		retErr = errors.Join(retErr, err)

		err = sqlSchemaTracker.OnSchemas(schemas)
		retErr = errors.Join(retErr, err)

		return retErr
	}
	schemacontroller.Register(ctx,
		cols,
		ctrl.K8s.Discovery(),
		ctrl.CRD.CustomResourceDefinition(),
		ctrl.API.APIService(),
		ctrl.K8s.AuthorizationV1().SelfSubjectAccessReviews(),
		onSchemasHandler,
		sf)

	err = ctrl.Start(ctx)
	require.NoError(err)

	foo := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "tests.test.cattle.io"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Scope: "Cluster",
			Group: "test.cattle.io",
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Kind:     "Test",
				ListKind: "TestList",
				Plural:   "tests",
				Singular: "test",
			},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name: "v1",
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
							Type: "object",
							Properties: map[string]apiextensionsv1.JSONSchemaProps{
								"bar": {
									Type: "string",
								},
								"foo": {
									Type: "string",
								},
							},
						},
					},
					Served:  true,
					Storage: true,
				},
			},
		},
	}

	foo, err = ctrl.CRD.CustomResourceDefinition().Create(foo)
	require.NoError(err)

	defer func() {
		err := ctrl.CRD.CustomResourceDefinition().Delete(foo.Name, &metav1.DeleteOptions{})
		require.NoError(err)
	}()

	time.Sleep(3 * time.Second)

	testSchema := sf.Schema("test.cattle.io.test")

	req, err := http.NewRequest("GET", "http://localhost:8080", nil)
	require.NoError(err)

	apiOp := &types.APIRequest{
		Request: req,
	}
	ch, err := proxyStore.Watch(apiOp, testSchema, types.WatchRequest{})
	require.NoError(err)
	require.NotNil(ch)

	fooWithColumn := foo.DeepCopy()
	fooWithColumn.Spec.Versions[0].AdditionalPrinterColumns = []apiextensionsv1.CustomResourceColumnDefinition{
		{
			Name:     "Foo",
			Type:     "string",
			JSONPath: ".foo",
		},
	}
	patch, err := createPatch(foo, fooWithColumn)
	require.NoError(err, "creating patch")

	_, err = ctrl.CRD.CustomResourceDefinition().Patch(foo.Name, k8stypes.MergePatchType, patch)
	require.NoError(err)

	select {
	case _, ok := <-ch:
		if ok {
			require.Fail("channel should be closed with no events")
		}
	case <-time.After(2 * time.Second):
		require.Fail("expected channel to be closed")
	}
}

func createPatch(oldCRD, newCRD *apiextensionsv1.CustomResourceDefinition) ([]byte, error) {
	oldCRDRaw, err := json.Marshal(oldCRD)
	if err != nil {
		return nil, err
	}
	newCRDRaw, err := json.Marshal(newCRD)
	if err != nil {
		return nil, err
	}

	patch, err := jsonpatch.CreateMergePatch(oldCRDRaw, newCRDRaw)
	if err != nil {
		return nil, err
	}
	return patch, nil
}

func TestIntegrationSuite(t *testing.T) {
	suite.Run(t, new(IntegrationSuite))
}
