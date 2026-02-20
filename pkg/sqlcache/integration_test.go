package sql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/go-logr/logr"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

const defaultTestNamespace = "sql-test"
const testLabel = "capitals.cattle.io/test"
const mdTestLabel = "metadata.labels[" + testLabel + "]"
const testFilter = "filter=" + mdTestLabel

var defaultPartition = partition.Partition{
	All: true,
}

// Always use a filter-test instead of a namespace test because some of the resources we care about
// in this test don't have namespaces, but all resources have labels.
func getFilteredQuery(query string, labelTest string) string {
	if strings.Contains(query, mdTestLabel+"=") {
		return query
	}
	continuationToken := "&"
	if len(query) == 0 {
		continuationToken = ""
	}
	return fmt.Sprintf("%s%s%s=%s", query, continuationToken, testFilter, labelTest)

}

type IntegrationSuite struct {
	suite.Suite
	testEnv   envtest.Environment
	clientset kubernetes.Clientset
	restCfg   *rest.Config
}

func (i *IntegrationSuite) SetupSuite() {
	i.testEnv = envtest.Environment{
		CRDDirectoryPaths: []string{"testdata/crds"},
	}
	restCfg, err := i.testEnv.Start()
	i.Require().NoError(err, "error when starting env test - this is likely because setup-envtest wasn't done. Check the README for more information")
	i.restCfg = restCfg
	clientset, err := kubernetes.NewForConfig(restCfg)
	i.Require().NoError(err)
	i.clientset = *clientset
	testNs := v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: defaultTestNamespace,
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

func createMCIO(ctx context.Context, client dynamic.ResourceInterface, gvr k8sschema.GroupVersionResource, name, thisTestLabel, cpu, memory string, podCount int) error {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": gvr.Group + "/" + gvr.Version,
			"kind":       "Cluster",
			"metadata": map[string]interface{}{
				"name": name,
				"labels": map[string]interface{}{
					testLabel: thisTestLabel,
				},
			},
			"status": map[string]interface{}{
				"allocatable": map[string]interface{}{
					"cpu":    cpu,
					"memory": memory,
					"pods":   fmt.Sprintf("%d", podCount),
				},
				"conditions": []interface{}{
					map[string]interface{}{
						"type":   "Ready",
						"status": "True",
					},
				},
			},
		},
	}
	newObj, err := client.Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	// This call is needed to get the above status fields stored in the object
	_, err = client.Update(ctx, newObj, metav1.UpdateOptions{})
	return err
}

func createMCIOProject(ctx context.Context, client dynamic.ResourceInterface, gvr k8sschema.GroupVersionResource, thisTestLabel, name, clusterName, displayName string, otherLabels map[string]string) error {
	labels := map[string]interface{}{
		testLabel: thisTestLabel,
	}
	if otherLabels != nil {
		for k, v := range otherLabels {
			labels[k] = v
		}
	}
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": gvr.Group + "/" + gvr.Version,
			"kind":       "Project",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": defaultTestNamespace,
				"labels":    labels,
			},
			"spec": map[string]interface{}{
				"clusterName": clusterName,
				"displayName": displayName,
			},
		},
	}
	_, err := client.Create(ctx, obj, metav1.CreateOptions{})
	return err
}

func createPCIO(ctx context.Context, client dynamic.ResourceInterface, gvr k8sschema.GroupVersionResource, name, thisTestLabel, clusterName string) error {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": gvr.Group + "/" + gvr.Version,
			"kind":       "Cluster",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": defaultTestNamespace,
				"labels": map[string]interface{}{
					testLabel: thisTestLabel,
				},
			},
		},
	}
	newObj, err := client.Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	unstructured.SetNestedField(newObj.Object, clusterName, "status", "clusterName")
	unstructured.SetNestedField(newObj.Object, true, "status", "ready")
	// This call is needed to get the above status fields stored in the object
	// PCIO CRDs have subresources, so we call `client.UpdateStatus` instead of `client.Update`
	_, err = client.UpdateStatus(ctx, newObj, metav1.UpdateOptions{})
	return err
}

func (i *IntegrationSuite) createNamespace(ctx context.Context, name string, thisTestLabel string, projectLabel string) error {
	obj := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"field.cattle.io/projectId": projectLabel,
				testLabel:                   thisTestLabel,
			},
		},
	}
	_, err := i.clientset.CoreV1().Namespaces().Create(ctx, obj, metav1.CreateOptions{})
	return err
}

func (i *IntegrationSuite) createSecret(ctx context.Context, name string, thisTestLabel string, projectLabel string, secretType string) error {
	obj := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: defaultTestNamespace,
			Labels: map[string]string{
				"management.cattle.io/project-scoped-secret": projectLabel,
				testLabel: thisTestLabel,
			},
		},
		Type: v1.SecretType(secretType),
	}
	_, err := i.clientset.CoreV1().Secrets(defaultTestNamespace).Create(ctx, obj, metav1.CreateOptions{})
	return err
}

func (i *IntegrationSuite) TestSQLCacheFilters() {
	fields := [][]string{{"id"}, {"metadata", "annotations", "somekey"}}
	require := i.Require()
	configMapWithAnnotations := func(name string, annotations map[string]string) v1.ConfigMap {
		return v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Namespace:   defaultTestNamespace,
				Annotations: annotations,
			},
		}
	}
	createConfigMaps := func(configMaps ...v1.ConfigMap) {
		for _, configMap := range configMaps {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			configMapClient := i.clientset.CoreV1().ConfigMaps(defaultTestNamespace)
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
	err = i.waitForCacheReady(configMapNames, defaultTestNamespace, cache)
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
			cfgMaps, total, _, continueToken, err := cache.ListByOptions(ctx, &options, partitions, defaultTestNamespace)
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
	dynamicResource := dynamicClient.Resource(configMapGVR).Namespace(defaultTestNamespace)
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
		currentResources, total, _, _, err := cache.ListByOptions(cacheCtx, &options, partitions, namespace)
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

func waitForObjectsBySchema(ctx context.Context, proxyStore *sqlproxy.Store, schema *types.APISchema, labelTest string, expectedNum int) error {
	q := getFilteredQuery("", labelTest)
	return wait.PollUntilContextCancel(ctx, time.Millisecond*100, true, func(ctx context.Context) (done bool, err error) {
		req, err := http.NewRequest("GET", "http://localhost:8080?"+q, nil)
		if err != nil {
			return false, err
		}
		apiOp := &types.APIRequest{
			Request: req,
		}
		partitions := []partition.Partition{defaultPartition}
		_, total, _, _, err := proxyStore.ListByPartitions(apiOp, schema, partitions)
		if err != nil {
			// note that we don't return the error since that would stop the polling
			return false, nil
		}
		return total == expectedNum, nil
	})
}

type ResetFunc func(k8sschema.GroupVersionKind) error

func (f ResetFunc) Reset(gvk k8sschema.GroupVersionKind) error {
	return f(gvk)
}

func (i *IntegrationSuite) TestProxyStore() {
	ctx, cancel := context.WithCancel(i.T().Context())
	defer cancel()

	requireT := i.Require()
	cols, err := common.NewDynamicColumns(i.restCfg)
	requireT.NoError(err)

	cf, err := client.NewFactory(i.restCfg, false)
	requireT.NoError(err)

	baseSchemas := types.EmptyAPISchemas()

	ccache := clustercache.NewClusterCache(ctx, cf.AdminDynamicClient())

	ctrl, err := server.NewController(i.restCfg, nil)
	requireT.NoError(err)

	asl := accesscontrol.NewAccessStore(ctx, true, ctrl.RBAC)
	sf := schema.NewCollection(ctx, baseSchemas, asl)

	err = resources.DefaultSchemas(ctx, baseSchemas, ccache, cf, sf, "")
	requireT.NoError(err)

	definitions.Register(ctx, baseSchemas, ctrl.K8s.Discovery(),
		ctrl.CRD.CustomResourceDefinition(), ctrl.API.APIService())

	summaryCache := summarycache.New(sf, ccache)
	summaryCache.Start(ctx)

	cacheFactory, err := factory.NewCacheFactoryWithContext(ctx, factory.CacheFactoryOptions{})
	requireT.NoError(err)

	proxyStore, err := sqlproxy.NewProxyStore(ctx, cols, cf, summaryCache, summaryCache, sf, cacheFactory, true)
	requireT.NoError(err)
	requireT.NotNil(proxyStore)

	resetCh := make(chan struct{}, 10)

	bananaGVK := k8sschema.GroupVersionKind{
		Group:   "fruits.cattle.io",
		Version: "v1",
		Kind:    "Banana",
	}

	sqlSchemaTracker := schematracker.NewSchemaTracker(ResetFunc(func(gvk k8sschema.GroupVersionKind) error {
		proxyStore.Reset(gvk)
		if gvk == bananaGVK {
			resetCh <- struct{}{}
		}
		return nil
	}))

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
	requireT.NoError(err)

	foo := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "bananas.fruits.cattle.io"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Scope: "Cluster",
			Group: bananaGVK.Group,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Kind:     bananaGVK.Kind,
				ListKind: "BananaList",
				Plural:   "bananas",
				Singular: "banana",
			},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name: bananaGVK.Version,
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
	requireT.NoError(err)

	defer func() {
		err := ctrl.CRD.CustomResourceDefinition().Delete(foo.Name, &metav1.DeleteOptions{})
		requireT.NoError(err)
	}()

	<-resetCh

	var (
		fruitsSchema *types.APISchema
		ch           chan watch.Event
	)
	requireT.EventuallyWithT(func(c *assert.CollectT) {
		fruitsSchema = sf.Schema("fruits.cattle.io.banana")
		require.NotNil(c, fruitsSchema)

		req, err := http.NewRequest("GET", "http://localhost:8080", nil)
		require.NoError(c, err)

		apiOp := &types.APIRequest{
			Request: req,
		}
		ch, err = proxyStore.Watch(apiOp, fruitsSchema, types.WatchRequest{})
		require.NoError(c, err)
		require.NotNil(c, ch)
	}, 15*time.Second, 500*time.Millisecond)

	fooWithColumn := foo.DeepCopy()
	fooWithColumn.Spec.Versions[0].AdditionalPrinterColumns = []apiextensionsv1.CustomResourceColumnDefinition{
		{
			Name:     "Foo",
			Type:     "string",
			JSONPath: ".foo",
		},
	}
	patch, err := createPatch(foo, fooWithColumn)
	requireT.NoError(err, "creating patch")

	_, err = ctrl.CRD.CustomResourceDefinition().Patch(foo.Name, k8stypes.MergePatchType, patch)
	requireT.NoError(err)

	<-resetCh

	select {
	case _, ok := <-ch:
		if ok {
			requireT.Fail("channel should be closed with no events")
		}
	case <-time.After(2 * time.Second):
		requireT.Fail("expected channel to be closed")
	}

	requireT.EventuallyWithT(func(c *assert.CollectT) {
		fruitsSchema = sf.Schema("fruits.cattle.io.banana")
		require.NotNil(c, fruitsSchema)

		req, err := http.NewRequest("GET", "http://localhost:8080", nil)
		require.NoError(c, err)

		apiOp := &types.APIRequest{
			Request: req,
		}
		ch, err = proxyStore.Watch(apiOp, fruitsSchema, types.WatchRequest{})
		require.NoError(c, err)
		require.NotNil(c, ch)
	}, 15*time.Second, 500*time.Millisecond)

	cancel()
	// Stop the registered refresher, otherwise we'll see an error
	time.Sleep(2 * time.Second)
}

func (i *IntegrationSuite) TestNamespaceProjectDependencies() {
	ctx, cancel := context.WithCancel(i.T().Context())
	defer cancel()
	requireT := i.Require()

	cols, ccache, ctrl, sf, proxyStore, err := i.setupTest(ctx)
	requireT.NoError(err)
	requireT.NotNil(proxyStore)
	labelTest := "NamespaceProjectDependencies"

	resetMCIOCh := make(chan struct{}, 10)

	mcioGVK := k8sschema.GroupVersionKind{
		Group:   "management.cattle.io",
		Version: "v3",
		Kind:    "Project",
	}
	mcioGVR := k8sschema.GroupVersionResource{
		Group:    "management.cattle.io",
		Version:  "v3",
		Resource: "projects",
	}

	sqlSchemaTracker := schematracker.NewSchemaTracker(ResetFunc(func(gvk k8sschema.GroupVersionKind) error {
		proxyStore.Reset(gvk)
		if gvk == mcioGVK {
			resetMCIOCh <- struct{}{}
		}
		return nil
	}))

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
	requireT.NoError(err)
	namespaceInfo := [][2]string{
		{"namibia", "windhoek"},
		{"togo", "lome"},
		{"nigeria", "abuja"},
		{"principe", "saotome"},
	}
	for _, info := range namespaceInfo {
		err = i.createNamespace(ctx, info[0], labelTest, info[1])
		requireT.NoError(err)
	}

	dynamicClient, err := dynamic.NewForConfig(i.restCfg)
	requireT.NoError(err)
	mcioClient := dynamicClient.Resource(mcioGVR).Namespace(defaultTestNamespace)
	mcioProjectInfo := [][2]string{
		{"windhoek", "afrikaans"},
		{"lome", "french"},
		{"abuja", "english"},
		{"saotome", "portuguese"},
	}
	for _, info := range mcioProjectInfo {
		err = createMCIOProject(ctx, mcioClient, mcioGVR, labelTest, info[0], "", info[1], nil)
		requireT.NoError(err)
	}

	var mcioSchema *types.APISchema
	requireT.EventuallyWithT(func(c *assert.CollectT) {
		mcioSchema = sf.Schema("management.cattle.io.project")
		require.NotNil(c, mcioSchema)
	}, 15*time.Second, 500*time.Millisecond)
	nsSchema := sf.Schema("namespace")

	tests := []struct {
		name      string
		query     string
		wantNames []string
	}{
		{
			name:  "sorts by name",
			query: "sort=metadata.name",
			wantNames: []string{
				"namibia",
				"nigeria",
				"principe",
				"togo",
			},
		},
		{
			name:  "sorts by field.cattle.io/projectId",
			query: "sort=metadata.labels[field.cattle.io/projectId],metadata.name",
			wantNames: []string{
				"nigeria",  // abuja
				"togo",     // lome
				"principe", // sao tome
				"namibia",  // windhoek
			},
		},
		{
			name:  "sorts by spec.displayName",
			query: "sort=spec.displayName,metadata.name",
			wantNames: []string{
				"namibia",  // afrikaans
				"nigeria",  // english
				"togo",     // french
				"principe", // portuguese
			},
		},
		{
			name:  "filter on spec.displayName",
			query: "filter=spec.displayName~en",
			wantNames: []string{
				"nigeria", // english
				"togo",    // french
			},
		},
	}

	err = waitForObjectsBySchema(ctx, proxyStore, mcioSchema, labelTest, len(mcioProjectInfo))
	requireT.NoError(err)
	err = waitForObjectsBySchema(ctx, proxyStore, nsSchema, labelTest, len(namespaceInfo))
	requireT.NoError(err)

	partitions := []partition.Partition{defaultPartition}
	for _, test := range tests {
		test := test
		i.Run(test.name, func() {
			q := getFilteredQuery(test.query, labelTest)
			req, err := http.NewRequest("GET", "http://localhost:8080?"+q, nil)
			requireT.NoError(err)
			apiOp := &types.APIRequest{
				Request: req,
			}

			got, total, _, continueToken, err := proxyStore.ListByPartitions(apiOp, nsSchema, partitions)
			if err != nil {
				i.Assert().NoError(err)
				return
			}
			i.Assert().Equal(len(test.wantNames), total)
			i.Assert().Equal("", continueToken)
			i.Assert().Len(got.Items, len(test.wantNames))
			gotNames := stringsFromULIst(got)
			i.Assert().Equal(test.wantNames, gotNames)
		})
	}
}

func (i *IntegrationSuite) TestSecretProjectDependencies() {
	ctx, cancel := context.WithCancel(i.T().Context())
	defer cancel()
	requireT := i.Require()
	labelTest := "SecretProjectDependencies"

	cols, ccache, ctrl, sf, proxyStore, err := i.setupTest(ctx)
	requireT.NoError(err)
	requireT.NotNil(proxyStore)

	resetMCIOCh := make(chan struct{}, 10)

	mcioGVK := k8sschema.GroupVersionKind{
		Group:   "management.cattle.io",
		Version: "v3",
		Kind:    "Project",
	}
	mcioGVR := k8sschema.GroupVersionResource{
		Group:    "management.cattle.io",
		Version:  "v3",
		Resource: "projects",
	}

	sqlSchemaTracker := schematracker.NewSchemaTracker(ResetFunc(func(gvk k8sschema.GroupVersionKind) error {
		proxyStore.Reset(gvk)
		if gvk == mcioGVK {
			resetMCIOCh <- struct{}{}
		}
		return nil
	}))

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
	requireT.NoError(err)
	secretInfo := [][3]string{
		{"morocco", "rabat", "france"},
		{"eritrea", "asmara", "italy"},
		{"kenya", "nairobi", "england"},
		{"benin", "portonovo", "france"},
	}
	projectInfo := [][3]string{
		{"rabat", "arabic", "casablanca"},
		{"asmara", "tigrinya", "keren"},
		{"nairobi", "english", "mombasa"},
		{"portonovo", "french", "cotonou"},
	}
	for _, info := range secretInfo {
		err = i.createSecret(ctx, info[0], labelTest, info[1], info[2])
		requireT.NoError(err)
	}
	dynamicClient, err := dynamic.NewForConfig(i.restCfg)
	requireT.NoError(err)
	mcioClient := dynamicClient.Resource(mcioGVR).Namespace(defaultTestNamespace)
	for _, info := range projectInfo {
		err = createMCIOProject(ctx, mcioClient, mcioGVR, labelTest, info[0], info[1], info[2], nil)
		requireT.NoError(err)
	}

	var mcioSchema *types.APISchema
	requireT.EventuallyWithT(func(c *assert.CollectT) {
		mcioSchema = sf.Schema("management.cattle.io.project")
		require.NotNil(c, mcioSchema)
	}, 15*time.Second, 500*time.Millisecond)
	secretSchema := sf.Schema("secret")

	tests := []struct {
		name      string
		query     string
		wantNames []string
	}{
		{
			name:  "sorts by name",
			query: "sort=metadata.name",
			wantNames: []string{
				"benin",
				"eritrea",
				"kenya",
				"morocco",
			},
		},
		{
			name:  "sorts by foreign-key label",
			query: "sort=metadata.labels[management.cattle.io/project-scoped-secret],metadata.name",
			wantNames: []string{
				"eritrea", // asmara
				"kenya",   // nairobi
				"benin",   // portonovo
				"morocco", // rabat
			},
		},
		{
			name:  "sorts by spec.displayName (other city)",
			query: "sort=spec.displayName,metadata.name",
			wantNames: []string{
				"morocco", // casablanca
				"benin",   // cotonou
				"eritrea", // keren
				"kenya",   // mombasa
			},
		},
		{
			name:  "sorts by spec.clusterName (language)",
			query: "sort=spec.clusterName,metadata.name",
			wantNames: []string{
				"morocco", // arabic
				"kenya",   // english
				"benin",   // french
				"eritrea", // tigrinya
			},
		},
		{
			name:  "sorts by type (colonizer)",
			query: "sort=_type,metadata.name",
			wantNames: []string{
				"kenya",   // england
				"benin",   // france
				"morocco", // france
				"eritrea", // italy
			},
		},
		{
			name:  "filter on spec.clusterName (language)",
			query: "filter=spec.clusterName~en",
			wantNames: []string{
				"benin", // french
				"kenya", // english
			},
		},
		{
			name:  "filter on spec.displayName (other city)",
			query: "filter=spec.displayName~as",
			wantNames: []string{
				"kenya",   // mombasa
				"morocco", // casablanca
			},
		},
		{
			name:  "filter on type (colonizer)",
			query: "filter=_type=france&sort=metadata.name",
			wantNames: []string{
				"benin",   // france
				"morocco", // france
			},
		},
	}

	err = waitForObjectsBySchema(ctx, proxyStore, mcioSchema, labelTest, len(projectInfo))
	requireT.NoError(err)
	err = waitForObjectsBySchema(ctx, proxyStore, secretSchema, labelTest, len(secretInfo))
	requireT.NoError(err)

	partitions := []partition.Partition{defaultPartition}
	for _, test := range tests {
		test := test
		i.Run(test.name, func() {
			q := getFilteredQuery(test.query, labelTest)
			req, err := http.NewRequest("GET", "http://localhost:8080?"+q, nil)
			requireT.NoError(err)
			apiOp := &types.APIRequest{
				Request: req,
			}

			got, total, _, continueToken, err := proxyStore.ListByPartitions(apiOp, secretSchema, partitions)
			if err != nil {
				i.Assert().NoError(err)
				return
			}
			i.Assert().Equal(len(test.wantNames), total)
			i.Assert().Equal("", continueToken)
			i.Assert().Len(got.Items, len(test.wantNames))
			gotNames := stringsFromULIst(got)
			i.Assert().Equal(test.wantNames, gotNames)
		})
	}
}

type summaryBlockT struct {
	Property string         `json:"property"`
	Counts   map[string]int `json:"counts"`
}

func (i *IntegrationSuite) TestSummaryFieldsOnMCIOProjects() {
	ctx, cancel := context.WithCancel(i.T().Context())
	defer cancel()
	requireT := i.Require()

	cols, ccache, ctrl, sf, proxyStore, err := i.setupTest(ctx)
	requireT.NoError(err)
	requireT.NotNil(proxyStore)
	labelTest := "SummaryFieldsOnMCIOProjects"

	resetMCIOCh := make(chan struct{}, 10)

	mcioGVK := k8sschema.GroupVersionKind{
		Group:   "management.cattle.io",
		Version: "v3",
		Kind:    "Project",
	}
	mcioGVR := k8sschema.GroupVersionResource{
		Group:    "management.cattle.io",
		Version:  "v3",
		Resource: "projects",
	}

	sqlSchemaTracker := schematracker.NewSchemaTracker(ResetFunc(func(gvk k8sschema.GroupVersionKind) error {
		proxyStore.Reset(gvk)
		if gvk == mcioGVK {
			resetMCIOCh <- struct{}{}
		}
		return nil
	}))

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
	requireT.NoError(err)
	dynamicClient, err := dynamic.NewForConfig(i.restCfg)
	requireT.NoError(err)
	mcioClient := dynamicClient.Resource(mcioGVR).Namespace(defaultTestNamespace)
	// This time use the format
	// projectName   clusterName   displayName
	// country       mountainName   high|low|meh
	type projectInfo struct {
		countryMountainRating [3]string
		labels                map[string]string
	}
	mcioProjectInfo := []projectInfo{
		{countryMountainRating: [3]string{"italy", "blanc", "high"},
			labels: map[string]string{"knot": "bowline",
				"apple": "envy", "dinner": "burritos"},
		},
		{countryMountainRating: [3]string{"armenia", "aragats", "high"},
			labels: map[string]string{"knot": "hitch",
				"apple": "ambrosia", "dinner": "tacos"},
		},
		{countryMountainRating: [3]string{"spain", "teide", "high"},
			labels: map[string]string{"knot": "bowline", "dinner": "tacos"},
		},
		{countryMountainRating: [3]string{"austria", "grossglockner", "high"},
			labels: map[string]string{"apple": "nicola", "dinner": "burritos"},
		},
		{countryMountainRating: [3]string{"slovakia", "gerlachovsky", "meh"},
			labels: map[string]string{"knot": "hitch"},
		},
		{countryMountainRating: [3]string{"netherlands", "vaalserberg", "low"},
			labels: map[string]string{"knot": "granny", "apple": "nicola", "dinner": "tacos"},
		},
		{countryMountainRating: [3]string{"latvia", "gaizinkalns", "low"},
			labels: map[string]string{"knot": "bowline", "apple": "ambrosia", "dinner": "tortas"},
		},
		{countryMountainRating: [3]string{"norway", "galdhopiggen", "meh"},
			labels: map[string]string{"knot": "hitch", "apple": "ambrosia", "dinner": "tacos"},
		},
		{countryMountainRating: [3]string{"sweden", "kebnekaise", "low"},
			labels: map[string]string{"knot": "bowline", "apple": "salish", "dinner": "tortas"},
		},
		{countryMountainRating: [3]string{"finland", "halti", "low"},
			labels: map[string]string{"knot": "bowline", "apple": "salish", "dinner": "burritos"},
		},
		{countryMountainRating: [3]string{"denmark", "mollehoj", "low"},
			labels: map[string]string{"knot": "bowline", "apple": "grapple", "dinner": "tacos"},
		},
	}
	for _, item := range mcioProjectInfo {
		info := item.countryMountainRating
		labels := item.labels
		err = createMCIOProject(ctx, mcioClient, mcioGVR, labelTest, info[0], info[1], info[2], labels)
		requireT.NoError(err)
	}

	var mcioSchema *types.APISchema
	requireT.EventuallyWithT(func(c *assert.CollectT) {
		mcioSchema = sf.Schema("management.cattle.io.project")
		require.NotNil(c, mcioSchema)
	}, 15*time.Second, 500*time.Millisecond)

	err = waitForObjectsBySchema(ctx, proxyStore, mcioSchema, labelTest, len(mcioProjectInfo))
	requireT.NoError(err)

	tests := []struct {
		name        string
		query       string
		wantSummary types.APISummary
	}{
		{
			name:  "mountain sizes, no tests",
			query: "summary=spec.displayName",
			wantSummary: types.APISummary{
				SummaryItems: []types.SummaryEntry{
					types.SummaryEntry{
						Property: "spec.displayName",
						Counts: map[string]int{
							"high": 4,
							"meh":  2,
							"low":  5,
						},
					},
				},
			},
		},
		{
			name:  "mountain sizes, mountainName contains a 'g'",
			query: "summary=spec.displayName&filter=spec.clusterName~g",
			wantSummary: types.APISummary{
				SummaryItems: []types.SummaryEntry{
					types.SummaryEntry{
						Property: "spec.displayName",
						Counts: map[string]int{
							"high": 2,
							"meh":  2,
							"low":  2,
						},
					},
				},
			},
		},
		{
			name:  "mountain sizes, bowline label",
			query: "summary=spec.displayName&filter=metadata.labels.knot=bowline",
			wantSummary: types.APISummary{
				SummaryItems: []types.SummaryEntry{
					types.SummaryEntry{
						Property: "spec.displayName",
						Counts: map[string]int{
							"high": 2,
							"low":  4,
						},
					},
				},
			},
		},
		{
			name:  "mountain sizes, hitch knot label, has a g",
			query: "summary=spec.displayName&filter=metadata.labels.knot=hitch&filter=spec.clusterName~g",
			wantSummary: types.APISummary{
				SummaryItems: []types.SummaryEntry{
					types.SummaryEntry{
						Property: "spec.displayName",
						Counts: map[string]int{
							"high": 1,
							"meh":  2,
						},
					},
				},
			},
		},
		{
			name:  "mountain sizes, non-hitch knot label, has a g",
			query: "summary=spec.displayName&filter=metadata.labels.knot!=hitch&filter=spec.clusterName~g",
			wantSummary: types.APISummary{
				SummaryItems: []types.SummaryEntry{
					types.SummaryEntry{
						Property: "spec.displayName",
						Counts: map[string]int{
							"high": 1,
							"low":  2,
						},
					},
				},
			},
		},
		{
			name:  "knot label only",
			query: "summary=metadata.labels.knot",
			wantSummary: types.APISummary{
				SummaryItems: []types.SummaryEntry{
					types.SummaryEntry{
						Property: "metadata.labels.knot",
						Counts: map[string]int{
							"hitch":   3,
							"bowline": 6,
							"granny":  1,
						},
					},
				},
			},
		},
		{
			name:  "apple label only",
			query: "summary=metadata.labels.apple",
			wantSummary: types.APISummary{
				SummaryItems: []types.SummaryEntry{
					types.SummaryEntry{
						Property: "metadata.labels.apple",
						Counts: map[string]int{
							"envy":     1,
							"ambrosia": 3,
							"nicola":   2,
							"salish":   2,
							"grapple":  1,
						},
					},
				},
			},
		},
		{
			name:  "multiple summary fields: knots, apple, mountain sizes; reg & label neg & pos filters",
			query: "summary=metadata.labels.knot,spec.displayName,metadata.labels.apple&filter=spec.clusterName~g&filter=metadata.labels.knot!=granny&filter=metadata.labels.dinner~ta",
			wantSummary: types.APISummary{
				SummaryItems: []types.SummaryEntry{
					types.SummaryEntry{
						Property: "metadata.labels.apple",
						Counts: map[string]int{
							"ambrosia": 3,
						},
					},
					types.SummaryEntry{
						Property: "metadata.labels.knot",
						Counts: map[string]int{
							"hitch":   2,
							"bowline": 1,
						},
					},
					types.SummaryEntry{
						Property: "spec.displayName",
						Counts: map[string]int{
							"high": 1,
							"meh":  1,
							"low":  1,
						},
					},
				},
			},
		},
	}

	partitions := []partition.Partition{defaultPartition}
	for _, test := range tests {
		test := test
		i.Run(test.name, func() {
			q := getFilteredQuery(test.query, labelTest)
			req, err := http.NewRequest("GET", "http://localhost:8080?"+q, nil)
			requireT.NoError(err)
			apiOp := &types.APIRequest{
				Request: req,
			}

			list, total, summary, continueToken, err := proxyStore.ListByPartitions(apiOp, mcioSchema, partitions)
			if err != nil {
				i.Assert().NoError(err)
				return
			}
			gotNames := stringsFromULIst(list)
			fmt.Printf("Got %d items, names: %s\n", total, gotNames)
			i.Assert().Equal("", continueToken)
			i.Assert().Equal(&test.wantSummary, summary)
		})
	}
}

func stringsFromULIst(ulist *unstructured.UnstructuredList) []string {
	names := make([]string, len(ulist.Items))
	for i, item := range ulist.Items {
		names[i] = item.GetName()
	}
	return names
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
	// SetLogger must be called otherwise controller-runtime complains.
	//
	// For testing we'll just ignore the logs since they won't be useful
	ctrl.SetLogger(logr.New(ctrllog.NullLogSink{}))
	suite.Run(t, new(IntegrationSuite))
}

func (i *IntegrationSuite) setupTest(ctx context.Context) (cols *common.DynamicColumns, ccache clustercache.ClusterCache, svrController *server.Controllers, sf *schema.Collection, proxyStore *sqlproxy.Store, err error) {
	var cf *client.Factory
	var cacheFactory *factory.CacheFactory

	if cols, err = common.NewDynamicColumns(i.restCfg); err != nil {
		return
	}
	if cf, err = client.NewFactory(i.restCfg, false); err != nil {
		return
	}

	baseSchemas := types.EmptyAPISchemas()

	ccache = clustercache.NewClusterCache(ctx, cf.AdminDynamicClient())

	if svrController, err = server.NewController(i.restCfg, nil); err != nil {
		return
	}

	asl := accesscontrol.NewAccessStore(ctx, true, svrController.RBAC)
	sf = schema.NewCollection(ctx, baseSchemas, asl)

	if err = resources.DefaultSchemas(ctx, baseSchemas, ccache, cf, sf, ""); err != nil {
		return
	}

	definitions.Register(ctx, baseSchemas, svrController.K8s.Discovery(),
		svrController.CRD.CustomResourceDefinition(), svrController.API.APIService())

	summaryCache := summarycache.New(sf, ccache)
	summaryCache.Start(ctx)

	if cacheFactory, err = factory.NewCacheFactoryWithContext(ctx, factory.CacheFactoryOptions{}); err != nil {
		return
	}

	proxyStore, err = sqlproxy.NewProxyStore(ctx, cols, cf, summaryCache, summaryCache, sf, cacheFactory, true)
	return
}
