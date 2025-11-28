package sql

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
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
const testFilter = "filter=metadata.labels[" + testLabel + "]"

var defaultPartition = partition.Partition{
	All: true,
}

// Always use a filter-test instead of a namespace test because some of the resources we care about
// in this test don't have namespaces, but all resources have labels.
func getFilteredQuery(query string, labelTest string) string {
	if strings.Contains(query, testFilter) {
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

func createMCIOProject(ctx context.Context, client dynamic.ResourceInterface, gvr k8sschema.GroupVersionResource, name, thisTestLabel, clusterName, displayName string) error {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": gvr.Group + "/" + gvr.Version,
			"kind":       "Project",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": defaultTestNamespace,
				"labels": map[string]interface{}{
					testLabel: thisTestLabel,
				},
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

func (i *IntegrationSuite) createSecret(ctx context.Context, name string, thisTestLabel string, projectLabel string) error {
	obj := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: defaultTestNamespace,
			Labels: map[string]string{
				"management.cattle.io/project-scoped-secret": projectLabel,
				testLabel: thisTestLabel,
			},
		},
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
			cfgMaps, total, continueToken, err := cache.ListByOptions(ctx, &options, partitions, defaultTestNamespace)
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
		_, total, _, err := proxyStore.ListByPartitions(apiOp, schema, partitions)
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

/**
 * Test the following:
 * 1. When a management.cattle.io.cluster is processed, its memory & cpu fields are transformed
 * 2. When a provisioning.cattle.io.cluster is processed, we copy in all the fields from the
 *    associated management.cattle.io.cluster
 * 3. We can sort provisioning.cattle.io.clusters on memory & cpu even when the original values
 *    have different units  (e.g.: ...Ki and ...Mi for memory, c and m for cpu)
 */

func (i *IntegrationSuite) TestProvisioningManagementClusterDependencies() {
	ctx, cancel := context.WithCancel(i.T().Context())
	defer cancel()
	requireT := i.Require()
	labelTest := "ProvisioningManagementClusterDependencies"

	cols, ccache, svrController, sf, proxyStore, err := i.setupTest(ctx)
	requireT.NoError(err)
	requireT.NotNil(proxyStore)

	resetMCIOCh := make(chan struct{}, 10)
	resetPCIOCh := make(chan struct{}, 10)

	mcioGVK := k8sschema.GroupVersionKind{
		Group:   "management.cattle.io",
		Version: "v3",
		Kind:    "Cluster",
	}
	mcioGVR := k8sschema.GroupVersionResource{
		Group:    "management.cattle.io",
		Version:  "v3",
		Resource: "clusters",
	}
	pcioGVK := k8sschema.GroupVersionKind{
		Group:   "provisioning.cattle.io",
		Version: "v1",
		Kind:    "Cluster",
	}
	pcioGVR := k8sschema.GroupVersionResource{
		Group:    "provisioning.cattle.io",
		Version:  "v1",
		Resource: "clusters",
	}

	sqlSchemaTracker := schematracker.NewSchemaTracker(ResetFunc(func(gvk k8sschema.GroupVersionKind) error {
		proxyStore.Reset(gvk)
		if gvk == mcioGVK {
			resetMCIOCh <- struct{}{}
		} else if gvk == pcioGVK {
			resetPCIOCh <- struct{}{}
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
		svrController.K8s.Discovery(),
		svrController.CRD.CustomResourceDefinition(),
		svrController.API.APIService(),
		svrController.K8s.AuthorizationV1().SelfSubjectAccessReviews(),
		onSchemasHandler,
		sf)

	err = svrController.Start(ctx)
	requireT.NoError(err)

	dynamicClient, err := dynamic.NewForConfig(i.restCfg)
	requireT.NoError(err)

	mcioClient := dynamicClient.Resource(mcioGVR)
	pcioClient := dynamicClient.Resource(pcioGVR).Namespace(defaultTestNamespace)

	mcioInfo := []struct {
		city   string
		cpu    string
		memory string
		pods   int
	}{
		{"kigali", "7000m", "900Ki", 17},
		{"luanda", "14250m", "2610Ki", 11},
		{"gaborone", "98m", "12Mi", 14},
		{"gitega", "325m", "4Mi", 8},
		{"bamako", "700m", "1200Ki", 20},
	}
	for _, info := range mcioInfo {
		err = createMCIO(ctx, mcioClient, mcioGVR, info.city, labelTest, info.cpu, info.memory, info.pods)
		requireT.NoError(err)
	}
	pcioInfo := [][2]string{
		{"rwanda", "kigali"},
		{"angola", "luanda"},
		{"botswana", "gaborone"},
		{"burundi", "gitega"},
		{"mali", "bamako"},
	}
	for _, info := range pcioInfo {
		err = createPCIO(ctx, pcioClient, pcioGVR, info[0], labelTest, info[1])
		requireT.NoError(err)
	}

	var mcioSchema *types.APISchema
	var pcioSchema *types.APISchema
	requireT.EventuallyWithT(func(c *assert.CollectT) {
		mcioSchema = sf.Schema("management.cattle.io.cluster")
		pcioSchema = sf.Schema("provisioning.cattle.io.cluster")
		require.NotNil(c, mcioSchema)
		require.NotNil(c, pcioSchema)
	}, 15*time.Second, 500*time.Millisecond)

	tests := []struct {
		name      string
		query     string
		wantNames []string
	}{
		{
			name:  "sorts by name",
			query: "sort=metadata.name",
			wantNames: []string{
				"angola",
				"botswana",
				"burundi",
				"mali",
				"rwanda",
			},
		},
		{
			name:  "sorts by cluster-name",
			query: "sort=status.clusterName,metadata.name",
			wantNames: []string{
				"mali",     // bamako
				"botswana", // gaborone
				"burundi",  // gitega
				"rwanda",   // kigali
				"angola",   // luanda
			},
		},
		{
			name:  "sorts by cpu",
			query: "sort=status.allocatable.cpuRaw,metadata.name",
			wantNames: []string{
				"botswana", // 98m
				"burundi",  // 325m
				"mali",     // 700m
				"rwanda",   // 7000m
				"angola",   // 14250m
			},
		},
		{
			name:  "sorts by memory",
			query: "sort=status.allocatable.memoryRaw,metadata.name",
			wantNames: []string{
				"rwanda",   // 900Ki
				"mali",     // 1200Ki
				"angola",   // 2610Ki
				"burundi",  // 4Mi
				"botswana", // 12Mi
			},
		},
		{
			name:  "sorts by pods",
			query: "sort=status.allocatable.pods,metadata.name",
			wantNames: []string{
				"burundi",  // 8
				"angola",   // 11
				"botswana", // 14
				"rwanda",   // 17
				"mali",     // 20
			},
		},
		{
			name:  "alpha-sort by raw cpu",
			query: "sort=status.allocatable.cpu,metadata.name",
			wantNames: []string{
				"angola",   // 14250m
				"burundi",  // 325m
				"rwanda",   // 7000m
				"mali",     // 700m
				"botswana", // 98m
			},
		},
		{
			name:  "alpha-sort by raw memory",
			query: "sort=status.allocatable.memory,metadata.name",
			wantNames: []string{
				"mali",     // 1200Ki
				"botswana", // 12Mi
				"angola",   // 2610Ki
				"burundi",  // 4Mi
				"rwanda",   // 900Ki
			},
		},
		{
			name:  "filter on original cpu",
			query: "filter=status.allocatable.cpu=7000m",
			wantNames: []string{
				"rwanda",
			},
		},
		{
			name:  "filter on processed cpu",
			query: "filter=status.allocatable.cpuRaw=14.25",
			wantNames: []string{
				"angola",
			},
		},
		{
			name:  "filter on original memory",
			query: "filter=status.allocatable.memory=12Mi",
			wantNames: []string{
				"botswana",
			},
		},
		{
			name:  "filter on processed memory",
			query: "filter=status.allocatable.memoryRaw=4194304",
			wantNames: []string{
				"burundi",
			},
		},
		{
			name:  "filter on pods",
			query: "filter=status.allocatable.pods=20",
			wantNames: []string{
				"mali",
			},
		},
	}

	err = waitForObjectsBySchema(ctx, proxyStore, mcioSchema, labelTest, len(mcioInfo))
	requireT.NoError(err)
	err = waitForObjectsBySchema(ctx, proxyStore, pcioSchema, labelTest, len(pcioInfo))
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

			got, total, continueToken, err := proxyStore.ListByPartitions(apiOp, pcioSchema, partitions)
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
		err = createMCIOProject(ctx, mcioClient, mcioGVR, info[0], labelTest, info[0], info[1])
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

			got, total, continueToken, err := proxyStore.ListByPartitions(apiOp, nsSchema, partitions)
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
	secretInfo := [][2]string{
		{"morocco", "rabat"},
		{"eritrea", "asmara"},
		{"kenya", "nairobi"},
		{"benin", "portonovo"},
	}
	projectInfo := [][3]string{
		{"rabat", "arabic", "casablanca"},
		{"asmara", "tigrinya", "keren"},
		{"nairobi", "english", "mombasa"},
		{"portonovo", "french", "cotonou"},
	}
	for _, info := range secretInfo {
		err = i.createSecret(ctx, info[0], labelTest, info[1])
		requireT.NoError(err)
	}
	dynamicClient, err := dynamic.NewForConfig(i.restCfg)
	requireT.NoError(err)
	mcioClient := dynamicClient.Resource(mcioGVR).Namespace(defaultTestNamespace)
	for _, info := range projectInfo {
		err = createMCIOProject(ctx, mcioClient, mcioGVR, info[0], labelTest, info[1], info[2])
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

			got, total, continueToken, err := proxyStore.ListByPartitions(apiOp, secretSchema, partitions)
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

func stringsFromULIst(ulist *unstructured.UnstructuredList) []string {
	names := make([]string, len(ulist.Items))
	for i, item := range ulist.Items {
		names[i] = item.GetName()
	}
	return names
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

	proxyStore, err = sqlproxy.NewProxyStore(ctx, cols, cf, summaryCache, summaryCache, cacheFactory, true)
	return
}
