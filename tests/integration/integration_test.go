package tests

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/rancher/steve/pkg/sqlcache/db"
	"github.com/rancher/steve/pkg/stores/sqlproxy"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

type IntegrationSuite struct {
	suite.Suite

	restCfg         *rest.Config
	client          *dynamic.DynamicClient
	discoveryMapper *restmapper.DeferredDiscoveryRESTMapper
	kubeconfigFile  string

	sqliteDatabaseFile string

	tearDownFuncs []func()
}

func (i *IntegrationSuite) SetupSuite() {
	logrus.SetLevel(logrus.ErrorLevel)

	ctx := context.Background()

	pathOptions := clientcmd.NewDefaultPathOptions()
	clientCmdConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(pathOptions.LoadingRules, &clientcmd.ConfigOverrides{})
	restCfg, err := clientCmdConfig.ClientConfig()
	// disable client-side rate limit for our tests
	restCfg.QPS = -1
	i.Require().Nil(restCfg.RateLimiter)

	kubeconfigFile := clientCmdConfig.ConfigAccess().GetExplicitFile()
	if !clientCmdConfig.ConfigAccess().IsExplicitFile() {
		kubeconfigFile = clientCmdConfig.ConfigAccess().GetDefaultFilename()
	}
	i.kubeconfigFile = kubeconfigFile

	i.Require().NoError(err, "ensure you have a Kubernetes cluster available")
	i.restCfg = restCfg

	i.client, err = dynamic.NewForConfig(i.restCfg)
	i.Require().NoError(err)

	clientset, err := kubernetes.NewForConfig(i.restCfg)
	i.Require().NoError(err)

	discoveryClient := memory.NewMemCacheClient(clientset.Discovery())
	i.discoveryMapper = restmapper.NewDeferredDiscoveryRESTMapper(discoveryClient)

	crdsDir := filepath.Join("testdata", "crds")
	i.doManifests(ctx, crdsDir, i.doApply)
	i.tearDownFuncs = append(i.tearDownFuncs, func() {
		i.doManifests(ctx, crdsDir, i.doDelete)
	})

	// TODO: Make this configurable?
	i.sqliteDatabaseFile, err = filepath.Abs(db.InformerObjectCacheDBPath)
	i.Require().NoError(err)

	// TODO: Move to Steve's constructor
	sqlproxy.TypeGuidanceTable[schema.GroupVersionKind{Group: "fruits.cattle.io", Version: "v1", Kind: "Banana"}] = map[string]string{
		"number": "INT",
	}
	sqlproxy.TypeSpecificIndexedFields["fruits.cattle.io_v1_Banana"] = [][]string{
		{"color"},
		{"number"},
		{"numberString"},
	}
}

func (i *IntegrationSuite) TearDownSuite() {
	for index := range i.tearDownFuncs {
		revIndex := len(i.tearDownFuncs) - index - 1
		i.tearDownFuncs[revIndex]()
	}
}

func (i *IntegrationSuite) doApply(ctx context.Context, obj *unstructured.Unstructured, gvr schema.GroupVersionResource) error {
	namespace := obj.GetNamespace()
	applyOpts := metav1.ApplyOptions{FieldManager: "integration-tests"}
	logrus.Println("Applying", obj.GetNamespace(), obj.GetName())
	newObj, err := i.client.Resource(gvr).Namespace(namespace).Apply(ctx, obj.GetName(), obj, applyOpts)
	if err != nil {
		return fmt.Errorf("applying %s/%s: %w", namespace, obj.GetName(), err)
	}
	status, found, err := unstructured.NestedFieldCopy(obj.Object, "status")
	if err != nil {
		return fmt.Errorf("status for %s/%s: %w", namespace, obj.GetName(), err)
	}
	if !found {
		return nil
	}

	newStatus, newFound, err := unstructured.NestedFieldCopy(newObj.Object, "status")
	if err != nil {
		return fmt.Errorf("status for %s/%s: %w", namespace, obj.GetName(), err)
	}
	if newFound && reflect.DeepEqual(status, newStatus) {
		return nil
	}

	unstructured.SetNestedField(newObj.Object, status, "status")
	_, err = i.client.Resource(gvr).Namespace(namespace).UpdateStatus(ctx, newObj, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("updatestatus for %s/%s: %w", namespace, obj.GetName(), err)
	}
	return nil
}

func (i *IntegrationSuite) doDelete(ctx context.Context, obj *unstructured.Unstructured, gvr schema.GroupVersionResource) error {
	namespace := obj.GetNamespace()
	deleteOpts := metav1.DeleteOptions{}
	logrus.Println("Deleting", obj.GetNamespace(), obj.GetName())
	err := i.client.Resource(gvr).Namespace(namespace).Delete(ctx, obj.GetName(), deleteOpts)
	if err != nil {
		return fmt.Errorf("deleting %s/%s: %w", namespace, obj.GetName(), err)
	}
	return nil
}

type DoFunc func(ctx context.Context, obj *unstructured.Unstructured, gvr schema.GroupVersionResource) error
type HeaderFunc func(ctx context.Context, parsed map[string]any) error

func (i *IntegrationSuite) doManifests(ctx context.Context, manifestsDir string, fn DoFunc) {
	entries, err := os.ReadDir(manifestsDir)
	i.Require().NoError(err)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		manifestFile := filepath.Join(manifestsDir, entry.Name())
		i.doManifest(ctx, manifestFile, fn)
	}
}

func (i *IntegrationSuite) doManifest(ctx context.Context, manifestFile string, doFn DoFunc) {
	noHeader := func(_ context.Context, _ map[string]any) error {
		return nil
	}
	i.doManifestWithHeader(ctx, manifestFile, noHeader, doFn)
}

func (i *IntegrationSuite) doManifestReversed(ctx context.Context, manifestFile string, doFn DoFunc) {
	noHeader := func(_ context.Context, _ map[string]any) error {
		return nil
	}
	i.doManifestWithHeaderReversed(ctx, manifestFile, noHeader, doFn)
}

func (i *IntegrationSuite) doManifestWithHeaderReversed(ctx context.Context, manifestFile string, headerFn HeaderFunc, doFn DoFunc) {
	var fns []func()
	acc := func(ctx context.Context, obj *unstructured.Unstructured, gvr schema.GroupVersionResource) error {
		fns = append(fns, func() {
			doFn(ctx, obj, gvr)
		})
		return nil
	}
	i.doManifestWithHeader(ctx, manifestFile, headerFn, acc)
	for i := range len(fns) {
		index := len(fns) - i - 1
		fns[index]()
	}
}

func (i *IntegrationSuite) doManifestWithHeader(ctx context.Context, manifestFile string, headerFn HeaderFunc, doFn DoFunc) {
	file, err := os.Open(manifestFile)
	i.Require().NoError(err)
	defer file.Close()

	i.doManifestReaderWithHeader(ctx, file, headerFn, doFn)
}

func (i *IntegrationSuite) doManifestString(ctx context.Context, manifestYAML string, doFn DoFunc) {
	noHeader := func(_ context.Context, _ map[string]any) error {
		return nil
	}
	i.doManifestStringWithHeader(ctx, manifestYAML, noHeader, doFn)
}

func (i *IntegrationSuite) doManifestStringReversed(ctx context.Context, manifestYAML string, doFn DoFunc) {
	noHeader := func(_ context.Context, _ map[string]any) error {
		return nil
	}
	i.doManifestStringWithHeaderReversed(ctx, manifestYAML, noHeader, doFn)
}

func (i *IntegrationSuite) doManifestStringWithHeaderReversed(ctx context.Context, manifestYAML string, headerFn HeaderFunc, doFn DoFunc) {
	var fns []func()
	acc := func(ctx context.Context, obj *unstructured.Unstructured, gvr schema.GroupVersionResource) error {
		fns = append(fns, func() {
			doFn(ctx, obj, gvr)
		})
		return nil
	}
	i.doManifestStringWithHeader(ctx, manifestYAML, headerFn, acc)
	for i := range len(fns) {
		index := len(fns) - i - 1
		fns[index]()
	}
}

func (i *IntegrationSuite) doManifestStringWithHeader(ctx context.Context, manifestYAML string, headerFn HeaderFunc, doFn DoFunc) {
	reader := strings.NewReader(manifestYAML)
	i.doManifestReaderWithHeader(ctx, reader, headerFn, doFn)
}

func (i *IntegrationSuite) doManifestReaderWithHeader(ctx context.Context, reader io.Reader, headerFn HeaderFunc, doFn DoFunc) {
	dec := yaml.NewDecoder(reader)
	for {
		obj := &unstructured.Unstructured{Object: map[string]any{}}
		err := dec.Decode(obj.Object)
		if errors.Is(err, io.EOF) {
			break
		}

		if obj.Object == nil {
			continue
		}

		if obj.GroupVersionKind() == schema.EmptyObjectKind.GroupVersionKind() {
			err = headerFn(ctx, obj.Object)
			i.Require().NoError(err)
			continue
		}

		var gvr schema.GroupVersionResource
		i.Require().EventuallyWithT(func(c *assert.CollectT) {
			i.discoveryMapper.Reset()
			gvk := obj.GroupVersionKind()
			restMapping, err := i.discoveryMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
			if assert.NoError(c, err, "mapping") {
				gvr = restMapping.Resource
			}
		}, time.Second*5, time.Millisecond*100)

		err = doFn(ctx, obj, gvr)
		i.Require().NoError(err)
	}
}

func (i *IntegrationSuite) waitForSchema(baseURL string, gvr schema.GroupVersionResource) {
	schemaID := fmt.Sprintf("%s.%s", gvr.Group, gvr.Resource)
	if gvr.Group == "" {
		schemaID = gvr.Resource
	}
	i.Require().EventuallyWithT(func(c *assert.CollectT) {
		url := baseURL + "/v1/schemaDefinitions/" + schemaID
		resp, err := http.Get(url)
		require.NoError(c, err)
		defer resp.Body.Close()
		require.Equal(c, http.StatusOK, resp.StatusCode, "url was %s", url)
	}, time.Second*15, time.Millisecond*200)
}

func (i *IntegrationSuite) maybeStopAndDebug(baseURL string) {
	if os.Getenv("INTEGRATION_TEST_DEBUG") == "true" {
		fmt.Printf(`###########################
#
# Integration tests stopped as requested
#
# You can now access the Kubernetes cluster and steve
#
# Kubernetes: KUBECONFIG=%s
# Steve URL: %s
# SQL cache database: %s
#
###########################
`, i.kubeconfigFile, baseURL, i.sqliteDatabaseFile)
		<-i.T().Context().Done()
		i.Require().FailNow("Troubleshooting done, exiting")
	}
}

func TestIntegrationSuite(t *testing.T) {
	shouldRun := os.Getenv("RUN_INTEGRATION_TESTS") == "true"
	if !shouldRun {
		t.Skip("Set RUN_INTEGRATION_TESTS=true to run integration tests")
	}

	// SetLogger must be called otherwise controller-runtime complains.
	//
	// For testing we'll just ignore the logs since they won't be useful
	ctrl.SetLogger(logr.New(ctrllog.NullLogSink{}))
	suite.Run(t, new(IntegrationSuite))
}
