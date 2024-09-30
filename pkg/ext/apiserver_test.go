package ext

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/rancher/wrangler/v3/pkg/kubeconfig"
)

var (
	schemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	addToScheme   = schemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(testTypeGV,
		&TestType{},
		&TestTypeList{},
	)
	metav1.AddToGroupVersion(scheme, testTypeGV)
	return nil
}

var (
	testTypeGV = schema.GroupVersion{
		Group:   "ext.cattle.io",
		Version: "v1",
	}

	testTypeListFixture = TestTypeList{
		TypeMeta: metav1.TypeMeta{
			Kind:       "TestList",
			APIVersion: testTypeGV.String(),
		},
	}

	testTypeFixture = TestType{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Test",
			APIVersion: testTypeGV.String(),
		},
	}
)

var _ runtime.Object = (*TestTypeList)(nil)

type TestTypeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
}

func (t *TestTypeList) DeepCopyObject() runtime.Object {
	return t
}

var _ runtime.Object = (*TestType)(nil)

type TestType struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
}

func (t *TestType) DeepCopyObject() runtime.Object {
	return t
}

type testStore struct {
}

func (t *testStore) Create(ctx Context, obj *TestType, opts *metav1.CreateOptions) (*TestType, error) {
	return &testTypeFixture, nil
}

func (t *testStore) Update(ctx Context, obj *TestType, opts *metav1.UpdateOptions) (*TestType, error) {
	return &testTypeFixture, nil
}

func (t *testStore) Get(ctx Context, name string, opts *metav1.GetOptions) (*TestType, error) {
	return &testTypeFixture, nil
}

func (t *testStore) List(ctx Context, opts *metav1.ListOptions) (*TestTypeList, error) {
	return &testTypeListFixture, nil
}

func (t *testStore) Watch(ctx Context, opts *metav1.ListOptions) (<-chan WatchEvent[*TestType], error) {
	return nil, nil
}

func (t *testStore) Delete(ctx Context, name string, opts *metav1.DeleteOptions) error {
	return nil
}

func authAsAdmin(req *http.Request) (*authenticator.Response, bool, error) {
	return &authenticator.Response{
		User: &user.DefaultInfo{
			Name:   "system:masters",
			Groups: []string{"system:masters"},
		},
	}, true, nil
}

func authzAllowAll(ctx context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
	return authorizer.DecisionAllow, "", nil
}

func TestExtensionAPIServerAuthentication(t *testing.T) {
	clientGetter := kubeconfig.GetNonInteractiveClientConfig(os.Getenv("KUBECONFIG"))
	restConfig, err := clientGetter.ClientConfig()
	require.NoError(t, err)

	client, err := kubernetes.NewForConfig(restConfig)
	require.NoError(t, err)

	store := &testStore{}
	extensionAPIServer, cleanup, err := setupExtensionAPIServer(t, store, func(opts *ExtensionAPIServerOptions) {
		// XXX: Find a way to get rid of this
		opts.BindPort = 32000
		opts.Client = client
		opts.Authorization = authorizer.AuthorizerFunc(authzAllowAll)
		opts.Authentication = AuthenticationOptions{
			EnableBuiltIn: true,
			CustomAuthenticator: authenticator.RequestFunc(func(req *http.Request) (*authenticator.Response, bool, error) {
				return nil, false, nil
			}),
		}
	})
	defer cleanup()

	require.NoError(t, err)

	paths := []string{
		"/",
		"/apis",
		"/apis/ext.cattle.io",
		"/apis/ext.cattle.io/v1",
		"/apis/ext.cattle.io/v1/testtypes",
		"/apis/ext.cattle.io/v1/testtypes/foo",
		"/openapi/v2",
		"/openapi/v3",
		"/openapi/v3/apis/ext.cattle.io/v1",
		"/unknown",
	}
	for _, path := range paths {
		name := strings.Replace(path, "/", "", 1)
		name = strings.ReplaceAll(name, "/", "-")
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/apis", nil)
			w := httptest.NewRecorder()

			extensionAPIServer.ServeHTTP(w, req)
			resp := w.Result()

			body, _ := io.ReadAll(resp.Body)

			responseStatus := metav1.Status{}
			json.Unmarshal(body, &responseStatus)

			expectedStatus := apierrors.NewUnauthorized("Unauthorized")
			expectedStatus.ErrStatus.Kind = "Status"
			expectedStatus.ErrStatus.APIVersion = "v1"

			require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
			require.Equal(t, expectedStatus.ErrStatus, responseStatus)
		})
	}
}

func TestExtensionAPIServer(t *testing.T) {
	store := &testStore{}
	extensionAPIServer, cleanup, err := setupExtensionAPIServer(t, store, func(opts *ExtensionAPIServerOptions) {
		// XXX: Find a way to get rid of this
		opts.BindPort = 32001
		opts.Authentication.CustomAuthenticator = authAsAdmin
		opts.Authorization = authorizer.AuthorizerFunc(authzAllowAll)
	})
	defer cleanup()

	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/apis", nil)
	w := httptest.NewRecorder()

	extensionAPIServer.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	require.Equal(t, http.StatusOK, resp.StatusCode)

	apiGroupList := metav1.APIGroupList{}
	err = json.Unmarshal(body, &apiGroupList)
	if err != nil {
		t.Fatal(err)
	}

	expected := metav1.APIGroupList{
		TypeMeta: metav1.TypeMeta{
			Kind: "APIGroupList",
		},
		Groups: []metav1.APIGroup{
			{
				Name: "ext.cattle.io",
				Versions: []metav1.GroupVersionForDiscovery{
					{
						GroupVersion: "ext.cattle.io/v1",
						Version:      "v1",
					},
				},
				PreferredVersion: metav1.GroupVersionForDiscovery{
					GroupVersion: "ext.cattle.io/v1",
					Version:      "v1",
				},
			},
		},
	}
	require.Equal(t, expected, apiGroupList)
}

type IntegrationSuite struct {
	suite.Suite
	testEnv   envtest.Environment
	clientset kubernetes.Clientset
	restCfg   rest.Config
}

func setupExtensionAPIServer[
	T Ptr[DerefT],
	DerefT any,
	TList Ptr[DerefTList],
	DerefTList any,
](t *testing.T, store Store[T, TList], optionSetter func(*ExtensionAPIServerOptions)) (*ExtensionAPIServer, func(), error) {
	codecs := serializer.NewCodecFactory(scheme)

	opts := ExtensionAPIServerOptions{
		GetOpenAPIDefinitions: getOpenAPIDefinitions,
		OpenAPIDefinitionNameReplacements: map[string]string{
			"com.github.rancher.steve.pkg.ext": "io.cattle.ext.v1",
		},
	}
	if optionSetter != nil {
		optionSetter(&opts)
	}
	extensionAPIServer, err := NewExtensionAPIServer(scheme, codecs, opts)
	if err != nil {
		return nil, func() {}, err
	}

	addToScheme(scheme)

	testResourceStore := MakeResourceStore("testtypes", "testtype", testTypeGV.WithKind("TestType"), opts.Authorization, store)
	extensionAPIServer.InstallResourceStore(testResourceStore)

	ctx, cancel := context.WithCancel(context.Background())

	stoppedCh := make(chan error, 1)
	readyCh := make(chan struct{}, 1)
	defer close(readyCh)
	go func() {
		err := extensionAPIServer.Run(ctx, readyCh)
		stoppedCh <- err
		close(stoppedCh)
	}()
	<-readyCh

	cleanup := func() {
		cancel()
		err := <-stoppedCh
		require.NoError(t, err)
	}

	return extensionAPIServer, cleanup, nil
}
