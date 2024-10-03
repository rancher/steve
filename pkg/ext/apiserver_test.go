package ext

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

var (
	schemeBuilderTest = runtime.NewSchemeBuilder(addKnownTypesTest)
	addToSchemeTest   = schemeBuilderTest.AddToScheme
)

func addKnownTypesTest(scheme *runtime.Scheme) error {
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

func TestExtensionAPIServer(t *testing.T) {
	scheme := runtime.NewScheme()
	AddToScheme(scheme)

	store := &testStore{}
	extensionAPIServer, cleanup, err := setupExtensionAPIServer(t, scheme, &TestType{}, &TestTypeList{}, store, func(opts *ExtensionAPIServerOptions) {
		// XXX: Find a way to get rid of this
		opts.BindPort = 32001
		opts.Authentication.CustomAuthenticator = authAsAdmin
		opts.Authorization = authorizer.AuthorizerFunc(authzAllowAll)
	})
	require.NoError(t, err)
	defer cleanup()

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

func setupExtensionAPIServer[
	T runtime.Object,
	TList runtime.Object,
](t *testing.T, scheme *runtime.Scheme, objT T, objTList TList, store Store[T, TList], optionSetter func(*ExtensionAPIServerOptions)) (*ExtensionAPIServer, func(), error) {

	addToSchemeTest(scheme)
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

	InstallStore(extensionAPIServer, objT, objTList, "testtypes", "testtype", testTypeGV.WithKind("TestType"), store)

	ctx, cancel := context.WithCancel(context.Background())

	stoppedCh := make(chan error, 1)
	readyCh := make(chan struct{}, 1)
	defer close(readyCh)
	go func() {
		err := extensionAPIServer.Run(ctx, readyCh)
		stoppedCh <- err
		close(stoppedCh)
	}()
	select {
	case err := <-stoppedCh:
		require.NoError(t, err)
	case <-readyCh:
	}

	cleanup := func() {
		cancel()
		err := <-stoppedCh
		require.NoError(t, err)
	}

	return extensionAPIServer, cleanup, nil
}
