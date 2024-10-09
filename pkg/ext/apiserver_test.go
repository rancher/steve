package ext

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
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

var _ Store[*TestTypeOther, *TestTypeOtherList] = (*testStoreOther)(nil)

// This store is meant to be able to test many stores
type testStoreOther struct {
}

func (t *testStoreOther) Create(ctx Context, obj *TestTypeOther, opts *metav1.CreateOptions) (*TestTypeOther, error) {
	return &testTypeOtherFixture, nil
}

func (t *testStoreOther) Update(ctx Context, obj *TestTypeOther, opts *metav1.UpdateOptions) (*TestTypeOther, error) {
	return &testTypeOtherFixture, nil
}

func (t *testStoreOther) Get(ctx Context, name string, opts *metav1.GetOptions) (*TestTypeOther, error) {
	return &testTypeOtherFixture, nil
}

func (t *testStoreOther) List(ctx Context, opts *metav1.ListOptions) (*TestTypeOtherList, error) {
	return &testTypeOtherListFixture, nil
}

func (t *testStoreOther) Watch(ctx Context, opts *metav1.ListOptions) (<-chan WatchEvent[*TestTypeOther], error) {
	return nil, nil
}

func (t *testStoreOther) Delete(ctx Context, name string, opts *metav1.DeleteOptions) error {
	return nil
}

// The POC had a bug where multiple resources couldn't be installed so we're
// testing this here
func (s *ExtensionAPIServerSuite) TestDiscoveryAndOpenAPI() {
	t := s.T()

	scheme := runtime.NewScheme()
	AddToScheme(scheme)

	differentVersion := schema.GroupVersion{
		Group:   "ext.cattle.io",
		Version: "v2",
	}

	differentGroupVersion := schema.GroupVersion{
		Group:   "ext2.cattle.io",
		Version: "v3",
	}
	scheme.AddKnownTypes(differentVersion, &TestType{}, &TestTypeList{})
	scheme.AddKnownTypes(differentGroupVersion, &TestType{}, &TestTypeList{})
	metav1.AddToGroupVersion(scheme, differentVersion)
	metav1.AddToGroupVersion(scheme, differentGroupVersion)

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", ":0")
	require.NoError(t, err)

	store := &testStore{}
	extensionAPIServer, cleanup, err := setupExtensionAPIServer(t, scheme, &TestType{}, &TestTypeList{}, store, func(opts *ExtensionAPIServerOptions) {
		opts.Listener = ln
		opts.Authorizer = authorizer.AuthorizerFunc(authzAllowAll)
		opts.Authenticator = authenticator.RequestFunc(authAsAdmin)
	}, func(s *ExtensionAPIServer) error {
		store := &testStoreOther{}
		err := InstallStore(s, &TestTypeOther{}, &TestTypeOtherList{}, "testtypeothers", "testtypeother", testTypeGV.WithKind("TestTypeOther"), store)
		if err != nil {
			return err
		}

		err = InstallStore(s, &TestType{}, &TestTypeList{}, "testtypes", "testtype", differentVersion.WithKind("TestType"), &testStore{})
		if err != nil {
			return err
		}

		err = InstallStore(s, &TestType{}, &TestTypeList{}, "testtypes", "testtype", differentGroupVersion.WithKind("TestType"), &testStore{})
		if err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)
	defer cleanup()

	tests := []struct {
		path               string
		got                any
		expectedStatusCode int
		expectedBody       any
	}{
		{
			path:               "/apis",
			got:                &metav1.APIGroupList{},
			expectedStatusCode: http.StatusOK,
			expectedBody: &metav1.APIGroupList{
				TypeMeta: metav1.TypeMeta{
					Kind: "APIGroupList",
				},
				Groups: []metav1.APIGroup{
					{
						Name: "ext.cattle.io",
						Versions: []metav1.GroupVersionForDiscovery{
							{
								GroupVersion: "ext.cattle.io/v2",
								Version:      "v2",
							},
							{
								GroupVersion: "ext.cattle.io/v1",
								Version:      "v1",
							},
						},
						PreferredVersion: metav1.GroupVersionForDiscovery{
							GroupVersion: "ext.cattle.io/v2",
							Version:      "v2",
						},
					},
					{
						Name: "ext2.cattle.io",
						Versions: []metav1.GroupVersionForDiscovery{
							{
								GroupVersion: "ext2.cattle.io/v3",
								Version:      "v3",
							},
						},
						PreferredVersion: metav1.GroupVersionForDiscovery{
							GroupVersion: "ext2.cattle.io/v3",
							Version:      "v3",
						},
					},
				},
			},
		},
		{
			path:               "/apis/ext.cattle.io",
			got:                &metav1.APIGroup{},
			expectedStatusCode: http.StatusOK,
			expectedBody: &metav1.APIGroup{
				TypeMeta: metav1.TypeMeta{
					Kind:       "APIGroup",
					APIVersion: "v1",
				},
				Name: "ext.cattle.io",
				Versions: []metav1.GroupVersionForDiscovery{
					{
						GroupVersion: "ext.cattle.io/v2",
						Version:      "v2",
					},
					{
						GroupVersion: "ext.cattle.io/v1",
						Version:      "v1",
					},
				},
				PreferredVersion: metav1.GroupVersionForDiscovery{
					GroupVersion: "ext.cattle.io/v2",
					Version:      "v2",
				},
			},
		},
		{
			path:               "/apis/ext2.cattle.io",
			got:                &metav1.APIGroup{},
			expectedStatusCode: http.StatusOK,
			expectedBody: &metav1.APIGroup{
				TypeMeta: metav1.TypeMeta{
					Kind:       "APIGroup",
					APIVersion: "v1",
				},
				Name: "ext2.cattle.io",
				Versions: []metav1.GroupVersionForDiscovery{
					{
						GroupVersion: "ext2.cattle.io/v3",
						Version:      "v3",
					},
				},
				PreferredVersion: metav1.GroupVersionForDiscovery{
					GroupVersion: "ext2.cattle.io/v3",
					Version:      "v3",
				},
			},
		},
		{
			path:               "/apis/ext.cattle.io/v1",
			got:                &metav1.APIResourceList{},
			expectedStatusCode: http.StatusOK,
			expectedBody: &metav1.APIResourceList{
				TypeMeta: metav1.TypeMeta{
					Kind:       "APIResourceList",
					APIVersion: "v1",
				},
				GroupVersion: "ext.cattle.io/v1",
				APIResources: []metav1.APIResource{
					{
						Name:         "testtypeothers",
						SingularName: "testtypeother",
						Namespaced:   false,
						Kind:         "TestTypeOther",
						Group:        "ext.cattle.io",
						Version:      "v1",
						Verbs: metav1.Verbs{
							"create", "delete", "get", "list", "patch", "update", "watch",
						},
					},
					{
						Name:         "testtypes",
						SingularName: "testtype",
						Namespaced:   false,
						Kind:         "TestType",
						Group:        "ext.cattle.io",
						Version:      "v1",
						Verbs: metav1.Verbs{
							"create", "delete", "get", "list", "patch", "update", "watch",
						},
					},
				},
			},
		},
		{
			path:               "/apis/ext.cattle.io/v2",
			got:                &metav1.APIResourceList{},
			expectedStatusCode: http.StatusOK,
			expectedBody: &metav1.APIResourceList{
				TypeMeta: metav1.TypeMeta{
					Kind:       "APIResourceList",
					APIVersion: "v1",
				},
				GroupVersion: "ext.cattle.io/v2",
				APIResources: []metav1.APIResource{
					{
						Name:         "testtypes",
						SingularName: "testtype",
						Namespaced:   false,
						Kind:         "TestType",
						Group:        "ext.cattle.io",
						Version:      "v2",
						Verbs: metav1.Verbs{
							"create", "delete", "get", "list", "patch", "update", "watch",
						},
					},
				},
			},
		},
		{
			path:               "/apis/ext2.cattle.io/v3",
			got:                &metav1.APIResourceList{},
			expectedStatusCode: http.StatusOK,
			expectedBody: &metav1.APIResourceList{
				TypeMeta: metav1.TypeMeta{
					Kind:       "APIResourceList",
					APIVersion: "v1",
				},
				GroupVersion: "ext2.cattle.io/v3",
				APIResources: []metav1.APIResource{
					{
						Name:         "testtypes",
						SingularName: "testtype",
						Namespaced:   false,
						Kind:         "TestType",
						Group:        "ext2.cattle.io",
						Version:      "v3",
						Verbs: metav1.Verbs{
							"create", "delete", "get", "list", "patch", "update", "watch",
						},
					},
				},
			},
		},
		{
			path:               "/openapi/v2",
			expectedStatusCode: http.StatusOK,
		},
		{
			path:               "/openapi/v3",
			expectedStatusCode: http.StatusOK,
		},
		{
			path:               "/openapi/v3/apis",
			expectedStatusCode: http.StatusOK,
		},
		{
			path:               "/openapi/v3/apis/ext.cattle.io",
			expectedStatusCode: http.StatusOK,
		},
		{
			path:               "/openapi/v3/apis/ext.cattle.io/v1",
			expectedStatusCode: http.StatusOK,
		},
		{
			path:               "/openapi/v3/apis/ext.cattle.io/v2",
			expectedStatusCode: http.StatusOK,
		},
		{
			path:               "/openapi/v3/apis/ext2.cattle.io",
			expectedStatusCode: http.StatusOK,
		},
		{
			path:               "/openapi/v3/apis/ext2.cattle.io/v3",
			expectedStatusCode: http.StatusOK,
		},
	}
	for _, test := range tests {
		name := strings.ReplaceAll(test.path, "/", "_")
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, test.path, nil)
			w := httptest.NewRecorder()

			extensionAPIServer.ServeHTTP(w, req)

			resp := w.Result()
			body, _ := io.ReadAll(resp.Body)

			require.Equal(t, test.expectedStatusCode, resp.StatusCode)
			if test.expectedBody != nil && test.got != nil {
				err = json.Unmarshal(body, test.got)
				require.NoError(t, err)
				require.Equal(t, test.expectedBody, test.got)
			}
		})
	}
}

func setupExtensionAPIServer[
	T runtime.Object,
	TList runtime.Object,
](
	t *testing.T,
	scheme *runtime.Scheme,
	objT T,
	objTList TList,
	store Store[T, TList],
	optionSetter func(*ExtensionAPIServerOptions),
	extensionAPIServerSetter func(*ExtensionAPIServer) error,
) (*ExtensionAPIServer, func(), error) {

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

	err = InstallStore(extensionAPIServer, objT, objTList, "testtypes", "testtype", testTypeGV.WithKind("TestType"), store)
	if err != nil {
		return nil, func() {}, fmt.Errorf("InstallStore: %w", err)
	}

	if extensionAPIServerSetter != nil {
		err = extensionAPIServerSetter(extensionAPIServer)
		if err != nil {
			return nil, func() {}, fmt.Errorf("extensionAPIServerSetter: %w", err)
		}
	}

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
