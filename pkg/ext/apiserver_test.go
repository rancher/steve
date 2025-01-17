package ext

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	regrest "k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
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

func TestStore(t *testing.T) {
	scheme := runtime.NewScheme()
	AddToScheme(scheme)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", ":0")
	require.NoError(t, err)

	store := newDefaultTestStore()
	store.items = make(map[string]*TestType)

	extensionAPIServer, err := setupExtensionAPIServer(t, scheme, store, func(opts *ExtensionAPIServerOptions) {
		opts.Listener = ln
		opts.Authorizer = authorizer.AuthorizerFunc(authzAllowAll)
		opts.Authenticator = authenticator.RequestFunc(authAsAdmin)
	}, nil)
	require.NoError(t, err)

	ts := httptest.NewServer(extensionAPIServer)
	defer ts.Close()

	recWatch, err := createRecordingWatcher(scheme, testTypeGV.WithResource("testtypes"), ts.URL)
	require.NoError(t, err)

	updatedObj := testTypeFixture.DeepCopy()
	updatedObj.Annotations = map[string]string{
		"foo": "bar",
	}

	updatedObjList := testTypeListFixture.DeepCopy()
	updatedObjList.Items = []TestType{*updatedObj}

	emptyList := testTypeListFixture.DeepCopy()
	emptyList.Items = []TestType{}

	createRequest := func(method string, path string, obj any) *http.Request {
		var body io.Reader
		if obj != nil {
			raw, err := json.Marshal(obj)
			require.NoError(t, err)
			body = bytes.NewReader(raw)
		}
		return httptest.NewRequest(method, path, body)
	}

	tests := []struct {
		name               string
		request            *http.Request
		newType            any
		expectedStatusCode int
		expectedBody       any
	}{
		{
			name:               "delete not existing",
			request:            createRequest(http.MethodDelete, "/apis/ext.cattle.io/v1/testtypes/foo", nil),
			expectedStatusCode: http.StatusNotFound,
		},
		{
			name:               "get empty list",
			request:            createRequest(http.MethodGet, "/apis/ext.cattle.io/v1/testtypes", nil),
			newType:            &TestTypeList{},
			expectedStatusCode: http.StatusOK,
			expectedBody:       emptyList,
		},
		{
			name:               "create testtype",
			request:            createRequest(http.MethodPost, "/apis/ext.cattle.io/v1/testtypes", testTypeFixture.DeepCopy()),
			newType:            &TestType{},
			expectedStatusCode: http.StatusCreated,
			expectedBody:       &testTypeFixture,
		},
		{
			name:               "get non-empty list",
			request:            createRequest(http.MethodGet, "/apis/ext.cattle.io/v1/testtypes", nil),
			newType:            &TestTypeList{},
			expectedStatusCode: http.StatusOK,
			expectedBody:       &testTypeListFixture,
		},
		{
			name:               "get specific object",
			request:            createRequest(http.MethodGet, "/apis/ext.cattle.io/v1/testtypes/foo", nil),
			newType:            &TestType{},
			expectedStatusCode: http.StatusOK,
			expectedBody:       &testTypeFixture,
		},
		{
			name:               "update",
			request:            createRequest(http.MethodPut, "/apis/ext.cattle.io/v1/testtypes/foo", updatedObj.DeepCopy()),
			newType:            &TestType{},
			expectedStatusCode: http.StatusOK,
			expectedBody:       updatedObj,
		},
		{
			name:               "get updated",
			request:            createRequest(http.MethodGet, "/apis/ext.cattle.io/v1/testtypes", nil),
			newType:            &TestTypeList{},
			expectedStatusCode: http.StatusOK,
			expectedBody:       updatedObjList,
		},
		{
			name:               "delete",
			request:            createRequest(http.MethodDelete, "/apis/ext.cattle.io/v1/testtypes/foo", nil),
			newType:            &TestType{},
			expectedStatusCode: http.StatusOK,
			expectedBody:       updatedObj,
		},
		{
			name:               "delete not found",
			request:            createRequest(http.MethodDelete, "/apis/ext.cattle.io/v1/testtypes/foo", nil),
			expectedStatusCode: http.StatusNotFound,
		},
		{
			name:               "get not found",
			request:            createRequest(http.MethodGet, "/apis/ext.cattle.io/v1/testtypes/foo", nil),
			expectedStatusCode: http.StatusNotFound,
		},
		{
			name:               "get empty list again",
			request:            createRequest(http.MethodGet, "/apis/ext.cattle.io/v1/testtypes", nil),
			newType:            &TestTypeList{},
			expectedStatusCode: http.StatusOK,
			expectedBody:       emptyList,
		},
		{
			name:               "create via update",
			newType:            &TestType{},
			request:            createRequest(http.MethodPut, "/apis/ext.cattle.io/v1/testtypes/foo", testTypeFixture.DeepCopy()),
			expectedStatusCode: http.StatusCreated,
			expectedBody:       &testTypeFixture,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := test.request
			w := httptest.NewRecorder()

			extensionAPIServer.ServeHTTP(w, req)

			resp := w.Result()
			body, _ := io.ReadAll(resp.Body)

			require.Equal(t, test.expectedStatusCode, resp.StatusCode)
			if test.expectedBody != nil && test.newType != nil {
				err = json.Unmarshal(body, test.newType)
				require.NoError(t, err)
				require.Equal(t, test.expectedBody, test.newType)
			}
		})
	}

	// Possibly flaky, find a better way to wait for all events
	time.Sleep(1 * time.Second)
	expectedEvents := []watch.Event{
		{Type: watch.Added, Object: testTypeFixture.DeepCopy()},
		{Type: watch.Modified, Object: updatedObj.DeepCopy()},
		{Type: watch.Deleted, Object: updatedObj.DeepCopy()},
		{Type: watch.Added, Object: testTypeFixture.DeepCopy()},
	}

	events := recWatch.getEvents()
	require.Equal(t, len(expectedEvents), len(events))

	for i, event := range events {
		raw, err := json.Marshal(event.Object)
		require.NoError(t, err)

		obj := &TestType{}
		err = json.Unmarshal(raw, obj)
		require.NoError(t, err)

		convertedEvent := watch.Event{
			Type:   event.Type,
			Object: obj,
		}

		require.Equal(t, expectedEvents[i], convertedEvent)
	}
}

// This store tests when there's only a subset of verbs supported
type partialStorage struct {
	gvk schema.GroupVersionKind
}

// New implements [regrest.Storage]
func (t *partialStorage) New() runtime.Object {
	obj := &TestType{}
	obj.GetObjectKind().SetGroupVersionKind(t.gvk)
	return obj
}

// Destroy implements [regrest.Storage]
func (t *partialStorage) Destroy() {
}

// GetSingularName implements [regrest.SingularNameProvider]
func (t *partialStorage) GetSingularName() string {
	return "testtype"
}

// NamespaceScoped implements [regrest.Scoper]
func (t *partialStorage) NamespaceScoped() bool {
	return false
}

// GroupVersionKind implements [regrest.GroupVersionKindProvider]
func (t *partialStorage) GroupVersionKind(_ schema.GroupVersion) schema.GroupVersionKind {
	return t.gvk
}

func (s *partialStorage) Create(ctx context.Context, obj runtime.Object, createValidation regrest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	if createValidation != nil {
		err := createValidation(ctx, obj)
		if err != nil {
			return obj, err
		}
	}

	return nil, nil
}

// The POC had a bug where multiple resources couldn't be installed so we're
// testing this here
func TestDiscoveryAndOpenAPI(t *testing.T) {
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

	partialGroupVersion := schema.GroupVersion{
		Group:   "ext.cattle.io",
		Version: "v4",
	}
	scheme.AddKnownTypes(differentVersion, &TestType{}, &TestTypeList{})
	scheme.AddKnownTypes(differentGroupVersion, &TestType{}, &TestTypeList{})
	scheme.AddKnownTypes(partialGroupVersion, &TestType{}, &TestTypeList{})
	metav1.AddToGroupVersion(scheme, differentVersion)
	metav1.AddToGroupVersion(scheme, differentGroupVersion)
	metav1.AddToGroupVersion(scheme, partialGroupVersion)

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", ":0")
	require.NoError(t, err)

	store := newDefaultTestStore()
	extensionAPIServer, err := setupExtensionAPIServer(t, scheme, store, func(opts *ExtensionAPIServerOptions) {
		opts.Listener = ln
		opts.Authorizer = authorizer.AuthorizerFunc(authzAllowAll)
		opts.Authenticator = authenticator.RequestFunc(authAsAdmin)
	}, func(s *ExtensionAPIServer) error {
		err = s.Install("testtypeothers", testTypeGV.WithKind("TestTypeOther"), &testStore[*TestTypeOther, *TestTypeOtherList]{
			singular: "testtypeother",
			objT:     &TestTypeOther{},
			objListT: &TestTypeOtherList{},
			gvk:      testTypeGV.WithKind("TestTypeOther"),
			gvr:      schema.GroupVersionResource{Group: testTypeGV.Group, Version: testTypeGV.Version, Resource: "testtypes"},
		})
		if err != nil {
			return err
		}

		err = s.Install("testtypes", differentVersion.WithKind("TestType"), &testStore[*TestType, *TestTypeList]{
			singular: "testtype",
			objT:     &TestType{},
			objListT: &TestTypeList{},
			gvk:      differentVersion.WithKind("TestType"),
			gvr:      schema.GroupVersionResource{Group: differentVersion.Group, Version: differentVersion.Version, Resource: "testtypes"},
		})
		if err != nil {
			return err
		}

		err = s.Install("testtypes", differentGroupVersion.WithKind("TestType"), &testStore[*TestType, *TestTypeList]{
			singular: "testtype",
			objT:     &TestType{},
			objListT: &TestTypeList{},
			gvk:      differentGroupVersion.WithKind("TestType"),
			gvr:      schema.GroupVersionResource{Group: differentGroupVersion.Group, Version: differentVersion.Version, Resource: "testtypes"},
		})
		if err != nil {
			return err
		}

		err = s.Install("testtypes", partialGroupVersion.WithKind("TestType"), &partialStorage{
			gvk: partialGroupVersion.WithKind("TestType"),
		})
		if err != nil {
			return err
		}

		return nil
	})
	require.NoError(t, err)

	tests := []struct {
		path               string
		got                any
		expectedStatusCode int
		expectedBody       any
		compareFunc        func(*testing.T, any)
	}{
		{
			path:               "/apis",
			got:                &metav1.APIGroupList{},
			expectedStatusCode: http.StatusOK,
			// This is needed because the k8s.io/apiserver library loops over the apigroups
			compareFunc: func(t *testing.T, gotObj any) {
				apiGroupList, ok := gotObj.(*metav1.APIGroupList)
				require.True(t, ok)

				expectedAPIGroupList := &metav1.APIGroupList{
					TypeMeta: metav1.TypeMeta{
						Kind: "APIGroupList",
					},
					Groups: []metav1.APIGroup{
						{
							Name: "ext.cattle.io",
							Versions: []metav1.GroupVersionForDiscovery{
								{
									GroupVersion: "ext.cattle.io/v4",
									Version:      "v4",
								},
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
				}
				sortAPIGroupList(apiGroupList)
				sortAPIGroupList(expectedAPIGroupList)
				require.Equal(t, expectedAPIGroupList, apiGroupList)
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
						GroupVersion: "ext.cattle.io/v4",
						Version:      "v4",
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
			path:               "/apis/ext.cattle.io/v4",
			got:                &metav1.APIResourceList{},
			expectedStatusCode: http.StatusOK,
			expectedBody: &metav1.APIResourceList{
				TypeMeta: metav1.TypeMeta{
					Kind:       "APIResourceList",
					APIVersion: "v1",
				},
				GroupVersion: "ext.cattle.io/v4",
				APIResources: []metav1.APIResource{
					{
						Name:         "testtypes",
						SingularName: "testtype",
						Namespaced:   false,
						Kind:         "TestType",
						Group:        "ext.cattle.io",
						Version:      "v4",
						// Only the create verb is supported for this store
						Verbs: metav1.Verbs{
							"create",
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
			if test.got != nil && test.compareFunc != nil {
				err = json.Unmarshal(body, test.got)
				require.NoError(t, err)
				test.compareFunc(t, test.got)
			}
		})
	}
}

// Because the k8s.io/apiserver library has non-deterministic map iteration, changing the order of groups and versions
func sortAPIGroupList(list *metav1.APIGroupList) {
	for _, group := range list.Groups {
		sort.Slice(group.Versions, func(i, j int) bool {
			return group.Versions[i].GroupVersion > group.Versions[j].GroupVersion
		})
	}
	sort.Slice(list.Groups, func(i, j int) bool {
		return list.Groups[i].Name > list.Groups[j].Name
	})
}

func TestNoStore(t *testing.T) {
	scheme := runtime.NewScheme()
	codecs := serializer.NewCodecFactory(scheme)

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", ":0")
	require.NoError(t, err)

	opts := ExtensionAPIServerOptions{
		GetOpenAPIDefinitions: getOpenAPIDefinitions,
		Listener:              ln,
		Authorizer:            authorizer.AuthorizerFunc(authzAllowAll),
		Authenticator:         authenticator.RequestFunc(authAsAdmin),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	extensionAPIServer, err := NewExtensionAPIServer(scheme, codecs, opts)
	require.NoError(t, err)

	err = extensionAPIServer.Run(ctx)
	require.NoError(t, err)
}

func setupExtensionAPIServer(
	t *testing.T,
	scheme *runtime.Scheme,
	store regrest.Storage,
	optionSetter func(*ExtensionAPIServerOptions),
	extensionAPIServerSetter func(*ExtensionAPIServer) error,
) (*ExtensionAPIServer, error) {
	fn := func(e *ExtensionAPIServer) error {
		err := e.Install("testtypes", testTypeGV.WithKind("TestType"), store)
		if err != nil {
			return fmt.Errorf("InstallStore: %w", err)
		}
		if extensionAPIServerSetter != nil {
			return extensionAPIServerSetter(e)
		}
		return nil
	}
	return setupExtensionAPIServerNoStore(t, scheme, optionSetter, fn)
}

func setupExtensionAPIServerNoStore(
	t *testing.T,
	scheme *runtime.Scheme,
	optionSetter func(*ExtensionAPIServerOptions),
	extensionAPIServerSetter func(*ExtensionAPIServer) error,
) (*ExtensionAPIServer, error) {

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
		return nil, err
	}

	if extensionAPIServerSetter != nil {
		err = extensionAPIServerSetter(extensionAPIServer)
		if err != nil {
			return nil, fmt.Errorf("extensionAPIServerSetter: %w", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})

	err = extensionAPIServer.Run(ctx)
	if err != nil {
		return nil, err
	}

	return extensionAPIServer, nil
}

type recordingWatcher struct {
	ch   <-chan watch.Event
	stop func()
}

func (w *recordingWatcher) getEvents() []watch.Event {
	w.stop()
	events := []watch.Event{}
	for event := range w.ch {
		events = append(events, event)
	}
	return events
}

func createRecordingWatcher(scheme *runtime.Scheme, gvr schema.GroupVersionResource, url string) (*recordingWatcher, error) {
	codecs := serializer.NewCodecFactory(scheme)

	gv := gvr.GroupVersion()
	client, err := dynamic.NewForConfig(&rest.Config{
		Host:    url,
		APIPath: "/apis",
		ContentConfig: rest.ContentConfig{
			NegotiatedSerializer: codecs,
			GroupVersion:         &gv,
		},
	})
	if err != nil {
		return nil, err
	}

	opts := metav1.ListOptions{
		Watch: true,
	}
	myWatch, err := client.Resource(gvr).Watch(context.Background(), opts)
	if err != nil {
		return nil, err
	}
	// Should be plenty enough for most tests
	ch := make(chan watch.Event, 100)
	go func() {
		for event := range myWatch.ResultChan() {
			ch <- event
		}
		close(ch)
	}()
	return &recordingWatcher{
		ch:   ch,
		stop: myWatch.Stop,
	}, nil
}

// This store tests the printed columns functionality
type customColumnsStore struct {
	*testStore[*TestType, *TestTypeList]

	lock      sync.Mutex
	columns   []metav1.TableColumnDefinition
	convertFn func(obj *TestType) []string
}

func (s *customColumnsStore) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return ConvertToTable(ctx, object, tableOptions, s.testStore.gvr.GroupResource(), s.columns, s.convertFn)
}

func (s *customColumnsStore) Set(columns []metav1.TableColumnDefinition, convertFn func(obj *TestType) []string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.columns = columns
	s.convertFn = convertFn
}

func TestCustomColumns(t *testing.T) {
	scheme := runtime.NewScheme()
	AddToScheme(scheme)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", ":0")
	require.NoError(t, err)

	store := &customColumnsStore{
		testStore: newDefaultTestStore(),
	}

	extensionAPIServer, err := setupExtensionAPIServerNoStore(t, scheme, func(opts *ExtensionAPIServerOptions) {
		opts.Listener = ln
		opts.Authorizer = authorizer.AuthorizerFunc(authzAllowAll)
		opts.Authenticator = authenticator.RequestFunc(authAsAdmin)
	}, func(s *ExtensionAPIServer) error {
		err := s.Install("testtypes", testTypeGV.WithKind("TestType"), store)
		if err != nil {
			return err
		}
		return nil
	})

	require.NoError(t, err)

	ts := httptest.NewServer(extensionAPIServer)
	defer ts.Close()

	createRequest := func(path string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		// This asks the apiserver to give back a metav1.Table for List and Get operations
		req.Header.Add("Accept", "application/json;as=Table;v=v1;g=meta.k8s.io")
		return req
	}

	columns := []metav1.TableColumnDefinition{
		{
			Name: "Name",
			Type: "name",
		},
		{
			Name: "Foo",
			Type: "string",
		},
		{
			Name: "Bar",
			Type: "number",
		},
	}
	convertFn := func(obj *TestType) []string {
		return []string{
			"the name is " + obj.GetName(),
			"the foo value",
			"the bar value",
		}
	}

	tests := []struct {
		name               string
		requests           []*http.Request
		columns            []metav1.TableColumnDefinition
		convertFn          func(obj *TestType) []string
		expectedStatusCode int
		expectedBody       any
	}{
		{
			name: "default",
			requests: []*http.Request{
				createRequest("/apis/ext.cattle.io/v1/testtypes"),
				createRequest("/apis/ext.cattle.io/v1/testtypes/foo"),
			},
			expectedStatusCode: http.StatusOK,
			expectedBody: &metav1.Table{
				TypeMeta: metav1.TypeMeta{Kind: "Table", APIVersion: "meta.k8s.io/v1"},
				ColumnDefinitions: []metav1.TableColumnDefinition{
					{Name: "Name", Type: "string", Format: "name", Description: "Name must be unique within a namespace. Is required when creating resources, although some resources may allow a client to request the generation of an appropriate name automatically. Name is primarily intended for creation idempotence and configuration definition. Cannot be updated. More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names#names"},
					{Name: "Created At", Type: "date", Description: "CreationTimestamp is a timestamp representing the server time when this object was created. It is not guaranteed to be set in happens-before order across separate operations. Clients may not set this value. It is represented in RFC3339 form and is in UTC.\n\nPopulated by the system. Read-only. Null for lists. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata"},
				},
				Rows: []metav1.TableRow{
					{
						Cells: []any{"foo", "0001-01-01T00:00:00Z"},
						Object: runtime.RawExtension{
							Raw: []byte(`{"kind":"PartialObjectMetadata","apiVersion":"meta.k8s.io/v1","metadata":{"name":"foo","creationTimestamp":null}}`),
						},
					},
				},
			},
		},
		{
			name: "custom include object default and metadata",
			requests: []*http.Request{
				createRequest("/apis/ext.cattle.io/v1/testtypes"),
				createRequest("/apis/ext.cattle.io/v1/testtypes/foo"),
				createRequest("/apis/ext.cattle.io/v1/testtypes?includeObject=Metadata"),
				createRequest("/apis/ext.cattle.io/v1/testtypes/foo?includeObject=Metadata"),
			},
			columns:            columns,
			convertFn:          convertFn,
			expectedStatusCode: http.StatusOK,
			expectedBody: &metav1.Table{
				TypeMeta: metav1.TypeMeta{Kind: "Table", APIVersion: "meta.k8s.io/v1"},
				ColumnDefinitions: []metav1.TableColumnDefinition{
					{Name: "Name", Type: "name"},
					{Name: "Foo", Type: "string"},
					{Name: "Bar", Type: "number"},
				},
				Rows: []metav1.TableRow{
					{
						Cells: []any{"the name is foo", "the foo value", "the bar value"},
						Object: runtime.RawExtension{
							Raw: []byte(`{"kind":"PartialObjectMetadata","apiVersion":"meta.k8s.io/v1","metadata":{"name":"foo","creationTimestamp":null}}`),
						},
					},
				},
			},
		},
		{
			name: "custom include object None",
			requests: []*http.Request{
				createRequest("/apis/ext.cattle.io/v1/testtypes?includeObject=None"),
				createRequest("/apis/ext.cattle.io/v1/testtypes/foo?includeObject=None"),
			},
			columns:            columns,
			convertFn:          convertFn,
			expectedStatusCode: http.StatusOK,
			expectedBody: &metav1.Table{
				TypeMeta: metav1.TypeMeta{Kind: "Table", APIVersion: "meta.k8s.io/v1"},
				ColumnDefinitions: []metav1.TableColumnDefinition{
					{Name: "Name", Type: "name"},
					{Name: "Foo", Type: "string"},
					{Name: "Bar", Type: "number"},
				},
				Rows: []metav1.TableRow{
					{
						Cells: []any{"the name is foo", "the foo value", "the bar value"},
					},
				},
			},
		},
		{
			name: "custom include object Object",
			requests: []*http.Request{
				createRequest("/apis/ext.cattle.io/v1/testtypes?includeObject=Object"),
				createRequest("/apis/ext.cattle.io/v1/testtypes/foo?includeObject=Object"),
			},
			columns:            columns,
			convertFn:          convertFn,
			expectedStatusCode: http.StatusOK,
			expectedBody: &metav1.Table{
				TypeMeta: metav1.TypeMeta{Kind: "Table", APIVersion: "meta.k8s.io/v1"},
				ColumnDefinitions: []metav1.TableColumnDefinition{
					{Name: "Name", Type: "name"},
					{Name: "Foo", Type: "string"},
					{Name: "Bar", Type: "number"},
				},
				Rows: []metav1.TableRow{
					{
						Cells: []any{"the name is foo", "the foo value", "the bar value"},
						Object: runtime.RawExtension{
							Raw: []byte(`{"kind":"TestType","apiVersion":"ext.cattle.io/v1","metadata":{"name":"foo","creationTimestamp":null}}`),
						},
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.columns != nil {
				store.Set(test.columns, test.convertFn)
			}

			for _, req := range test.requests {
				w := httptest.NewRecorder()

				extensionAPIServer.ServeHTTP(w, req)

				resp := w.Result()
				body, _ := io.ReadAll(resp.Body)

				require.Equal(t, test.expectedStatusCode, resp.StatusCode)
				if test.expectedBody != nil {
					table := &metav1.Table{}
					err = json.Unmarshal(body, table)
					require.NoError(t, err)
					require.Equal(t, test.expectedBody, table)
				}
			}
		})
	}

}
