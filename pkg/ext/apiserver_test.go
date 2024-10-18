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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
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

type mapStore struct {
	items  map[string]*TestType
	events chan WatchEvent[*TestType]
}

func newMapStore() *mapStore {
	return &mapStore{
		items:  make(map[string]*TestType),
		events: make(chan WatchEvent[*TestType], 100),
	}
}

func (t *mapStore) Create(ctx Context, obj *TestType, opts *metav1.CreateOptions) (*TestType, error) {
	if _, found := t.items[obj.Name]; found {
		return nil, apierrors.NewAlreadyExists(ctx.GroupVersionResource.GroupResource(), obj.Name)
	}
	t.items[obj.Name] = obj
	t.events <- WatchEvent[*TestType]{
		Event:  watch.Added,
		Object: obj,
	}
	return obj, nil
}

func (t *mapStore) Update(ctx Context, obj *TestType, opts *metav1.UpdateOptions) (*TestType, error) {
	if _, found := t.items[obj.Name]; !found {
		return nil, apierrors.NewNotFound(ctx.GroupVersionResource.GroupResource(), obj.Name)
	}
	obj.ManagedFields = []metav1.ManagedFieldsEntry{}
	t.items[obj.Name] = obj
	t.events <- WatchEvent[*TestType]{
		Event:  watch.Modified,
		Object: obj,
	}
	return obj, nil
}

func (t *mapStore) Get(ctx Context, name string, opts *metav1.GetOptions) (*TestType, error) {
	obj, found := t.items[name]
	if !found {
		return nil, apierrors.NewNotFound(ctx.GroupVersionResource.GroupResource(), name)
	}
	return obj, nil
}

func (t *mapStore) List(ctx Context, opts *metav1.ListOptions) (*TestTypeList, error) {
	items := []TestType{}
	for _, obj := range t.items {
		items = append(items, *obj)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Name > items[j].Name
	})
	list := &TestTypeList{
		Items: items,
	}
	return list, nil
}

func (t *mapStore) Watch(ctx Context, opts *metav1.ListOptions) (<-chan WatchEvent[*TestType], error) {
	return t.events, nil
}

func (t *mapStore) Delete(ctx Context, name string, opts *metav1.DeleteOptions) error {
	obj, found := t.items[name]
	if !found {
		return apierrors.NewNotFound(ctx.GroupVersionResource.GroupResource(), name)
	}

	delete(t.items, name)
	t.events <- WatchEvent[*TestType]{
		Event:  watch.Deleted,
		Object: obj,
	}
	return nil
}

func TestStore(t *testing.T) {
	scheme := runtime.NewScheme()
	AddToScheme(scheme)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", ":0")
	require.NoError(t, err)

	store := newMapStore()
	extensionAPIServer, cleanup, err := setupExtensionAPIServer(t, scheme, &TestType{}, &TestTypeList{}, store, func(opts *ExtensionAPIServerOptions) {
		opts.Listener = ln
		opts.Authorizer = authorizer.AuthorizerFunc(authzAllowAll)
		opts.Authenticator = authenticator.RequestFunc(authAsAdmin)
	}, nil)
	require.NoError(t, err)
	defer cleanup()

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

	err = extensionAPIServer.Run(ctx)
	require.NoError(t, err)

	cleanup := func() {
		cancel()
	}

	return extensionAPIServer, cleanup, nil
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
