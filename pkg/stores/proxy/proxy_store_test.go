package proxy

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	schema2 "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/rancher/wrangler/v3/pkg/schemas/validation"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
	clientgotesting "k8s.io/client-go/testing"

	"github.com/rancher/apiserver/pkg/apierror"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/wrangler/v3/pkg/schemas"

	"github.com/rancher/steve/pkg/client"
)

var c *watch.FakeWatcher

type testFactory struct {
	*client.Factory

	fakeClient *fake.FakeDynamicClient
}

func (t *testFactory) TableAdminClientForWatch(ctx *types.APIRequest, schema *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error) {
	return t.fakeClient.Resource(schema2.GroupVersionResource{}), nil
}

func (t *testFactory) TableClient(ctx *types.APIRequest, schema *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error) {
	return t.fakeClient.Resource(schema2.GroupVersionResource{}).Namespace(namespace), nil
}

func TestWatchNamesErrReceive(t *testing.T) {
	testClientFactory, err := client.NewFactory(&rest.Config{}, false)
	assert.Nil(t, err)

	fakeClient := fake.NewSimpleDynamicClient(runtime.NewScheme())
	c = watch.NewFakeWithChanSize(5, true)
	defer c.Stop()
	errMsgsToSend := []string{"err1", "err2", "err3"}
	c.Add(&v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "testsecret1"}})
	for index := range errMsgsToSend {
		c.Error(&metav1.Status{
			Message: errMsgsToSend[index],
		})
	}
	c.Add(&v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "testsecret2"}})
	fakeClient.PrependWatchReactor("*", func(action clientgotesting.Action) (handled bool, ret watch.Interface, err error) {
		return true, c, nil
	})
	testStore := Store{
		clientGetter: &testFactory{Factory: testClientFactory,
			fakeClient: fakeClient,
		},
	}
	apiSchema := &types.APISchema{Schema: &schemas.Schema{Attributes: map[string]interface{}{"table": "something"}}}
	wc, err := testStore.WatchNames(&types.APIRequest{Namespace: "", Schema: apiSchema, Request: &http.Request{}}, apiSchema, types.WatchRequest{}, sets.NewString("testsecret1", "testsecret2"))
	assert.Nil(t, err)

	eg := errgroup.Group{}
	eg.Go(func() error { return receiveUntil(wc, 5*time.Second) })

	err = eg.Wait()
	assert.Nil(t, err)

	assert.Equal(t, 0, len(c.ResultChan()), "Expected all secrets to have been received")
}

func TestByNames(t *testing.T) {
	s := Store{}
	apiSchema := &types.APISchema{Schema: &schemas.Schema{}}
	apiOp := &types.APIRequest{Namespace: "*", Schema: apiSchema, Request: &http.Request{}}
	names := sets.NewString("some-resource", "some-other-resource")
	result, warn, err := s.ByNames(apiOp, apiSchema, names)
	assert.NotNil(t, result)
	assert.Len(t, result.Items, 0)
	assert.Nil(t, err)
	assert.Nil(t, warn)
}

func receiveUntil(wc chan watch.Event, d time.Duration) error {
	timer := time.NewTicker(d)
	defer timer.Stop()
	secretNames := []string{"testsecret1", "testsecret2"}
	errMsgs := []string{"err1", "err2", "err3"}
	for {
		select {
		case event, ok := <-wc:
			if !ok {
				return errors.New("watch chan should not have been closed")
			}

			if event.Type == watch.Error {
				status, ok := event.Object.(*metav1.Status)
				if !ok {
					continue
				}
				if strings.HasSuffix(status.Message, errMsgs[0]) {
					errMsgs = errMsgs[1:]
				}
			}
			secret, ok := event.Object.(*v1.Secret)
			if !ok {
				continue
			}
			if secret.Name == secretNames[0] {
				secretNames = secretNames[1:]
			}
			if len(secretNames) == 0 && len(errMsgs) == 0 {
				return nil
			}
			continue
		case <-timer.C:
			return errors.New("timed out waiting to receiving objects from chan")
		}
	}
}

func TestCreate(t *testing.T) {
	type input struct {
		apiOp  *types.APIRequest
		schema *types.APISchema
		params types.APIObject
	}

	type expected struct {
		value   *unstructured.Unstructured
		warning []types.Warning
		err     error
	}

	testCases := []struct {
		name              string
		namespace         string
		input             input
		expected          expected
		createReactorFunc clientgotesting.ReactionFunc
	}{
		{
			name: "creating resource - namespace scoped",
			input: input{
				apiOp: &types.APIRequest{
					Schema: &types.APISchema{
						Schema: &schemas.Schema{
							ID: "testing",
						},
					},
					Request: &http.Request{URL: &url.URL{}},
				},
				schema: &types.APISchema{
					Schema: &schemas.Schema{
						ID: "testing",
						Attributes: map[string]interface{}{
							"version":    "v1",
							"kind":       "Secret",
							"namespaced": true,
						},
					},
				},
				params: types.APIObject{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "Secret",
						"metadata": map[string]interface{}{
							"name":      "testing-secret",
							"namespace": "testing-ns",
						},
					},
				},
			},
			createReactorFunc: func(action clientgotesting.Action) (handled bool, ret runtime.Object, err error) {
				return false, ret, nil
			},
			expected: expected{
				value: &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Secret",
					"metadata": map[string]interface{}{
						"name":      "testing-secret",
						"namespace": "testing-ns",
					},
				}},
				warning: []types.Warning{},
				err:     nil,
			},
		},
		{
			name: "creating resource - cluster scoped",
			input: input{
				apiOp: &types.APIRequest{
					Schema: &types.APISchema{
						Schema: &schemas.Schema{
							ID: "testing",
						},
					},
					Request: &http.Request{URL: &url.URL{}},
				},
				schema: &types.APISchema{
					Schema: &schemas.Schema{
						ID: "testing",
						Attributes: map[string]interface{}{
							"version":    "v1",
							"kind":       "Secret",
							"namespaced": false,
						},
					},
				},
				params: types.APIObject{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "Secret",
						"metadata": map[string]interface{}{
							"name": "testing-secret",
						},
					},
				},
			},
			createReactorFunc: func(action clientgotesting.Action) (handled bool, ret runtime.Object, err error) {
				return false, ret, nil
			},
			expected: expected{
				value: &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Secret",
					"metadata": map[string]interface{}{
						"name": "testing-secret",
					},
				}},
				warning: []types.Warning{},
				err:     nil,
			},
		},
		{
			name: "missing name",
			input: input{
				apiOp: &types.APIRequest{
					Schema: &types.APISchema{
						Schema: &schemas.Schema{
							ID: "testing",
						},
					},
					Request: &http.Request{URL: &url.URL{}},
				},
				schema: &types.APISchema{
					Schema: &schemas.Schema{
						ID: "testing",
						Attributes: map[string]interface{}{
							"version": "v1",
							"kind":    "Secret",
						},
					},
				},
				params: types.APIObject{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "Secret",
						"metadata": map[string]interface{}{
							"namespace":    "testing-ns",
							"generateName": "testing-gen-name",
						},
					},
				},
			},
			createReactorFunc: func(action clientgotesting.Action) (handled bool, ret runtime.Object, err error) {
				return false, ret, nil
			},
			expected: expected{
				value: &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Secret",
					"metadata": map[string]interface{}{
						"generateName": "testing-gen-name",
						"namespace":    "testing-ns",
					},
				}},
				warning: []types.Warning{},
				err:     nil,
			},
		},
		{
			name: "missing name / generateName",
			input: input{
				apiOp: &types.APIRequest{
					Schema: &types.APISchema{
						Schema: &schemas.Schema{
							ID: "testing",
						},
					},
					Request: &http.Request{URL: &url.URL{}},
				},
				schema: &types.APISchema{
					Schema: &schemas.Schema{
						ID: "testing",
						Attributes: map[string]interface{}{
							"version": "v1",
							"kind":    "Secret",
						},
					},
				},
				params: types.APIObject{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "Secret",
						"metadata": map[string]interface{}{
							"namespace": "testing-ns",
						},
					},
				},
			},
			createReactorFunc: func(action clientgotesting.Action) (handled bool, ret runtime.Object, err error) {
				return false, ret, nil
			},
			expected: expected{
				value: &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Secret",
					"metadata": map[string]interface{}{
						"generateName": "t-",
						"namespace":    "testing-ns",
					},
				}},
				warning: []types.Warning{},
				err:     nil,
			},
		},
		{
			name: "missing namespace in the params (should copy from apiOp)",
			input: input{
				apiOp: &types.APIRequest{
					Schema: &types.APISchema{
						Schema: &schemas.Schema{
							ID: "testing",
						},
					},
					Namespace: "testing-ns",
					Request:   &http.Request{URL: &url.URL{}},
				},
				schema: &types.APISchema{
					Schema: &schemas.Schema{
						ID: "testing",
						Attributes: map[string]interface{}{
							"version":    "v1",
							"kind":       "Secret",
							"namespaced": true,
						},
					},
				},
				params: types.APIObject{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "Secret",
						"metadata": map[string]interface{}{
							"name": "testing-secret",
						},
					},
				},
			},
			createReactorFunc: func(action clientgotesting.Action) (handled bool, ret runtime.Object, err error) {
				return false, ret, nil
			},
			expected: expected{
				value: &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Secret",
					"metadata": map[string]interface{}{
						"name":      "testing-secret",
						"namespace": "testing-ns",
					},
				}},
				warning: []types.Warning{},
				err:     nil,
			},
		},
		{
			name: "missing namespace - namespace scoped",
			input: input{
				apiOp: &types.APIRequest{
					Schema: &types.APISchema{
						Schema: &schemas.Schema{
							ID: "testing",
						},
					},
					Request: &http.Request{URL: &url.URL{}},
				},
				schema: &types.APISchema{
					Schema: &schemas.Schema{
						ID: "testing",
						Attributes: map[string]interface{}{
							"namespaced": true,
						},
					},
				},
				params: types.APIObject{},
			},
			expected: expected{
				value:   nil,
				warning: nil,
				err: apierror.NewAPIError(
					validation.InvalidBodyContent,
					errNamespaceRequired,
				),
			},
		},
		{
			name: "error response",
			input: input{
				apiOp: &types.APIRequest{
					Schema: &types.APISchema{
						Schema: &schemas.Schema{
							ID: "testing",
						},
					},
					Request: &http.Request{URL: &url.URL{}},
				},
				schema: &types.APISchema{
					Schema: &schemas.Schema{
						ID: "testing",
						Attributes: map[string]interface{}{
							"version":    "v1",
							"kind":       "Secret",
							"namespaced": true,
						},
					},
				},
				params: types.APIObject{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "Secret",
						"metadata": map[string]interface{}{
							"name":      "testing-secret",
							"namespace": "testing-ns",
						},
					},
				},
			},
			createReactorFunc: func(action clientgotesting.Action) (handled bool, ret runtime.Object, err error) {
				return true, nil, apierrors.NewUnauthorized("sample reason")
			},
			expected: expected{
				value:   nil,
				warning: []types.Warning{},
				err:     apierrors.NewUnauthorized("sample reason"),
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			testClientFactory, err := client.NewFactory(&rest.Config{}, false)
			assert.NoError(t, err)

			fakeClient := fake.NewSimpleDynamicClient(runtime.NewScheme())

			if tt.createReactorFunc != nil {
				fakeClient.PrependReactor("create", "*", tt.createReactorFunc)
			}

			testStore := Store{
				clientGetter: &testFactory{Factory: testClientFactory,
					fakeClient: fakeClient,
				},
			}

			value, warning, err := testStore.Create(tt.input.apiOp, tt.input.schema, tt.input.params)

			assert.Equal(t, tt.expected.value, value)
			assert.Equal(t, tt.expected.warning, warning)
			assert.Equal(t, tt.expected.err, err)
		})
	}
}

func TestUpdate(t *testing.T) {
	type input struct {
		apiOp  *types.APIRequest
		schema *types.APISchema
		params types.APIObject
		id     string
	}

	type expected struct {
		value   *unstructured.Unstructured
		warning []types.Warning
		err     error
	}

	sampleCreateInput := input{
		apiOp: &types.APIRequest{
			Request: &http.Request{
				URL:    &url.URL{},
				Method: http.MethodPost,
			},
			Schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "testing",
				},
			},
			Method: http.MethodPost,
		},
		schema: &types.APISchema{
			Schema: &schemas.Schema{
				ID: "testing",
				Attributes: map[string]interface{}{
					"kind":       "Secret",
					"version":    "v1",
					"namespaced": true,
				},
			},
		},
		params: types.APIObject{
			Object: map[string]interface{}{
				"kind":       "Secret",
				"apiVersion": "v1",
				"metadata": map[string]interface{}{
					"name":            "testing-secret",
					"namespace":       "testing-ns",
					"resourceVersion": "1",
				},
			},
		},
	}

	testCases := []struct {
		name               string
		updateCallbackFunc clientgotesting.ReactionFunc
		createInput        *input
		updateInput        input
		expected           expected
	}{
		{
			name: "update - usual request",
			updateCallbackFunc: func(action clientgotesting.Action) (handled bool, ret runtime.Object, err error) {
				return false, ret, nil
			},
			createInput: &sampleCreateInput,
			updateInput: input{
				apiOp: &types.APIRequest{
					Request: &http.Request{
						URL:    &url.URL{},
						Method: http.MethodPut,
					},
					Schema: &types.APISchema{
						Schema: &schemas.Schema{
							ID: "testing",
						},
					},
					Method: http.MethodPut,
				},

				schema: &types.APISchema{
					Schema: &schemas.Schema{
						ID: "testing",
						Attributes: map[string]interface{}{
							"version":    "v2",
							"kind":       "Secret",
							"namespaced": true,
						},
					},
				},
				params: types.APIObject{
					Object: map[string]interface{}{
						"apiVersion": "v2",
						"kind":       "Secret",
						"metadata": map[string]interface{}{
							"name":            "testing-secret",
							"namespace":       "testing-ns",
							"resourceVersion": "1",
						},
					},
				},
			},
			expected: expected{
				value: &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "v2",
					"kind":       "Secret",
					"metadata": map[string]interface{}{
						"name":            "testing-secret",
						"namespace":       "testing-ns",
						"resourceVersion": "1",
					},
				}},
				warning: []types.Warning{},
				err:     nil,
			},
		},
		{
			name: "update - different apiVersion and kind (params and schema) - should copy from schema",
			updateCallbackFunc: func(action clientgotesting.Action) (handled bool, ret runtime.Object, err error) {
				return false, ret, nil
			},
			createInput: &sampleCreateInput,
			updateInput: input{
				apiOp: &types.APIRequest{
					Request: &http.Request{
						URL:    &url.URL{},
						Method: http.MethodPut,
					},
					Schema: &types.APISchema{
						Schema: &schemas.Schema{
							ID: "testing",
						},
					},
					Method: http.MethodPut,
				},

				schema: &types.APISchema{
					Schema: &schemas.Schema{
						ID: "testing",
						Attributes: map[string]interface{}{
							"version":    "v2",
							"kind":       "Secret",
							"namespaced": true,
						},
					},
				},
				params: types.APIObject{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata": map[string]interface{}{
							"name":            "testing-secret",
							"namespace":       "testing-ns",
							"resourceVersion": "1",
						},
					},
				},
			},
			expected: expected{
				value: &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "v2",
					"kind":       "Secret",
					"metadata": map[string]interface{}{
						"name":            "testing-secret",
						"namespace":       "testing-ns",
						"resourceVersion": "1",
					},
				}},
				warning: []types.Warning{},
				err:     nil,
			},
		},
		{
			name: "update - missing apiVersion and kind in params - should copy from schema",
			updateCallbackFunc: func(action clientgotesting.Action) (handled bool, ret runtime.Object, err error) {
				return false, ret, nil
			},
			createInput: &sampleCreateInput,
			updateInput: input{
				apiOp: &types.APIRequest{
					Request: &http.Request{
						URL:    &url.URL{},
						Method: http.MethodPost,
					},
					Schema: &types.APISchema{
						Schema: &schemas.Schema{
							ID: "testing",
						},
					},
					Method: http.MethodPost,
				},

				schema: &types.APISchema{
					Schema: &schemas.Schema{
						ID: "testing",
						Attributes: map[string]interface{}{
							"version":    "v2",
							"kind":       "Secret",
							"namespaced": true,
						},
					},
				},
				params: types.APIObject{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":            "testing-secret",
							"namespace":       "testing-ns",
							"resourceVersion": "1",
						},
					},
				},
			},
			expected: expected{
				value: &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "v2",
					"kind":       "Secret",
					"metadata": map[string]interface{}{
						"name":            "testing-secret",
						"namespace":       "testing-ns",
						"resourceVersion": "1",
					},
				}},
				warning: []types.Warning{},
				err:     nil,
			},
		},
		{
			name: "update - missing resource version",
			updateCallbackFunc: func(action clientgotesting.Action) (handled bool, ret runtime.Object, err error) {
				return false, ret, nil
			},
			updateInput: input{
				apiOp: &types.APIRequest{
					Request: &http.Request{
						URL:    &url.URL{},
						Method: http.MethodPut,
					},
					Schema: &types.APISchema{
						Schema: &schemas.Schema{
							ID: "testing",
						},
					},
					Method: http.MethodPut,
				},

				schema: &types.APISchema{
					Schema: &schemas.Schema{
						ID: "testing",
						Attributes: map[string]interface{}{
							"version":    "v1",
							"kind":       "Secret",
							"namespaced": true,
						},
					},
				},
				params: types.APIObject{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "Secret",
						"metadata": map[string]interface{}{
							"name":      "testing-secret",
							"namespace": "testing-ns",
						},
					},
				},
			},
			expected: expected{
				value:   nil,
				warning: nil,
				err:     errors.New(errResourceVersionRequired),
			},
		},
		{
			name: "update - error request",
			updateCallbackFunc: func(action clientgotesting.Action) (handled bool, ret runtime.Object, err error) {
				return true, nil, apierrors.NewUnauthorized("sample reason")
			},
			createInput: &sampleCreateInput,
			updateInput: input{
				apiOp: &types.APIRequest{
					Request: &http.Request{
						URL:    &url.URL{},
						Method: http.MethodPut,
					},
					Schema: &types.APISchema{
						Schema: &schemas.Schema{
							ID: "testing",
						},
					},
					Method: http.MethodPut,
				},

				schema: &types.APISchema{
					Schema: &schemas.Schema{
						ID: "testing",
						Attributes: map[string]interface{}{
							"kind":       "Secret",
							"namespaced": true,
						},
					},
				},
				params: types.APIObject{
					Object: map[string]interface{}{
						"apiVersion": "v2",
						"metadata": map[string]interface{}{
							"name":            "testing-secret",
							"namespace":       "testing-ns",
							"resourceVersion": "1",
						},
					},
				},
			},
			expected: expected{
				value:   nil,
				warning: nil,
				err:     apierrors.NewUnauthorized("sample reason"),
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			testClientFactory, err := client.NewFactory(&rest.Config{}, false)
			assert.NoError(t, err)

			fakeClient := fake.NewSimpleDynamicClient(runtime.NewScheme())

			if tt.updateCallbackFunc != nil {
				fakeClient.PrependReactor("update", "*", tt.updateCallbackFunc)
			}

			testStore := Store{
				clientGetter: &testFactory{Factory: testClientFactory,
					fakeClient: fakeClient,
				},
			}

			// Creating the object first, so we can update it later (this function is not the SUT)
			if tt.createInput != nil {
				_, _, err = testStore.Create(tt.createInput.apiOp, tt.createInput.schema, tt.createInput.params)
				assert.NoError(t, err)
			}

			value, warning, err := testStore.Update(tt.updateInput.apiOp, tt.updateInput.schema, tt.updateInput.params, tt.updateInput.id)

			assert.Equal(t, tt.expected.value, value)
			assert.Equal(t, tt.expected.warning, warning)

			if tt.expected.err != nil {
				assert.Equal(t, tt.expected.err.Error(), err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
