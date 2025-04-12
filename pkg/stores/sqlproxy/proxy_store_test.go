package sqlproxy

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/rancher/wrangler/v3/pkg/schemas/validation"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/rancher/steve/pkg/attributes"
	"github.com/rancher/steve/pkg/resources/common"
	"github.com/rancher/steve/pkg/sqlcache/informer"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/rancher/steve/pkg/stores/sqlpartition/listprocessor"
	"github.com/rancher/steve/pkg/stores/sqlproxy/tablelistconvert"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/pkg/errors"
	"github.com/rancher/apiserver/pkg/apierror"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/client"
	"github.com/rancher/wrangler/v3/pkg/schemas"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	schema2 "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
	clientgotesting "k8s.io/client-go/testing"
)

//go:generate mockgen --build_flags=--mod=mod -package sqlproxy -destination ./proxy_mocks_test.go github.com/rancher/steve/pkg/stores/sqlproxy Cache,ClientGetter,CacheFactory,SchemaColumnSetter,RelationshipNotifier,TransformBuilder
//go:generate mockgen --build_flags=--mod=mod -package sqlproxy -destination ./sql_informer_mocks_test.go github.com/rancher/steve/pkg/sqlcache/informer ByOptionsLister
//go:generate mockgen --build_flags=--mod=mod -package sqlproxy -destination ./dynamic_mocks_test.go k8s.io/client-go/dynamic ResourceInterface

var c *watch.FakeWatcher

type testFactory struct {
	*client.Factory

	fakeClient *fake.FakeDynamicClient
}

func (t *testFactory) TableClient(ctx *types.APIRequest, schema *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error) {
	return t.fakeClient.Resource(schema2.GroupVersionResource{}).Namespace(namespace), nil
}

func TestNewProxyStore(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}
	var tests []testCase
	tests = append(tests, testCase{
		description: "NewProxyStore() with no errors returned should returned no errors. Should initialize and assign" +
			" a namespace cache.",
		test: func(t *testing.T) {
			scc := NewMockSchemaColumnSetter(gomock.NewController(t))
			cg := NewMockClientGetter(gomock.NewController(t))
			rn := NewMockRelationshipNotifier(gomock.NewController(t))
			cf := NewMockCacheFactory(gomock.NewController(t))
			ri := NewMockResourceInterface(gomock.NewController(t))
			bloi := NewMockByOptionsLister(gomock.NewController(t))
			c := factory.Cache{
				ByOptionsLister: &informer.Informer{
					ByOptionsLister: bloi,
				},
			}

			nsSchema := baseNSSchema
			scc.EXPECT().SetColumns(context.Background(), &nsSchema).Return(nil)
			cg.EXPECT().TableAdminClient(nil, &nsSchema, "", &WarningBuffer{}).Return(ri, nil)
			cf.EXPECT().CacheFor(context.Background(), [][]string{{`id`}, {`metadata`, `state`, `name`}, {"metadata", "labels", "field.cattle.io/projectId"}}, gomock.Any(), &tablelistconvert.Client{ResourceInterface: ri}, attributes.GVK(&nsSchema), false, true).Return(c, nil)

			s, err := NewProxyStore(context.Background(), scc, cg, rn, nil, cf)
			assert.Nil(t, err)
			assert.Equal(t, scc, s.columnSetter)
			assert.Equal(t, cg, s.clientGetter)
			assert.Equal(t, rn, s.notifier)
			assert.Equal(t, s.cacheFactory, cf)
			assert.NotNil(t, s.namespaceCache)
		},
	})
	tests = append(tests, testCase{
		description: "NewProxyStore() with schema column setter SetColumns() error returned should return not return and error" +
			" and not set namespace cache.",
		test: func(t *testing.T) {
			scc := NewMockSchemaColumnSetter(gomock.NewController(t))
			cg := NewMockClientGetter(gomock.NewController(t))
			rn := NewMockRelationshipNotifier(gomock.NewController(t))
			cf := NewMockCacheFactory(gomock.NewController(t))

			nsSchema := baseNSSchema
			scc.EXPECT().SetColumns(context.Background(), &nsSchema).Return(fmt.Errorf("error"))

			s, err := NewProxyStore(context.Background(), scc, cg, rn, nil, cf)
			assert.Nil(t, err)
			assert.Equal(t, scc, s.columnSetter)
			assert.Equal(t, cg, s.clientGetter)
			assert.Equal(t, rn, s.notifier)
			assert.Equal(t, s.cacheFactory, cf)
			assert.Nil(t, s.namespaceCache)
		},
	})
	tests = append(tests, testCase{
		description: "NewProxyStore() with client getter TableAdminClient() error returned should return not return and error" +
			" and not set namespace cache.",
		test: func(t *testing.T) {
			scc := NewMockSchemaColumnSetter(gomock.NewController(t))
			cg := NewMockClientGetter(gomock.NewController(t))
			rn := NewMockRelationshipNotifier(gomock.NewController(t))
			cf := NewMockCacheFactory(gomock.NewController(t))

			nsSchema := baseNSSchema
			scc.EXPECT().SetColumns(context.Background(), &nsSchema).Return(nil)
			cg.EXPECT().TableAdminClient(nil, &nsSchema, "", &WarningBuffer{}).Return(nil, fmt.Errorf("error"))

			s, err := NewProxyStore(context.Background(), scc, cg, rn, nil, cf)
			assert.Nil(t, err)
			assert.Equal(t, scc, s.columnSetter)
			assert.Equal(t, cg, s.clientGetter)
			assert.Equal(t, rn, s.notifier)
			assert.Equal(t, s.cacheFactory, cf)
			assert.Nil(t, s.namespaceCache)
		},
	})
	tests = append(tests, testCase{
		description: "NewProxyStore() with client getter TableAdminClient() error returned should return not return and error" +
			" and not set namespace cache.",
		test: func(t *testing.T) {
			scc := NewMockSchemaColumnSetter(gomock.NewController(t))
			cg := NewMockClientGetter(gomock.NewController(t))
			rn := NewMockRelationshipNotifier(gomock.NewController(t))
			cf := NewMockCacheFactory(gomock.NewController(t))
			ri := NewMockResourceInterface(gomock.NewController(t))

			nsSchema := baseNSSchema
			scc.EXPECT().SetColumns(context.Background(), &nsSchema).Return(nil)
			cg.EXPECT().TableAdminClient(nil, &nsSchema, "", &WarningBuffer{}).Return(ri, nil)
			cf.EXPECT().CacheFor(context.Background(), [][]string{{`id`}, {`metadata`, `state`, `name`}, {"metadata", "labels", "field.cattle.io/projectId"}}, gomock.Any(), &tablelistconvert.Client{ResourceInterface: ri}, attributes.GVK(&nsSchema), false, true).Return(factory.Cache{}, fmt.Errorf("error"))

			s, err := NewProxyStore(context.Background(), scc, cg, rn, nil, cf)
			assert.Nil(t, err)
			assert.Equal(t, scc, s.columnSetter)
			assert.Equal(t, cg, s.clientGetter)
			assert.Equal(t, rn, s.notifier)
			assert.Equal(t, s.cacheFactory, cf)
			assert.Nil(t, s.namespaceCache)
		},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestListByPartitions(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}
	var tests []testCase
	tests = append(tests, testCase{
		description: "client ListByPartitions() with no errors returned should returned no errors. Should pass fields" +
			" from schema.",
		test: func(t *testing.T) {
			nsi := NewMockCache(gomock.NewController(t))
			cg := NewMockClientGetter(gomock.NewController(t))
			cf := NewMockCacheFactory(gomock.NewController(t))
			ri := NewMockResourceInterface(gomock.NewController(t))
			bloi := NewMockByOptionsLister(gomock.NewController(t))
			tb := NewMockTransformBuilder(gomock.NewController(t))
			inf := &informer.Informer{
				ByOptionsLister: bloi,
			}
			c := factory.Cache{
				ByOptionsLister: inf,
			}
			s := &Store{
				ctx:              context.Background(),
				namespaceCache:   nsi,
				clientGetter:     cg,
				cacheFactory:     cf,
				transformBuilder: tb,
			}
			var partitions []partition.Partition
			req := &types.APIRequest{
				Request: &http.Request{
					URL: &url.URL{},
				},
			}
			schema := &types.APISchema{
				Schema: &schemas.Schema{Attributes: map[string]interface{}{
					"columns": []common.ColumnDefinition{
						{
							Field: "some.field",
						},
					},
					"verbs": []string{"list", "watch"},
				}},
			}
			expectedItems := []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
			}
			listToReturn := &unstructured.UnstructuredList{
				Items: make([]unstructured.Unstructured, len(expectedItems), len(expectedItems)),
			}
			gvk := schema2.GroupVersionKind{
				Group:   "some",
				Version: "test",
				Kind:    "gvk",
			}
			typeSpecificIndexedFields["some_test_gvk"] = [][]string{{"gvk", "specific", "fields"}}

			attributes.SetGVK(schema, gvk)
			// ListByPartitions copies point so we need some original record of items to ensure as asserting listToReturn's
			// items is equal to the list returned by ListByParititons doesn't ensure no mutation happened
			copy(listToReturn.Items, expectedItems)
			opts, err := listprocessor.ParseQuery(req, nil)
			assert.Nil(t, err)
			cg.EXPECT().TableAdminClient(req, schema, "", &WarningBuffer{}).Return(ri, nil)
			// This tests that fields are being extracted from schema columns and the type specific fields map
			cf.EXPECT().CacheFor(context.Background(), [][]string{{"some", "field"}, {`id`}, {`metadata`, `state`, `name`}, {"gvk", "specific", "fields"}}, gomock.Any(), &tablelistconvert.Client{ResourceInterface: ri}, attributes.GVK(schema), attributes.Namespaced(schema), true).Return(c, nil)
			tb.EXPECT().GetTransformFunc(attributes.GVK(schema)).Return(func(obj interface{}) (interface{}, error) { return obj, nil })
			bloi.EXPECT().ListByOptions(req.Context(), &opts, partitions, req.Namespace).Return(listToReturn, len(listToReturn.Items), "", nil)
			list, total, contToken, err := s.ListByPartitions(req, schema, partitions)
			assert.Nil(t, err)
			assert.Equal(t, expectedItems, list)
			assert.Equal(t, len(expectedItems), total)
			assert.Equal(t, "", contToken)
		},
	})
	tests = append(tests, testCase{
		description: "client ListByPartitions() with ParseQuery error returned should return an error.",
		test: func(t *testing.T) {
			nsi := NewMockCache(gomock.NewController(t))
			cg := NewMockClientGetter(gomock.NewController(t))
			cf := NewMockCacheFactory(gomock.NewController(t))
			tb := NewMockTransformBuilder(gomock.NewController(t))

			s := &Store{
				ctx:              context.Background(),
				namespaceCache:   nsi,
				clientGetter:     cg,
				cacheFactory:     cf,
				transformBuilder: tb,
			}
			var partitions []partition.Partition
			req := &types.APIRequest{
				Request: &http.Request{
					URL: &url.URL{RawQuery: "projectsornamespaces=somethin"},
				},
			}
			schema := &types.APISchema{
				Schema: &schemas.Schema{Attributes: map[string]interface{}{
					"columns": []common.ColumnDefinition{
						{
							Field: "some.field",
						},
					},
					"verbs": []string{"list", "watch"},
				}},
			}
			expectedItems := []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
			}
			listToReturn := &unstructured.UnstructuredList{
				Items: make([]unstructured.Unstructured, len(expectedItems), len(expectedItems)),
			}
			gvk := schema2.GroupVersionKind{
				Group:   "some",
				Version: "test",
				Kind:    "gvk",
			}
			typeSpecificIndexedFields["some_test_gvk"] = [][]string{{"gvk", "specific", "fields"}}

			attributes.SetGVK(schema, gvk)
			// ListByPartitions copies point so we need some original record of items to ensure as asserting listToReturn's
			// items is equal to the list returned by ListByParititons doesn't ensure no mutation happened
			copy(listToReturn.Items, expectedItems)

			nsi.EXPECT().ListByOptions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, 0, "", fmt.Errorf("error")).Times(2)
			_, err := listprocessor.ParseQuery(req, nsi)
			assert.NotNil(t, err)

			_, _, _, err = s.ListByPartitions(req, schema, partitions)
			assert.NotNil(t, err)
		},
	})
	tests = append(tests, testCase{
		description: "client ListByPartitions() with no errors returned should returned no errors. Should pass fields" +
			" from schema.",
		test: func(t *testing.T) {
			nsi := NewMockCache(gomock.NewController(t))
			cg := NewMockClientGetter(gomock.NewController(t))
			cf := NewMockCacheFactory(gomock.NewController(t))
			tb := NewMockTransformBuilder(gomock.NewController(t))

			s := &Store{
				ctx:              context.Background(),
				namespaceCache:   nsi,
				clientGetter:     cg,
				cacheFactory:     cf,
				transformBuilder: tb,
			}
			var partitions []partition.Partition
			req := &types.APIRequest{
				Request: &http.Request{
					URL: &url.URL{},
				},
			}
			schema := &types.APISchema{
				Schema: &schemas.Schema{Attributes: map[string]interface{}{
					"columns": []common.ColumnDefinition{
						{
							Field: "some.field",
						},
					},
					"verbs": []string{"list", "watch"},
				}},
			}
			expectedItems := []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
			}
			listToReturn := &unstructured.UnstructuredList{
				Items: make([]unstructured.Unstructured, len(expectedItems), len(expectedItems)),
			}
			gvk := schema2.GroupVersionKind{
				Group:   "some",
				Version: "test",
				Kind:    "gvk",
			}
			typeSpecificIndexedFields["some_test_gvk"] = [][]string{{"gvk", "specific", "fields"}}

			attributes.SetGVK(schema, gvk)
			// ListByPartitions copies point so we need some original record of items to ensure as asserting listToReturn's
			// items is equal to the list returned by ListByParititons doesn't ensure no mutation happened
			copy(listToReturn.Items, expectedItems)
			_, err := listprocessor.ParseQuery(req, nil)
			assert.Nil(t, err)
			cg.EXPECT().TableAdminClient(req, schema, "", &WarningBuffer{}).Return(nil, fmt.Errorf("error"))

			_, _, _, err = s.ListByPartitions(req, schema, partitions)
			assert.NotNil(t, err)
		},
	})
	tests = append(tests, testCase{
		description: "client ListByPartitions() should detect listable-but-unwatchable schema, still work normally",
		test: func(t *testing.T) {
			nsi := NewMockCache(gomock.NewController(t))
			cg := NewMockClientGetter(gomock.NewController(t))
			cf := NewMockCacheFactory(gomock.NewController(t))
			ri := NewMockResourceInterface(gomock.NewController(t))
			bloi := NewMockByOptionsLister(gomock.NewController(t))
			tb := NewMockTransformBuilder(gomock.NewController(t))
			inf := &informer.Informer{
				ByOptionsLister: bloi,
			}
			c := factory.Cache{
				ByOptionsLister: inf,
			}
			s := &Store{
				ctx:              context.Background(),
				namespaceCache:   nsi,
				clientGetter:     cg,
				cacheFactory:     cf,
				transformBuilder: tb,
			}
			var partitions []partition.Partition
			req := &types.APIRequest{
				Request: &http.Request{
					URL: &url.URL{},
				},
			}
			schema := &types.APISchema{
				Schema: &schemas.Schema{Attributes: map[string]interface{}{
					"columns": []common.ColumnDefinition{
						{
							Field: "some.field",
						},
					},
					// note: no watch here
					"verbs": []string{"list"},
				}},
			}
			expectedItems := []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
			}
			listToReturn := &unstructured.UnstructuredList{
				Items: make([]unstructured.Unstructured, len(expectedItems), len(expectedItems)),
			}
			gvk := schema2.GroupVersionKind{
				Group:   "some",
				Version: "test",
				Kind:    "gvk",
			}
			typeSpecificIndexedFields["some_test_gvk"] = [][]string{{"gvk", "specific", "fields"}}

			attributes.SetGVK(schema, gvk)
			// ListByPartitions copies point so we need some original record of items to ensure as asserting listToReturn's
			// items is equal to the list returned by ListByParititons doesn't ensure no mutation happened
			copy(listToReturn.Items, expectedItems)
			opts, err := listprocessor.ParseQuery(req, nil)
			assert.Nil(t, err)
			cg.EXPECT().TableAdminClient(req, schema, "", &WarningBuffer{}).Return(ri, nil)

			// This tests that fields are being extracted from schema columns and the type specific fields map
			// note also the watchable bool is expected to be false
			cf.EXPECT().CacheFor(context.Background(), [][]string{{"some", "field"}, {`id`}, {`metadata`, `state`, `name`}, {"gvk", "specific", "fields"}}, gomock.Any(), &tablelistconvert.Client{ResourceInterface: ri}, attributes.GVK(schema), attributes.Namespaced(schema), false).Return(c, nil)

			tb.EXPECT().GetTransformFunc(attributes.GVK(schema)).Return(func(obj interface{}) (interface{}, error) { return obj, nil })
			bloi.EXPECT().ListByOptions(req.Context(), &opts, partitions, req.Namespace).Return(listToReturn, len(listToReturn.Items), "", nil)
			list, total, contToken, err := s.ListByPartitions(req, schema, partitions)
			assert.Nil(t, err)
			assert.Equal(t, expectedItems, list)
			assert.Equal(t, len(expectedItems), total)
			assert.Equal(t, "", contToken)
		},
	})
	tests = append(tests, testCase{
		description: "client ListByPartitions() with CacheFor() error returned should returned an errors. Should pass fields",
		test: func(t *testing.T) {
			nsi := NewMockCache(gomock.NewController(t))
			cg := NewMockClientGetter(gomock.NewController(t))
			cf := NewMockCacheFactory(gomock.NewController(t))
			ri := NewMockResourceInterface(gomock.NewController(t))
			tb := NewMockTransformBuilder(gomock.NewController(t))

			s := &Store{
				ctx:              context.Background(),
				namespaceCache:   nsi,
				clientGetter:     cg,
				cacheFactory:     cf,
				transformBuilder: tb,
			}
			var partitions []partition.Partition
			req := &types.APIRequest{
				Request: &http.Request{
					URL: &url.URL{},
				},
			}
			schema := &types.APISchema{
				Schema: &schemas.Schema{Attributes: map[string]interface{}{
					"columns": []common.ColumnDefinition{
						{
							Field: "some.field",
						},
					},
					"verbs": []string{"list", "watch"},
				}},
			}
			expectedItems := []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
			}
			listToReturn := &unstructured.UnstructuredList{
				Items: make([]unstructured.Unstructured, len(expectedItems), len(expectedItems)),
			}
			gvk := schema2.GroupVersionKind{
				Group:   "some",
				Version: "test",
				Kind:    "gvk",
			}
			typeSpecificIndexedFields["some_test_gvk"] = [][]string{{"gvk", "specific", "fields"}}

			attributes.SetGVK(schema, gvk)
			// ListByPartitions copies point so we need some original record of items to ensure as asserting listToReturn's
			// items is equal to the list returned by ListByParititons doesn't ensure no mutation happened
			copy(listToReturn.Items, expectedItems)
			_, err := listprocessor.ParseQuery(req, nil)
			assert.Nil(t, err)
			cg.EXPECT().TableAdminClient(req, schema, "", &WarningBuffer{}).Return(ri, nil)
			// This tests that fields are being extracted from schema columns and the type specific fields map
			tb.EXPECT().GetTransformFunc(attributes.GVK(schema)).Return(func(obj interface{}) (interface{}, error) { return obj, nil })
			cf.EXPECT().CacheFor(context.Background(), [][]string{{"some", "field"}, {`id`}, {`metadata`, `state`, `name`}, {"gvk", "specific", "fields"}}, gomock.Any(), &tablelistconvert.Client{ResourceInterface: ri}, attributes.GVK(schema), attributes.Namespaced(schema), true).Return(factory.Cache{}, fmt.Errorf("error"))

			_, _, _, err = s.ListByPartitions(req, schema, partitions)
			assert.NotNil(t, err)
		},
	})
	tests = append(tests, testCase{
		description: "client ListByPartitions() with ListByOptions() error returned should return an errors. Should pass fields" +
			" from schema.",
		test: func(t *testing.T) {
			nsi := NewMockCache(gomock.NewController(t))
			cg := NewMockClientGetter(gomock.NewController(t))
			cf := NewMockCacheFactory(gomock.NewController(t))
			ri := NewMockResourceInterface(gomock.NewController(t))
			bloi := NewMockByOptionsLister(gomock.NewController(t))
			tb := NewMockTransformBuilder(gomock.NewController(t))
			inf := &informer.Informer{
				ByOptionsLister: bloi,
			}
			c := factory.Cache{
				ByOptionsLister: inf,
			}
			s := &Store{
				ctx:              context.Background(),
				namespaceCache:   nsi,
				clientGetter:     cg,
				cacheFactory:     cf,
				transformBuilder: tb,
			}
			var partitions []partition.Partition
			req := &types.APIRequest{
				Request: &http.Request{
					URL: &url.URL{},
				},
			}
			schema := &types.APISchema{
				Schema: &schemas.Schema{Attributes: map[string]interface{}{
					"columns": []common.ColumnDefinition{
						{
							Field: "some.field",
						},
					},
					"verbs": []string{"list", "watch"},
				}},
			}
			expectedItems := []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
			}
			listToReturn := &unstructured.UnstructuredList{
				Items: make([]unstructured.Unstructured, len(expectedItems), len(expectedItems)),
			}
			gvk := schema2.GroupVersionKind{
				Group:   "some",
				Version: "test",
				Kind:    "gvk",
			}
			typeSpecificIndexedFields["some_test_gvk"] = [][]string{{"gvk", "specific", "fields"}}

			attributes.SetGVK(schema, gvk)
			// ListByPartitions copies point so we need some original record of items to ensure as asserting listToReturn's
			// items is equal to the list returned by ListByParititons doesn't ensure no mutation happened
			copy(listToReturn.Items, expectedItems)
			opts, err := listprocessor.ParseQuery(req, nil)
			assert.Nil(t, err)
			cg.EXPECT().TableAdminClient(req, schema, "", &WarningBuffer{}).Return(ri, nil)
			// This tests that fields are being extracted from schema columns and the type specific fields map
			cf.EXPECT().CacheFor(context.Background(), [][]string{{"some", "field"}, {`id`}, {`metadata`, `state`, `name`}, {"gvk", "specific", "fields"}}, gomock.Any(), &tablelistconvert.Client{ResourceInterface: ri}, attributes.GVK(schema), attributes.Namespaced(schema), true).Return(c, nil)
			bloi.EXPECT().ListByOptions(req.Context(), &opts, partitions, req.Namespace).Return(nil, 0, "", fmt.Errorf("error"))
			tb.EXPECT().GetTransformFunc(attributes.GVK(schema)).Return(func(obj interface{}) (interface{}, error) { return obj, nil })

			_, _, _, err = s.ListByPartitions(req, schema, partitions)
			assert.NotNil(t, err)
		},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestReset(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}
	var tests []testCase
	tests = append(tests, testCase{
		description: "client Reset() with no errors returned should returned no errors.",
		test: func(t *testing.T) {
			nsc := NewMockCache(gomock.NewController(t))
			cg := NewMockClientGetter(gomock.NewController(t))
			cf := NewMockCacheFactory(gomock.NewController(t))
			cs := NewMockSchemaColumnSetter(gomock.NewController(t))
			ri := NewMockResourceInterface(gomock.NewController(t))
			tb := NewMockTransformBuilder(gomock.NewController(t))
			nsc2 := factory.Cache{}
			s := &Store{
				ctx:              context.Background(),
				namespaceCache:   nsc,
				clientGetter:     cg,
				cacheFactory:     cf,
				columnSetter:     cs,
				cfInitializer:    func() (CacheFactory, error) { return cf, nil },
				transformBuilder: tb,
			}
			nsSchema := baseNSSchema
			cf.EXPECT().Reset().Return(nil)
			cs.EXPECT().SetColumns(gomock.Any(), gomock.Any()).Return(nil)
			cg.EXPECT().TableAdminClient(nil, &nsSchema, "", &WarningBuffer{}).Return(ri, nil)
			cf.EXPECT().CacheFor(context.Background(), [][]string{{`id`}, {`metadata`, `state`, `name`}, {"metadata", "labels", "field.cattle.io/projectId"}}, gomock.Any(), &tablelistconvert.Client{ResourceInterface: ri}, attributes.GVK(&nsSchema), false, true).Return(nsc2, nil)
			tb.EXPECT().GetTransformFunc(attributes.GVK(&nsSchema)).Return(func(obj interface{}) (interface{}, error) { return obj, nil })
			err := s.Reset()
			assert.Nil(t, err)
			assert.Equal(t, nsc2, s.namespaceCache)
		},
	})
	tests = append(tests, testCase{
		description: "client Reset() with cache factory Reset() error returned, should return an error.",
		test: func(t *testing.T) {
			nsi := NewMockCache(gomock.NewController(t))
			cg := NewMockClientGetter(gomock.NewController(t))
			cf := NewMockCacheFactory(gomock.NewController(t))
			cs := NewMockSchemaColumnSetter(gomock.NewController(t))
			tb := NewMockTransformBuilder(gomock.NewController(t))

			s := &Store{
				ctx:              context.Background(),
				namespaceCache:   nsi,
				clientGetter:     cg,
				cacheFactory:     cf,
				columnSetter:     cs,
				cfInitializer:    func() (CacheFactory, error) { return cf, nil },
				transformBuilder: tb,
			}

			cf.EXPECT().Reset().Return(fmt.Errorf("error"))
			err := s.Reset()
			assert.NotNil(t, err)
		},
	})
	tests = append(tests, testCase{
		description: "client Reset() with column setter error returned, should return an error.",
		test: func(t *testing.T) {
			nsi := NewMockCache(gomock.NewController(t))
			cg := NewMockClientGetter(gomock.NewController(t))
			cf := NewMockCacheFactory(gomock.NewController(t))
			cs := NewMockSchemaColumnSetter(gomock.NewController(t))
			tb := NewMockTransformBuilder(gomock.NewController(t))

			s := &Store{
				ctx:              context.Background(),
				namespaceCache:   nsi,
				clientGetter:     cg,
				cacheFactory:     cf,
				columnSetter:     cs,
				cfInitializer:    func() (CacheFactory, error) { return cf, nil },
				transformBuilder: tb,
			}

			cf.EXPECT().Reset().Return(nil)
			cs.EXPECT().SetColumns(gomock.Any(), gomock.Any()).Return(fmt.Errorf("error"))
			err := s.Reset()
			assert.NotNil(t, err)
		},
	})
	tests = append(tests, testCase{
		description: "client Reset() with column getter TableAdminClient() error returned, should return an error.",
		test: func(t *testing.T) {
			nsi := NewMockCache(gomock.NewController(t))
			cg := NewMockClientGetter(gomock.NewController(t))
			cf := NewMockCacheFactory(gomock.NewController(t))
			cs := NewMockSchemaColumnSetter(gomock.NewController(t))
			tb := NewMockTransformBuilder(gomock.NewController(t))

			s := &Store{
				ctx:              context.Background(),
				namespaceCache:   nsi,
				clientGetter:     cg,
				cacheFactory:     cf,
				columnSetter:     cs,
				cfInitializer:    func() (CacheFactory, error) { return cf, nil },
				transformBuilder: tb,
			}
			nsSchema := baseNSSchema

			cf.EXPECT().Reset().Return(nil)
			cs.EXPECT().SetColumns(gomock.Any(), gomock.Any()).Return(nil)
			cg.EXPECT().TableAdminClient(nil, &nsSchema, "", &WarningBuffer{}).Return(nil, fmt.Errorf("error"))
			err := s.Reset()
			assert.NotNil(t, err)
		},
	})
	tests = append(tests, testCase{
		description: "client Reset() with cache factory CacheFor() error returned, should return an error.",
		test: func(t *testing.T) {
			nsc := NewMockCache(gomock.NewController(t))
			cg := NewMockClientGetter(gomock.NewController(t))
			cf := NewMockCacheFactory(gomock.NewController(t))
			cs := NewMockSchemaColumnSetter(gomock.NewController(t))
			ri := NewMockResourceInterface(gomock.NewController(t))
			tb := NewMockTransformBuilder(gomock.NewController(t))

			s := &Store{
				ctx:              context.Background(),
				namespaceCache:   nsc,
				clientGetter:     cg,
				cacheFactory:     cf,
				columnSetter:     cs,
				cfInitializer:    func() (CacheFactory, error) { return cf, nil },
				transformBuilder: tb,
			}
			nsSchema := baseNSSchema

			cf.EXPECT().Reset().Return(nil)
			cs.EXPECT().SetColumns(gomock.Any(), gomock.Any()).Return(nil)
			cg.EXPECT().TableAdminClient(nil, &nsSchema, "", &WarningBuffer{}).Return(ri, nil)
			cf.EXPECT().CacheFor(context.Background(), [][]string{{`id`}, {`metadata`, `state`, `name`}, {"metadata", "labels", "field.cattle.io/projectId"}}, gomock.Any(), &tablelistconvert.Client{ResourceInterface: ri}, attributes.GVK(&nsSchema), false, true).Return(factory.Cache{}, fmt.Errorf("error"))
			tb.EXPECT().GetTransformFunc(attributes.GVK(&nsSchema)).Return(func(obj interface{}) (interface{}, error) { return obj, nil })
			err := s.Reset()
			assert.NotNil(t, err)
		},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
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
	wc, err := testStore.WatchNames(&types.APIRequest{Namespace: "", Schema: apiSchema, Request: &http.Request{}}, apiSchema, types.WatchRequest{}, sets.New[string]("testsecret1", "testsecret2"))
	assert.Nil(t, err)

	eg := errgroup.Group{}
	eg.Go(func() error { return receiveUntil(wc, 5*time.Second) })

	err = eg.Wait()
	assert.Nil(t, err)

	assert.Equal(t, 0, len(c.ResultChan()), "Expected all secrets to have been received")
}

func (t *testFactory) TableAdminClientForWatch(ctx *types.APIRequest, schema *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error) {
	return t.fakeClient.Resource(schema2.GroupVersionResource{}), nil
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
			name: "missing namespace in the params / should copy from apiOp",
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
			assert.Nil(t, err)

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
