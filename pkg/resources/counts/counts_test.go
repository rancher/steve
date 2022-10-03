package counts_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/rancher/apiserver/pkg/server"
	"github.com/rancher/apiserver/pkg/store/empty"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/steve/pkg/attributes"
	"github.com/rancher/steve/pkg/clustercache"
	"github.com/rancher/steve/pkg/resources/counts"
	"github.com/rancher/steve/pkg/schema"
	"github.com/rancher/wrangler/pkg/schemas"
	"github.com/rancher/wrangler/pkg/summary"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	schema2 "k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	testGroup           = "test.k8s.io"
	testVersion         = "v1"
	testResource        = "testCRD"
	testNotUsedResource = "testNotUsedCRD"
	testNewResource     = "testNewCRD"
)

func TestWatch(t *testing.T) {
	tests := []struct {
		name            string
		event           string // the event to send, can be "add", "remove", or "change"
		newSchema       bool
		countsForSchema int
		errDesired      bool
	}{
		{
			name:            "add of known schema",
			event:           "add",
			newSchema:       false,
			countsForSchema: 2,
			errDesired:      false,
		},
		{
			name:            "add of unknown schema",
			event:           "add",
			newSchema:       true,
			countsForSchema: 0,
			errDesired:      true,
		},
		{
			name:            "change of known schema",
			event:           "change",
			newSchema:       false,
			countsForSchema: 0,
			errDesired:      true,
		},
		{
			name:            "change of unknown schema",
			event:           "change",
			newSchema:       true,
			countsForSchema: 0,
			errDesired:      true,
		},
		{
			name:            "remove of known schema",
			event:           "remove",
			newSchema:       false,
			countsForSchema: 0,
			errDesired:      false,
		},
		{
			name:            "remove of unknown schema",
			event:           "remove",
			newSchema:       true,
			countsForSchema: 0,
			errDesired:      true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			testSchema := makeSchema(testResource)
			testNotUsedSchema := makeSchema(testNotUsedResource)
			testNewSchema := makeSchema(testNewResource)
			addGenericPermissionsToSchema(testSchema, "list")
			addGenericPermissionsToSchema(testNotUsedSchema, "list")
			testSchemas := types.EmptyAPISchemas()
			testSchemas.MustAddSchema(*testSchema)
			testSchemas.MustAddSchema(*testNotUsedSchema)
			testOp := &types.APIRequest{
				Schemas:       testSchemas,
				AccessControl: &server.SchemaBasedAccess{},
				Request:       &http.Request{},
			}
			fakeCache := NewFakeClusterCache()
			gvk := attributes.GVK(testSchema)
			newGVK := attributes.GVK(testNewSchema)
			fakeCache.AddSummaryObj(makeSummarizedObject(gvk, "testName1", "testNs", "1"))
			counts.Register(testSchemas, fakeCache)

			// next, get the channel our results will be delivered on
			countSchema := testSchemas.LookupSchema("count")
			// channel will stream our events after we call the handlers to simulate/add/remove/change events
			resChannel, err := countSchema.Store.Watch(testOp, nil, types.WatchRequest{})
			assert.NoError(t, err, "got an error when trying to watch counts, did not expect one")

			// call the handlers, triggering the update to receive the event
			if test.event == "add" {
				var summarizedObject *summary.SummarizedObject
				var testGVK schema2.GroupVersionKind
				if test.newSchema {
					summarizedObject = makeSummarizedObject(newGVK, "testNew", "testNs", "1")
					testGVK = newGVK
				} else {
					summarizedObject = makeSummarizedObject(gvk, "testName2", "testNs", "2")
					testGVK = gvk
				}
				err = fakeCache.addHandler(testGVK, "n/a", summarizedObject)
				assert.NoError(t, err, "did not expect error when calling add method")
			} else if test.event == "change" {
				var summarizedObject *summary.SummarizedObject
				var testGVK schema2.GroupVersionKind
				var changedSummarizedObject *summary.SummarizedObject
				if test.newSchema {
					summarizedObject = makeSummarizedObject(newGVK, "testNew", "testNs", "1")
					changedSummarizedObject = makeSummarizedObject(newGVK, "testNew", "testNs", "2")
					testGVK = newGVK
				} else {
					summarizedObject = makeSummarizedObject(gvk, "testName1", "testNs", "2")
					changedSummarizedObject = makeSummarizedObject(gvk, "testName1", "testNs", "3")
					testGVK = gvk
				}
				err = fakeCache.changeHandler(testGVK, "n/a", changedSummarizedObject, summarizedObject)
				assert.NoError(t, err, "did not expect error when calling change method")
			} else if test.event == "remove" {
				var summarizedObject *summary.SummarizedObject
				var testGVK schema2.GroupVersionKind
				if test.newSchema {
					summarizedObject = makeSummarizedObject(newGVK, "testNew", "testNs", "2")
					testGVK = newGVK
				} else {
					summarizedObject = makeSummarizedObject(gvk, "testName1", "testNs", "2")
					testGVK = gvk
				}
				err = fakeCache.removeHandler(testGVK, "n/a", summarizedObject)
				assert.NoError(t, err, "did not expect error when calling add method")
			} else {
				assert.Failf(t, "unexpected event", "%s is not one of the allowed values of add, change, remove", test.event)
			}
			// need to call the event handler to force the event to stream
			outputCount, err := receiveWithTimeout(resChannel, 100*time.Millisecond)
			if test.errDesired {
				assert.Errorf(t, err, "expected no value from channel, but got one %+v", outputCount)
			} else {
				assert.NoError(t, err, "got an error when attempting to get a value from the result channel")
				assert.NotNilf(t, outputCount, "expected a new count value, did not get one")
				count := outputCount.Object.Object.(counts.Count)
				assert.Len(t, count.Counts, 1, "only expected one count event")
				itemCount, ok := count.Counts[testResource]
				assert.True(t, ok, "expected an item count for %s", testResource)
				assert.Equal(t, test.countsForSchema, itemCount.Summary.Count, "expected counts to be correct")
			}
		})
	}
}

// receiveWithTimeout tries to get a value from input within duration. Returns an error if no input was received during that period
func receiveWithTimeout(input chan types.APIEvent, duration time.Duration) (*types.APIEvent, error) {
	select {
	case value := <-input:
		return &value, nil
	case <-time.After(duration):
		return nil, fmt.Errorf("timeout error, no value recieved after %f seconds", duration.Seconds())
	}
}

// addGenericPermissions grants the specified verb for all namespaces and all resourceNames
func addGenericPermissionsToSchema(schema *types.APISchema, verb string) {
	if verb == "create" {
		schema.CollectionMethods = append(schema.CollectionMethods, http.MethodPost)
	} else if verb == "get" {
		schema.ResourceMethods = append(schema.ResourceMethods, http.MethodGet)
	} else if verb == "list" || verb == "watch" {
		// list and watch use the same permission checks, so we handle in one case
		schema.CollectionMethods = append(schema.CollectionMethods, http.MethodGet, http.MethodPost)
	} else if verb == "update" {
		schema.ResourceMethods = append(schema.ResourceMethods, http.MethodPut)
	} else if verb == "delete" {
		schema.ResourceMethods = append(schema.ResourceMethods, http.MethodDelete)
	} else {
		panic(fmt.Sprintf("Can't add generic permissions for verb %s", verb))
	}
	currentAccess := schema.Attributes["access"].(accesscontrol.AccessListByVerb)
	currentAccess[verb] = []accesscontrol.Access{
		{
			Namespace:    "*",
			ResourceName: "*",
		},
	}
}

func makeSchema(resourceType string) *types.APISchema {
	return &types.APISchema{
		Schema: &schemas.Schema{
			ID:                resourceType,
			CollectionMethods: []string{},
			ResourceMethods:   []string{},
			ResourceFields: map[string]schemas.Field{
				"name":  {Type: "string"},
				"value": {Type: "string"},
			},
			Attributes: map[string]interface{}{
				"group":    testGroup,
				"version":  testVersion,
				"kind":     resourceType,
				"resource": resourceType,
				"verbs":    []string{"get", "list", "watch", "delete", "update", "create"},
				"access":   accesscontrol.AccessListByVerb{},
			},
		},
		Store: &empty.Store{},
	}
}

type fakeClusterCache struct {
	summarizedObjects []*summary.SummarizedObject
	addHandler        clustercache.Handler
	removeHandler     clustercache.Handler
	changeHandler     clustercache.ChangeHandler
}

func NewFakeClusterCache() *fakeClusterCache {
	return &fakeClusterCache{
		summarizedObjects: []*summary.SummarizedObject{},
		addHandler:        nil,
		removeHandler:     nil,
		changeHandler:     nil,
	}
}

func (f *fakeClusterCache) Get(gvk schema2.GroupVersionKind, namespace, name string) (interface{}, bool, error) {
	return nil, false, nil
}

func (f *fakeClusterCache) List(gvk schema2.GroupVersionKind) []interface{} {
	var retList []interface{}
	for _, summaryObj := range f.summarizedObjects {
		if summaryObj.GroupVersionKind() != gvk {
			// only list the summary objects for the provided gvk
			continue
		}
		retList = append(retList, summaryObj)
	}
	return retList
}

func (f *fakeClusterCache) OnAdd(ctx context.Context, handler clustercache.Handler) {
	f.addHandler = handler
}

func (f *fakeClusterCache) OnRemove(ctx context.Context, handler clustercache.Handler) {
	f.removeHandler = handler
}

func (f *fakeClusterCache) OnChange(ctx context.Context, handler clustercache.ChangeHandler) {
	f.changeHandler = handler
}

func (f *fakeClusterCache) OnSchemas(schemas *schema.Collection) error {
	return nil
}

func (f *fakeClusterCache) AddSummaryObj(summaryObj *summary.SummarizedObject) {
	f.summarizedObjects = append(f.summarizedObjects, summaryObj)
}

func makeSummarizedObject(gvk schema2.GroupVersionKind, name string, namespace string, version string) *summary.SummarizedObject {
	apiVersion, kind := gvk.ToAPIVersionAndKind()
	return &summary.SummarizedObject{
		Summary: summary.Summary{
			State:         "",
			Error:         false,
			Transitioning: false,
		},
		PartialObjectMetadata: metav1.PartialObjectMetadata{
			TypeMeta: metav1.TypeMeta{
				APIVersion: apiVersion,
				Kind:       kind,
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:            name,
				Namespace:       namespace,
				ResourceVersion: version, // any non-zero value should work here. 0 seems to have specific meaning for counts
			},
		},
	}
}
