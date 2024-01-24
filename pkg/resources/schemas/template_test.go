// Package schemas handles streaming schema updates and changes.
package schemas_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/accesscontrol"
	acfake "github.com/rancher/steve/pkg/accesscontrol/fake"
	"github.com/rancher/steve/pkg/attributes"
	"github.com/rancher/steve/pkg/resources/schemas"
	schemafake "github.com/rancher/steve/pkg/schema/fake"
	v1schema "github.com/rancher/wrangler/v2/pkg/schemas"
	"github.com/stretchr/testify/assert"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
)

var setupTimeout = time.Millisecond * 50

const resourceType = "schemas"

func Test_WatchChangeDetection(t *testing.T) {
	ctrl := gomock.NewController(t)
	asl := acfake.NewMockAccessSetLookup(ctrl)
	userInfo := &user.DefaultInfo{
		Name:   "test",
		UID:    "test",
		Groups: nil,
		Extra:  nil,
	}
	accessSet := &accesscontrol.AccessSet{}
	// always return the same empty accessSet for the test user
	asl.EXPECT().AccessFor(userInfo).Return(accessSet).AnyTimes()

	req := httptest.NewRequest("GET", "/", nil)

	type testValues struct {
		expectedChanges []types.APIEvent
		mockFactory     *schemafake.MockFactory
		eventsReady     chan struct{}
	}
	tests := []struct {
		name  string
		setup func(*gomock.Controller) testValues
	}{
		{
			name: "Schemas have no change",
			setup: func(ctrl *gomock.Controller) testValues {
				factory := schemafake.NewMockFactory(ctrl)
				eventsReady := make(chan struct{})
				baseSchemas := types.EmptyAPISchemas()
				updateSchemas := types.EmptyAPISchemas()

				testSchema := types.APISchema{
					Schema: &v1schema.Schema{
						ID:                "pod",
						PluralName:        "pods",
						CollectionMethods: []string{"GET"},
						ResourceMethods:   []string{"GET"},
					},
				}

				// initial schemas
				baseSchemas.AddSchema(testSchema)
				factory.EXPECT().Schemas(userInfo).Return(baseSchemas, nil)

				updateSchemas.AddSchema(testSchema)

				// return updated schemas for the second request
				factory.EXPECT().Schemas(userInfo).DoAndReturn(func(_ user.Info) (*types.APISchemas, error) {
					// signal that initial Schemas were called
					close(eventsReady)
					return updateSchemas, nil
				})

				expectedEvents := []types.APIEvent{}
				return testValues{expectedEvents, factory, eventsReady}
			},
		},
		{
			name: "New schema is added to schemas.",
			setup: func(ctrl *gomock.Controller) testValues {
				factory := schemafake.NewMockFactory(ctrl)
				eventsReady := make(chan struct{})
				baseSchemas := types.EmptyAPISchemas()
				updateSchemas := types.EmptyAPISchemas()

				testSchema := types.APISchema{
					Schema: &v1schema.Schema{
						ID:                "pod",
						PluralName:        "pods",
						CollectionMethods: []string{"GET"},
						ResourceMethods:   []string{"GET"},
					},
				}
				testSchemaNew := types.APISchema{
					Schema: &v1schema.Schema{
						ID:                "secret",
						PluralName:        "secrets",
						CollectionMethods: []string{"GET"},
						ResourceMethods:   []string{"GET"},
					},
				}
				baseSchemas.AddSchema(testSchema)
				// initial schemas
				factory.EXPECT().Schemas(userInfo).Return(baseSchemas, nil)

				updateSchemas.AddSchema(testSchema)
				updateSchemas.AddSchema((testSchemaNew))

				// return updated schemas for the second request
				factory.EXPECT().Schemas(userInfo).DoAndReturn(func(_ user.Info) (*types.APISchemas, error) {
					// signal that initial Schemas were called
					close(eventsReady)
					return updateSchemas, nil
				})

				expectedEvents := []types.APIEvent{
					{
						Name:         types.CreateAPIEvent,
						ResourceType: "schema",
						Object: types.APIObject{
							Type:   resourceType,
							ID:     testSchemaNew.ID,
							Object: &testSchemaNew,
						},
					},
				}
				return testValues{expectedEvents, factory, eventsReady}
			},
		},
		{
			name: "Schema is deleted from schemas.",
			setup: func(ctrl *gomock.Controller) testValues {
				factory := schemafake.NewMockFactory(ctrl)
				eventsReady := make(chan struct{})
				baseSchemas := types.EmptyAPISchemas()
				updateSchemas := types.EmptyAPISchemas()

				testSchema := types.APISchema{
					Schema: &v1schema.Schema{
						ID:                "pod",
						PluralName:        "pods",
						CollectionMethods: []string{"GET"},
						ResourceMethods:   []string{"GET"},
					},
				}
				testSchemaToDelete := types.APISchema{
					Schema: &v1schema.Schema{
						ID:                "secret",
						PluralName:        "secrets",
						CollectionMethods: []string{"GET"},
						ResourceMethods:   []string{"GET"},
					},
				}
				baseSchemas.AddSchema(testSchema)
				baseSchemas.AddSchema(testSchemaToDelete)
				// initial schemas
				factory.EXPECT().Schemas(userInfo).Return(baseSchemas, nil)

				updateSchemas.AddSchema(testSchema)

				// return updated schemas for the second request
				factory.EXPECT().Schemas(userInfo).DoAndReturn(func(_ user.Info) (*types.APISchemas, error) {
					// signal that initial Schemas were called
					close(eventsReady)
					return updateSchemas, nil
				})

				expectedEvents := []types.APIEvent{
					{
						Name:         types.RemoveAPIEvent,
						ResourceType: "schema",
						Object: types.APIObject{
							Type:   resourceType,
							ID:     testSchemaToDelete.ID,
							Object: &testSchemaToDelete,
						},
					},
				}
				return testValues{expectedEvents, factory, eventsReady}
			},
		},
		{
			name: "Empty Schemas",
			setup: func(ctrl *gomock.Controller) testValues {
				factory := schemafake.NewMockFactory(ctrl)
				eventsReady := make(chan struct{})

				// initial schemas
				factory.EXPECT().Schemas(userInfo).Return(types.EmptyAPISchemas(), nil)

				// return updated schemas for the second request
				factory.EXPECT().Schemas(userInfo).DoAndReturn(func(_ user.Info) (*types.APISchemas, error) {
					// signal that initial Schemas were called
					close(eventsReady)
					return types.EmptyAPISchemas(), nil
				})

				return testValues{nil, factory, eventsReady}
			},
		},
		{
			name: "Schema kind attribute is updated",
			setup: func(ctrl *gomock.Controller) testValues {
				factory := schemafake.NewMockFactory(ctrl)
				eventsReady := make(chan struct{})
				baseSchemas := types.EmptyAPISchemas()
				updateSchemas := types.EmptyAPISchemas()

				testSchema := types.APISchema{
					Schema: &v1schema.Schema{
						ID:                "pod",
						PluralName:        "pods",
						CollectionMethods: []string{"GET"},
						ResourceMethods:   []string{"GET"},
					},
				}
				baseSchemas.AddSchema(testSchema)
				// initial schemas
				factory.EXPECT().Schemas(userInfo).Return(baseSchemas, nil)

				// add kind attribute
				attributes.SetKind(&testSchema, "newKind")
				updateSchemas.AddSchema(testSchema)

				// return updated schemas for the second request
				factory.EXPECT().Schemas(userInfo).DoAndReturn(func(_ user.Info) (*types.APISchemas, error) {
					// signal that initial Schemas were called
					close(eventsReady)
					return updateSchemas, nil
				})

				expectedEvents := []types.APIEvent{
					{
						Name:         types.ChangeAPIEvent,
						ResourceType: "schema",
						Object: types.APIObject{
							Type:   resourceType,
							ID:     testSchema.ID,
							Object: &testSchema,
						},
					},
				}
				return testValues{expectedEvents, factory, eventsReady}
			},
		},
		{
			name: "Schema access attribute is updated",
			setup: func(ctrl *gomock.Controller) testValues {
				factory := schemafake.NewMockFactory(ctrl)
				eventsReady := make(chan struct{})
				baseSchemas := types.EmptyAPISchemas()
				updateSchemas := types.EmptyAPISchemas()

				testSchema := types.APISchema{
					Schema: &v1schema.Schema{
						ID:                "pod",
						PluralName:        "pods",
						CollectionMethods: []string{"GET"},
						ResourceMethods:   []string{"GET"},
					},
				}
				baseSchemas.AddSchema(testSchema)
				// initial schemas
				factory.EXPECT().Schemas(userInfo).Return(baseSchemas, nil)

				// add access attribute
				attributes.SetAccess(&testSchema, map[string]string{"List": "*"})
				updateSchemas.AddSchema(testSchema)

				// return updated schemas for the second request
				factory.EXPECT().Schemas(userInfo).DoAndReturn(func(_ user.Info) (*types.APISchemas, error) {
					// signal that schemas were requested
					close(eventsReady)
					return updateSchemas, nil
				})

				expectedEvents := []types.APIEvent{}
				return testValues{expectedEvents, factory, eventsReady}
			},
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			// create new context for the test user
			testCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			apiOp := &types.APIRequest{
				Request: req.WithContext(request.WithUser(testCtx, userInfo)),
			}

			// create test factory
			ctrl := gomock.NewController(t)
			values := test.setup(ctrl)

			// store onChange cb use to trigger the notifier that will be set in schemas.SetupWatcher(..)
			var onChangeCB func()
			values.mockFactory.EXPECT().OnChange(gomock.AssignableToTypeOf(testCtx), gomock.AssignableToTypeOf(onChangeCB)).
				Do(func(_ context.Context, cb func()) {
					onChangeCB = cb
				})

			baseSchemas := types.EmptyAPISchemas()

			// create a new store and add it to baseSchemas
			schemas.SetupWatcher(testCtx, baseSchemas, asl, values.mockFactory)
			schema := baseSchemas.LookupSchema(resourceType)

			// Start watching
			resultChan, err := schema.Store.Watch(apiOp, nil, types.WatchRequest{})
			assert.NoError(t, err, "Unexpected error starting Watch")

			// wait for the store's go routines to start watching for onChange events
			time.Sleep(setupTimeout)

			// trigger watch notification that fetches new schemas
			onChangeCB()

			select {
			case <-values.eventsReady:
				// New schema was requested now we sleep to give time for watcher to send events
				time.Sleep(setupTimeout)
			case <-time.After(setupTimeout):
				// When we continue here then the test will fail due to missing mock calls not being called.
			}

			// verify correct results are sent
			hasExpectedResults(t, values.expectedChanges, resultChan, setupTimeout)
		})
	}
}

func Test_AccessSetAndChangeSignal(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	asl := acfake.NewMockAccessSetLookup(ctrl)
	userInfo := &user.DefaultInfo{
		Name:   "test",
		UID:    "test",
		Groups: nil,
		Extra:  nil,
	}
	accessSet := &accesscontrol.AccessSet{}
	changedSet := &accesscontrol.AccessSet{ID: "1"}

	// return access set with ID "" the first time then "1" for subsequent request
	gomock.InOrder(
		asl.EXPECT().AccessFor(userInfo).Return(accessSet),
		asl.EXPECT().AccessFor(userInfo).Return(changedSet).AnyTimes(),
	)

	req := httptest.NewRequest("GET", "/", nil)

	factory := schemafake.NewMockFactory(ctrl)
	baseSchemas := types.EmptyAPISchemas()
	onChangeUpdateSchemas := types.EmptyAPISchemas()
	userAccessUpdateSchemas := types.EmptyAPISchemas()

	testSchema := types.APISchema{
		Schema: &v1schema.Schema{
			ID:                "pod",
			PluralName:        "pods",
			CollectionMethods: []string{"GET"},
			ResourceMethods:   []string{"GET"},
		},
	}

	// initial schemas
	baseSchemas.AddSchema(testSchema)
	factory.EXPECT().Schemas(userInfo).Return(baseSchemas, nil)

	testSchemaNew := types.APISchema{
		Schema: &v1schema.Schema{
			ID:                "secret",
			PluralName:        "secrets",
			CollectionMethods: []string{"GET"},
			ResourceMethods:   []string{"GET"},
		},
	}
	onChangeUpdateSchemas.AddSchema(testSchema)
	onChangeUpdateSchemas.AddSchema((testSchemaNew))

	// return updated schemas with new schemas added
	factory.EXPECT().Schemas(userInfo).Return(onChangeUpdateSchemas, nil)

	userAccessUpdateSchemas.AddSchema(testSchema)

	// return updated schemas with new schemas removed
	factory.EXPECT().Schemas(userInfo).Return(userAccessUpdateSchemas, nil)

	expectedEvents := []types.APIEvent{
		{
			Name:         types.CreateAPIEvent,
			ResourceType: "schema",
			Object: types.APIObject{
				Type:   resourceType,
				ID:     testSchemaNew.ID,
				Object: &testSchemaNew,
			},
		},
		{
			Name:         types.RemoveAPIEvent,
			ResourceType: "schema",
			Object: types.APIObject{
				Type:   resourceType,
				ID:     testSchemaNew.ID,
				Object: &testSchemaNew,
			},
		},
	}
	// create new context for the test user
	testCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	apiOp := &types.APIRequest{
		Request: req.WithContext(request.WithUser(testCtx, userInfo)),
	}

	// store onChange cb use to trigger the notifier that will be set in schemas.SetupWatcher(..)
	var onChangeCB func()
	factory.EXPECT().OnChange(gomock.AssignableToTypeOf(testCtx), gomock.AssignableToTypeOf(onChangeCB)).
		Do(func(_ context.Context, cb func()) {
			onChangeCB = cb
		})

	watcherSchema := types.EmptyAPISchemas()

	// create a new store and add it to watcherSchema
	schemas.SetupWatcher(testCtx, watcherSchema, asl, factory)
	schema := watcherSchema.LookupSchema(resourceType)

	// Start watching
	resultChan, err := schema.Store.Watch(apiOp, nil, types.WatchRequest{})
	assert.NoError(t, err, "Unexpected error starting Watch")

	// wait for the store's go routines to start watching for onChange events
	time.Sleep(setupTimeout)

	// trigger watch notification that fetches new schemas
	onChangeCB()

	// wait for user access set to be checked (2 seconds)
	time.Sleep(time.Millisecond * 2100)

	// verify correct results are sent
	hasExpectedResults(t, expectedEvents, resultChan, setupTimeout)

}

// hasExpectedResults verifies the list of expected apiEvents are all received from the provided channel.
func hasExpectedResults(t *testing.T, expectedEvents []types.APIEvent, resultChan chan types.APIEvent, timeout time.Duration) {
	t.Helper()
	numEventsSent := 0
	for {
		select {
		case event, ok := <-resultChan:
			if !ok {
				if numEventsSent == len(expectedEvents) {
					// we got everything we expect
					return
				}
				assert.Fail(t, "result channel unexpectedly closed")
			}
			if numEventsSent >= len(expectedEvents) {
				assert.Failf(t, "too many events", "received unexpected events on channel %+v", event)
				return
			}
			eventJSON, err := json.Marshal(event)
			assert.NoError(t, err, "failed to marshal new event")
			expectedJSON, err := json.Marshal(event)
			assert.NoError(t, err, "failed to marshal expected event")
			assert.JSONEq(t, string(expectedJSON), string(eventJSON), "incorrect event received")

		case <-time.After(timeout):
			if numEventsSent != len(expectedEvents) {
				assert.Fail(t, "timeout waiting for results")
			}
			return
		}
		numEventsSent++
	}
}
