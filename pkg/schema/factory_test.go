package schema

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/wrangler/v3/pkg/schemas"
	k8sSchema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
)

const (
	testGroup   = "test.k8s.io"
	testVersion = "v1"
)

type schemaTestConfig struct {
	permissionVerbs        []string
	desiredResourceVerbs   []string
	desiredCollectionVerbs []string
	errDesired             bool
}

func TestSchemas(t *testing.T) {
	tests := []struct {
		name   string
		config schemaTestConfig
	}{
		{
			name: "basic get schema test",
			config: schemaTestConfig{
				permissionVerbs:        []string{"get"},
				desiredResourceVerbs:   []string{"GET"},
				desiredCollectionVerbs: []string{"GET"},
				errDesired:             false,
			},
		},
		{
			name: "basic patch schema test",
			config: schemaTestConfig{
				permissionVerbs:        []string{"patch"},
				desiredResourceVerbs:   []string{"PATCH"},
				desiredCollectionVerbs: []string{},
				errDesired:             false,
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			// test caching functionality
			mockLookup := newMockAccessSetLookup()
			userName := "testUser"
			testUser := user.DefaultInfo{
				Name:   userName,
				UID:    userName,
				Groups: []string{},
				Extra:  map[string][]string{},
			}

			collection := NewCollection(context.TODO(), types.EmptyAPISchemas(), mockLookup)
			collection.schemas = map[string]*types.APISchema{"testCRD": makeSchema("testCRD")}
			runSchemaTest(t, test.config, mockLookup, collection, &testUser)
		})
	}
}
func TestSchemaCache(t *testing.T) {
	// Schemas are a frequently used resource. It's important that the cache doesn't have a leak given size/frequency of resource
	tests := []struct {
		name   string
		before schemaTestConfig
		after  schemaTestConfig
	}{
		{
			name: "permissions increase, cache size same",
			before: schemaTestConfig{
				permissionVerbs:        []string{"get"},
				desiredResourceVerbs:   []string{"GET"},
				desiredCollectionVerbs: []string{"GET"},
				errDesired:             false,
			},
			after: schemaTestConfig{
				permissionVerbs:        []string{"get", "create", "delete"},
				desiredResourceVerbs:   []string{"GET", "DELETE"},
				desiredCollectionVerbs: []string{"GET", "POST"},
				errDesired:             false,
			},
		},
		{
			name: "permissions decrease, cache size same",
			before: schemaTestConfig{
				permissionVerbs:        []string{"get", "create", "delete"},
				desiredResourceVerbs:   []string{"GET", "DELETE"},
				desiredCollectionVerbs: []string{"GET", "POST"},
				errDesired:             false,
			},
			after: schemaTestConfig{
				permissionVerbs:        []string{"get"},
				desiredResourceVerbs:   []string{"GET"},
				desiredCollectionVerbs: []string{"GET"},
				errDesired:             false,
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			// test caching functionality
			mockLookup := newMockAccessSetLookup()
			userName := "testUser"
			testUser := user.DefaultInfo{
				Name:   userName,
				UID:    userName,
				Groups: []string{},
				Extra:  map[string][]string{},
			}
			collection := NewCollection(context.TODO(), types.EmptyAPISchemas(), mockLookup)
			collection.schemas = map[string]*types.APISchema{"testCRD": makeSchema("testCRD")}
			runSchemaTest(t, test.before, mockLookup, collection, &testUser)
			assert.Len(t, collection.cache.Keys(), 1, "expected cache to be size 1")
			mockLookup.Clear()
			runSchemaTest(t, test.after, mockLookup, collection, &testUser)
			assert.Len(t, collection.cache.Keys(), 1, "expected cache to be size 1")
		})
	}
}

func runSchemaTest(t *testing.T, config schemaTestConfig, lookup *mockAccessSetLookup, collection *Collection, testUser user.Info) {
	for _, verb := range config.permissionVerbs {
		lookup.AddAccessForUser(testUser, verb, k8sSchema.GroupResource{Group: testGroup, Resource: "testCRD"}, "*", "*")
	}

	collection.schemas = map[string]*types.APISchema{"testCRD": makeSchema("testCRD")}
	userSchemas, err := collection.Schemas(testUser)
	if config.errDesired {
		assert.Error(t, err, "expected error but none was found")
	}
	var testSchema *types.APISchema
	for schemaName, userSchema := range userSchemas.Schemas {
		if schemaName == "testCRD" {
			testSchema = userSchema
		}
	}
	assert.NotNil(t, testSchema, "expected a test schema, but was nil")
	assert.Len(t, testSchema.ResourceMethods, len(config.desiredResourceVerbs), "did not get as many verbs as expected for resource methods")
	assert.Len(t, testSchema.CollectionMethods, len(config.desiredCollectionVerbs), "did not get as many verbs as expected for resource methods")
	for _, verb := range config.desiredResourceVerbs {
		assert.Contains(t, testSchema.ResourceMethods, verb, "did not find %s in resource methods %v", verb, testSchema.ResourceMethods)
	}
	for _, verb := range config.desiredCollectionVerbs {
		assert.Contains(t, testSchema.CollectionMethods, verb, "did not find %s in resource methods %v", verb, testSchema.CollectionMethods)
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
				"resource": resourceType,
				"verbs":    []string{"get", "list", "watch", "delete", "update", "create", "patch"},
			},
		},
	}
}
