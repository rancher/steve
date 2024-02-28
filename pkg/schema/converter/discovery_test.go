package converter

import (
	"fmt"
	"testing"

	openapiv2 "github.com/google/gnostic-models/openapiv2"
	"github.com/rancher/apiserver/pkg/types"
	wranglerSchema "github.com/rancher/wrangler/v2/pkg/schemas"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/openapi"
	restclient "k8s.io/client-go/rest"
)

func TestAddDiscovery(t *testing.T) {
	tests := []struct {
		name                 string
		discoveryErr         error
		groups               []schema.GroupVersion
		groupVersionOverride bool
		resources            map[schema.GroupVersion][]metav1.APIResource
		wantError            bool
		desiredSchema        map[string]*types.APISchema
	}{
		{
			name:   "basic test case, one schema",
			groups: []schema.GroupVersion{{Group: "TestGroup", Version: "v1"}},
			resources: map[schema.GroupVersion][]metav1.APIResource{
				{Group: "TestGroup", Version: "v1"}: {
					{

						Name:         "testResources",
						SingularName: "testResource",
						Kind:         "TestResource",
						Namespaced:   true,
						Verbs:        metav1.Verbs{"get"},
					},
				},
			},
			wantError: false,
			desiredSchema: map[string]*types.APISchema{
				"testgroup.v1.testresource": {
					Schema: &wranglerSchema.Schema{
						ID:         "testgroup.v1.testresource",
						PluralName: "TestGroup.v1.testResources",
						Attributes: map[string]interface{}{
							"group":      "TestGroup",
							"version":    "v1",
							"kind":       "TestResource",
							"resource":   "testResources",
							"verbs":      []string{"get"},
							"namespaced": true,
						},
					},
				},
			},
		},
		{
			name: "discovery error but still got some information",
			discoveryErr: &discovery.ErrGroupDiscoveryFailed{Groups: map[schema.GroupVersion]error{
				schema.GroupVersion{
					Group:   "NotFound",
					Version: "v1",
				}: fmt.Errorf("group Not found"),
			},
			},
			groups: []schema.GroupVersion{{Group: "TestGroup", Version: "v1"}},
			resources: map[schema.GroupVersion][]metav1.APIResource{
				{Group: "TestGroup", Version: "v1"}: {
					{

						Name:         "testResources",
						SingularName: "testResource",
						Kind:         "TestResource",
						Namespaced:   true,
						Verbs:        metav1.Verbs{"get"},
					},
				},
			},
			wantError: false,
			desiredSchema: map[string]*types.APISchema{
				"testgroup.v1.testresource": {
					Schema: &wranglerSchema.Schema{
						ID:         "testgroup.v1.testresource",
						PluralName: "TestGroup.v1.testResources",
						Attributes: map[string]interface{}{
							"group":      "TestGroup",
							"version":    "v1",
							"kind":       "TestResource",
							"resource":   "testResources",
							"verbs":      []string{"get"},
							"namespaced": true,
						},
					},
				},
			},
		},
		{
			name:         "discovery error, not partial",
			discoveryErr: fmt.Errorf("cluster unavailable"),
			groups:       []schema.GroupVersion{{Group: "TestGroup", Version: "v1"}},
			resources: map[schema.GroupVersion][]metav1.APIResource{
				{Group: "TestGroup", Version: "v1"}: {
					{
						Name:         "testResources",
						SingularName: "testResource",
						Kind:         "TestResource",
						Namespaced:   true,
						Verbs:        metav1.Verbs{"get"},
					},
				},
			},
			wantError:     true,
			desiredSchema: map[string]*types.APISchema{},
		},
		{
			name:   "bad group version",
			groups: []schema.GroupVersion{{Group: "Invalid/Group", Version: "v2"}},
			resources: map[schema.GroupVersion][]metav1.APIResource{
				{Group: "Invalid/Group", Version: "v2"}: {
					{
						Name:         "testResources",
						SingularName: "testResource",
						Kind:         "TestResource",
						Namespaced:   true,
						Verbs:        metav1.Verbs{"get"},
					},
				},
			},
			wantError: true,
			desiredSchema: map[string]*types.APISchema{
				"core..testresource": {
					Schema: &wranglerSchema.Schema{
						ID:         "core..testresource",
						PluralName: "core..testResources",
						Attributes: map[string]interface{}{
							"group":      "",
							"version":    "",
							"kind":       "TestResource",
							"resource":   "testResources",
							"verbs":      []string{"get"},
							"namespaced": true,
						},
					},
				},
			},
		},
		{
			name:                 "override groups and versions",
			groups:               []schema.GroupVersion{{Group: "autoscaling", Version: "v1"}, {Group: "extensions", Version: "v1"}},
			groupVersionOverride: true,
			resources: map[schema.GroupVersion][]metav1.APIResource{
				{Group: "autoscaling", Version: "v1"}: {
					{
						Name:         "testAutoscalings",
						SingularName: "testAutoscaling",
						Kind:         "TestAutoscaling",
						Namespaced:   true,
						Verbs:        metav1.Verbs{"get"},
					},
				},
				{Group: "extensions", Version: "v1"}: {
					{
						Name:         "testExtensions",
						SingularName: "testExtension",
						Kind:         "TestExtension",
						Namespaced:   true,
						Verbs:        metav1.Verbs{"get"},
					},
				},
			},
			wantError: false,
			desiredSchema: map[string]*types.APISchema{
				"autoscaling.v1.testautoscaling": {
					Schema: &wranglerSchema.Schema{
						ID:         "autoscaling.v1.testautoscaling",
						PluralName: "autoscaling.v1.testAutoscalings",
						Attributes: map[string]interface{}{
							"group":            "autoscaling",
							"version":          "v1",
							"kind":             "TestAutoscaling",
							"resource":         "testAutoscalings",
							"verbs":            []string{"get"},
							"namespaced":       true,
							"preferredVersion": "v2beta2",
						},
					},
				},
				"extensions.v1.testextension": {
					Schema: &wranglerSchema.Schema{
						ID:         "extensions.v1.testextension",
						PluralName: "extensions.v1.testExtensions",
						Attributes: map[string]interface{}{
							"group":          "extensions",
							"version":        "v1",
							"kind":           "TestExtension",
							"resource":       "testExtensions",
							"verbs":          []string{"get"},
							"namespaced":     true,
							"preferredGroup": "apps",
						},
					},
				},
			},
		},
		{
			name:                 "eligible for override, but override version not found",
			groups:               []schema.GroupVersion{{Group: "autoscaling", Version: "v1"}},
			groupVersionOverride: false,
			resources: map[schema.GroupVersion][]metav1.APIResource{
				{Group: "autoscaling", Version: "v1"}: {
					{
						Name:         "testAutoscalings",
						SingularName: "testAutoscaling",
						Kind:         "TestAutoscaling",
						Namespaced:   true,
						Verbs:        metav1.Verbs{"get"},
					},
				},
			},
			wantError: false,
			desiredSchema: map[string]*types.APISchema{
				"autoscaling.v1.testautoscaling": {
					Schema: &wranglerSchema.Schema{
						ID:         "autoscaling.v1.testautoscaling",
						PluralName: "autoscaling.v1.testAutoscalings",
						Attributes: map[string]interface{}{
							"group":      "autoscaling",
							"version":    "v1",
							"kind":       "TestAutoscaling",
							"resource":   "testAutoscalings",
							"verbs":      []string{"get"},
							"namespaced": true,
						},
					},
				},
			},
		},
		{
			name:   "skip resource with / silently",
			groups: []schema.GroupVersion{{Group: "TestGroup", Version: "v1"}},
			resources: map[schema.GroupVersion][]metav1.APIResource{
				{Group: "TestGroup", Version: "v1"}: {
					{
						Name:         "test/Resources",
						SingularName: "test/Resource",
						Kind:         "Test/Resource",
						Namespaced:   true,
						Verbs:        metav1.Verbs{"get"},
					},
					{
						Name:         "testResources",
						SingularName: "testResource",
						Kind:         "TestResource",
						Namespaced:   false,
						Verbs:        metav1.Verbs{"get"},
					},
				},
			},
			wantError: false,
			desiredSchema: map[string]*types.APISchema{
				"testgroup.v1.testresource": {
					Schema: &wranglerSchema.Schema{
						ID:         "testgroup.v1.testresource",
						PluralName: "TestGroup.v1.testResources",
						Attributes: map[string]interface{}{
							"group":      "TestGroup",
							"version":    "v1",
							"kind":       "TestResource",
							"resource":   "testResources",
							"verbs":      []string{"get"},
							"namespaced": false,
						},
					},
				},
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			testDiscovery := fakeDiscovery{}
			for _, gvr := range test.groups {
				gvr := gvr
				testDiscovery.AddGroup(gvr.Group, gvr.Version, test.groupVersionOverride)
			}
			for gvr, resourceSlice := range test.resources {
				for _, resource := range resourceSlice {
					resource := resource
					testDiscovery.AddResource(gvr.Group, gvr.Version, resource)
				}
			}
			testDiscovery.GroupResourcesErr = test.discoveryErr
			schemas := map[string]*types.APISchema{}
			err := addDiscovery(&testDiscovery, schemas)
			if test.wantError {
				assert.Error(t, err, "expected an error but did not get one")
			} else {
				assert.NoError(t, err, "got an error but did not expect one")
			}
			assert.Equal(t, test.desiredSchema, schemas, "schemas were not as expected")
		})
	}
}

type fakeDiscovery struct {
	Groups            []*metav1.APIGroup
	Resources         []*metav1.APIResourceList
	Document          *openapiv2.Document
	GroupResourcesErr error
	DocumentErr       error
}

// ServerGroupsAndResources is the only method we actually need for the test - just returns what is on the struct
func (f *fakeDiscovery) ServerGroupsAndResources() ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	return f.Groups, f.Resources, f.GroupResourcesErr
}

func (f *fakeDiscovery) AddGroup(groupName string, preferredVersion string, includeOverrideVersion bool) {
	if f.Groups == nil {
		f.Groups = []*metav1.APIGroup{}
	}
	groupVersion := fmt.Sprintf("%s/%s", groupName, preferredVersion)
	found := -1
	for i := range f.Groups {
		if f.Groups[i].Name == groupName {
			found = i
		}
	}
	group := metav1.APIGroup{
		Name: groupName,
		PreferredVersion: metav1.GroupVersionForDiscovery{
			GroupVersion: groupVersion,
			Version:      preferredVersion,
		},
		Versions: []metav1.GroupVersionForDiscovery{
			{
				GroupVersion: groupVersion,
				Version:      preferredVersion,
			},
		},
	}

	// if we should include override versions in list of versions, figure out if we have an override and add it
	if includeOverrideVersion {
		if override, ok := preferredVersionOverride[groupVersion]; ok {
			group.Versions = append(group.Versions, metav1.GroupVersionForDiscovery{
				GroupVersion: fmt.Sprintf("%s/%s", groupName, override),
				Version:      override,
			})
		}
	}
	if found >= 0 {
		f.Groups[found] = &group
	} else {
		f.Groups = append(f.Groups, &group)
	}
}

func (f *fakeDiscovery) AddResource(group, version string, resource metav1.APIResource) {
	if f.Resources == nil {
		f.Resources = []*metav1.APIResourceList{}
	}
	groupVersion := fmt.Sprintf("%s/%s", group, version)
	found := -1
	// first, find the APIResourceList for our group
	for i := range f.Resources {
		if f.Resources[i].GroupVersion == groupVersion {
			found = i
		}
	}

	if found >= 0 {
		currentResourceList := f.Resources[found]
		resourceFound := -1
		// next, find the APIResource for our resource
		for i := range currentResourceList.APIResources {
			if currentResourceList.APIResources[i].Name == resource.Name {
				resourceFound = i
			}
		}
		if resourceFound >= 0 {
			currentResourceList.APIResources[resourceFound] = resource
		} else {
			currentResourceList.APIResources = append(currentResourceList.APIResources, resource)
		}
		f.Resources[found] = currentResourceList
	} else {
		currentResourceList := &metav1.APIResourceList{
			GroupVersion: groupVersion,
			APIResources: []metav1.APIResource{resource},
		}
		f.Resources = append(f.Resources, currentResourceList)
	}
}

// The rest of these methods are just here to conform to discovery.DiscoveryInterface
func (f *fakeDiscovery) RESTClient() restclient.Interface            { return nil }
func (f *fakeDiscovery) ServerGroups() (*metav1.APIGroupList, error) { return nil, nil }
func (f *fakeDiscovery) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	return nil, nil
}
func (f *fakeDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return nil, nil
}
func (f *fakeDiscovery) ServerPreferredNamespacedResources() ([]*metav1.APIResourceList, error) {
	return nil, nil
}
func (f *fakeDiscovery) ServerVersion() (*version.Info, error) { return nil, nil }
func (f *fakeDiscovery) OpenAPISchema() (*openapiv2.Document, error) {
	return f.Document, f.DocumentErr
}
func (f *fakeDiscovery) OpenAPIV3() openapi.Client                { return nil }
func (f *fakeDiscovery) WithLegacy() discovery.DiscoveryInterface { return f }
