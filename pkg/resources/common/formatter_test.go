package common

import (
	"bytes"
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/apiserver/pkg/urlbuilder"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/steve/pkg/accesscontrol/fake"
	"github.com/rancher/steve/pkg/attributes"
	"github.com/rancher/wrangler/v3/pkg/schemas"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	schema2 "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
)

func Test_includeFields(t *testing.T) {
	tests := []struct {
		name    string
		request *types.APIRequest
		unstr   *unstructured.Unstructured
		want    *unstructured.Unstructured
	}{
		{
			name: "include top level field",
			request: &types.APIRequest{
				Query: url.Values{
					"include": []string{"metadata"},
				},
			},
			unstr: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"creationTimestamp": "2022-04-11T22:05:27Z",
						"name":              "kube-root-ca.crt",
						"namespace":         "c-m-w466b2vg",
						"resourceVersion":   "36948",
						"uid":               "1c497934-52cb-42ab-a613-dedfd5fb207b",
					},
					"data": map[string]interface{}{
						"ca.crt": "-----BEGIN CERTIFICATE-----\nMIIC5zCCAc+gAwIBAg\n-----END CERTIFICATE-----\n",
					},
				},
			},
			want: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"creationTimestamp": "2022-04-11T22:05:27Z",
						"name":              "kube-root-ca.crt",
						"namespace":         "c-m-w466b2vg",
						"resourceVersion":   "36948",
						"uid":               "1c497934-52cb-42ab-a613-dedfd5fb207b",
					},
				},
			},
		},
		{
			name: "include sub field",
			request: &types.APIRequest{
				Query: url.Values{
					"include": []string{"metadata.managedFields"},
				},
			},
			unstr: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"creationTimestamp": "2022-04-11T22:05:27Z",
						"name":              "kube-root-ca.crt",
						"namespace":         "c-m-w466b2vg",
						"resourceVersion":   "36948",
						"uid":               "1c497934-52cb-42ab-a613-dedfd5fb207b",
						"managedFields": []map[string]interface{}{
							{
								"apiVersion": "v1",
								"fieldsType": "FieldsV1",
								"fieldsV1": map[string]interface{}{
									"f:data": map[string]interface{}{
										".":        map[string]interface{}{},
										"f:ca.crt": map[string]interface{}{},
									},
								},
								"manager":   "kube-controller-manager",
								"operation": "Update",
								"time":      "2022-04-11T22:05:27Z",
							},
						},
					},
					"data": map[string]interface{}{
						"ca.crt": "-----BEGIN CERTIFICATE-----\nMIIC5zCCAc+gAwIBAg\n-----END CERTIFICATE-----\n",
					},
				},
			},
			want: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"managedFields": []map[string]interface{}{
							{
								"apiVersion": "v1",
								"fieldsType": "FieldsV1",
								"fieldsV1": map[string]interface{}{
									"f:data": map[string]interface{}{
										".":        map[string]interface{}{},
										"f:ca.crt": map[string]interface{}{},
									},
								},
								"manager":   "kube-controller-manager",
								"operation": "Update",
								"time":      "2022-04-11T22:05:27Z",
							},
						},
					},
				},
			},
		},
		{
			name: "include invalid field",
			request: &types.APIRequest{
				Query: url.Values{
					"include": []string{"foo.bar"},
				},
			},
			unstr: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"creationTimestamp": "2022-04-11T22:05:27Z",
						"name":              "kube-root-ca.crt",
						"namespace":         "c-m-w466b2vg",
						"resourceVersion":   "36948",
						"uid":               "1c497934-52cb-42ab-a613-dedfd5fb207b",
					},
					"data": map[string]interface{}{
						"ca.crt": "-----BEGIN CERTIFICATE-----\nMIIC5zCCAc+gAwIBAg\n-----END CERTIFICATE-----\n",
					},
				},
			},
			want: &unstructured.Unstructured{
				Object: map[string]interface{}{},
			},
		},
		{
			name: "include multiple fields",
			request: &types.APIRequest{
				Query: url.Values{
					"include": []string{"kind", "apiVersion", "metadata.name"},
				},
			},
			unstr: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata": map[string]interface{}{
						"creationTimestamp": "2022-04-11T22:05:27Z",
						"name":              "kube-root-ca.crt",
						"namespace":         "c-m-w466b2vg",
						"resourceVersion":   "36948",
						"uid":               "1c497934-52cb-42ab-a613-dedfd5fb207b",
					},
					"data": map[string]interface{}{
						"ca.crt": "-----BEGIN CERTIFICATE-----\nMIIC5zCCAc+gAwIBAg\n-----END CERTIFICATE-----\n",
					},
				},
			},
			want: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata": map[string]interface{}{
						"name": "kube-root-ca.crt",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			includeFields(tt.request, tt.unstr)
			assert.Equal(t, tt.want, tt.unstr)
		})
	}
}

func Test_excludeFields(t *testing.T) {
	tests := []struct {
		name    string
		request *types.APIRequest
		unstr   *unstructured.Unstructured
		want    *unstructured.Unstructured
	}{
		{
			name: "exclude top level field",
			request: &types.APIRequest{
				Query: url.Values{
					"exclude": []string{"metadata"},
				},
			},
			unstr: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"creationTimestamp": "2022-04-11T22:05:27Z",
						"name":              "kube-root-ca.crt",
						"namespace":         "c-m-w466b2vg",
						"resourceVersion":   "36948",
						"uid":               "1c497934-52cb-42ab-a613-dedfd5fb207b",
					},
					"data": map[string]interface{}{
						"ca.crt": "-----BEGIN CERTIFICATE-----\nMIIC5zCCAc+gAwIBAg\n-----END CERTIFICATE-----\n",
					},
				},
			},
			want: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"data": map[string]interface{}{
						"ca.crt": "-----BEGIN CERTIFICATE-----\nMIIC5zCCAc+gAwIBAg\n-----END CERTIFICATE-----\n",
					},
				},
			},
		},
		{
			name: "exclude sub field",
			request: &types.APIRequest{
				Query: url.Values{
					"exclude": []string{"metadata.managedFields"},
				},
			},
			unstr: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"creationTimestamp": "2022-04-11T22:05:27Z",
						"name":              "kube-root-ca.crt",
						"namespace":         "c-m-w466b2vg",
						"resourceVersion":   "36948",
						"uid":               "1c497934-52cb-42ab-a613-dedfd5fb207b",
						"managedFields": []map[string]interface{}{
							{
								"apiVersion": "v1",
								"fieldsType": "FieldsV1",
								"fieldsV1": map[string]interface{}{
									"f:data": map[string]interface{}{
										".":        map[string]interface{}{},
										"f:ca.crt": map[string]interface{}{},
									},
								},
								"manager":   "kube-controller-manager",
								"operation": "Update",
								"time":      "2022-04-11T22:05:27Z",
							},
						},
					},
					"data": map[string]interface{}{
						"ca.crt": "-----BEGIN CERTIFICATE-----\nMIIC5zCCAc+gAwIBAg\n-----END CERTIFICATE-----\n",
					},
				},
			},
			want: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"creationTimestamp": "2022-04-11T22:05:27Z",
						"name":              "kube-root-ca.crt",
						"namespace":         "c-m-w466b2vg",
						"resourceVersion":   "36948",
						"uid":               "1c497934-52cb-42ab-a613-dedfd5fb207b",
					},
					"data": map[string]interface{}{
						"ca.crt": "-----BEGIN CERTIFICATE-----\nMIIC5zCCAc+gAwIBAg\n-----END CERTIFICATE-----\n",
					},
				},
			},
		},
		{
			name: "exclude invalid field",
			request: &types.APIRequest{
				Query: url.Values{
					"exclude": []string{"foo.bar"},
				},
			},
			unstr: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"creationTimestamp": "2022-04-11T22:05:27Z",
						"name":              "kube-root-ca.crt",
						"namespace":         "c-m-w466b2vg",
						"resourceVersion":   "36948",
						"uid":               "1c497934-52cb-42ab-a613-dedfd5fb207b",
					},
					"data": map[string]interface{}{
						"ca.crt": "-----BEGIN CERTIFICATE-----\nMIIC5zCCAc+gAwIBAg\n-----END CERTIFICATE-----\n",
					},
				},
			},
			want: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"creationTimestamp": "2022-04-11T22:05:27Z",
						"name":              "kube-root-ca.crt",
						"namespace":         "c-m-w466b2vg",
						"resourceVersion":   "36948",
						"uid":               "1c497934-52cb-42ab-a613-dedfd5fb207b",
					},
					"data": map[string]interface{}{
						"ca.crt": "-----BEGIN CERTIFICATE-----\nMIIC5zCCAc+gAwIBAg\n-----END CERTIFICATE-----\n",
					},
				},
			},
		},
		{
			name: "exclude multiple fields",
			request: &types.APIRequest{
				Query: url.Values{
					"exclude": []string{"kind", "apiVersion", "metadata.name"},
				},
			},
			unstr: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata": map[string]interface{}{
						"creationTimestamp": "2022-04-11T22:05:27Z",
						"name":              "kube-root-ca.crt",
						"namespace":         "c-m-w466b2vg",
						"resourceVersion":   "36948",
						"uid":               "1c497934-52cb-42ab-a613-dedfd5fb207b",
					},
					"data": map[string]interface{}{
						"ca.crt": "-----BEGIN CERTIFICATE-----\nMIIC5zCCAc+gAwIBAg\n-----END CERTIFICATE-----\n",
					},
				},
			},
			want: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"creationTimestamp": "2022-04-11T22:05:27Z",
						"namespace":         "c-m-w466b2vg",
						"resourceVersion":   "36948",
						"uid":               "1c497934-52cb-42ab-a613-dedfd5fb207b",
					},
					"data": map[string]interface{}{
						"ca.crt": "-----BEGIN CERTIFICATE-----\nMIIC5zCCAc+gAwIBAg\n-----END CERTIFICATE-----\n",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			excludeFields(tt.request, tt.unstr)
			assert.Equal(t, tt.want, tt.unstr)
		})
	}
}

func Test_excludeValues(t *testing.T) {
	tests := []struct {
		name    string
		request *types.APIRequest
		unstr   *unstructured.Unstructured
		want    *unstructured.Unstructured
	}{
		{
			name: "exclude top level value",
			request: &types.APIRequest{
				Query: url.Values{
					"excludeValues": []string{"data"},
				},
			},
			unstr: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"creationTimestamp": "2022-04-11T22:05:27Z",
						"name":              "kube-root-ca.crt",
						"namespace":         "c-m-w466b2vg",
						"resourceVersion":   "36948",
						"uid":               "1c497934-52cb-42ab-a613-dedfd5fb207b",
					},
					"data": map[string]interface{}{
						"ca.crt": "-----BEGIN CERTIFICATE-----\nMIIC5zCCAc+gAwIBAg\n-----END CERTIFICATE-----\n",
					},
				},
			},
			want: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"creationTimestamp": "2022-04-11T22:05:27Z",
						"name":              "kube-root-ca.crt",
						"namespace":         "c-m-w466b2vg",
						"resourceVersion":   "36948",
						"uid":               "1c497934-52cb-42ab-a613-dedfd5fb207b",
					},
					"data": map[string]interface{}{
						"ca.crt": "",
					},
				},
			},
		},
		{
			name: "exclude sub field value",
			request: &types.APIRequest{
				Query: url.Values{
					"excludeValues": []string{"metadata.annotations"},
				},
			},
			unstr: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "apps/v1",
					"kind":       "Deployment",
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							"deployment.kubernetes.io/revision": "2",
							"meta.helm.sh/release-name":         "fleet-agent-local",
							"meta.helm.sh/release-namespace":    "cattle-fleet-local-system",
						},
						"creationTimestamp": "2022-04-11T22:05:27Z",
						"name":              "fleet-agent",
						"namespace":         "cattle-fleet-local-system",
						"resourceVersion":   "36948",
						"uid":               "1c497934-52cb-42ab-a613-dedfd5fb207b",
					},
					"spec": map[string]interface{}{
						"replicas": 1,
					},
				},
			},
			want: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "apps/v1",
					"kind":       "Deployment",
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							"deployment.kubernetes.io/revision": "",
							"meta.helm.sh/release-name":         "",
							"meta.helm.sh/release-namespace":    "",
						},
						"creationTimestamp": "2022-04-11T22:05:27Z",
						"name":              "fleet-agent",
						"namespace":         "cattle-fleet-local-system",
						"resourceVersion":   "36948",
						"uid":               "1c497934-52cb-42ab-a613-dedfd5fb207b",
					},
					"spec": map[string]interface{}{
						"replicas": 1,
					},
				},
			},
		},
		{
			name: "exclude invalid value",
			request: &types.APIRequest{
				Query: url.Values{
					"excludeValues": []string{"foo.bar"},
				},
			},
			unstr: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"creationTimestamp": "2022-04-11T22:05:27Z",
						"name":              "kube-root-ca.crt",
						"namespace":         "c-m-w466b2vg",
						"resourceVersion":   "36948",
						"uid":               "1c497934-52cb-42ab-a613-dedfd5fb207b",
					},
					"data": map[string]interface{}{
						"ca.crt": "-----BEGIN CERTIFICATE-----\nMIIC5zCCAc+gAwIBAg\n-----END CERTIFICATE-----\n",
					},
				},
			},
			want: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"creationTimestamp": "2022-04-11T22:05:27Z",
						"name":              "kube-root-ca.crt",
						"namespace":         "c-m-w466b2vg",
						"resourceVersion":   "36948",
						"uid":               "1c497934-52cb-42ab-a613-dedfd5fb207b",
					},
					"data": map[string]interface{}{
						"ca.crt": "-----BEGIN CERTIFICATE-----\nMIIC5zCCAc+gAwIBAg\n-----END CERTIFICATE-----\n",
					},
				},
			},
		},
		{
			name: "exclude multiple values",
			request: &types.APIRequest{
				Query: url.Values{
					"excludeValues": []string{"metadata.annotations", "metadata.labels"},
				},
			},
			unstr: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "apps/v1",
					"kind":       "Deployment",
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							"deployment.kubernetes.io/revision": "2",
							"meta.helm.sh/release-name":         "fleet-agent-local",
							"meta.helm.sh/release-namespace":    "cattle-fleet-local-system",
						},
						"labels": map[string]interface{}{
							"app.kubernetes.io/managed-by": "Helm",
							"objectset.rio.cattle.io/hash": "362023f752e7f1989d8b652e029bd2c658ae7c44",
						},
						"creationTimestamp": "2022-04-11T22:05:27Z",
						"name":              "fleet-agent",
						"namespace":         "cattle-fleet-local-system",
						"resourceVersion":   "36948",
						"uid":               "1c497934-52cb-42ab-a613-dedfd5fb207b",
					},
					"spec": map[string]interface{}{
						"replicas": 1,
					},
				},
			},
			want: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "apps/v1",
					"kind":       "Deployment",
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							"deployment.kubernetes.io/revision": "",
							"meta.helm.sh/release-name":         "",
							"meta.helm.sh/release-namespace":    "",
						},
						"labels": map[string]interface{}{
							"app.kubernetes.io/managed-by": "",
							"objectset.rio.cattle.io/hash": "",
						},
						"creationTimestamp": "2022-04-11T22:05:27Z",
						"name":              "fleet-agent",
						"namespace":         "cattle-fleet-local-system",
						"resourceVersion":   "36948",
						"uid":               "1c497934-52cb-42ab-a613-dedfd5fb207b",
					},
					"spec": map[string]interface{}{
						"replicas": 1,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			excludeValues(tt.request, tt.unstr)
			assert.Equal(t, tt.want, tt.unstr)
		})
	}
}

func Test_selfLink(t *testing.T) {
	tests := []struct {
		name              string
		group             string
		version           string
		resource          string
		resourceName      string
		resourceNamespace string
		want              string
	}{
		{
			name:              "empty group",
			group:             "",
			version:           "v1",
			resource:          "pods",
			resourceName:      "rancher",
			resourceNamespace: "cattle-system",
			want:              "/api/v1/namespaces/cattle-system/pods/rancher",
		},
		{
			name:              "third party crd",
			group:             "fake.group.io",
			version:           "v4",
			resource:          "new-crd",
			resourceName:      "new-resource",
			resourceNamespace: "random-ns",
			want:              "/apis/fake.group.io/v4/namespaces/random-ns/new-crd/new-resource",
		},
		{
			name:         "non-namespaced third party crd",
			group:        "fake.group.io",
			version:      "v4",
			resource:     "new-crd",
			resourceName: "new-resource",
			want:         "/apis/fake.group.io/v4/new-crd/new-resource",
		},
		{
			name:         "rancher crd, non namespaced",
			group:        "management.cattle.io",
			version:      "v3",
			resource:     "cluster",
			resourceName: "c-123xyz",
			want:         "/v1/management.cattle.io.cluster/c-123xyz",
		},
		{
			name:              "rancher crd, namespaced",
			group:             "management.cattle.io",
			version:           "v3",
			resource:          "catalogtemplates",
			resourceName:      "built-in",
			resourceNamespace: "cattle-global-data",
			want:              "/v1/management.cattle.io.catalogtemplates/cattle-global-data/built-in",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			gvr := schema2.GroupVersionResource{
				Group:    test.group,
				Version:  test.version,
				Resource: test.resource,
			}
			obj := unstructured.Unstructured{}
			obj.SetName(test.resourceName)
			obj.SetNamespace(test.resourceNamespace)
			assert.Equal(t, test.want, selfLink(gvr, &obj), "did not get expected prefix for object")
		})
	}
}

func Test_formatterLinks(t *testing.T) {
	t.Parallel()
	type permissions struct {
		hasGet    bool
		hasUpdate bool
		hasRemove bool
		hasPatch  bool
	}
	tests := []struct {
		name         string
		hasUser      bool
		permissions  *permissions
		schema       *types.APISchema
		apiObject    types.APIObject
		currentLinks map[string]string
		wantLinks    map[string]string
	}{
		{
			name: "no schema",
			currentLinks: map[string]string{
				"default": "defaultVal",
			},
			wantLinks: map[string]string{
				"default": "defaultVal",
			},
		},
		{
			name: "no gvr in schema",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "example",
					Attributes: map[string]interface{}{
						"some": "thing",
					},
				},
			},
			currentLinks: map[string]string{
				"default": "defaultVal",
			},
			wantLinks: map[string]string{
				"default": "defaultVal",
			},
		},
		{
			name: "api object has no accessor",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "example",
					Attributes: map[string]interface{}{
						"group":    "",
						"version":  "v1",
						"resource": "pods",
					},
				},
			},
			apiObject: types.APIObject{
				ID:     "example",
				Object: struct{}{},
			},
			currentLinks: map[string]string{
				"default": "defaultVal",
			},
			wantLinks: map[string]string{
				"default": "defaultVal",
			},
		},
		{
			name: "no user info",
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "example",
					Attributes: map[string]interface{}{
						"group":    "",
						"version":  "v1",
						"resource": "pods",
					},
				},
			},
			apiObject: types.APIObject{
				ID: "example",
				Object: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "example-pod",
						Namespace: "example-ns",
					},
				},
			},
			currentLinks: map[string]string{
				"default": "defaultVal",
			},
			wantLinks: map[string]string{
				"default": "defaultVal",
			},
		},
		{
			name:    "no accessSet",
			hasUser: true,
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "example",
					Attributes: map[string]interface{}{
						"group":    "",
						"version":  "v1",
						"resource": "pods",
					},
				},
			},
			apiObject: types.APIObject{
				ID: "example",
				Object: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "example-pod",
						Namespace: "example-ns",
					},
				},
			},
			currentLinks: map[string]string{
				"default": "defaultVal",
			},
			wantLinks: map[string]string{
				"default": "defaultVal",
			},
		},
		{
			name:        "no update/remove permissions",
			hasUser:     true,
			permissions: &permissions{},
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "example",
					Attributes: map[string]interface{}{
						"group":    "",
						"version":  "v1",
						"resource": "pods",
					},
				},
			},
			apiObject: types.APIObject{
				ID: "example",
				Object: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "example-pod",
						Namespace: "example-ns",
					},
				},
			},
			currentLinks: map[string]string{
				"default": "defaultVal",
				"update":  "../v1/namespaces/example-ns/pods/example-pod",
				"remove":  "../v1/namespaces/example-ns/pods/example-pod",
			},
			wantLinks: map[string]string{
				"default": "defaultVal",
				"view":    "/api/v1/namespaces/example-ns/pods/example-pod",
			},
		},
		{
			name:    "update but no remove permissions",
			hasUser: true,
			permissions: &permissions{
				hasUpdate: true,
			},
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "example",
					Attributes: map[string]interface{}{
						"group":    "",
						"version":  "v1",
						"resource": "pods",
					},
				},
			},
			apiObject: types.APIObject{
				ID: "example",
				Object: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "example-pod",
						Namespace: "example-ns",
					},
				},
			},
			currentLinks: map[string]string{
				"default": "defaultVal",
				"update":  "../v1/namespaces/example-ns/pods/example-pod",
				"remove":  "../v1/namespaces/example-ns/pods/example-pod",
			},
			wantLinks: map[string]string{
				"default": "defaultVal",
				"update":  "../v1/namespaces/example-ns/pods/example-pod",
				"view":    "/api/v1/namespaces/example-ns/pods/example-pod",
			},
		},
		{
			name:    "remove but no update permissions",
			hasUser: true,
			permissions: &permissions{
				hasRemove: true,
			},
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "example",
					Attributes: map[string]interface{}{
						"group":    "",
						"version":  "v1",
						"resource": "pods",
					},
				},
			},
			apiObject: types.APIObject{
				ID: "example",
				Object: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "example-pod",
						Namespace: "example-ns",
					},
				},
			},
			currentLinks: map[string]string{
				"default": "defaultVal",
				"update":  "../v1/namespaces/example-ns/pods/example-pod",
				"remove":  "../v1/namespaces/example-ns/pods/example-pod",
			},
			wantLinks: map[string]string{
				"default": "defaultVal",
				"remove":  "../v1/namespaces/example-ns/pods/example-pod",
				"view":    "/api/v1/namespaces/example-ns/pods/example-pod",
			},
		},
		{
			name:    "update and remove permissions",
			hasUser: true,
			permissions: &permissions{
				hasUpdate: true,
				hasRemove: true,
			},
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "example",
					Attributes: map[string]interface{}{
						"group":    "",
						"version":  "v1",
						"resource": "pods",
					},
				},
			},
			apiObject: types.APIObject{
				ID: "example",
				Object: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "example-pod",
						Namespace: "example-ns",
					},
				},
			},
			currentLinks: map[string]string{
				"default": "defaultVal",
				"update":  "../v1/namespaces/example-ns/pods/example-pod",
				"remove":  "../v1/namespaces/example-ns/pods/example-pod",
			},
			wantLinks: map[string]string{
				"default": "defaultVal",
				"update":  "../v1/namespaces/example-ns/pods/example-pod",
				"remove":  "../v1/namespaces/example-ns/pods/example-pod",
				"view":    "/api/v1/namespaces/example-ns/pods/example-pod",
			},
		},
		{
			name:    "update and remove permissions, but blocked",
			hasUser: true,
			permissions: &permissions{
				hasUpdate: true,
				hasRemove: true,
			},
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "example",
					Attributes: map[string]interface{}{
						"group":    "",
						"version":  "v1",
						"resource": "pods",
						"disallowMethods": map[string]bool{
							http.MethodPut:    true,
							http.MethodDelete: true,
						},
					},
				},
			},
			apiObject: types.APIObject{
				ID: "example",
				Object: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "example-pod",
						Namespace: "example-ns",
					},
				},
			},
			currentLinks: map[string]string{
				"default": "defaultVal",
				"update":  "../v1/namespaces/example-ns/pods/example-pod",
				"remove":  "../v1/namespaces/example-ns/pods/example-pod",
			},
			wantLinks: map[string]string{
				"default": "defaultVal",
				"update":  "blocked",
				"remove":  "blocked",
				"view":    "/api/v1/namespaces/example-ns/pods/example-pod",
			},
		},
		{
			name:    "patch permissions",
			hasUser: true,
			permissions: &permissions{
				hasPatch: true,
			},
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "example",
					Attributes: map[string]interface{}{
						"group":    "apps",
						"version":  "v1",
						"resource": "deployments",
					},
				},
			},
			apiObject: types.APIObject{
				ID: "example",
				Object: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "example-deployment",
						Namespace: "example-ns",
					},
				},
			},
			currentLinks: map[string]string{
				"default": "defaultVal",
				"view":    "/apis/apps/v1/namespaces/example-ns/deployments/example-deployment",
			},
			wantLinks: map[string]string{
				"default": "defaultVal",
				"patch":   "/apis/apps/v1/namespaces/example-ns/deployments/example-deployment",
				"view":    "/apis/apps/v1/namespaces/example-ns/deployments/example-deployment",
			},
		},
		{
			name:    "patch permissions, but blocked",
			hasUser: true,
			permissions: &permissions{
				hasPatch: true,
			},
			schema: &types.APISchema{
				Schema: &schemas.Schema{
					ID: "example",
					Attributes: map[string]interface{}{
						"group":    "apps",
						"version":  "v1",
						"resource": "deployments",
						"disallowMethods": map[string]bool{
							http.MethodPatch: true,
						},
					},
				},
			},
			apiObject: types.APIObject{
				ID: "example",
				Object: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "example-deployment",
						Namespace: "example-ns",
					},
				},
			},
			currentLinks: map[string]string{
				"default": "defaultVal",
				"view":    "/apis/apps/v1/namespaces/example-ns/deployments/example-deployment",
			},
			wantLinks: map[string]string{
				"default": "defaultVal",
				"patch":   "blocked",
				"view":    "/apis/apps/v1/namespaces/example-ns/deployments/example-deployment",
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			defaultUserInfo := user.DefaultInfo{
				Name:   "test-user",
				Groups: []string{"groups"},
			}
			ctrl := gomock.NewController(t)
			asl := fake.NewMockAccessSetLookup(ctrl)
			if test.permissions != nil {
				gvr := attributes.GVR(test.schema)
				meta, err := meta.Accessor(test.apiObject.Object)
				accessSet := accesscontrol.AccessSet{}
				require.NoError(t, err)
				if test.permissions.hasUpdate {
					accessSet.Add("update", gvr.GroupResource(), accesscontrol.Access{
						Namespace:    meta.GetNamespace(),
						ResourceName: meta.GetName(),
					})
				}
				if test.permissions.hasRemove {
					accessSet.Add("delete", gvr.GroupResource(), accesscontrol.Access{
						Namespace:    meta.GetNamespace(),
						ResourceName: meta.GetName(),
					})
				}
				if test.permissions.hasPatch {
					accessSet.Add("patch", gvr.GroupResource(), accesscontrol.Access{
						Namespace:    meta.GetNamespace(),
						ResourceName: meta.GetName(),
					})
				}
				asl.EXPECT().AccessFor(&defaultUserInfo).Return(&accessSet)
			} else {
				asl.EXPECT().AccessFor(&defaultUserInfo).Return(nil).AnyTimes()
			}
			ctx := context.Background()
			if test.hasUser {
				ctx = request.WithUser(ctx, &defaultUserInfo)
			}
			httpRequest, err := http.NewRequestWithContext(ctx, "", "", bytes.NewBuffer([]byte{}))
			require.NoError(t, err)
			request := &types.APIRequest{
				Request:    httpRequest,
				URLBuilder: &urlbuilder.DefaultURLBuilder{},
			}
			resource := &types.RawResource{
				Schema:    test.schema,
				APIObject: test.apiObject,
				Links:     test.currentLinks,
			}
			fmtter := formatter(nil, asl)
			fmtter(request, resource)
			require.Equal(t, test.wantLinks, resource.Links)

		})
	}
}
