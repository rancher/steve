package formatters

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	pbchart "k8s.io/helm/pkg/proto/hapi/chart"
	rspb "k8s.io/helm/pkg/proto/hapi/release"
)

var rv2 = rspb.Release{
	Name: "helmV3Release",
	Chart: &pbchart.Chart{
		Metadata: &pbchart.Metadata{
			Name:    "chartName",
			Version: "1.0.0",
		},
		Values: &pbchart.Config{
			Values: map[string]*pbchart.Value{
				"key": {Value: "value"},
			},
		},
	},
	Version:   1,
	Namespace: "default",
}

func Test_HandleHelmData(t *testing.T) {
	const helmRelease = `{
  "name": "helmV3Release",
  "chart": {
    "values": {
      "key": "value"
    }
  },
  "version": 1,
  "namespace": "default"
}
`
	var r map[string]any
	if err := json.Unmarshal([]byte(helmRelease), &r); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		resource *types.RawResource
		request  *types.APIRequest
		want     *types.RawResource
	}{
		{
			name: "When receiving a SECRET resource with includeHelmData set to TRUE and owner set to HELM, it should decode the helm3 release",
			resource: newSecret("helm", map[string]any{
				"release": toGzippedBase64(t, helmRelease),
			}),
			request: newRequest("true"),
			want: newSecret("helm", map[string]any{
				"release": r,
			}),
		},
		{
			name: "When receiving a SECRET resource with includeHelmData set to FALSE and owner set to HELM, it should drop the helm data",
			resource: newSecret("helm", map[string]any{
				"release": toGzippedBase64(t, helmRelease),
			}),
			request: newRequest("false"),
			want:    newSecret("helm", map[string]any{}),
		},
		{
			name: "When receiving a SECRET resource WITHOUT the includeHelmData query parameter and owner set to HELM, it should drop the helm data",
			resource: newSecret("helm", map[string]any{
				"release": base64.StdEncoding.EncodeToString([]byte(toGzippedBase64(t, helmRelease))),
			}),
			request: newRequest(""),
			want:    newSecret("helm", map[string]any{}),
		},
		{
			name: "When receiving a non-helm SECRET or CONFIGMAP resource with includeHelmData set to TRUE, it shouldn't change the resource",
			resource: &types.RawResource{
				Type: "pod",
				APIObject: types.APIObject{Object: &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Pod",
					"data": map[string]interface{}{
						"key": "value",
					},
				}}},
			},
			request: newRequest("true"),
			want: &types.RawResource{
				Type: "pod",
				APIObject: types.APIObject{Object: &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Pod",
					"data": map[string]interface{}{
						"key": "value",
					},
				}}},
			},
		},
		{
			name: "When receiving a non-helm SECRET or CONFIGMAP resource with includeHelmData set to FALSE, it shouldn't change the resource",
			resource: &types.RawResource{
				Type: "pod",
				APIObject: types.APIObject{Object: &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Pod",
					"data": map[string]interface{}{
						"key": "value",
					},
				}}},
			},
			request: newRequest("false"),
			want: &types.RawResource{
				Type: "pod",
				APIObject: types.APIObject{Object: &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Pod",
					"data": map[string]interface{}{
						"key": "value",
					},
				}}},
			},
		},
		{
			name: "When receiving a non-helm SECRET or CONFIGMAP resource WITHOUT the includeHelmData query parameter, it shouldn't change the resource",
			resource: &types.RawResource{
				Type: "pod",
				APIObject: types.APIObject{Object: &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Pod",
					"data": map[string]interface{}{
						"key": "value",
					},
				}}},
			},
			request: newRequest(""),
			want: &types.RawResource{
				Type: "pod",
				APIObject: types.APIObject{Object: &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Pod",
					"data": map[string]interface{}{
						"key": "value",
					},
				}}},
			},
		},
		{
			name: "When receiving a CONFIGMAP resource with includeHelmData set to TRUE and owner set to HELM, it should decode the helm3 release",
			resource: newConfigMap("helm", map[string]any{
				"release": toGzippedBase64(t, helmRelease),
			}),
			request: newRequest("true"),
			want: newConfigMap("helm", map[string]any{
				"release": r,
			}),
		},
		{
			name: "When receiving a CONFIGMAP resource with includeHelmData set to FALSE and owner set to HELM, it should drop the helm data",
			resource: newConfigMap("helm", map[string]any{
				"release": toGzippedBase64(t, helmRelease),
			}),
			request: newRequest("false"),
			want:    newConfigMap("helm", map[string]any{}),
		},
		{
			name: "When receiving a CONFIGMAP resource WITHOUT the includeHelmData query parameter and owner set to HELM, it should drop the helm data",
			resource: newConfigMap("helm", map[string]any{
				"release": toGzippedBase64(t, helmRelease),
			}),
			request: newRequest(""),
			want:    newConfigMap("helm", map[string]any{}),
		},
		{
			name: "[no magic gzip] When receiving a SECRET resource with includeHelmData set to TRUE and owner set to HELM, it should decode the helm3 release",
			resource: newSecret("helm", map[string]any{
				"release": base64.StdEncoding.EncodeToString([]byte(helmRelease)),
			}),
			request: newRequest("true"),
			want: newSecret("helm", map[string]any{
				"release": r,
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			HandleHelmData(tt.request, tt.resource)
			// data will be serialized, so it makes sense to compare their JSON representation
			assert.Empty(t, cmp.Diff(toPlainMap(t, tt.resource), toPlainMap(t, tt.want)))
		})
	}
}

func Test_HandleLegacyHelmV2Data(t *testing.T) {
	tests := []struct {
		name     string
		resource *types.RawResource
		request  *types.APIRequest
		want     *types.RawResource
	}{
		{
			name: "When receiving a SECRET resource with includeHelmData set to TRUE and owner set to TILLER, it should decode the helm2 release",
			resource: newSecret("TILLER", map[string]interface{}{
				"release": newV2Release(),
			}),
			request: newRequest("true"),
			want: newSecret("TILLER", map[string]interface{}{
				"release": &rv2,
			}),
		},
		{
			name: "When receiving a SECRET resource with includeHelmData set to FALSE and owner set to TILLER, it should drop the helm data",
			resource: newSecret("TILLER", map[string]interface{}{
				"release": newV2Release(),
			}),
			request: newRequest("false"),
			want:    newSecret("TILLER", map[string]interface{}{}),
		},
		{
			name: "When receiving a SECRET resource WITHOUT the includeHelmData query parameter and owner set to TILLER, it should drop the helm data",
			resource: newSecret("TILLER", map[string]interface{}{
				"release": newV2Release(),
			}),
			request: newRequest(""),
			want:    newSecret("TILLER", map[string]interface{}{}),
		},
		{
			name: "When receiving a CONFIGMAP resource with includeHelmData set to TRUE and owner set to TILLER, it should decode the helm2 release",
			resource: newConfigMap("TILLER", map[string]interface{}{
				"release": newV2Release(),
			}),
			request: newRequest("true"),
			want: newConfigMap("TILLER", map[string]interface{}{
				"release": &rv2,
			}),
		},
		{
			name: "When receiving a CONFIGMAP resource with includeHelmData set to FALSE and owner set to TILLER, it should drop the helm data",
			resource: newConfigMap("TILLER", map[string]interface{}{
				"release": newV2Release(),
			}),
			request: newRequest("false"),
			want:    newConfigMap("TILLER", map[string]interface{}{}),
		},
		{
			name: "When receiving a CONFIGMAP resource WITHOUT the includeHelmData query parameter and owner set to TILLER, it should drop the helm data",
			resource: newConfigMap("TILLER", map[string]interface{}{
				"release": newV2Release(),
			}),
			request: newRequest(""),
			want:    newConfigMap("TILLER", map[string]interface{}{}),
		},
		{
			name: "[no magic gzip] When receiving a SECRET resource with includeHelmData set to TRUE and owner set to TILLER, it should decode the helm2 release",
			resource: newSecret("TILLER", map[string]interface{}{
				"release": newV2ReleaseWithoutGzip(),
			}),
			request: newRequest("true"),
			want: newSecret("TILLER", map[string]interface{}{
				"release": &rv2,
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			HandleHelmData(tt.request, tt.resource)
			u, ok := tt.resource.APIObject.Object.(*unstructured.Unstructured)
			assert.True(t, ok)
			rl, ok := u.UnstructuredContent()["data"].(map[string]interface{})["release"]
			if ok {
				u, ok = tt.want.APIObject.Object.(*unstructured.Unstructured)
				assert.True(t, ok)
				rl2, ok := u.UnstructuredContent()["data"].(map[string]interface{})["release"]
				assert.True(t, ok)
				assert.True(t, proto.Equal(rl.(proto.Message), rl2.(proto.Message)))
			} else {
				assert.Equal(t, tt.resource, tt.want)
			}
		})
	}
}

func toPlainMap(t *testing.T, v interface{}) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		t.Fatal(err)
	}
	var res map[string]any
	if err := json.NewDecoder(&buf).Decode(&res); err != nil {
		t.Fatal(err)
	}
	pruneNullValues(res)
	return res
}

func pruneNullValues(m map[string]any) {
	for k, v := range m {
		if v == nil {
			delete(m, k)
		} else if mm, ok := v.(map[string]any); ok {
			pruneNullValues(mm)
		}
	}
	return
}

func newSecret(owner string, data map[string]any) *types.RawResource {
	for k, v := range data {
		if s, ok := v.(string); ok {
			data[k] = base64.StdEncoding.EncodeToString([]byte(s))
		}
	}
	secret := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Secret",
		"data":       data,
	}}
	if owner == "TILLER" {
		secret.SetLabels(map[string]string{"OWNER": owner})
	} else {
		secret.SetLabels(map[string]string{"owner": owner})
	}
	return &types.RawResource{
		Type:      "secret",
		APIObject: types.APIObject{Object: secret},
	}
}

func newConfigMap(owner string, data map[string]interface{}) *types.RawResource {
	cfgMap := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "configmap",
		"data":       data,
	}}
	if owner == "TILLER" {
		cfgMap.SetLabels(map[string]string{"OWNER": owner})
	} else {
		cfgMap.SetLabels(map[string]string{"owner": owner})
	}
	return &types.RawResource{
		Type:      "configmap",
		APIObject: types.APIObject{Object: cfgMap},
	}
}

func newV2Release() string {
	a := rv2
	b, err := proto.Marshal(&a)
	if err != nil {
		logrus.Errorf("Failed to marshal release: %v", err)
	}
	buf := bytes.Buffer{}
	gz := gzip.NewWriter(&buf)
	gz.Write(b)
	gz.Close()
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func newV2ReleaseWithoutGzip() string {
	a := rv2
	b, err := proto.Marshal(&a)
	if err != nil {
		logrus.Errorf("Failed to marshal release: %v", err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func toGzippedBase64(t *testing.T, v string) string {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	defer gz.Close()
	if _, err := gz.Write([]byte(v)); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func newRequest(value string) *types.APIRequest {
	req := &types.APIRequest{Query: url.Values{}}
	if value != "" {
		req.Query.Add("includeHelmData", value)
	}
	return req
}
