package formatters

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/release"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	pbchart "k8s.io/helm/pkg/proto/hapi/chart"
	rspb "k8s.io/helm/pkg/proto/hapi/release"
	"net/url"
	"testing"

	"github.com/rancher/apiserver/pkg/types"
)

var r = release.Release{
	Name: "helmV3Release",
	Chart: &chart.Chart{
		Values: map[string]interface{}{
			"key": "value",
		},
	},
	Version:   1,
	Namespace: "default",
}

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
	tests := []struct {
		name        string
		resource    *types.RawResource
		request     *types.APIRequest
		want        *types.RawResource
		helmVersion int
	}{ //helm v3
		{
			name: "When receiving a SECRET resource with includeHelmData set to TRUE and owner set to HELM, it should decode the helm3 release",
			resource: newSecret("helm", map[string]interface{}{
				"release": base64.StdEncoding.EncodeToString([]byte(newV3Release())),
			}),
			request: newRequest("true"),
			want: newSecret("helm", map[string]interface{}{
				"release": &r,
			}),
			helmVersion: 3,
		},
		{
			name: "When receiving a SECRET resource with includeHelmData set to FALSE and owner set to HELM, it should drop the helm data",
			resource: newSecret("helm", map[string]interface{}{
				"release": base64.StdEncoding.EncodeToString([]byte(newV3Release())),
			}),
			request:     newRequest("false"),
			want:        newSecret("helm", map[string]interface{}{}),
			helmVersion: 3,
		},
		{
			name: "When receiving a SECRET resource WITHOUT the includeHelmData query parameter and owner set to HELM, it should drop the helm data",
			resource: newSecret("helm", map[string]interface{}{
				"release": base64.StdEncoding.EncodeToString([]byte(newV3Release())),
			}),
			request:     newRequest(""),
			want:        newSecret("helm", map[string]interface{}{}),
			helmVersion: 3,
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
			helmVersion: 3,
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
			helmVersion: 3,
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
			helmVersion: 3,
		},
		{
			name: "When receiving a CONFIGMAP resource with includeHelmData set to TRUE and owner set to HELM, it should decode the helm3 release",
			resource: newConfigMap("helm", map[string]interface{}{
				"release": newV3Release(),
			}),
			request: newRequest("true"),
			want: newConfigMap("helm", map[string]interface{}{
				"release": &r,
			}),
			helmVersion: 3,
		},
		{
			name: "When receiving a CONFIGMAP resource with includeHelmData set to FALSE and owner set to HELM, it should drop the helm data",
			resource: newConfigMap("helm", map[string]interface{}{
				"release": newV3Release(),
			}),
			request:     newRequest("false"),
			want:        newConfigMap("helm", map[string]interface{}{}),
			helmVersion: 3,
		},
		{
			name: "When receiving a CONFIGMAP resource WITHOUT the includeHelmData query parameter and owner set to HELM, it should drop the helm data",
			resource: newConfigMap("helm", map[string]interface{}{
				"release": newV3Release(),
			}),
			request:     newRequest(""),
			want:        newConfigMap("helm", map[string]interface{}{}),
			helmVersion: 3,
		},
		//helm v2
		{
			name: "When receiving a SECRET resource with includeHelmData set to TRUE and owner set to TILLER, it should decode the helm2 release",
			resource: newSecret("TILLER", map[string]interface{}{
				"release": base64.StdEncoding.EncodeToString([]byte(newV2Release())),
			}),
			request: newRequest("true"),
			want: newSecret("TILLER", map[string]interface{}{
				"release": &rv2,
			}),
			helmVersion: 2,
		},
		{
			name: "When receiving a SECRET resource with includeHelmData set to FALSE and owner set to TILLER, it should drop the helm data",
			resource: newSecret("TILLER", map[string]interface{}{
				"release": base64.StdEncoding.EncodeToString([]byte(newV2Release())),
			}),
			request:     newRequest("false"),
			want:        newSecret("TILLER", map[string]interface{}{}),
			helmVersion: 2,
		},
		{
			name: "When receiving a SECRET resource WITHOUT the includeHelmData query parameter and owner set to TILLER, it should drop the helm data",
			resource: newSecret("TILLER", map[string]interface{}{
				"release": base64.StdEncoding.EncodeToString([]byte(newV2Release())),
			}),
			request:     newRequest(""),
			want:        newSecret("TILLER", map[string]interface{}{}),
			helmVersion: 2,
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
			helmVersion: 2,
		},
		{
			name: "When receiving a CONFIGMAP resource with includeHelmData set to FALSE and owner set to TILLER, it should drop the helm data",
			resource: newConfigMap("TILLER", map[string]interface{}{
				"release": newV2Release(),
			}),
			request:     newRequest("false"),
			want:        newConfigMap("TILLER", map[string]interface{}{}),
			helmVersion: 2,
		},
		{
			name: "When receiving a CONFIGMAP resource WITHOUT the includeHelmData query parameter and owner set to TILLER, it should drop the helm data",
			resource: newConfigMap("TILLER", map[string]interface{}{
				"release": newV2Release(),
			}),
			request:     newRequest(""),
			want:        newConfigMap("TILLER", map[string]interface{}{}),
			helmVersion: 2,
		},
		{
			name: "[no magic gzip] When receiving a SECRET resource with includeHelmData set to TRUE and owner set to HELM, it should decode the helm3 release",
			resource: newSecret("helm", map[string]interface{}{
				"release": base64.StdEncoding.EncodeToString([]byte(newV3ReleaseWithoutGzip())),
			}),
			request: newRequest("true"),
			want: newSecret("helm", map[string]interface{}{
				"release": &r,
			}),
			helmVersion: 3,
		},
		{
			name: "[no magic gzip] When receiving a SECRET resource with includeHelmData set to TRUE and owner set to TILLER, it should decode the helm2 release",
			resource: newSecret("TILLER", map[string]interface{}{
				"release": base64.StdEncoding.EncodeToString([]byte(newV2ReleaseWithoutGzip())),
			}),
			request: newRequest("true"),
			want: newSecret("TILLER", map[string]interface{}{
				"release": &rv2,
			}),
			helmVersion: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			HandleHelmData(tt.request, tt.resource)
			if tt.helmVersion == 2 {
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
			} else {
				assert.Equal(t, tt.resource, tt.want)
			}
		})
	}
}

func newSecret(owner string, data map[string]interface{}) *types.RawResource {
	secret := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Secret",
		"data":       data,
	}}
	if owner == "helm" {
		secret.SetLabels(map[string]string{"owner": owner})
	}
	if owner == "TILLER" {
		secret.SetLabels(map[string]string{"OWNER": owner})
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
	if owner == "helm" {
		cfgMap.SetLabels(map[string]string{"owner": owner})
	}
	if owner == "TILLER" {
		cfgMap.SetLabels(map[string]string{"OWNER": owner})
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

func newV3Release() string {
	b, err := json.Marshal(r)
	if err != nil {
		logrus.Errorf("Failed to marshal release: %v", err)
	}
	buf := bytes.Buffer{}
	gz := gzip.NewWriter(&buf)
	gz.Write(b)
	gz.Close()
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func newV3ReleaseWithoutGzip() string {
	b, err := json.Marshal(r)
	if err != nil {
		logrus.Errorf("Failed to marshal release: %v", err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func newRequest(value string) *types.APIRequest {
	req := &types.APIRequest{Query: url.Values{}}
	if value != "" {
		req.Query.Add("includeHelmData", value)
	}
	return req
}
