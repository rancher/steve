package db

import (
	"bytes"
	"io"
	"math/rand"
	"testing"

	"github.com/rancher/wrangler/v3/pkg/data"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var random = rand.New(rand.NewSource(0))

var allEncodings = []struct {
	name     string
	encoding Encoding
}{
	{name: "gob", encoding: GobEncoding},
	{name: "json", encoding: JSONEncoding},
	{name: "gob+gzip", encoding: GzippedGobEncoding},
	{name: "json+gzip", encoding: GzippedJSONEncoding},
}

func TestNonNilEmptySlice(t *testing.T) {
	t.Parallel()

	testData := map[string]any{
		"kind": "TestObject",
		"data": map[string]any{
			"foo":       "bar",
			"empty":     []any{}, // but not nil
			"not-empty": []any{"one", "two", "three"},
		},
		"status": map[string]any{
			"conditions": []any{
				map[string]any{
					"type":       "Ready",
					"also-empty": []any{},
				},
			},
		},
	}
	testObject := &unstructured.Unstructured{Object: testData}
	for _, tt := range allEncodings {
		enc := encodingForType(tt.encoding)
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := enc.Encode(&buf, testObject); err != nil {
				t.Fatal(err)
			}
			var dest unstructured.Unstructured
			if err := enc.Decode(bytes.NewReader(buf.Bytes()), &dest); err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, testObject.Object, dest.Object)
			assert.NotNil(t, data.GetValueN(dest.Object, "data", "empty"))
		})
	}
}

func TestEquality(t *testing.T) {
	cm := prepareTestObject(t, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test",
		},
		// a single entry map with 10K zero characters should account for ~10KB ConfigMap size,
		Data: generateTestData(10000),
	}, corev1.SchemeGroupVersion.WithKind("ConfigMap"))
	pod := prepareTestObject(t, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "default",
			Name:        "test",
			Finalizers:  []string{"cleanup"},
			Annotations: map[string]string{"annotation": "test"},
			Labels:      map[string]string{"label": "test"},
		},
		Spec: corev1.PodSpec{
			Resources: &corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:    resource.MustParse("200m"),
					corev1.ResourceMemory: resource.MustParse("2G"),
				},
			},
			Containers: []corev1.Container{{
				Name:  "test",
				Image: "testimage",
				VolumeMounts: []corev1.VolumeMount{{
					Name:      "test",
					MountPath: "/test",
				}},
			}},
			Volumes: []corev1.Volume{{
				Name: "test",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			}},
		},
	}, corev1.SchemeGroupVersion.WithKind("Pod"))
	for _, tt := range allEncodings {
		enc := encodingForType(tt.encoding)
		t.Run(tt.name, func(t *testing.T) {
			for _, testObject := range []*unstructured.Unstructured{cm, pod} {
				var buf bytes.Buffer
				if err := enc.Encode(&buf, testObject); err != nil {
					t.Fatal(err)
				}

				var dest unstructured.Unstructured
				if err := enc.Decode(&buf, &dest); err != nil {
					t.Fatal(err)
				}
				assert.Equal(t, testObject, &dest)
			}
		})
	}
}

func BenchmarkEncodings(b *testing.B) {
	cm := prepareTestObject(b, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test",
		},
		// a single entry map with 10K zero characters should account for ~10KB ConfigMap size,
		Data: generateTestData(10000),
	}, corev1.SchemeGroupVersion.WithKind("ConfigMap"))
	pod := prepareTestObject(b, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "default",
			Name:        "test",
			Annotations: map[string]string{"annotation": "test"},
			Labels:      map[string]string{"label": "test"},
		},
		Spec: corev1.PodSpec{
			Resources: &corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:    resource.MustParse("200m"),
					corev1.ResourceMemory: resource.MustParse("2G"),
				},
			},
			Containers: []corev1.Container{
				{
					Name:  "test",
					Image: "testimage",
					VolumeMounts: []corev1.VolumeMount{{
						Name:      "test",
						MountPath: "/test",
					}},
				},
			},
			Volumes: []corev1.Volume{{
				Name: "test",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			}},
		},
	}, corev1.SchemeGroupVersion.WithKind("Pod"))

	for _, tt := range allEncodings {
		enc := encodingForType(tt.encoding)
		for _, tc := range []struct {
			objType    string
			testObject *unstructured.Unstructured
		}{
			{"10KB-configmap", cm},
			{"pod", pod},
		} {
			b.Run(tt.name+"-"+tc.objType, func(b *testing.B) {
				b.Run("encoding", func(b *testing.B) {
					for b.Loop() {
						w := new(discardWriter)
						if err := enc.Encode(w, tc.testObject); err != nil {
							b.Error(err)
						}
						b.ReportMetric(float64(w.count), "bytes")
					}
				})

				var buf bytes.Buffer
				if err := enc.Encode(&buf, tc.testObject); err != nil {
					b.Fatal(err)
				}
				serialized := buf.Bytes()
				b.Run("decoding", func(b *testing.B) {
					for b.Loop() {
						var dest unstructured.Unstructured
						if err := enc.Decode(bytes.NewReader(serialized), &dest); err != nil {
							b.Fatal(err)
						}
					}
				})
			})
		}
	}
}

func prepareTestObject(t testing.TB, obj runtime.Object, gvk schema.GroupVersionKind) *unstructured.Unstructured {
	t.Helper()
	data, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Fatal(err)
	}
	uns := &unstructured.Unstructured{Object: data}
	uns.SetGroupVersionKind(gvk)
	return uns
}

type discardWriter struct {
	count int
}

func (d *discardWriter) Write(p []byte) (int, error) {
	n, err := io.Discard.Write(p)
	d.count += n
	return n, err
}

// single-entry map, whose value is a randomly-generated string of n size
func generateTestData(n int) map[string]string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[random.Int63()%int64(len(letters))]
	}
	return map[string]string{
		"data": string(b),
	}
}
