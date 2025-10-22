package db

import (
	"bytes"
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var random = rand.New(rand.NewSource(0))

func TestEquality(t *testing.T) {
	testObject := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "default",
			Name:        "test",
			Annotations: map[string]string{"annotation": "test"},
			Labels:      map[string]string{"label": "test"},
		},
		Spec: corev1.PodSpec{
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
	}
	tests := []struct {
		name     string
		encoding encoding
	}{
		{name: "gob", encoding: &gobEncoding{}},
		{name: "json", encoding: &jsonEncoding{}},
		{name: "gob+gz", encoding: gzipped(&gobEncoding{})},
		{name: "json+gz", encoding: gzipped(&jsonEncoding{})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := tt.encoding.Encode(&buf, testObject); err != nil {
				t.Fatal(err)
			}
			var dest *corev1.Pod
			if err := tt.encoding.Decode(bytes.NewReader(buf.Bytes()), &dest); err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, testObject, dest)
		})
	}
}

func BenchmarkEncodings(b *testing.B) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test",
		},
		// a single entry map with 10K zero characters should account for ~10KB ConfigMap size,
		Data: generateTestData(10000),
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "default",
			Name:        "test",
			Annotations: map[string]string{"annotation": "test"},
			Labels:      map[string]string{"label": "test"},
		},
		Spec: corev1.PodSpec{
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
	}

	tests := []struct {
		name     string
		encoding Encoding
	}{
		{name: "gob", encoding: GobEncoding},
		{name: "json", encoding: JSONEncoding},
		{name: "gob+gzip", encoding: GzippedGobEncoding},
		{name: "json+gzip", encoding: GzippedJSONEncoding},
	}
	for _, tt := range tests {
		enc := encodingForType(tt.encoding)
		for objType, testObject := range map[string]any{
			"10KB-configmap": cm,
			"pod":            pod,
		} {
			b.Run(tt.name+"-"+objType, func(b *testing.B) {
				b.Run("encoding", func(b *testing.B) {
					for b.Loop() {
						w := new(discardWriter)
						if err := enc.Encode(w, testObject); err != nil {
							b.Error(err)
						}
						b.ReportMetric(float64(w.count), "bytes")
					}
				})

				var buf bytes.Buffer
				if err := enc.Encode(&buf, testObject); err != nil {
					b.Fatal(err)
				}
				serialized := buf.Bytes()
				b.Run("decoding", func(b *testing.B) {
					var dest corev1.ConfigMap
					for b.Loop() {
						if err := enc.Decode(bytes.NewReader(serialized), &dest); err != nil {
							b.Fatal(err)
						}
					}
				})
			})
		}
	}
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
