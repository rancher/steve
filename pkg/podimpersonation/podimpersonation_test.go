package podimpersonation

import (
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
	"time"
)

func TestAugmentPod(t *testing.T) {
	var (
		zero = int64(0)
	)
	testCases := []struct {
		name          string
		imageOverride string
		envVars       []v1.EnvVar
	}{
		{
			name:          "Should mount volume to container, create an init container and use regular image",
			imageOverride: "",
			envVars:       []v1.EnvVar{{Name: "KUBECONFIG", Value: ".kube/config"}},
		},
		{
			name:          "Should mount volume to container, create an init container and use overridden image",
			imageOverride: "rancher/notShell:v1.0.0",
			envVars:       []v1.EnvVar{{Name: "KUBECONFIG", Value: ".kube/config"}},
		},
		{
			name:          "Should not create init container if there's no KUBECONFIG envVar",
			imageOverride: "",
			envVars:       []v1.EnvVar{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := newPod(tc.envVars)
			impersonator := New("", nil, time.Minute, func() string { return "rancher/shell:v0.1.22" })
			pod := impersonator.augmentPod(p, nil, &v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s"}}, tc.imageOverride)

			assert.Len(t, pod.Spec.Volumes, len(p.Spec.Volumes)+4, "expected four new volumes")
			if len(tc.envVars) != 0 {
				assert.Len(t, pod.Spec.Containers[0].VolumeMounts, len(p.Spec.Containers[0].VolumeMounts)+1, "expected kubeconfig volume to be mounted")
				assert.Len(t, pod.Spec.InitContainers, len(p.Spec.InitContainers)+1, "expected an init container to be created")
				if tc.imageOverride != "" {
					assert.Equal(t, pod.Spec.InitContainers[len(pod.Spec.InitContainers)-1].Image, tc.imageOverride, "expected image to be the one received as parameter")
				} else {
					assert.Equal(t, pod.Spec.InitContainers[len(pod.Spec.InitContainers)-1].Image, impersonator.imageName(), "expected image to be the impersonator image")
				}
				assert.Equal(t, pod.Spec.InitContainers[len(pod.Spec.InitContainers)-1].SecurityContext.RunAsUser, &zero, "expected init container to run as user zero")
				assert.Equal(t, pod.Spec.InitContainers[len(pod.Spec.InitContainers)-1].SecurityContext.RunAsGroup, &zero, "expected init container to run as group zero")
			} else {
				assert.Len(t, pod.Spec.InitContainers, len(p.Spec.InitContainers), "expected no init container to be created")
			}
			assert.Equal(t, pod.Spec.Containers[len(pod.Spec.Containers)-1].Name, "proxy", "expected the container proxy to be created")
		})
	}
}

func newPod(env []v1.EnvVar) *v1.Pod {
	return &v1.Pod{
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{{
				Name: "volume1",
				VolumeSource: v1.VolumeSource{
					ConfigMap: &v1.ConfigMapVolumeSource{
						LocalObjectReference: v1.LocalObjectReference{
							Name: "cfgMap",
						},
					},
				},
			}},
			Containers: []v1.Container{
				{
					Name:  "shell",
					Image: "rancher/shell:v0.1.22",
					Env:   env,
					VolumeMounts: []v1.VolumeMount{{
						Name:      "volume1",
						MountPath: "/home/vol",
					}},
				},
			},
			ServiceAccountName:           "svc-account-1",
			AutomountServiceAccountToken: nil,
			SecurityContext:              nil,
		},
	}
}
