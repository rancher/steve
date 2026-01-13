package podimpersonation

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
			podOpts := &PodOptions{ImageOverride: tc.imageOverride}
			pod := impersonator.augmentPod(p, nil, &v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s"}}, podOpts)

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

func TestAugmentPodNonRoot(t *testing.T) {
	var (
		fBool     = false
		tBool     = true
		runAsUser = int64(1000)
		fsGroup   = int64(1000)
	)

	testCases := []struct {
		name             string
		securityContext  *v1.PodSecurityContext
		podOptions       *PodOptions
		expectRootInit   bool
		expectRootProxy  bool
		expectChownCmd   bool
		expectUsername   string
		expectKubeconfig string
	}{
		{
			name: "Non-root pod with FSGroup should not force root",
			securityContext: &v1.PodSecurityContext{
				RunAsNonRoot: &tBool,
				RunAsUser:    &runAsUser,
				FSGroup:      &fsGroup,
			},
			podOptions:       &PodOptions{},
			expectRootInit:   false,
			expectRootProxy:  false,
			expectChownCmd:   false,
			expectUsername:   "shell",
			expectKubeconfig: "/home/shell/.kube/config",
		},
		{
			name: "Non-root pod without FSGroup should not use chown",
			securityContext: &v1.PodSecurityContext{
				RunAsNonRoot: &tBool,
				RunAsUser:    &runAsUser,
			},
			podOptions:       &PodOptions{},
			expectRootInit:   false,
			expectRootProxy:  false,
			expectChownCmd:   false,
			expectUsername:   "shell",
			expectKubeconfig: "/home/shell/.kube/config",
		},
		{
			name: "Non-root pod with custom username",
			securityContext: &v1.PodSecurityContext{
				RunAsNonRoot: &tBool,
				RunAsUser:    &runAsUser,
				FSGroup:      &fsGroup,
			},
			podOptions: &PodOptions{
				Username: "kuberlr",
			},
			expectRootInit:   false,
			expectRootProxy:  false,
			expectChownCmd:   false,
			expectUsername:   "kuberlr",
			expectKubeconfig: "/home/kuberlr/.kube/config",
		},
		{
			name:             "Root pod (nil SecurityContext) should force root",
			securityContext:  nil,
			podOptions:       &PodOptions{},
			expectRootInit:   true,
			expectRootProxy:  true,
			expectChownCmd:   true,
			expectUsername:   "root",
			expectKubeconfig: "/root/.kube/config",
		},
		{
			name: "Root pod with FSGroup should not use chown",
			securityContext: &v1.PodSecurityContext{
				RunAsNonRoot: &fBool, // or omit to default to false
				FSGroup:      &fsGroup,
			},
			podOptions:       &PodOptions{},
			expectRootInit:   true,
			expectRootProxy:  true,
			expectChownCmd:   false, // FSGroup handles permissions
			expectUsername:   "root",
			expectKubeconfig: "/root/.kube/config",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			env := []v1.EnvVar{{Name: "KUBECONFIG", Value: ".kube/config"}}
			p := &v1.Pod{
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
						},
					},
					SecurityContext: tc.securityContext,
				},
			}

			impersonator := New("", nil, time.Minute, func() string { return "rancher/shell:v0.1.22" })
			pod := impersonator.augmentPod(p, nil, &v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s"}}, tc.podOptions)

			// Check init container exists
			assert.GreaterOrEqual(t, len(pod.Spec.InitContainers), 1, "expected one init container")
			var initContainer v1.Container
			for _, c := range pod.Spec.InitContainers {
				if c.Name == "init-kubeconfig-volume" {
					initContainer = c
					break
				}
			}
			assert.NotNil(t, initContainer, "expected init container")

			// Check init container SecurityContext
			if tc.expectRootInit {
				assert.NotNil(t, initContainer.SecurityContext, "expected init container SecurityContext to be set")
				assert.NotNil(t, initContainer.SecurityContext.RunAsUser, "expected init container RunAsUser to be set")
				assert.Equal(t, int64(0), *initContainer.SecurityContext.RunAsUser, "expected init container to run as root")
			} else {
				// Non-root should have nil SecurityContext (inherit from pod)
				assert.Nil(t, initContainer.SecurityContext, "expected init container SecurityContext to be nil (inherit from pod)")
			}

			// Check init container command
			if tc.expectChownCmd {
				assert.Contains(t, initContainer.Command[2], "chown", "expected init command to include chown")
			} else {
				assert.NotContains(t, initContainer.Command[2], "chown", "expected init command to not include chown")
			}

			// Check proxy container
			proxyContainer := pod.Spec.Containers[len(pod.Spec.Containers)-1]
			assert.Equal(t, "proxy", proxyContainer.Name, "expected proxy container")

			// Check proxy SecurityContext
			if tc.expectRootProxy {
				assert.NotNil(t, proxyContainer.SecurityContext.RunAsUser, "expected proxy RunAsUser to be set")
				assert.Equal(t, int64(0), *proxyContainer.SecurityContext.RunAsUser, "expected proxy to run as root")
			} else {
				// Non-root should not have RunAsUser set (inherit from pod)
				assert.Nil(t, proxyContainer.SecurityContext.RunAsUser, "expected proxy RunAsUser to be nil (inherit from pod)")
			}

			// Check kubeconfig path
			assert.Equal(t, tc.expectKubeconfig, proxyContainer.Env[0].Value, "expected correct KUBECONFIG path")
			assert.Equal(t, tc.expectKubeconfig, proxyContainer.VolumeMounts[0].MountPath, "expected correct volume mount path")
		})
	}
}

func TestUserKubeConfigPath(t *testing.T) {
	testCases := []struct {
		name     string
		username string
		expected string
	}{
		{
			name:     "Root user",
			username: "root",
			expected: "/root/.kube/config",
		},
		{
			name:     "Empty username defaults to root",
			username: "",
			expected: "/root/.kube/config",
		},
		{
			name:     "Shell user",
			username: "shell",
			expected: "/home/shell/.kube/config",
		},
		{
			name:     "Custom username",
			username: "kuberlr",
			expected: "/home/kuberlr/.kube/config",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := userKubeConfigPath(tc.username)
			assert.Equal(t, tc.expected, result, "expected correct kubeconfig path")
		})
	}
}

func TestAugmentPodUserId(t *testing.T) {
	customUserId := int64(2000)

	testCases := []struct {
		name           string
		podOptions     *PodOptions
		expectUserId   int64
		expectChownCmd bool
	}{
		{
			name:           "Default UserId should be 1000",
			podOptions:     &PodOptions{},
			expectUserId:   1000,
			expectChownCmd: true,
		},
		{
			name: "Custom UserId should be used",
			podOptions: &PodOptions{
				UserId: &customUserId,
			},
			expectUserId:   2000,
			expectChownCmd: true,
		},
		{
			name: "Nil UserId should default to 1000",
			podOptions: &PodOptions{
				UserId: nil,
			},
			expectUserId:   1000,
			expectChownCmd: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			env := []v1.EnvVar{{Name: "KUBECONFIG", Value: ".kube/config"}}
			p := &v1.Pod{
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
						},
					},
					SecurityContext: nil, // Root pod to trigger chown
				},
			}

			impersonator := New("", nil, time.Minute, func() string { return "rancher/shell:v0.1.22" })
			pod := impersonator.augmentPod(p, nil, &v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s"}}, tc.podOptions)

			// Find init container
			assert.GreaterOrEqual(t, len(pod.Spec.InitContainers), 1, "expected at least one init container")
			var initContainer v1.Container
			for _, c := range pod.Spec.InitContainers {
				if c.Name == "init-kubeconfig-volume" {
					initContainer = c
					break
				}
			}
			assert.NotEmpty(t, initContainer.Name, "expected to find init-kubeconfig-volume container")

			// Check init container command
			if tc.expectChownCmd {
				assert.Contains(t, initContainer.Command[2], "chown", "expected init command to include chown")
				// Verify the specific userId is in the chown command
				assert.Contains(t, initContainer.Command[2], fmt.Sprintf("chown %d", tc.expectUserId),
					"expected chown command to use userId %d", tc.expectUserId)
			}
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
