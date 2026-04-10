package podimpersonation

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"

	"k8s.io/client-go/kubernetes/fake"
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
			name:           "Default UserID should be 1000",
			podOptions:     &PodOptions{},
			expectUserId:   1000,
			expectChownCmd: true,
		},
		{
			name: "Custom UserID should be used",
			podOptions: &PodOptions{
				UserID: &customUserId,
			},
			expectUserId:   2000,
			expectChownCmd: true,
		},
		{
			name: "Nil UserID should default to 1000",
			podOptions: &PodOptions{
				UserID: nil,
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

// newFakeClientWithGenerateName returns a fake client that honours GenerateName
// by appending a counter suffix, since the upstream fake does not implement this.
func newFakeClientWithGenerateName(objects ...runtime.Object) *fake.Clientset {
	client := fake.NewSimpleClientset(objects...)
	counter := 0
	client.PrependReactor("create", "*", func(action k8stesting.Action) (bool, runtime.Object, error) {
		createAction := action.(k8stesting.CreateAction)
		obj := createAction.GetObject()
		acc, ok := obj.(metav1.Object)
		if ok && acc.GetName() == "" && acc.GetGenerateName() != "" {
			counter++
			acc.SetName(fmt.Sprintf("%s%d", acc.GetGenerateName(), counter))
		}
		return false, nil, nil // let the default reactor handle the actual create
	})
	return client
}

func TestCreateExtraRoleBindings(t *testing.T) {
	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-role",
			UID:  "test-role-uid",
		},
	}
	sa := &v1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-sa",
			Namespace: "test-ns",
		},
	}

	testCases := []struct {
		name              string
		extraClusterRoles []string
		expectCRBCount    int
		expectErr         bool
	}{
		{
			name:              "No extra cluster roles creates no bindings",
			extraClusterRoles: []string{},
			expectCRBCount:    0,
		},
		{
			name:              "Single extra cluster role creates one binding",
			extraClusterRoles: []string{"cluster-admin"},
			expectCRBCount:    1,
		},
		{
			name:              "Multiple extra cluster roles creates one binding each",
			extraClusterRoles: []string{"cluster-admin", "view", "edit"},
			expectCRBCount:    3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := newFakeClientWithGenerateName()
			impersonator := New("", nil, time.Minute, func() string { return "rancher/shell:v0.1.22" })

			err := impersonator.createExtraRoleBindings(context.Background(), role, sa, tc.extraClusterRoles, client)
			require.NoError(t, err)

			crbList, err := client.RbacV1().ClusterRoleBindings().List(context.Background(), metav1.ListOptions{})
			require.NoError(t, err)
			assert.Len(t, crbList.Items, tc.expectCRBCount)

			for i, crb := range crbList.Items {
				// Each CRB must have the label linking it to the role
				assert.Equal(t, role.Name, crb.Labels[extraCRBLabel], "expected extraCRBLabel to point to the role")

				// Each CRB must reference the expected extra cluster role
				assert.Equal(t, tc.extraClusterRoles[i], crb.RoleRef.Name, "expected RoleRef to point to extra cluster role")
				assert.Equal(t, "ClusterRole", crb.RoleRef.Kind)
				assert.Equal(t, rbacv1.GroupName, crb.RoleRef.APIGroup)

				// Each CRB must bind the service account
				require.Len(t, crb.Subjects, 1)
				assert.Equal(t, "ServiceAccount", crb.Subjects[0].Kind)
				assert.Equal(t, sa.Name, crb.Subjects[0].Name)
				assert.Equal(t, sa.Namespace, crb.Subjects[0].Namespace)
			}
		})
	}
}

func TestDeleteRoleAndExtraCRBs(t *testing.T) {
	roleName := "test-role"

	testCases := []struct {
		name          string
		existingCRBs  []rbacv1.ClusterRoleBinding
		expectRemoved int // number of extra CRBs expected to be deleted
	}{
		{
			name:          "No extra CRBs deletes only the role",
			existingCRBs:  []rbacv1.ClusterRoleBinding{},
			expectRemoved: 0,
		},
		{
			name: "Deletes CRBs labeled for the role",
			existingCRBs: []rbacv1.ClusterRoleBinding{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "crb-1",
						Labels: map[string]string{extraCRBLabel: roleName},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "crb-2",
						Labels: map[string]string{extraCRBLabel: roleName},
					},
				},
			},
			expectRemoved: 2,
		},
		{
			name: "Does not delete CRBs labeled for a different role",
			existingCRBs: []rbacv1.ClusterRoleBinding{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "crb-other",
						Labels: map[string]string{extraCRBLabel: "other-role"},
					},
				},
			},
			expectRemoved: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			role := &rbacv1.ClusterRole{
				ObjectMeta: metav1.ObjectMeta{Name: roleName},
			}
			objects := []runtime.Object{role}
			for i := range tc.existingCRBs {
				objects = append(objects, &tc.existingCRBs[i])
			}
			client := fake.NewSimpleClientset(objects...)

			impersonator := New("", nil, time.Minute, func() string { return "rancher/shell:v0.1.22" })
			err := impersonator.deleteRoleAndExtraCRBs(context.Background(), client, roleName)
			require.NoError(t, err)

			// The role itself should be gone
			_, getErr := client.RbacV1().ClusterRoles().Get(context.Background(), roleName, metav1.GetOptions{})
			assert.Error(t, getErr, "expected role to be deleted")

			// Verify remaining CRBs
			remaining, listErr := client.RbacV1().ClusterRoleBindings().List(context.Background(), metav1.ListOptions{})
			require.NoError(t, listErr)
			expectedRemaining := len(tc.existingCRBs) - tc.expectRemoved
			assert.Len(t, remaining.Items, expectedRemaining, "unexpected number of remaining CRBs")
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
