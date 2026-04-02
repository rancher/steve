package tests

import (
	"context"
	"path/filepath"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
)

// TestExtRBAC verifies that the RBAC resources for ext tests can be created successfully.
// The actual authorization logic is tested in pkg/ext unit tests.
func (i *IntegrationSuite) TestExtRBAC() {
	ctx := i.T().Context()
	testFile := filepath.Join("testdata", "ext", "rbac.yaml")

	// Apply RBAC resources
	gvrs := make(map[k8sschema.GroupVersionResource]struct{})
	i.doManifest(ctx, testFile, func(ctx context.Context, obj *unstructured.Unstructured, gvr k8sschema.GroupVersionResource) error {
		gvrs[gvr] = struct{}{}
		return i.doApply(ctx, obj, gvr)
	})
	defer i.doManifestReversed(ctx, testFile, i.doDelete)

	// Wait for RBAC to sync
	time.Sleep(2 * time.Second)

	i.Run("RBAC resources created", func() {
		// Verify ClusterRoles were created
		roles := []string{"read-only", "read-write", "update-not-create", "all", "other", "openapi-v2-only-read", "openapi-read"}
		for _, name := range roles {
			_, err := i.k8sClient.RbacV1().ClusterRoles().Get(ctx, name, metav1.GetOptions{})
			i.Require().NoError(err, "ClusterRole %s should exist", name)
		}

		// Verify ClusterRoleBindings were created
		bindings := []string{"read-only", "read-write", "update-not-create", "all", "other", "openapi-v2", "openapi-v3"}
		for _, name := range bindings {
			_, err := i.k8sClient.RbacV1().ClusterRoleBindings().Get(ctx, name, metav1.GetOptions{})
			i.Require().NoError(err, "ClusterRoleBinding %s should exist", name)
		}
	})
}
