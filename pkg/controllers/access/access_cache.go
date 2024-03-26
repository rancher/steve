package access

import (
	"context"
	"fmt"
	"github.com/rancher/wrangler/v2/pkg/generic"

	"github.com/rancher/steve/pkg/accesscontrol"
	v12 "github.com/rancher/wrangler/v2/pkg/generated/controllers/rbac/v1"
	v1 "k8s.io/api/rbac/v1"
)

const (
	// kinds
	kindUser        = "User"
	kindRole        = "Role"
	kindClusterRole = "ClusterRole"
	rbacAPIGroup    = "rbac.authorization.k8s.io/v1"

	// handler names
	purgeUserCacheForRBSubjects          = "purge-user-cache-for-rb-subjects"
	purgeUserCacheForCRBSubjects         = "purge-user-cache-for-crb-subjects"
	purgeUserCacheForRoleSubjects        = "purge-user-cache-for-role-subjects"
	purgeUserCacheForClusterRoleSubjects = "purge-user-cache-for-clusterrole-subjects"

	// indexer names
	rbToRoleIndexName         = "rb-to-role"
	rbToClusterRoleIndexName  = "rb-to-cluster-role"
	crbToClusterRoleIndexName = "crb-to-cluster-role"
)

// validate handlers' interface conformity
var _ generic.ObjectHandler[*v1.RoleBinding] = ((*handler)(nil)).onRoleBindingChange
var _ generic.ObjectHandler[*v1.ClusterRoleBinding] = ((*handler)(nil)).onClusterRoleBindingChange
var _ generic.ObjectHandler[*v1.Role] = ((*handler)(nil)).onRoleChange
var _ generic.ObjectHandler[*v1.ClusterRole] = ((*handler)(nil)).onClusterRoleChange

type handler struct {
	crbCache v12.ClusterRoleBindingCache
	rbCache  v12.RoleBindingCache
	asl      accesscontrol.AccessSetLookup
}

func Register(ctx context.Context,
	rbs v12.RoleBindingController,
	crbs v12.ClusterRoleBindingController,
	roles v12.RoleController,
	cRoles v12.ClusterRoleController,
	asl accesscontrol.AccessSetLookup) {

	h := &handler{
		crbCache: crbs.Cache(),
		rbCache:  rbs.Cache(),
		asl:      asl,
	}
	rbs.Cache().AddIndexer(rbToRoleIndexName, rbToRoleIndexer)
	rbs.Cache().AddIndexer(rbToClusterRoleIndexName, rbToClusterRoleIndexer)
	crbs.Cache().AddIndexer(crbToClusterRoleIndexName, crbToClusterRoleIndexer)

	rbs.OnChange(ctx, purgeUserCacheForRBSubjects, h.onRoleBindingChange)
	crbs.OnChange(ctx, purgeUserCacheForCRBSubjects, h.onClusterRoleBindingChange)
	roles.OnChange(ctx, purgeUserCacheForRoleSubjects, h.onRoleChange)
	cRoles.OnChange(ctx, purgeUserCacheForClusterRoleSubjects, h.onClusterRoleChange)
}

func getRoleBindingUsers(obj *v1.RoleBinding) ([]string, error) {
	var usersM map[string]struct{}
	var users []string
	for _, subject := range obj.Subjects {
		if subject.Kind != kindUser {
			continue
		}
		if subject.APIGroup != rbacAPIGroup {
			continue
		}
		usersM[subject.String()] = struct{}{}
	}
	for user := range usersM {
		users = append(users, user)
	}
	return users, nil
}

// keep
func getClusterRoleBindingUsers(obj *v1.ClusterRoleBinding) ([]string, error) {
	var usersM map[string]struct{}
	var users []string
	for _, subject := range obj.Subjects {
		if subject.Kind != kindUser {
			continue
		}
		if subject.APIGroup != rbacAPIGroup {
			continue
		}
		usersM[subject.String()] = struct{}{}
	}
	for user := range usersM {
		users = append(users, user)
	}
	return users, nil
}

// Indexer functions

func rbToRoleIndexer(obj *v1.RoleBinding) ([]string, error) {
	if obj.RoleRef.Kind != kindRole {
		return nil, nil
	}
	if obj.RoleRef.APIGroup != rbacAPIGroup {
		return nil, nil
	}
	return []string{fmt.Sprintf("%s/%s", obj.Namespace, obj.RoleRef.Name)}, nil
}

func rbToClusterRoleIndexer(obj *v1.RoleBinding) ([]string, error) {
	if obj.RoleRef.Kind != kindClusterRole {
		return nil, nil
	}
	if obj.RoleRef.APIGroup != rbacAPIGroup {
		return nil, nil
	}
	return []string{obj.RoleRef.Name}, nil
}

func crbToClusterRoleIndexer(obj *v1.ClusterRoleBinding) ([]string, error) {
	if obj.RoleRef.Kind != kindClusterRole {
		return nil, nil
	}
	return []string{fmt.Sprintf("%s", obj.RoleRef.Name)}, nil
}

func (h *handler) getRoleSubjects(obj *v1.Role) ([]string, error) {
	var usersM map[string]struct{}
	rbs, err := h.rbCache.GetByIndex(rbToRoleIndexName, fmt.Sprintf("%s/%s", obj.Namespace, obj.Name))
	if err != nil {
		return nil, err
	}
	for _, rb := range rbs {
		if rb.Namespace != obj.Namespace {
			continue
		}
		if rb.RoleRef.Kind != obj.Kind {
			continue
		}
		if rb.RoleRef.Name != obj.Name {
			continue
		}
		users, err := getRoleBindingUsers(rb)
		if err != nil {
			return nil, err
		}
		for _, user := range users {
			usersM[user] = struct{}{}
		}
	}

	var users []string
	for user := range usersM {
		users = append(users, user)
	}
	return users, nil
}

func (h *handler) getClusterRoleSubjects(obj *v1.ClusterRole) ([]string, error) {
	var usersM map[string]struct{}
	rbs, err := h.rbCache.GetByIndex(rbToClusterRoleIndexName, fmt.Sprintf("%s", obj.Name))
	if err != nil {
		return nil, err
	}
	for _, rb := range rbs {
		if rb.RoleRef.Kind != obj.Kind {
			continue
		}
		if rb.RoleRef.Name != obj.Name {
			continue
		}
		users, err := getRoleBindingUsers(rb)
		if err != nil {
			return nil, err
		}
		for _, user := range users {
			usersM[user] = struct{}{}
		}
	}

	crbs, err := h.crbCache.GetByIndex(crbToClusterRoleIndexName, fmt.Sprintf("%s", obj.Name))
	if err != nil {
		return nil, err
	}
	for _, crb := range crbs {
		if crb.RoleRef.Kind != obj.Kind {
			continue
		}
		if crb.RoleRef.Name != obj.Name {
			continue
		}
		users, err := getClusterRoleBindingUsers(crb)
		if err != nil {
			return nil, err
		}
		for _, user := range users {
			usersM[user] = struct{}{}
		}
	}

	var users []string
	for user := range usersM {
		users = append(users, user)
	}
	return users, nil
}

func (h *handler) purgeUserDataForUsers(users []string) {
	for _, user := range users {
		h.asl.PurgeUserData(user)
	}
}

// onRoleBindingChange purges user data from access cache for all subjects of the RoleBinding
func (h *handler) onRoleBindingChange(_ string, binding *v1.RoleBinding) (*v1.RoleBinding, error) {
	users, err := getRoleBindingUsers(binding)
	if err != nil {
		return nil, err
	}
	h.purgeUserDataForUsers(users)
	return nil, nil
}

// onClusterRoleBindingChange purges user data from access cache for all subjects of the ClusterRoleBinding
func (h *handler) onClusterRoleBindingChange(_ string, binding *v1.ClusterRoleBinding) (*v1.ClusterRoleBinding, error) {
	users, err := getClusterRoleBindingUsers(binding)
	if err != nil {
		return nil, err
	}
	h.purgeUserDataForUsers(users)
	return nil, nil
}

// onRoleChange purges userdata from access cache for all subject of all RoleBindings referencing the given Role
func (h *handler) onRoleChange(_ string, role *v1.Role) (*v1.Role, error) {
	users, err := h.getRoleSubjects(role)
	if err != nil {
		return nil, err
	}
	h.purgeUserDataForUsers(users)
	return nil, nil
}

// onClusterRoleChange purges userdata from access cache for all subject of all RoleBindings and ClusterRoleBindings
// referencing the given ClusterRole
func (h *handler) onClusterRoleChange(_ string, role *v1.ClusterRole) (*v1.ClusterRole, error) {
	users, err := h.getClusterRoleSubjects(role)
	if err != nil {
		return nil, err
	}
	h.purgeUserDataForUsers(users)
	return nil, nil
}
