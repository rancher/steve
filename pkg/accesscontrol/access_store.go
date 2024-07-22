package accesscontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"sort"
	"time"

	"github.com/rancher/steve/pkg/internal/cache"

	v1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/rbac/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apiserver/pkg/authentication/user"
)

//go:generate mockgen --build_flags=--mod=mod -package fake -destination fake/AccessSetLookup.go "github.com/rancher/steve/pkg/accesscontrol" AccessSetLookup

const accessSetsCacheTTL = 24 * time.Hour

type AccessSetLookup interface {
	AccessFor(user user.Info) AccessSet
	PurgeUserData(id string)
}

type policyRules interface {
	get(string) *accessSet
	getRoleBindings(string) []*rbacv1.RoleBinding
	getClusterRoleBindings(string) []*rbacv1.ClusterRoleBinding
}

type roleRevisions interface {
	roleRevision(string, string) string
}

type AccessSetID string

type AccessStore struct {
	usersPolicyRules  policyRules
	groupsPolicyRules policyRules
	roles             roleRevisions
	cache             cache.Cache[AccessSetID, AccessSet]
}

type roleKey struct {
	namespace string
	name      string
}

func NewAccessStore(ctx context.Context, cacheResults bool, rbac v1.Interface) *AccessStore {
	as := &AccessStore{
		usersPolicyRules:  newPolicyRuleIndex(true, rbac),
		groupsPolicyRules: newPolicyRuleIndex(false, rbac),
		roles:             newRoleRevision(ctx, rbac),
	}
	if cacheResults {
		as.cache = cache.NewCache[AccessSetID, AccessSet](50, accessSetsCacheTTL)
	}
	return as
}

func (l *AccessStore) AccessFor(user user.Info) AccessSet {
	var cacheKey AccessSetID
	if l.cache != nil {
		cacheKey = AccessSetID(l.CacheKey(user))
		if as, ok := l.cache.Get(cacheKey); ok {
			return as
		}
	}

	result := l.usersPolicyRules.get(user.GetName())
	for _, group := range user.GetGroups() {
		result.Merge(l.groupsPolicyRules.get(group))
	}

	if l.cache != nil {
		result.id = string(cacheKey)
		l.cache.Set(cacheKey, result)
	}

	return result
}

func (l *AccessStore) PurgeUserData(id string) {
	l.cache.Delete(AccessSetID(id))
}

func (l *AccessStore) CacheKey(user user.Info) string {
	d := sha256.New()

	groupBase := user.GetGroups()
	groups := make([]string, len(groupBase))
	copy(groups, groupBase)
	sort.Strings(groups)

	l.addRolesToHash(d, user.GetName(), l.usersPolicyRules)
	for _, group := range groups {
		l.addRolesToHash(d, group, l.groupsPolicyRules)
	}

	return hex.EncodeToString(d.Sum(nil))
}

func (l *AccessStore) addRolesToHash(digest hash.Hash, subjectName string, rules policyRules) {
	for _, crb := range rules.getClusterRoleBindings(subjectName) {
		digest.Write([]byte(crb.RoleRef.Name))
		digest.Write([]byte(l.roles.roleRevision("", crb.RoleRef.Name)))
	}

	for _, rb := range rules.getRoleBindings(subjectName) {
		digest.Write([]byte(rb.RoleRef.Name))
		if rb.Namespace != "" {
			digest.Write([]byte(rb.Namespace))
		}
		digest.Write([]byte(l.roles.roleRevision(rb.Namespace, rb.RoleRef.Name)))
	}
}
