package accesscontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"sort"
	"time"

	v1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/rbac/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/util/cache"
	"k8s.io/apiserver/pkg/authentication/user"
)

//go:generate mockgen --build_flags=--mod=mod -package fake -destination fake/AccessSetLookup.go "github.com/rancher/steve/pkg/accesscontrol" AccessSetLookup

type AccessSetLookup interface {
	AccessFor(user user.Info) *AccessSet
	PurgeUserData(id string)
}

type policyRules interface {
	get(string) *AccessSet
	getRoleBindings(string) []*rbacv1.RoleBinding
	getClusterRoleBindings(string) []*rbacv1.ClusterRoleBinding
}

type roleRevisions interface {
	roleRevision(string, string) string
}

// accessStoreCache is a subset of the methods implemented by LRUExpireCache
type accessStoreCache interface {
	Add(key interface{}, value interface{}, ttl time.Duration)
	Get(key interface{}) (interface{}, bool)
	Remove(key interface{})
}

type AccessStore struct {
	usersPolicyRules  policyRules
	groupsPolicyRules policyRules
	roles             roleRevisions
	cache             accessStoreCache
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
		as.cache = cache.NewLRUExpireCache(50)
	}
	return as
}

func (l *AccessStore) AccessFor(user user.Info) *AccessSet {
	if l.cache == nil {
		return l.newAccessSet(user)
	}

	cacheKey := l.CacheKey(user)

	if val, ok := l.cache.Get(cacheKey); ok {
		as, _ := val.(*AccessSet)
		return as
	}

	result := l.newAccessSet(user)
	result.ID = cacheKey
	l.cache.Add(cacheKey, result, 24*time.Hour)

	return result
}

func (l *AccessStore) newAccessSet(user user.Info) *AccessSet {
	result := l.usersPolicyRules.get(user.GetName())
	for _, group := range user.GetGroups() {
		result.Merge(l.groupsPolicyRules.get(group))
	}
	return result
}

func (l *AccessStore) PurgeUserData(id string) {
	l.cache.Remove(id)
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
