package accesscontrol

import (
	"slices"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/singleflight"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/util/cache"
	"k8s.io/apiserver/pkg/authentication/user"
)

type policyRulesMock struct {
	roleRefs map[string]subjectGrants
}

func (p policyRulesMock) getRoleRefs(s string) subjectGrants {
	return p.roleRefs[s]
}

func TestAccessStore_userGrantsFor(t *testing.T) {
	testUser := &user.DefaultInfo{
		Name: "user-12345",
		Groups: []string{
			"users",
			"mygroup",
		},
	}

	tests := []struct {
		name   string
		store  *AccessStore
		verify func(t *testing.T, store *AccessStore, res string)
	}{
		{
			name: "consistently produces the same value",
			store: &AccessStore{
				usersPolicyRules: &policyRulesMock{
					roleRefs: map[string]subjectGrants{
						testUser.Name: {
							roleBindings: []roleRef{
								{namespace: "testns", roleName: "testrole", resourceVersion: "testrolev1"},
							},
							clusterRoleBindings: []roleRef{
								{roleName: "testclusterrole", resourceVersion: "testclusterrolev1"},
							},
						},
					},
				},
				groupsPolicyRules: &policyRulesMock{},
			},
			verify: func(t *testing.T, store *AccessStore, res string) {
				// iterate enough times to make possibly random iterators repeating order by coincidence
				for range 5 {
					if res != store.userGrantsFor(testUser).hash() {
						t.Fatal("hash is not the same on consecutive runs")
					}
				}
			},
		},
		{
			name: "group permissions are taken into account",
			store: &AccessStore{
				usersPolicyRules: &policyRulesMock{},
				groupsPolicyRules: &policyRulesMock{
					roleRefs: map[string]subjectGrants{
						testUser.Groups[0]: {
							roleBindings: []roleRef{
								{namespace: "testns", roleName: "testrole", resourceVersion: "testrolegroup0"},
							},
						},
						testUser.Groups[1]: {
							clusterRoleBindings: []roleRef{
								{roleName: "testclusterrole", resourceVersion: "testclusterrolegroup1"},
							},
						},
					},
				},
			},
			verify: func(t *testing.T, store *AccessStore, res string) {
				// remove groups
				testUserAlt := *testUser
				testUserAlt.Groups = []string{}

				if store.userGrantsFor(&testUserAlt).hash() == res {
					t.Fatal("CacheKey does not use groups for hashing")
				}
			},
		},
		{
			name: "different groups order produces the same value",
			store: &AccessStore{
				usersPolicyRules: &policyRulesMock{},
				groupsPolicyRules: &policyRulesMock{
					roleRefs: map[string]subjectGrants{
						testUser.Groups[0]: {
							roleBindings: []roleRef{
								{namespace: "testns", roleName: "testrole", resourceVersion: "testrolegroup0"},
							},
						},
						testUser.Groups[0]: {
							clusterRoleBindings: []roleRef{
								{roleName: "testclusterrole", resourceVersion: "testclusterrolegroup1"},
							},
						},
					},
				},
			},
			verify: func(t *testing.T, store *AccessStore, res string) {
				// swap order of groups
				testUserAlt := &user.DefaultInfo{Name: testUser.Name, Groups: slices.Clone(testUser.Groups)}
				slices.Reverse(testUserAlt.Groups)

				if store.userGrantsFor(testUserAlt).hash() != res {
					t.Fatal("CacheKey varies depending on groups order")
				}
			},
		},
		{
			name: "role changes produce a different value",
			store: &AccessStore{
				usersPolicyRules: &policyRulesMock{
					roleRefs: map[string]subjectGrants{
						testUser.Name: {
							roleBindings: []roleRef{
								{namespace: "testns", roleName: "testrole", resourceVersion: "testrolev1"},
							},
						},
					},
				},
				groupsPolicyRules: &policyRulesMock{},
			},
			verify: func(t *testing.T, store *AccessStore, res string) {
				// new mock returns different resource version
				store.usersPolicyRules = &policyRulesMock{
					roleRefs: map[string]subjectGrants{
						testUser.Name: {
							roleBindings: []roleRef{
								{namespace: "testns", roleName: "testrole", resourceVersion: "testrolev2"},
							},
						},
					},
				}
				if store.userGrantsFor(testUser).hash() == res {
					t.Fatal("CacheKey did not change when on role change")
				}
			},
		},
		{
			name: "new groups produce a different value",
			store: &AccessStore{
				usersPolicyRules: &policyRulesMock{},
				groupsPolicyRules: &policyRulesMock{
					roleRefs: map[string]subjectGrants{
						testUser.Groups[0]: {
							roleBindings: []roleRef{
								{namespace: "testns", roleName: "testrole", resourceVersion: "testrolegroup0"},
							},
						},
						"newgroup": {
							clusterRoleBindings: []roleRef{
								{roleName: "testclusterrole", resourceVersion: "testclusterrolegroup1"},
							},
						},
					},
				},
			},
			verify: func(t *testing.T, store *AccessStore, res string) {
				testUserAlt := &user.DefaultInfo{
					Name:   testUser.Name,
					Groups: append(slices.Clone(testUser.Groups), "newgroup"),
				}
				if store.userGrantsFor(testUserAlt).hash() == res {
					t.Fatal("CacheKey did not change when new group was added")
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.verify(t, tt.store, tt.store.userGrantsFor(testUser).hash())
		})
	}
}

func TestAccessStore_AccessFor(t *testing.T) {
	testUser := &user.DefaultInfo{
		Name:   "test-user",
		Groups: []string{"users", "mygroup"},
	}
	asCache := cache.NewLRUExpireCache(10)
	store := &AccessStore{
		concurrentAccessFor: new(singleflight.Group),
		usersPolicyRules: &policyRulesMock{
			roleRefs: map[string]subjectGrants{
				testUser.Name: {
					clusterRoleBindings: []roleRef{
						{
							roleName: "testclusterrole", resourceVersion: "testclusterrolev1",
							rules: []rbacv1.PolicyRule{{
								Verbs:         []string{"get"},
								APIGroups:     []string{corev1.GroupName},
								Resources:     []string{"ConfigMap"},
								ResourceNames: []string{All},
							}},
						},
					},
				},
			},
		},
		groupsPolicyRules: &policyRulesMock{
			roleRefs: map[string]subjectGrants{
				testUser.Groups[0]: {
					roleBindings: []roleRef{
						{
							namespace: "testns", roleName: "testrole", resourceVersion: "testrolev1",
							rules: []rbacv1.PolicyRule{{
								Verbs:         []string{"list"},
								APIGroups:     []string{appsv1.GroupName},
								Resources:     []string{"Deployment"},
								ResourceNames: []string{All},
							}},
						},
					},
				},
			},
		},
		cache: asCache,
	}

	validateAccessSet := func(as *AccessSet) {
		if as == nil {
			t.Fatal("AccessFor() returned nil")
		}
		if as.ID == "" {
			t.Fatal("AccessSet has empty ID")
		}
		if !as.Grants("get", corev1.Resource("ConfigMap"), "anyns", "cm") ||
			!as.Grants("list", appsv1.Resource("Deployment"), "testns", "deploy") {
			t.Error("AccessSet does not grant desired permissions")
		}
		// wrong ns
		if as.Grants("list", appsv1.Resource("Deployment"), "default", "deploy") ||
			// wrong verbs
			as.Grants("delete", corev1.Resource("ConfigMap"), "default", "cm") ||
			as.Grants("get", appsv1.Resource("Deployment"), "testns", "deploy") ||
			// wrong resource
			as.Grants("get", corev1.Resource("Secret"), "testns", "s") {
			t.Error("AccessSet grants undesired permissions")
		}
	}

	as := store.AccessFor(testUser)
	validateAccessSet(as)
	if got, want := len(asCache.Keys()), 1; got != want {
		t.Errorf("Unexpected number of cache keys: got %d, want %d", got, want)
	}

	as = store.AccessFor(testUser)
	validateAccessSet(as)
	if got, want := len(asCache.Keys()), 1; got != want {
		t.Errorf("Unexpected increase in cache size, got %d, want %d", got, want)
	}

	store.PurgeUserData(as.ID)
	if got, want := len(asCache.Keys()), 0; got != want {
		t.Errorf("Cache was not purged, got len %d, want %d", got, want)
	}
}

type spyCache struct {
	accessStoreCache

	mu       sync.Mutex
	setCalls map[any]int
}

func (c *spyCache) Add(k interface{}, v interface{}, ttl time.Duration) {
	defer c.observeAdd(k)

	time.Sleep(1 * time.Millisecond) // allow other routines to wake up, simulating heavy load
	c.accessStoreCache.Add(k, v, ttl)
}

func (c *spyCache) observeAdd(k interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.setCalls == nil {
		c.setCalls = make(map[any]int)
	}
	c.setCalls[k]++
}

func TestAccessStore_AccessFor_concurrent(t *testing.T) {
	testUser := &user.DefaultInfo{Name: "test-user"}
	asCache := &spyCache{accessStoreCache: cache.NewLRUExpireCache(100)}
	store := &AccessStore{
		concurrentAccessFor: new(singleflight.Group),
		usersPolicyRules: &policyRulesMock{
			roleRefs: map[string]subjectGrants{
				testUser.Name: {
					roleBindings: []roleRef{
						{namespace: "testns", roleName: "testrole", resourceVersion: "testrolev1"},
					},
				},
			},
		},
		cache: asCache,
	}

	const n = 5 // observation showed cases with up to 5 (or more) concurrent queries for the same user

	wait := make(chan struct{})
	var wg sync.WaitGroup
	var id string
	for range n {
		wg.Add(1)
		go func() {
			<-wait
			id = store.AccessFor(testUser).ID
			wg.Done()
		}()
	}
	close(wait)
	wg.Wait()

	if got, want := len(asCache.setCalls), 1; got != want {
		t.Errorf("Unexpected number of cache entries: got %d, want %d", got, want)
	}
	if got, want := asCache.setCalls[id], 1; got != want {
		t.Errorf("Unexpected number of calls to cache.Set(): got %d, want %d", got, want)
	}
}
