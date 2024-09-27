package accesscontrol

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/cache"
	"k8s.io/apiserver/pkg/authentication/user"
)

func TestAccessStore_CacheKey(t *testing.T) {
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
					getRBFunc: func(s string) []*rbacv1.RoleBinding {
						return []*rbacv1.RoleBinding{
							makeRB("testns", "testrb", testUser.Name, "testrole"),
						}
					},
					getCRBFunc: func(s string) []*rbacv1.ClusterRoleBinding {
						return []*rbacv1.ClusterRoleBinding{
							makeCRB("testcrb", testUser.Name, "testclusterrole"),
						}
					},
				},
				groupsPolicyRules: &policyRulesMock{},
				roles: roleRevisionsMock(func(ns, name string) string {
					return fmt.Sprintf("%s%srev", ns, name)
				}),
			},
			verify: func(t *testing.T, store *AccessStore, res string) {
				// iterate enough times to make possibly random iterators repeating order by coincidence
				for range 5 {
					if res != store.CacheKey(testUser) {
						t.Fatal("CacheKey is not the same on consecutive runs")
					}
				}
			},
		},
		{
			name: "group permissions are taken into account",
			store: &AccessStore{
				usersPolicyRules: &policyRulesMock{},
				groupsPolicyRules: &policyRulesMock{
					getRBFunc: func(s string) []*rbacv1.RoleBinding {
						return []*rbacv1.RoleBinding{
							makeRB("testns", "testrb", testUser.Name, "testrole"),
						}
					},
					getCRBFunc: func(s string) []*rbacv1.ClusterRoleBinding {
						return []*rbacv1.ClusterRoleBinding{
							makeCRB("testcrb", testUser.Name, "testclusterrole"),
						}
					},
				},
				roles: roleRevisionsMock(func(ns, name string) string {
					return fmt.Sprintf("%s%srev", ns, name)
				}),
			},
			verify: func(t *testing.T, store *AccessStore, res string) {
				// remove users
				testUserAlt := *testUser
				testUserAlt.Groups = []string{}

				if store.CacheKey(&testUserAlt) == res {
					t.Fatal("CacheKey does not use groups for hashing")
				}
			},
		},
		{
			name: "different groups order produces the same value",
			store: &AccessStore{
				usersPolicyRules: &policyRulesMock{},
				groupsPolicyRules: &policyRulesMock{
					getRBFunc: func(s string) []*rbacv1.RoleBinding {
						if s == testUser.Groups[0] {
							return []*rbacv1.RoleBinding{
								makeRB("testns", "testrb", testUser.Name, "testrole"),
							}
						}
						return nil
					},
					getCRBFunc: func(s string) []*rbacv1.ClusterRoleBinding {
						if s == testUser.Groups[1] {
							return []*rbacv1.ClusterRoleBinding{
								makeCRB("testcrb", testUser.Name, "testclusterrole"),
							}
						}
						return nil
					},
				},
				roles: roleRevisionsMock(func(ns, name string) string {
					return fmt.Sprintf("%s%srev", ns, name)
				}),
			},
			verify: func(t *testing.T, store *AccessStore, res string) {
				// swap order of groups
				testUserAlt := &user.DefaultInfo{Name: testUser.Name, Groups: slices.Clone(testUser.Groups)}
				slices.Reverse(testUserAlt.Groups)

				if store.CacheKey(testUserAlt) != res {
					t.Fatal("CacheKey varies depending on groups order")
				}
			},
		},
		{
			name: "role changes produce a different value",
			store: &AccessStore{
				usersPolicyRules: &policyRulesMock{
					getRBFunc: func(s string) []*rbacv1.RoleBinding {
						return []*rbacv1.RoleBinding{
							makeRB("testns", "testrb", testUser.Name, "testrole"),
						}
					},
				},
				groupsPolicyRules: &policyRulesMock{},
				roles: roleRevisionsMock(func(ns, name string) string {
					return "rev1"
				}),
			},
			verify: func(t *testing.T, store *AccessStore, res string) {
				store.roles = roleRevisionsMock(func(ns, name string) string {
					return "rev2"

				})
				if store.CacheKey(testUser) == res {
					t.Fatal("CacheKey did not change when on role change")
				}
			},
		},
		{
			name: "new groups produce a different value",
			store: &AccessStore{
				usersPolicyRules: &policyRulesMock{},
				groupsPolicyRules: &policyRulesMock{
					getRBFunc: func(s string) []*rbacv1.RoleBinding {
						return []*rbacv1.RoleBinding{
							makeRB("testns", "testrb", testUser.Name, "testrole"),
						}
					},
					getCRBFunc: func(s string) []*rbacv1.ClusterRoleBinding {
						if s == "newgroup" {
							return []*rbacv1.ClusterRoleBinding{
								makeCRB("testcrb", testUser.Name, "testclusterrole"),
							}
						}
						return nil
					},
				},
				roles: roleRevisionsMock(func(ns, name string) string {
					return fmt.Sprintf("%s%srev", ns, name)
				}),
			},
			verify: func(t *testing.T, store *AccessStore, res string) {
				testUserAlt := &user.DefaultInfo{
					Name:   testUser.Name,
					Groups: append(slices.Clone(testUser.Groups), "newgroup"),
				}
				if store.CacheKey(testUserAlt) == res {
					t.Fatal("CacheKey did not change when new group was added")
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.verify(t, tt.store, tt.store.CacheKey(testUser))
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
		usersPolicyRules: &policyRulesMock{
			getRBFunc: func(s string) []*rbacv1.RoleBinding {
				return []*rbacv1.RoleBinding{
					makeRB("testns", "testrb", testUser.Name, "testrole"),
				}
			},
			getFunc: func(_ string) *AccessSet {
				return &AccessSet{
					set: map[key]resourceAccessSet{
						{"get", corev1.Resource("ConfigMap")}: map[Access]bool{
							{Namespace: All, ResourceName: All}: true,
						},
					},
				}
			},
		},
		groupsPolicyRules: &policyRulesMock{
			getCRBFunc: func(s string) []*rbacv1.ClusterRoleBinding {
				if s == "mygroup" {
					return []*rbacv1.ClusterRoleBinding{
						makeCRB("testcrb", testUser.Name, "testclusterrole"),
					}
				}
				return nil
			},
			getFunc: func(_ string) *AccessSet {
				return &AccessSet{
					set: map[key]resourceAccessSet{
						{"list", appsv1.Resource("Deployment")}: map[Access]bool{
							{Namespace: "testns", ResourceName: All}: true,
						},
					},
				}
			},
		},
		roles: roleRevisionsMock(func(ns, name string) string {
			return fmt.Sprintf("%s%srev", ns, name)
		}),
		cache: asCache,
	}

	validateAccessSet := func(as *AccessSet) {
		if as == nil {
			t.Fatal("AccessFor() returned nil")
		}
		if as.ID == "" {
			t.Fatal("AccessSet has empty ID")
		}
		if !as.Grants("get", corev1.Resource("ConfigMap"), "default", "cm") ||
			!as.Grants("list", appsv1.Resource("Deployment"), "testns", "deploy") {
			t.Error("AccessSet does not grant desired permissions")
		}
		// wrong verbs
		if as.Grants("delete", corev1.Resource("ConfigMap"), "default", "cm") ||
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
		roles: roleRevisionsMock(func(ns, name string) string {
			return fmt.Sprintf("%s%srev", ns, name)
		}),
		usersPolicyRules: &policyRulesMock{
			getRBFunc: func(s string) []*rbacv1.RoleBinding {
				return []*rbacv1.RoleBinding{
					makeRB("testns", "testrb", testUser.Name, "testrole"),
				}
			},
			getFunc: func(_ string) *AccessSet {
				return &AccessSet{
					set: map[key]resourceAccessSet{
						{"get", corev1.Resource("ConfigMap")}: map[Access]bool{
							{Namespace: All, ResourceName: All}: true,
						},
					},
				}
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

func makeRB(ns, name, user, role string) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Subjects: []rbacv1.Subject{
			{Name: user},
		},
		RoleRef: rbacv1.RoleRef{Name: role},
	}
}

func makeCRB(name, user, role string) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Subjects: []rbacv1.Subject{
			{Name: user},
		},
		RoleRef: rbacv1.RoleRef{Name: role},
	}
}

type policyRulesMock struct {
	getFunc    func(string) *AccessSet
	getRBFunc  func(string) []*rbacv1.RoleBinding
	getCRBFunc func(string) []*rbacv1.ClusterRoleBinding
}

func (p policyRulesMock) get(s string) *AccessSet {
	if p.getFunc == nil {
		return nil
	}
	return p.getFunc(s)
}

func (p policyRulesMock) getRoleBindings(s string) []*rbacv1.RoleBinding {
	if p.getRBFunc == nil {
		return nil
	}
	return p.getRBFunc(s)
}

func (p policyRulesMock) getClusterRoleBindings(s string) []*rbacv1.ClusterRoleBinding {
	if p.getCRBFunc == nil {
		return nil
	}
	return p.getCRBFunc(s)
}

type roleRevisionsMock func(ns, name string) string

func (fn roleRevisionsMock) roleRevision(ns, name string) string {
	return fn(ns, name)
}
