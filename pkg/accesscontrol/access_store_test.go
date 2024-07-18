package accesscontrol

import (
	"fmt"
	"slices"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	getRBFunc  func(string) []*rbacv1.RoleBinding
	getCRBFunc func(string) []*rbacv1.ClusterRoleBinding
}

func (p policyRulesMock) get(string) *AccessSet {
	panic("implement me")
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
