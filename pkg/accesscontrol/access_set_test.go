package accesscontrol

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/rbac/v1"
)

func TestAccessSet_AddNonResourceURLs(t *testing.T) {
	sampleRule := &v1.PolicyRule{
		NonResourceURLs: []string{"/healthz", "/metrics"},
		Verbs:           []string{"get", "post"},
	}

	testCases := []struct {
		name  string
		verbs []string
		urls  []string
		want  []*v1.PolicyRule
	}{
		{
			name:  "valid case",
			verbs: []string{"get", "post"},
			urls:  []string{"/healthz", "/metrics"},
			want: []*v1.PolicyRule{
				sampleRule,
				sampleRule,
				sampleRule,
				sampleRule,
			},
		},
		{
			name:  "empty urls",
			verbs: []string{"get", "post"},
			urls:  []string{},
			want:  nil,
		},
		{
			name:  "empty verbs",
			verbs: []string{},
			urls:  []string{"/healthz", "/metrics"},
			want:  nil,
		},
		{
			name:  "nil",
			verbs: nil,
			urls:  nil,
			want:  nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			accessSet := &AccessSet{}
			accessSet.AddNonResouceURLs(tt.verbs, tt.urls)

			rules := getNonResourceRules(accessSet)
			assert.Equal(t, tt.want, rules, "unexpected rules")
		})
	}
}

func TestAcessSet_AddNonResourceRule(t *testing.T) {
	testCases := []struct {
		name     string
		rule     *v1.PolicyRule
		expected *v1.PolicyRule
		exists   bool
	}{
		{
			name: "add valid rule",
			rule: &v1.PolicyRule{
				NonResourceURLs: []string{"/healthz"},
				Verbs:           []string{"get"},
			},
			expected: &v1.PolicyRule{
				NonResourceURLs: []string{"/healthz"},
				Verbs:           []string{"get"},
			},
			exists: true,
		},
		{
			name:     "add nil rule",
			rule:     nil,
			expected: nil,
			exists:   false,
		},
		{
			name: "add rule with multiple verbs and urls",
			rule: &v1.PolicyRule{
				NonResourceURLs: []string{"/healthz", "/metrics"},
				Verbs:           []string{"get", "post"},
			},
			expected: &v1.PolicyRule{
				NonResourceURLs: []string{"/healthz", "/metrics"},
				Verbs:           []string{"get", "post"},
			},
			exists: true,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			accessSet := &AccessSet{}
			accessSet.AddNonResourceRule(tt.rule)

			rules := getNonResourceRules(accessSet)
			if tt.exists {
				assert.Contains(t, rules, tt.expected, "rule not found")
			} else {
				assert.NotContains(t, rules, tt.expected, "unexpected rule found")
			}
		})

	}
}

func TestAccessSet_GrantsNonResource(t *testing.T) {
	testCases := []struct {
		name   string
		verb   string
		url    string
		rule   *v1.PolicyRule
		expect bool
	}{
		{
			name: "direct match",
			verb: "get",
			url:  "/healthz",
			rule: &v1.PolicyRule{
				NonResourceURLs: []string{"/healthz"},
				Verbs:           []string{"get"},
			},
			expect: true,
		},
		{
			name: "more than one url in the rule",
			verb: "get",
			url:  "/healthz",
			rule: &v1.PolicyRule{
				NonResourceURLs: []string{"/healthz", "/metrics"},
				Verbs:           []string{"get"},
			},
			expect: true,
		},
		{
			name: "wildcard in url",
			verb: "get",
			url:  "/api/resource",
			rule: &v1.PolicyRule{
				NonResourceURLs: []string{"/api/*"},
				Verbs:           []string{"get"},
			},
			expect: true,
		},
		{
			name: "invalid wildcard",
			verb: "get",
			url:  "/*", // that's invalid according to k8s rules
			rule: &v1.PolicyRule{
				NonResourceURLs: []string{"/api/*"},
				Verbs:           []string{"get"},
			},
			expect: false,
		},
		{
			name: "wrong verb",
			verb: "post",
			url:  "/healthz",
			rule: &v1.PolicyRule{
				NonResourceURLs: []string{"/healthz"},
				Verbs:           []string{"get"},
			},
			expect: false,
		},
		{
			name: "wrong url",
			verb: "post",
			url:  "/metrics",
			rule: &v1.PolicyRule{
				NonResourceURLs: []string{"/healthz"},
				Verbs:           []string{"post"},
			},
			expect: false,
		},
		{
			name:   "no matching rule",
			verb:   "post",
			url:    "/healthz",
			rule:   nil,
			expect: false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			accessSet := &AccessSet{}

			if tt.rule != nil {
				accessSet.AddNonResourceRule(tt.rule)
			}

			res := accessSet.GrantsNonResource(tt.verb, tt.url)
			assert.Equal(t, tt.expect, res)
		})
	}
}

func TestAccessSet_Merge(t *testing.T) {
	testCases := []struct {
		name  string
		left  *AccessSet
		right *AccessSet
		want  *AccessSet
	}{
		{
			name: "merging NonResouceURLs",
			left: &AccessSet{
				nonResourceSet: map[nonResourceKey]*v1.PolicyRule{
					{url: "/healthz", verb: "get"}: {
						NonResourceURLs: []string{"/healthz"},
						Verbs:           []string{"get"},
					},
				},
			},
			right: &AccessSet{
				nonResourceSet: map[nonResourceKey]*v1.PolicyRule{
					{url: "/metrics", verb: "post"}: {
						NonResourceURLs: []string{"/metrics"},
						Verbs:           []string{"post"},
					},
				},
			},
			want: &AccessSet{
				nonResourceSet: map[nonResourceKey]*v1.PolicyRule{
					{url: "/healthz", verb: "get"}: {
						NonResourceURLs: []string{"/healthz"},
						Verbs:           []string{"get"},
					},
					{url: "/metrics", verb: "post"}: {
						NonResourceURLs: []string{"/metrics"},
						Verbs:           []string{"post"},
					},
				},
			},
		},
		{
			name: "merging NonResouceURLs - repeated items",
			left: &AccessSet{
				nonResourceSet: map[nonResourceKey]*v1.PolicyRule{
					{url: "/healthz", verb: "get"}: {
						NonResourceURLs: []string{"/healthz"},
						Verbs:           []string{"get"},
					},
					{url: "/metrics", verb: "post"}: {
						NonResourceURLs: []string{"/metrics"},
						Verbs:           []string{"post"},
					},
				},
			},
			right: &AccessSet{
				nonResourceSet: map[nonResourceKey]*v1.PolicyRule{
					{url: "/metrics", verb: "post"}: {
						NonResourceURLs: []string{"/metrics"},
						Verbs:           []string{"post"},
					},
				},
			},
			want: &AccessSet{
				nonResourceSet: map[nonResourceKey]*v1.PolicyRule{
					{url: "/healthz", verb: "get"}: {
						NonResourceURLs: []string{"/healthz"},
						Verbs:           []string{"get"},
					},
					{url: "/metrics", verb: "post"}: {
						NonResourceURLs: []string{"/metrics"},
						Verbs:           []string{"post"},
					},
				},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			tt.left.Merge(tt.right)
			assert.Equal(t, tt.want, tt.left)
		})
	}
}

// getNonResourceRules is test utility function to abstract rules extraction
// from the nonResourceSet inside the AccessSet object, if you even need to
// change the internal implementation of AccessSet, update this function to
// handle you changes
func getNonResourceRules(accessSet *AccessSet) []*v1.PolicyRule {
	var rules []*v1.PolicyRule
	for _, rule := range accessSet.nonResourceSet {
		rules = append(rules, rule)
	}
	return rules
}
