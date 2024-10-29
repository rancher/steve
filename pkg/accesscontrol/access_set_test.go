package accesscontrol

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAccessSet_AddNonResourceURLs(t *testing.T) {
	testCases := []struct {
		name  string
		verbs []string
		urls  []string
		want  []nonResourceKey
	}{
		{
			name:  "valid case",
			verbs: []string{"get", "post"},
			urls:  []string{"/healthz", "/metrics"},
			want: []nonResourceKey{
				{"get", "/healthz"},
				{"get", "/metrics"},
				{"post", "/healthz"},
				{"post", "/metrics"},
			},
		},
		{
			name:  "url wildcard",
			verbs: []string{"get"},
			urls:  []string{"/metrics/*"},
			want: []nonResourceKey{
				{"get", "/metrics/*"},
			},
		},
		{
			name:  "verb wildcard",
			verbs: []string{"*"},
			urls:  []string{"/metrics"},
			want: []nonResourceKey{
				{"*", "/metrics"},
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
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			accessSet := &AccessSet{}
			accessSet.AddNonResourceURLs(tt.verbs, tt.urls)

			if len(tt.want) > 0 {
				for _, key := range tt.want {
					assert.Contains(t, accessSet.nonResourceSet, key)
				}
			} else {
				assert.Len(t, accessSet.nonResourceSet, 0)
			}
		})
	}
}

func TestAccessSet_GrantsNonResource(t *testing.T) {
	testCases := []struct {
		name   string
		verb   string
		url    string
		keys   map[nonResourceKey]struct{}
		expect bool
	}{
		{
			name: "direct match",
			verb: "get",
			url:  "/healthz",
			keys: map[nonResourceKey]struct{}{
				{verb: "get", url: "/healthz"}: {},
			},
			expect: true,
		},
		{
			name: "wildcard in url",
			verb: "get",
			url:  "/api/resource",
			keys: map[nonResourceKey]struct{}{
				{verb: "get", url: "/api/*"}: {},
			},
			expect: true,
		},
		{
			name: "wildcard in verb",
			verb: "get",
			url:  "/healthz",
			keys: map[nonResourceKey]struct{}{
				{verb: "*", url: "/healthz"}: {},
			},
			expect: true,
		},
		{
			name: "invalid wildcard",
			verb: "get",
			url:  "/*", // that's invalid according to k8s rules
			keys: map[nonResourceKey]struct{}{
				{verb: "get", url: "/api/*"}: {},
			},
			expect: false,
		},
		{
			name: "wrong verb",
			verb: "post",
			url:  "/healthz",
			keys: map[nonResourceKey]struct{}{
				{verb: "get", url: "/healthz"}: {},
			},
			expect: false,
		},
		{
			name: "wrong url",
			verb: "post",
			url:  "/metrics",
			keys: map[nonResourceKey]struct{}{
				{verb: "post", url: "/healthz"}: {},
			},
			expect: false,
		},
		{
			name:   "no matching rule",
			verb:   "post",
			url:    "/healthz",
			keys:   map[nonResourceKey]struct{}{},
			expect: false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			accessSet := &AccessSet{}

			for rule := range tt.keys {
				accessSet.AddNonResourceURLs([]string{rule.verb}, []string{rule.url})
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
				nonResourceSet: map[nonResourceKey]struct{}{
					{url: "/healthz", verb: "get"}: {},
				},
			},
			right: &AccessSet{
				nonResourceSet: map[nonResourceKey]struct{}{
					{url: "/metrics", verb: "post"}: {},
				},
			},
			want: &AccessSet{
				nonResourceSet: map[nonResourceKey]struct{}{
					{url: "/healthz", verb: "get"}:  {},
					{url: "/metrics", verb: "post"}: {},
				},
			},
		},
		{
			name: "merging NonResouceURLs - repeated items",
			left: &AccessSet{
				nonResourceSet: map[nonResourceKey]struct{}{
					{url: "/healthz", verb: "get"}:  {},
					{url: "/metrics", verb: "post"}: {},
				},
			},
			right: &AccessSet{
				nonResourceSet: map[nonResourceKey]struct{}{
					{url: "/metrics", verb: "post"}: {},
				},
			},
			want: &AccessSet{
				nonResourceSet: map[nonResourceKey]struct{}{
					{url: "/healthz", verb: "get"}:  {},
					{url: "/metrics", verb: "post"}: {},
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
