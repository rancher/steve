/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://apache.org

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package accesscontrol

import (
	"strings"

	rbacv1 "k8s.io/api/rbac/v1"
)

// These funcs are borrowed from "pkg/apis/rbac/v1/evaluation_helpers.go" of k8s.io/kubernetes
// This is duplicated locally to avoid a heavy, fragile direct import of k8s.io/kubernetes.

func NonResourceURLMatches(rule *rbacv1.PolicyRule, requestedURL string) bool {
	for _, ruleURL := range rule.NonResourceURLs {
		if ruleURL == rbacv1.NonResourceAll {
			return true
		}
		if ruleURL == requestedURL {
			return true
		}
		if strings.HasSuffix(ruleURL, "*") && strings.HasPrefix(requestedURL, strings.TrimRight(ruleURL, "*")) {
			return true
		}
	}

	return false
}

func VerbMatches(rule *rbacv1.PolicyRule, requestedVerb string) bool {
	for _, ruleVerb := range rule.Verbs {
		if ruleVerb == rbacv1.VerbAll {
			return true
		}
		if ruleVerb == requestedVerb {
			return true
		}
	}

	return false
}
