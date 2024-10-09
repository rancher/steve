package ext

import (
	"context"

	"github.com/rancher/steve/pkg/accesscontrol"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

var _ authorizer.Authorizer = (*AccessSetAuthorizer)(nil)

type AccessSetAuthorizer struct {
	asl accesscontrol.AccessSetLookup
}

func NewAccessSetAuthorizer(asl accesscontrol.AccessSetLookup) *AccessSetAuthorizer {
	return &AccessSetAuthorizer{
		asl: asl,
	}
}

// Authorize implements [authorizer.Authorizer].
func (a *AccessSetAuthorizer) Authorize(ctx context.Context, attrs authorizer.Attributes) (authorized authorizer.Decision, reason string, err error) {
	if !attrs.IsResourceRequest() {
		// XXX: Implement
		return authorizer.DecisionDeny, "AccessSetAuthorizer does not support nonResourceURLs requests", nil
	}

	verb := attrs.GetVerb()
	namespace := attrs.GetNamespace()
	name := attrs.GetName()
	gr := schema.GroupResource{
		Group:    attrs.GetAPIGroup(),
		Resource: attrs.GetResource(),
	}

	accessSet := a.asl.AccessFor(attrs.GetUser())
	if accessSet.Grants(verb, gr, namespace, name) {
		return authorizer.DecisionAllow, "", nil
	}

	// An empty string reason will still provide enough information such as:
	//
	//     testtypes.ext.cattle.io is forbidden: User "unknown-user" cannot list resource "testtypes" in API group "ext.cattle.io" at the cluster scope
	return authorizer.DecisionDeny, "", nil
}
