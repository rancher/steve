package ext

import (
	"context"

	"github.com/rancher/steve/pkg/accesscontrol"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
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

func (a *AccessSetAuthorizer) Authorize(ctx context.Context, attrs authorizer.Attributes) (authorized authorizer.Decision, reason string, err error) {
	if !attrs.IsResourceRequest() {
		// XXX: Implement
		return authorizer.DecisionDeny, "", nil
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

	return authorizer.DecisionDeny, "", nil
}

func (a *AccessSetAuthorizer) hasUser(name string) bool {
	accessSet := a.asl.AccessFor(&user.DefaultInfo{
		Name: name,
	})
	return accessSet != nil
}
