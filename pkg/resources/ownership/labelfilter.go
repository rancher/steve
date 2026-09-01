package ownership

import (
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
	"k8s.io/apiserver/pkg/authentication/user"
)

// userIDLabelFilter scopes visibility to objects whose owner label matches
// the requesting user, unless that user is an admin.
type userIDLabelFilter struct {
	label string
}

// NewUserIDLabelFilter returns a Filter that scopes visibility to objects
// whose metadata.labels[label] matches the requesting user's name, unless
// the requesting user is an admin. Callers (rancher/rancher's pkg/ext,
// which owns the actual GVKs and label constants for its
// extension-apiserver-backed resources) register this via
// ownership.Register for whichever GVKs need it -- see
// pkg/ext/stores/install.go's InstallStores.
func NewUserIDLabelFilter(label string) Filter {
	return &userIDLabelFilter{label: label}
}

func (f *userIDLabelFilter) SQLFilter(userInfo user.Info, isAdmin bool) *sqltypes.OrFilter {
	if isAdmin {
		return nil
	}
	return &sqltypes.OrFilter{
		Filters: []sqltypes.Filter{
			{
				Field:   []string{"metadata", "labels", f.label},
				Matches: []string{userInfo.GetName()},
				Op:      sqltypes.Eq,
			},
		},
	}
}

func (f *userIDLabelFilter) Matches(userInfo user.Info, isAdmin bool, labels map[string]string) bool {
	if isAdmin {
		return true
	}
	return labels[f.label] == userInfo.GetName()
}
