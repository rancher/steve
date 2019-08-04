package accesscontrol

import (
	"fmt"

	"github.com/rancher/norman/pkg/authorization"
	"github.com/rancher/norman/pkg/types"
)

type AccessControl struct {
	authorization.AllAccess
}

func NewAccessControl() *AccessControl {
	return &AccessControl{}
}

func (a *AccessControl) CanWatch(apiOp *types.APIRequest, schema *types.Schema) error {
	access := GetAccessListMap(schema)
	if !access.Grants("watch", "*", "*") {
		return fmt.Errorf("watch not allowed")
	}
	return nil
}
