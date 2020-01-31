package schema

import (
	"fmt"
	"net/http"

	"github.com/rancher/norman/v2/pkg/api/builtin"
	"github.com/rancher/norman/v2/pkg/types"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/steve/pkg/attributes"
	"github.com/rancher/steve/pkg/table"
	"k8s.io/apiserver/pkg/authentication/user"
)

func newSchemas() (*types.Schemas, error) {
	s, err := types.NewSchemas(builtin.Schemas)
	if err != nil {
		return nil, err
	}
	s.DefaultMapper = func() types.Mapper {
		return newDefaultMapper()
	}

	return s, nil
}

func (c *Collection) Schemas(user user.Info) (*types.Schemas, error) {
	access := c.as.AccessFor(user)
	return c.schemasForSubject(access)
}

func (c *Collection) schemasForSubject(access *accesscontrol.AccessSet) (*types.Schemas, error) {
	result, err := newSchemas()
	if err != nil {
		return nil, err
	}

	if _, err := result.AddSchemas(c.baseSchema); err != nil {
		return nil, err
	}

	for _, template := range c.templates {
		if template.RegisterType != nil {
			s, err := result.Import(template.RegisterType)
			if err != nil {
				return nil, err
			}
			c.applyTemplates(result, s)
		}
	}

	for _, s := range c.schemas {
		gr := attributes.GR(s)

		if gr.Resource == "" {
			if err := result.AddSchema(*s); err != nil {
				return nil, err
			}
			continue
		}

		verbs := attributes.Verbs(s)
		verbAccess := accesscontrol.AccessListMap{}

		for _, verb := range verbs {
			a := access.AccessListFor(verb, gr)
			if len(a) > 0 {
				verbAccess[verb] = a
			}
		}

		if len(verbAccess) == 0 {
			continue
		}

		s = s.DeepCopy()
		attributes.SetAccess(s, verbAccess)
		if verbAccess.AnyVerb("list", "get") {
			s.ResourceMethods = append(s.ResourceMethods, http.MethodGet)
			s.CollectionMethods = append(s.CollectionMethods, http.MethodGet)
		}
		if verbAccess.AnyVerb("delete") {
			s.ResourceMethods = append(s.ResourceMethods, http.MethodDelete)
		}
		if verbAccess.AnyVerb("update") {
			s.ResourceMethods = append(s.ResourceMethods, http.MethodPut)
			s.ResourceMethods = append(s.ResourceMethods, http.MethodPatch)
		}
		if verbAccess.AnyVerb("create") {
			s.CollectionMethods = append(s.CollectionMethods, http.MethodPost)
		}

		c.applyTemplates(result, s)

		if err := result.AddSchema(*s); err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (c *Collection) applyTemplates(schemas *types.Schemas, schema *types.Schema) {
	templates := []*Template{
		c.templates[schema.ID],
		c.templates[fmt.Sprintf("%s/%s", attributes.Group(schema), attributes.Kind(schema))],
		c.templates[""],
	}

	for _, t := range templates {
		if t == nil {
			continue
		}
		if t.Mapper != nil {
			schemas.AddMapper(schema.ID, t.Mapper)
		}
		if schema.Formatter == nil {
			schema.Formatter = t.Formatter
		}
		if schema.Store == nil {
			if t.StoreFactory == nil {
				schema.Store = t.Store
			} else {
				schema.Store = t.StoreFactory(templates[2].Store)
			}
		}
		if t.Customize != nil {
			t.Customize(schema)
		}
		if len(t.Columns) > 0 {
			schemas.AddMapper(schema.ID, table.NewColumns(t.ComputedColumns, t.Columns...))
		}
	}
}
