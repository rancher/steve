package schemas

import (
	"net/http"

	"github.com/rancher/naok/pkg/accesscontrol"
	"github.com/rancher/naok/pkg/attributes"
	"github.com/rancher/norman/pkg/api/builtin"
	"github.com/rancher/norman/pkg/types"
)

func DefaultSchemaFactory() (*types.Schemas, error) {
	return types.NewSchemas(builtin.Schemas)
}

func (s *schemas) Schemas(subjectKey string, access *accesscontrol.AccessSet, schemasFactory func() (*types.Schemas, error)) (*types.Schemas, error) {
	cached, ok := s.access.Load(subjectKey)
	if ok {
		return cached.(*types.Schemas), nil
	}

	if schemasFactory == nil {
		schemasFactory = DefaultSchemaFactory
	}

	result, err := schemasFactory()
	if err != nil {
		return nil, err
	}

	for _, s := range s.openSchemas {
		if err := result.AddSchema(*s); err != nil {
			return nil, err
		}
	}

	for _, s := range s.schemas {
		gr := attributes.GR(s)

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
		}
		if verbAccess.AnyVerb("create") {
			s.CollectionMethods = append(s.CollectionMethods, http.MethodPost)
		}
		if err := result.AddSchema(*s); err != nil {
			return nil, err
		}
	}

	return result, nil
}
