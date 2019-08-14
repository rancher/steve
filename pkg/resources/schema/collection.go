package schema

import (
	"strings"

	"github.com/rancher/naok/pkg/accesscontrol"
	"github.com/rancher/naok/pkg/attributes"
	"github.com/rancher/norman/pkg/types"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
)

type Factory interface {
	Schemas(user user.Info) (*types.Schemas, error)
	ByGVR(gvr schema.GroupVersionResource) string
}

type Collection struct {
	toSync     int32
	baseSchema *types.Schemas
	schemas    map[string]*types.Schema
	templates  map[string]*Template
	byGVR      map[schema.GroupVersionResource]string

	as *accesscontrol.AccessStore
}

type Template struct {
	Group        string
	Kind         string
	ID           string
	RegisterType interface{}
	Customize    func(*types.Schema)
	Formatter    types.Formatter
	Store        types.Store
	Mapper       types.Mapper
}

func NewCollection(baseSchema *types.Schemas, access *accesscontrol.AccessStore) *Collection {
	return &Collection{
		baseSchema: baseSchema,
		schemas:    map[string]*types.Schema{},
		templates:  map[string]*Template{},
		byGVR:      map[schema.GroupVersionResource]string{},
		as:         access,
	}
}

func (c *Collection) Reset(schemas map[string]*types.Schema) {
	byGVR := map[schema.GroupVersionResource]string{}

	for _, s := range schemas {
		gvr := attributes.GVR(s)
		if gvr.Resource != "" {
			gvr.Resource = strings.ToLower(gvr.Resource)
			byGVR[gvr] = s.ID
		}

		kind := attributes.Kind(s)
		if kind != "" {
			gvr.Resource = strings.ToLower(kind)
			byGVR[gvr] = s.ID
		}
	}

	c.schemas = schemas
	c.byGVR = byGVR
}

func (c *Collection) ByGVR(gvr schema.GroupVersionResource) string {
	gvr.Resource = strings.ToLower(gvr.Resource)
	return c.byGVR[gvr]
}

func (c *Collection) AddTemplate(template *Template) {
	if template.Kind != "" {
		c.templates[template.Group+"/"+template.Kind] = template
	}
	if template.ID != "" {
		c.templates[template.ID] = template
	}
	if template.Kind == "" && template.Group == "" && template.ID == "" {
		c.templates[""] = template
	}
}
