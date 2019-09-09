package schema

import (
	"strings"

	"github.com/rancher/naok/pkg/accesscontrol"
	"github.com/rancher/naok/pkg/attributes"
	"github.com/rancher/naok/pkg/table"
	"github.com/rancher/norman/pkg/data"
	"github.com/rancher/norman/pkg/types"
	"github.com/rancher/wrangler/pkg/name"
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
	byGVK      map[schema.GroupVersionKind]string

	as *accesscontrol.AccessStore
}

type Template struct {
	Group           string
	Kind            string
	ID              string
	RegisterType    interface{}
	Customize       func(*types.Schema)
	Formatter       types.Formatter
	Store           types.Store
	Mapper          types.Mapper
	Columns         []table.Column
	ComputedColumns func(data.Object)
}

func NewCollection(baseSchema *types.Schemas, access *accesscontrol.AccessStore) *Collection {
	return &Collection{
		baseSchema: baseSchema,
		schemas:    map[string]*types.Schema{},
		templates:  map[string]*Template{},
		byGVR:      map[schema.GroupVersionResource]string{},
		byGVK:      map[schema.GroupVersionKind]string{},
		as:         access,
	}
}

func (c *Collection) Reset(schemas map[string]*types.Schema) {
	byGVR := map[schema.GroupVersionResource]string{}
	byGVK := map[schema.GroupVersionKind]string{}

	for _, s := range schemas {
		gvr := attributes.GVR(s)
		if gvr.Resource != "" {
			byGVR[gvr] = s.ID
		}
		gvk := attributes.GVK(s)
		if gvk.Kind != "" {
			byGVK[gvk] = s.ID
		}
	}

	c.schemas = schemas
	c.byGVR = byGVR
	c.byGVK = byGVK
}

func (c *Collection) Schema(id string) *types.Schema {
	return c.schemas[id]
}

func (c *Collection) IDs() (result []string) {
	seen := map[string]bool{}
	for _, id := range c.byGVR {
		if seen[id] {
			continue
		}
		seen[id] = true
		result = append(result, id)
	}
	return
}

func (c *Collection) ByGVR(gvr schema.GroupVersionResource) string {
	id, ok := c.byGVR[gvr]
	if ok {
		return id
	}
	gvr.Resource = name.GuessPluralName(strings.ToLower(gvr.Resource))
	return c.byGVR[gvr]
}

func (c *Collection) ByGVK(gvk schema.GroupVersionKind) string {
	return c.byGVK[gvk]
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
