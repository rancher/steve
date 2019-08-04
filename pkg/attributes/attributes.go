package attributes

import (
	"github.com/rancher/norman/pkg/types"
	"github.com/rancher/norman/pkg/types/convert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func Namespaced(s *types.Schema) bool {
	if s == nil {
		return false
	}
	return convert.ToBool(s.Attributes["namespaced"])
}

func SetNamespaced(s *types.Schema, value bool) {
	setVal(s, "namespaced", value)
}

func str(s *types.Schema, key string) string {
	return convert.ToString(s.Attributes[key])
}

func setVal(s *types.Schema, key string, value interface{}) {
	if s.Attributes == nil {
		s.Attributes = map[string]interface{}{}
	}
	s.Attributes[key] = value
}

func Group(s *types.Schema) string {
	return str(s, "group")
}

func SetGroup(s *types.Schema, value string) {
	setVal(s, "group", value)
}

func Version(s *types.Schema) string {
	return str(s, "version")
}

func SetVersion(s *types.Schema, value string) {
	setVal(s, "version", value)
}

func Resource(s *types.Schema) string {
	return str(s, "resource")
}

func SetResource(s *types.Schema, value string) {
	setVal(s, "resource", value)
}

func Kind(s *types.Schema) string {
	return str(s, "kind")
}

func SetKind(s *types.Schema, value string) {
	setVal(s, "kind", value)
}

func GVK(s *types.Schema) schema.GroupVersionKind {
	return schema.GroupVersionKind{
		Group:   Group(s),
		Version: Version(s),
		Kind:    Kind(s),
	}
}

func SetGVK(s *types.Schema, gvk schema.GroupVersionKind) {
	SetGroup(s, gvk.Group)
	SetVersion(s, gvk.Version)
	SetKind(s, gvk.Kind)
}

func GVR(s *types.Schema) schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    Group(s),
		Version:  Version(s),
		Resource: Resource(s),
	}
}

func SetGVR(s *types.Schema, gvk schema.GroupVersionResource) {
	SetGroup(s, gvk.Group)
	SetVersion(s, gvk.Version)
	SetResource(s, gvk.Resource)
}

func Verbs(s *types.Schema) []string {
	return convert.ToStringSlice(s.Attributes["verbs"])
}

func SetVerbs(s *types.Schema, verbs []string) {
	setVal(s, "verbs", verbs)
}

func GR(s *types.Schema) schema.GroupResource {
	return schema.GroupResource{
		Group:    Group(s),
		Resource: Resource(s),
	}
}

func SetGR(s *types.Schema, gr schema.GroupResource) {
	SetGroup(s, gr.Group)
	SetResource(s, gr.Resource)
}

func SetAccess(s *types.Schema, access interface{}) {
	setVal(s, "access", access)
}

func Access(s *types.Schema) interface{} {
	return s.Attributes["access"]
}

func SetAPIResource(s *types.Schema, resource v1.APIResource) {
	SetResource(s, resource.Name)
	SetVerbs(s, resource.Verbs)
	SetNamespaced(s, resource.Namespaced)
}
