package accesscontrol

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

//func makeAccess(ns, resourceName string) {}

func TestAccess_Grants(t *testing.T) {
	type fields struct {
		Namespace    string
		ResourceName string
	}
	type args struct {
		namespace string
		name      string
	}

	tests := []struct {
		name   string
		fields fields
		args   map[args]bool
	}{
		{
			"all names in all namespaces",
			fields{All, All},
			map[args]bool{
				{"any-ns", "any-name"}: true,
			},
		},
		{
			"specific name in all namespaces",
			fields{All, "resource-name"},
			map[args]bool{
				{"any-ns", "any-name"}:      false,
				{"any-ns", "resource-name"}: true,
			},
		},
		{
			"all names in specific name",
			fields{"my-ns", All},
			map[args]bool{
				{"any-ns", "any-name"}: false,
				{"my-ns", "any-name"}:  true,
			},
		},
		{
			"specific name in specific namespace",
			fields{"my-ns", "resource-name"},
			map[args]bool{
				{"any-ns", "any-name"}:      false,
				{"my-ns", "any-name"}:       false,
				{"any-ns", "resource-name"}: false,
				{"my-ns", "resource-name"}:  true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := Access{
				Namespace:    tt.fields.Namespace,
				ResourceName: tt.fields.ResourceName,
			}
			for args, want := range tt.args {
				if got := a.Grants(args.namespace, args.name); got != want {
					t.Errorf("Grants() = %v, want %v", got, want)
				}
			}
		})
	}
}

func TestAccessList_Grants(t *testing.T) {
	type args struct {
		namespace string
		name      string
	}

	tests := []struct {
		name string
		list []Access
		args map[args]bool
	}{
		{
			"all names in all namespaces",
			[]Access{
				{All, All},
				{"my-ns", "resource-name"},
			},
			map[args]bool{
				{"any-ns", "any-name"}:      true,
				{"any-ns", "resource-name"}: true,
				{"my-ns", "any-name"}:       true,
			},
		},
		{
			"multiple names in single namespace",
			[]Access{
				{"my-ns", "resource-name"},
				{"my-ns", "resource-name-2"},
			},
			map[args]bool{
				{"any-ns", "resource-name"}:  false,
				{"my-ns", "any-name"}:        false,
				{"my-ns", "resource-name"}:   true,
				{"my-ns", "resource-name-2"}: true,
			},
		},
		{
			"same name in multiple namespaces",
			[]Access{
				{"my-ns", "resource-name"},
				{"my-ns-2", "resource-name"},
			},
			map[args]bool{
				{"any-ns", "resource-name"}:  false,
				{"my-ns", "any-name"}:        false,
				{"my-ns", "resource-name"}:   true,
				{"my-ns-2", "resource-name"}: true,
			},
		},
		{
			"different namespace and name",
			[]Access{
				{"my-ns", "resource-name"},
				{"my-ns-2", "resource-name-2"},
			},
			map[args]bool{
				{"my-ns", "any-name"}:          false,
				{"my-ns", "resource-name"}:     true,
				{"my-ns-2", "resource-name-2"}: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			al := AccessList(tt.list)
			for args, want := range tt.args {
				if got := al.Grants(args.namespace, args.name); got != want {
					t.Errorf("Grants() = %v, want %v", got, want)
				}
			}
		})
	}
}

func TestAccessSet_Grants(t *testing.T) {
	gr := corev1.Resource("ConfigMap") // example resource
	type fields struct {
		verb      string
		gr        schema.GroupResource
		namespace string
		name      string
	}
	type args struct {
		verb      string
		gr        schema.GroupResource
		namespace string
		name      string
	}

	tests := []struct {
		name string
		adds []fields
		args map[args]bool
	}{
		{
			"empty accessset",
			nil,
			map[args]bool{
				{"any-verb", gr, "any-ns", "any-name"}: false,
			},
		},
		{
			"all verbs single resource",
			[]fields{
				{All, gr, "my-ns", "resource-name"},
			},
			map[args]bool{
				{"any-verb", gr, "any-ns", "any-name"}:                             false,
				{"any-verb", gr, "my-ns", "any-name"}:                              false,
				{"any-verb", gr, "any-ns", "resource-name"}:                        false,
				{"any-verb", corev1.Resource("Secret"), "any-ns", "resource-name"}: false,
				{"any-verb", gr, "my-ns", "resource-name"}:                         true,
			},
		},
		{
			"single verbs all resources",
			[]fields{
				{"get", gr, All, All},
			},
			map[args]bool{
				{"any-verb", gr, "any-ns", "any-name"}:                   false,
				{"get", corev1.Resource("Secret"), "any-ns", "any-name"}: false,
				{"get", gr, "any-ns", "any-name"}:                        true,
			},
		},
		{
			"multipe verbs mixed resources",
			[]fields{
				{"get", gr, All, All},
				{"list", gr, "my-ns", All},
			},
			map[args]bool{
				{"any-verb", gr, "any-ns", "any-name"}: false,
				{"list", gr, "any-ns", "*"}:            false,
				{"get", gr, "any-ns", "any-name"}:      true,
				{"list", gr, "my-ns", "*"}:             true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			as := NewAccessSet()
			for _, add := range tt.adds {
				as.Add(add.verb, add.gr, Access{add.namespace, add.name})
			}
			for args, want := range tt.args {
				if got := as.Grants(args.verb, args.gr, args.namespace, args.name); got != want {
					t.Errorf("Grants(%v) = %v, want %v", args, got, want)
				}
			}
		})
	}
}
