package listprocessor

import (
	"testing"

	"github.com/rancher/wrangler/v2/pkg/generic"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
)

func TestFilterList(t *testing.T) {
	tests := []struct {
		name    string
		objects [][]unstructured.Unstructured
		filters []OrFilter
		want    []unstructured.Unstructured
	}{
		{
			name: "single filter",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "fuji",
							},
							"data": map[string]interface{}{
								"color": "pink",
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "granny-smith",
							},
							"data": map[string]interface{}{
								"color": "green",
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"data", "color"},
							match: "pink",
						},
					},
				},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
			},
		},
		{
			name: "multi filter",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "fuji",
							},
							"data": map[string]interface{}{
								"color": "pink",
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "honeycrisp",
							},
							"data": map[string]interface{}{
								"color": "pink",
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "granny-smith",
							},
							"data": map[string]interface{}{
								"color": "green",
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"data", "color"},
							match: "pink",
						},
					},
				},
				{
					filters: []Filter{
						{
							field: []string{"metadata", "name"},
							match: "honey",
						},
					},
				},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "honeycrisp",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
			},
		},
		{
			name: "no matches",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "fuji",
							},
							"data": map[string]interface{}{
								"color": "pink",
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "granny-smith",
							},
							"data": map[string]interface{}{
								"color": "green",
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"data", "color"},
							match: "purple",
						},
					},
				},
			},
			want: []unstructured.Unstructured{},
		},
		{
			name: "no filters",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "fuji",
							},
							"data": map[string]interface{}{
								"color": "pink",
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "granny-smith",
							},
							"data": map[string]interface{}{
								"color": "green",
							},
						},
					},
				},
			},
			filters: []OrFilter{},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "granny-smith",
						},
						"data": map[string]interface{}{
							"color": "green",
						},
					},
				},
			},
		},
		{
			name: "filter field does not match",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "fuji",
							},
							"data": map[string]interface{}{
								"color": "pink",
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "honeycrisp",
							},
							"data": map[string]interface{}{
								"color": "pink",
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "granny-smith",
							},
							"data": map[string]interface{}{
								"color": "green",
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"spec", "volumes"},
							match: "hostPath",
						},
					},
				},
			},
			want: []unstructured.Unstructured{},
		},
		{
			name: "filter subfield does not match",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "fuji",
							},
							"data": map[string]interface{}{
								"color": "pink",
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "honeycrisp",
							},
							"data": map[string]interface{}{
								"color": "pink",
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "granny-smith",
							},
							"data": map[string]interface{}{
								"color": "green",
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"data", "productType"},
							match: "tablet",
						},
					},
				},
			},
			want: []unstructured.Unstructured{},
		},
		{
			name: "almost valid filter key",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "granny-smith",
							},
							"data": map[string]interface{}{
								"color": "green",
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"data", "color", "shade"},
							match: "green",
						},
					},
				},
			},
			want: []unstructured.Unstructured{},
		},
		{
			name: "match string array",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "apple",
							},
							"data": map[string]interface{}{
								"colors": []interface{}{
									"pink",
									"red",
									"green",
									"yellow",
								},
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "berry",
							},
							"data": map[string]interface{}{
								"colors": []interface{}{
									"blue",
									"red",
									"black",
								},
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "banana",
							},
							"data": map[string]interface{}{
								"colors": []interface{}{
									"yellow",
								},
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"data", "colors"},
							match: "yellow",
						},
					},
				},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "fruit",
						"metadata": map[string]interface{}{
							"name": "apple",
						},
						"data": map[string]interface{}{
							"colors": []interface{}{
								"pink",
								"red",
								"green",
								"yellow",
							},
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "fruit",
						"metadata": map[string]interface{}{
							"name": "banana",
						},
						"data": map[string]interface{}{
							"colors": []interface{}{
								"yellow",
							},
						},
					},
				},
			},
		},
		{
			name: "match object array",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "apple",
							},
							"data": map[string]interface{}{
								"varieties": []interface{}{
									map[string]interface{}{
										"name":  "fuji",
										"color": "pink",
									},
									map[string]interface{}{
										"name":  "granny-smith",
										"color": "green",
									},
									map[string]interface{}{
										"name":  "red-delicious",
										"color": "red",
									},
								},
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "berry",
							},
							"data": map[string]interface{}{
								"varieties": []interface{}{
									map[string]interface{}{
										"name":  "blueberry",
										"color": "blue",
									},
									map[string]interface{}{
										"name":  "raspberry",
										"color": "red",
									},
									map[string]interface{}{
										"name":  "blackberry",
										"color": "black",
									},
								},
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "banana",
							},
							"data": map[string]interface{}{
								"varieties": []interface{}{
									map[string]interface{}{
										"name":  "cavendish",
										"color": "yellow",
									},
									map[string]interface{}{
										"name":  "plantain",
										"color": "green",
									},
								},
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"data", "varieties", "color"},
							match: "red",
						},
					},
				},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "fruit",
						"metadata": map[string]interface{}{
							"name": "apple",
						},
						"data": map[string]interface{}{
							"varieties": []interface{}{
								map[string]interface{}{
									"name":  "fuji",
									"color": "pink",
								},
								map[string]interface{}{
									"name":  "granny-smith",
									"color": "green",
								},
								map[string]interface{}{
									"name":  "red-delicious",
									"color": "red",
								},
							},
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "fruit",
						"metadata": map[string]interface{}{
							"name": "berry",
						},
						"data": map[string]interface{}{
							"varieties": []interface{}{
								map[string]interface{}{
									"name":  "blueberry",
									"color": "blue",
								},
								map[string]interface{}{
									"name":  "raspberry",
									"color": "red",
								},
								map[string]interface{}{
									"name":  "blackberry",
									"color": "black",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "match nested array",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "apple",
							},
							"data": map[string]interface{}{
								"attributes": []interface{}{
									[]interface{}{
										"pink",
										"green",
										"red",
										"purple",
									},
									[]interface{}{
										"fuji",
										"granny-smith",
										"red-delicious",
										"black-diamond",
									},
								},
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "berry",
							},
							"data": map[string]interface{}{
								"attributes": []interface{}{
									[]interface{}{
										"blue",
										"red",
										"black",
									},
									[]interface{}{
										"blueberry",
										"raspberry",
										"blackberry",
									},
								},
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "banana",
							},
							"data": map[string]interface{}{
								"attributes": []interface{}{
									[]interface{}{
										"yellow",
										"green",
									},
									[]interface{}{
										"cavendish",
										"plantain",
									},
								},
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"data", "attributes"},
							match: "black",
						},
					},
				},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "fruit",
						"metadata": map[string]interface{}{
							"name": "apple",
						},
						"data": map[string]interface{}{
							"attributes": []interface{}{
								[]interface{}{
									"pink",
									"green",
									"red",
									"purple",
								},
								[]interface{}{
									"fuji",
									"granny-smith",
									"red-delicious",
									"black-diamond",
								},
							},
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "fruit",
						"metadata": map[string]interface{}{
							"name": "berry",
						},
						"data": map[string]interface{}{
							"attributes": []interface{}{
								[]interface{}{
									"blue",
									"red",
									"black",
								},
								[]interface{}{
									"blueberry",
									"raspberry",
									"blackberry",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "match nested object array",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "apple",
							},
							"data": map[string]interface{}{
								"attributes": []interface{}{
									[]interface{}{
										map[string]interface{}{
											"pink": "fuji",
										},
										map[string]interface{}{
											"green": "granny-smith",
										},
										map[string]interface{}{
											"pink": "honeycrisp",
										},
									},
								},
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "berry",
							},
							"data": map[string]interface{}{
								"attributes": []interface{}{
									[]interface{}{
										map[string]interface{}{
											"blue": "blueberry",
										},
										map[string]interface{}{
											"red": "raspberry",
										},
										map[string]interface{}{
											"black": "blackberry",
										},
									},
								},
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "banana",
							},
							"data": map[string]interface{}{
								"attributes": []interface{}{
									[]interface{}{
										map[string]interface{}{
											"yellow": "cavendish",
										},
										map[string]interface{}{
											"green": "plantain",
										},
									},
								},
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"data", "attributes", "green"},
							match: "plantain",
						},
					},
				},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "fruit",
						"metadata": map[string]interface{}{
							"name": "banana",
						},
						"data": map[string]interface{}{
							"attributes": []interface{}{
								[]interface{}{
									map[string]interface{}{
										"yellow": "cavendish",
									},
									map[string]interface{}{
										"green": "plantain",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "single or filter, filter on one value",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "pink-lady",
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "pomegranate",
							},
							"data": map[string]interface{}{
								"color": "pink",
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"metadata", "name"},
							match: "pink",
						},
						{
							field: []string{"data", "color"},
							match: "pink",
						},
					},
				},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "fruit",
						"metadata": map[string]interface{}{
							"name": "pink-lady",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "fruit",
						"metadata": map[string]interface{}{
							"name": "pomegranate",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
			},
		},
		{
			name: "single or filter, filter on different value",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "pink-lady",
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "pomegranate",
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"metadata", "name"},
							match: "pink",
						},
						{
							field: []string{"metadata", "name"},
							match: "pom",
						},
					},
				},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "fruit",
						"metadata": map[string]interface{}{
							"name": "pink-lady",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "fruit",
						"metadata": map[string]interface{}{
							"name": "pomegranate",
						},
					},
				},
			},
		},
		{
			name: "single or filter, no matches",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "pink-lady",
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "pomegranate",
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"metadata", "name"},
							match: "blue",
						},
						{
							field: []string{"metadata", "name"},
							match: "watermelon",
						},
					},
				},
			},
			want: []unstructured.Unstructured{},
		},
		{
			name: "and-ed or filters",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "pink-lady",
							},
							"data": map[string]interface{}{
								"flavor": "sweet",
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "pomegranate",
							},
							"data": map[string]interface{}{
								"color":  "pink",
								"flavor": "sweet",
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "grapefruit",
							},
							"data": map[string]interface{}{
								"color": "pink",
								"data": map[string]interface{}{
									"flavor": "bitter",
								},
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"metadata", "name"},
							match: "pink",
						},
						{
							field: []string{"data", "color"},
							match: "pink",
						},
					},
				},
				{
					filters: []Filter{
						{
							field: []string{"data", "flavor"},
							match: "sweet",
						},
					},
				},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "fruit",
						"metadata": map[string]interface{}{
							"name": "pink-lady",
						},
						"data": map[string]interface{}{
							"flavor": "sweet",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "fruit",
						"metadata": map[string]interface{}{
							"name": "pomegranate",
						},
						"data": map[string]interface{}{
							"color":  "pink",
							"flavor": "sweet",
						},
					},
				},
			},
		},
		{
			name: "not filter",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "fuji",
							},
							"data": map[string]interface{}{
								"color": "pink",
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "granny-smith",
							},
							"data": map[string]interface{}{
								"color": "green",
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"data", "color"},
							match: "pink",
							op:    "!=",
						},
					},
				},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "granny-smith",
						},
						"data": map[string]interface{}{
							"color": "green",
						},
					},
				},
			},
		},
		{
			name: "or'ed not filter",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "fuji",
							},
							"data": map[string]interface{}{
								"color": "pink",
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "granny-smith",
							},
							"data": map[string]interface{}{
								"color": "green",
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"data", "color"},
							match: "pink",
							op:    "!=",
						},
						{
							field: []string{"data", "color"},
							match: "green",
							op:    "!=",
						},
					},
				},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "granny-smith",
						},
						"data": map[string]interface{}{
							"color": "green",
						},
					},
				},
			},
		},
		{
			name: "mixed or'ed filter",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "fuji",
							},
							"data": map[string]interface{}{
								"color": "pink",
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "granny-smith",
							},
							"data": map[string]interface{}{
								"color": "green",
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"data", "color"},
							match: "pink",
							op:    "!=",
						},
						{
							field: []string{"metadata", "name"},
							match: "fuji",
						},
					},
				},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "granny-smith",
						},
						"data": map[string]interface{}{
							"color": "green",
						},
					},
				},
			},
		},
		{
			name: "anded and or'ed mixed equality filter",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "fuji",
							},
							"data": map[string]interface{}{
								"color": "pink",
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "honeycrisp",
							},
							"data": map[string]interface{}{
								"color": "pink",
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "apple",
							"metadata": map[string]interface{}{
								"name": "granny-smith",
							},
							"data": map[string]interface{}{
								"color": "green",
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"metadata", "name"},
							match: "fuji",
							op:    "!=",
						},
					},
				},
				{
					filters: []Filter{
						{
							field: []string{"data", "color"},
							match: "pink",
						},
					},
				},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "honeycrisp",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
			},
		},
		{
			name: "match string array with not",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "apple",
							},
							"data": map[string]interface{}{
								"colors": []interface{}{
									"pink",
									"red",
									"green",
									"yellow",
								},
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "berry",
							},
							"data": map[string]interface{}{
								"colors": []interface{}{
									"blue",
									"red",
									"black",
								},
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "banana",
							},
							"data": map[string]interface{}{
								"colors": []interface{}{
									"yellow",
								},
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"data", "colors"},
							match: "yellow",
							op:    "!=",
						},
					},
				},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "fruit",
						"metadata": map[string]interface{}{
							"name": "berry",
						},
						"data": map[string]interface{}{
							"colors": []interface{}{
								"blue",
								"red",
								"black",
							},
						},
					},
				},
			},
		},
		{
			name: "match object array with not",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "apple",
							},
							"data": map[string]interface{}{
								"varieties": []interface{}{
									map[string]interface{}{
										"name":  "fuji",
										"color": "pink",
									},
									map[string]interface{}{
										"name":  "granny-smith",
										"color": "green",
									},
									map[string]interface{}{
										"name":  "red-delicious",
										"color": "red",
									},
								},
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "berry",
							},
							"data": map[string]interface{}{
								"varieties": []interface{}{
									map[string]interface{}{
										"name":  "blueberry",
										"color": "blue",
									},
									map[string]interface{}{
										"name":  "raspberry",
										"color": "red",
									},
									map[string]interface{}{
										"name":  "blackberry",
										"color": "black",
									},
								},
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "banana",
							},
							"data": map[string]interface{}{
								"varieties": []interface{}{
									map[string]interface{}{
										"name":  "cavendish",
										"color": "yellow",
									},
									map[string]interface{}{
										"name":  "plantain",
										"color": "green",
									},
								},
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"data", "varieties", "color"},
							match: "red",
							op:    "!=",
						},
					},
				},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "fruit",
						"metadata": map[string]interface{}{
							"name": "banana",
						},
						"data": map[string]interface{}{
							"varieties": []interface{}{
								map[string]interface{}{
									"name":  "cavendish",
									"color": "yellow",
								},
								map[string]interface{}{
									"name":  "plantain",
									"color": "green",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "match nested array with not",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "apple",
							},
							"data": map[string]interface{}{
								"attributes": []interface{}{
									[]interface{}{
										"pink",
										"green",
										"red",
										"purple",
									},
									[]interface{}{
										"fuji",
										"granny-smith",
										"red-delicious",
										"black-diamond",
									},
								},
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "berry",
							},
							"data": map[string]interface{}{
								"attributes": []interface{}{
									[]interface{}{
										"blue",
										"red",
										"black",
									},
									[]interface{}{
										"blueberry",
										"raspberry",
										"blackberry",
									},
								},
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "banana",
							},
							"data": map[string]interface{}{
								"attributes": []interface{}{
									[]interface{}{
										"yellow",
										"green",
									},
									[]interface{}{
										"cavendish",
										"plantain",
									},
								},
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"data", "attributes"},
							match: "black",
							op:    "!=",
						},
					},
				},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "fruit",
						"metadata": map[string]interface{}{
							"name": "banana",
						},
						"data": map[string]interface{}{
							"attributes": []interface{}{
								[]interface{}{
									"yellow",
									"green",
								},
								[]interface{}{
									"cavendish",
									"plantain",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "match nested object array with mixed equality",
			objects: [][]unstructured.Unstructured{
				{
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "apple",
							},
							"data": map[string]interface{}{
								"attributes": []interface{}{
									[]interface{}{
										map[string]interface{}{
											"pink": "fuji",
										},
										map[string]interface{}{
											"green": "granny-smith",
										},
										map[string]interface{}{
											"pink": "honeycrisp",
										},
									},
								},
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "berry",
							},
							"data": map[string]interface{}{
								"attributes": []interface{}{
									[]interface{}{
										map[string]interface{}{
											"blue": "blueberry",
										},
										map[string]interface{}{
											"red": "raspberry",
										},
										map[string]interface{}{
											"black": "blackberry",
										},
									},
								},
							},
						},
					},
					{
						Object: map[string]interface{}{
							"kind": "fruit",
							"metadata": map[string]interface{}{
								"name": "banana",
							},
							"data": map[string]interface{}{
								"attributes": []interface{}{
									[]interface{}{
										map[string]interface{}{
											"yellow": "cavendish",
										},
										map[string]interface{}{
											"green": "plantain",
										},
									},
								},
							},
						},
					},
				},
			},
			filters: []OrFilter{
				{
					filters: []Filter{
						{
							field: []string{"data", "attributes", "green"},
							match: "plantain",
							op:    "!=",
						},
						{
							field: []string{"data", "attributes", "green"},
							match: "granny-smith",
						},
					},
				},
				{
					filters: []Filter{
						{
							field: []string{"metadata", "name"},
							match: "banana",
						},
					},
				},
			},
			want: []unstructured.Unstructured{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ch := make(chan []unstructured.Unstructured)
			go func() {
				for _, o := range test.objects {
					ch <- o
				}
				close(ch)
			}()
			got := FilterList(ch, test.filters)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestSortList(t *testing.T) {
	tests := []struct {
		name    string
		objects []unstructured.Unstructured
		sort    Sort
		want    []unstructured.Unstructured
	}{
		{
			name: "sort metadata.name",
			objects: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "honeycrisp",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "granny-smith",
						},
						"data": map[string]interface{}{
							"color": "green",
						},
					},
				},
			},
			sort: Sort{
				primaryField: []string{"metadata", "name"},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "granny-smith",
						},
						"data": map[string]interface{}{
							"color": "green",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "honeycrisp",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
			},
		},
		{
			name: "reverse sort metadata.name",
			objects: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "honeycrisp",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "granny-smith",
						},
						"data": map[string]interface{}{
							"color": "green",
						},
					},
				},
			},
			sort: Sort{
				primaryField: []string{"metadata", "name"},
				primaryOrder: DESC,
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "honeycrisp",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "granny-smith",
						},
						"data": map[string]interface{}{
							"color": "green",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
			},
		},
		{
			name: "invalid field",
			objects: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "granny-smith",
						},
						"data": map[string]interface{}{
							"color": "green",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "honeycrisp",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
			},
			sort: Sort{
				primaryField: []string{"data", "productType"},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "granny-smith",
						},
						"data": map[string]interface{}{
							"color": "green",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "honeycrisp",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
			},
		},
		{
			name: "unsorted",
			objects: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "granny-smith",
						},
						"data": map[string]interface{}{
							"color": "green",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "honeycrisp",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
			},
			sort: Sort{},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "granny-smith",
						},
						"data": map[string]interface{}{
							"color": "green",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "honeycrisp",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
			},
		},
		{
			name: "primary sort ascending, secondary sort ascending",
			objects: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "honeycrisp",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "granny-smith",
						},
						"data": map[string]interface{}{
							"color": "green",
						},
					},
				},
			},
			sort: Sort{
				primaryField:   []string{"data", "color"},
				secondaryField: []string{"metadata", "name"},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "granny-smith",
						},
						"data": map[string]interface{}{
							"color": "green",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "honeycrisp",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
			},
		},
		{
			name: "primary sort ascending, secondary sort descending",
			objects: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "honeycrisp",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "granny-smith",
						},
						"data": map[string]interface{}{
							"color": "green",
						},
					},
				},
			},
			sort: Sort{
				primaryField:   []string{"data", "color"},
				secondaryField: []string{"metadata", "name"},
				secondaryOrder: DESC,
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "granny-smith",
						},
						"data": map[string]interface{}{
							"color": "green",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "honeycrisp",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
			},
		},
		{
			name: "primary sort descending, secondary sort ascending",
			objects: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "honeycrisp",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "granny-smith",
						},
						"data": map[string]interface{}{
							"color": "green",
						},
					},
				},
			},
			sort: Sort{
				primaryField:   []string{"data", "color"},
				primaryOrder:   DESC,
				secondaryField: []string{"metadata", "name"},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "honeycrisp",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "granny-smith",
						},
						"data": map[string]interface{}{
							"color": "green",
						},
					},
				},
			},
		},
		{
			name: "primary sort descending, secondary sort descending",
			objects: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "honeycrisp",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "granny-smith",
						},
						"data": map[string]interface{}{
							"color": "green",
						},
					},
				},
			},
			sort: Sort{
				primaryField:   []string{"data", "color"},
				primaryOrder:   DESC,
				secondaryField: []string{"metadata", "name"},
				secondaryOrder: DESC,
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "honeycrisp",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "fuji",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name": "granny-smith",
						},
						"data": map[string]interface{}{
							"color": "green",
						},
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := SortList(test.objects, test.sort)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestPaginateList(t *testing.T) {
	objects := []unstructured.Unstructured{
		{
			Object: map[string]interface{}{
				"kind": "apple",
				"metadata": map[string]interface{}{
					"name": "fuji",
				},
			},
		},
		{
			Object: map[string]interface{}{
				"kind": "apple",
				"metadata": map[string]interface{}{
					"name": "honeycrisp",
				},
			},
		},
		{
			Object: map[string]interface{}{
				"kind": "apple",
				"metadata": map[string]interface{}{
					"name": "granny-smith",
				},
			},
		},
		{
			Object: map[string]interface{}{
				"kind": "apple",
				"metadata": map[string]interface{}{
					"name": "red-delicious",
				},
			},
		},
		{
			Object: map[string]interface{}{
				"kind": "apple",
				"metadata": map[string]interface{}{
					"name": "crispin",
				},
			},
		},
		{
			Object: map[string]interface{}{
				"kind": "apple",
				"metadata": map[string]interface{}{
					"name": "bramley",
				},
			},
		},
		{
			Object: map[string]interface{}{
				"kind": "apple",
				"metadata": map[string]interface{}{
					"name": "golden-delicious",
				},
			},
		},
		{
			Object: map[string]interface{}{
				"kind": "apple",
				"metadata": map[string]interface{}{
					"name": "macintosh",
				},
			},
		},
	}
	tests := []struct {
		name       string
		objects    []unstructured.Unstructured
		pagination Pagination
		want       []unstructured.Unstructured
		wantPages  int
	}{
		{
			name:    "pagesize=3, page=unset",
			objects: objects,
			pagination: Pagination{
				pageSize: 3,
			},
			want:      objects[:3],
			wantPages: 3,
		},
		{
			name:    "pagesize=3, page=1",
			objects: objects,
			pagination: Pagination{
				pageSize: 3,
				page:     1,
			},
			want:      objects[:3],
			wantPages: 3,
		},
		{
			name:    "pagesize=3, page=2",
			objects: objects,
			pagination: Pagination{
				pageSize: 3,
				page:     2,
			},
			want:      objects[3:6],
			wantPages: 3,
		},
		{
			name:    "pagesize=3, page=last",
			objects: objects,
			pagination: Pagination{
				pageSize: 3,
				page:     3,
			},
			want:      objects[6:],
			wantPages: 3,
		},
		{
			name:    "pagesize=3, page>last",
			objects: objects,
			pagination: Pagination{
				pageSize: 3,
				page:     37,
			},
			want:      []unstructured.Unstructured{},
			wantPages: 3,
		},
		{
			name:    "pagesize=3, page<0",
			objects: objects,
			pagination: Pagination{
				pageSize: 3,
				page:     -4,
			},
			want:      objects[:3],
			wantPages: 3,
		},
		{
			name:       "pagesize=0",
			objects:    objects,
			pagination: Pagination{},
			want:       objects,
			wantPages:  0,
		},
		{
			name:    "pagesize=-1",
			objects: objects,
			pagination: Pagination{
				pageSize: -7,
			},
			want:      objects,
			wantPages: 0,
		},
		{
			name:    "even page size, even list size",
			objects: objects,
			pagination: Pagination{
				pageSize: 2,
				page:     2,
			},
			want:      objects[2:4],
			wantPages: 4,
		},
		{
			name:    "even page size, odd list size",
			objects: objects[1:],
			pagination: Pagination{
				pageSize: 2,
				page:     2,
			},
			want:      objects[3:5],
			wantPages: 4,
		},
		{
			name:    "odd page size, even list size",
			objects: objects,
			pagination: Pagination{
				pageSize: 5,
				page:     2,
			},
			want:      objects[5:],
			wantPages: 2,
		},
		{
			name:    "odd page size, odd list size",
			objects: objects[1:],
			pagination: Pagination{
				pageSize: 3,
				page:     2,
			},
			want:      objects[4:7],
			wantPages: 3,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, gotPages := PaginateList(test.objects, test.pagination)
			assert.Equal(t, test.want, got)
			assert.Equal(t, test.wantPages, gotPages)
		})
	}
}

func TestFilterByProjectsAndNamespaces(t *testing.T) {
	tests := []struct {
		name    string
		objects []unstructured.Unstructured
		filter  ProjectsOrNamespacesFilter
		want    []unstructured.Unstructured
	}{
		{
			name: "filter by one project",
			objects: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "fuji",
							"namespace": "n1",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "granny-smith",
							"namespace": "n2",
						},
					},
				},
			},
			filter: ProjectsOrNamespacesFilter{
				filter: map[string]struct{}{
					"p-abcde": struct{}{},
				},
				op: eq,
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "fuji",
							"namespace": "n1",
						},
					},
				},
			},
		},
		{
			name: "filter by multiple projects",
			objects: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "fuji",
							"namespace": "n1",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "honeycrisp",
							"namespace": "n2",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "granny-smith",
							"namespace": "n3",
						},
					},
				},
			},
			filter: ProjectsOrNamespacesFilter{
				filter: map[string]struct{}{
					"p-abcde": struct{}{},
					"p-fghij": struct{}{},
				},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "fuji",
							"namespace": "n1",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "honeycrisp",
							"namespace": "n2",
						},
					},
				},
			},
		},
		{
			name: "filter by one namespace",
			objects: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "fuji",
							"namespace": "n1",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "granny-smith",
							"namespace": "n2",
						},
					},
				},
			},
			filter: ProjectsOrNamespacesFilter{
				filter: map[string]struct{}{
					"n1": struct{}{},
				},
				op: eq,
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "fuji",
							"namespace": "n1",
						},
					},
				},
			},
		},
		{
			name: "filter by multiple namespaces",
			objects: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "fuji",
							"namespace": "n1",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "honeycrisp",
							"namespace": "n2",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "granny-smith",
							"namespace": "n3",
						},
					},
				},
			},
			filter: ProjectsOrNamespacesFilter{
				filter: map[string]struct{}{
					"n1": struct{}{},
					"n2": struct{}{},
				},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "fuji",
							"namespace": "n1",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "honeycrisp",
							"namespace": "n2",
						},
					},
				},
			},
		},
		{
			name: "filter by namespaces and projects",
			objects: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "fuji",
							"namespace": "n1",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "honeycrisp",
							"namespace": "n2",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "granny-smith",
							"namespace": "n3",
						},
					},
				},
			},
			filter: ProjectsOrNamespacesFilter{
				filter: map[string]struct{}{
					"n1":      struct{}{},
					"p-fghij": struct{}{},
				},
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "fuji",
							"namespace": "n1",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "honeycrisp",
							"namespace": "n2",
						},
					},
				},
			},
		},
		{
			name: "no matches",
			objects: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "fuji",
							"namespace": "n1",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "honeycrisp",
							"namespace": "n2",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "granny-smith",
							"namespace": "n3",
						},
					},
				},
			},
			filter: ProjectsOrNamespacesFilter{
				filter: map[string]struct{}{
					"foobar": struct{}{},
				},
			},
			want: []unstructured.Unstructured{},
		},
		{
			name: "no filters",
			objects: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "fuji",
							"namespace": "n1",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "honeycrisp",
							"namespace": "n2",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "granny-smith",
							"namespace": "n3",
						},
					},
				},
			},
			filter: ProjectsOrNamespacesFilter{},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "fuji",
							"namespace": "n1",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "honeycrisp",
							"namespace": "n2",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "granny-smith",
							"namespace": "n3",
						},
					},
				},
			},
		},
		{
			name: "filter by one project negated",
			objects: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "fuji",
							"namespace": "n1",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "honeycrisp",
							"namespace": "n2",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "granny-smith",
							"namespace": "n3",
						},
					},
				},
			},
			filter: ProjectsOrNamespacesFilter{
				filter: map[string]struct{}{
					"p-abcde": struct{}{},
				},
				op: notEq,
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "honeycrisp",
							"namespace": "n2",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "granny-smith",
							"namespace": "n3",
						},
					},
				},
			},
		},
		{
			name: "filter by multiple projects negated",
			objects: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "fuji",
							"namespace": "n1",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "honeycrisp",
							"namespace": "n2",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "granny-smith",
							"namespace": "n3",
						},
					},
				},
			},
			filter: ProjectsOrNamespacesFilter{
				filter: map[string]struct{}{
					"p-abcde": struct{}{},
					"p-fghij": struct{}{},
				},
				op: notEq,
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "granny-smith",
							"namespace": "n3",
						},
					},
				},
			},
		},
		{
			name: "filter by one namespace negated",
			objects: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "fuji",
							"namespace": "n1",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "granny-smith",
							"namespace": "n2",
						},
					},
				},
			},
			filter: ProjectsOrNamespacesFilter{
				filter: map[string]struct{}{
					"n1": struct{}{},
				},
				op: notEq,
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "granny-smith",
							"namespace": "n2",
						},
					},
				},
			},
		},
		{
			name: "filter by multiple namespaces negated",
			objects: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "fuji",
							"namespace": "n1",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "honeycrisp",
							"namespace": "n2",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "granny-smith",
							"namespace": "n3",
						},
					},
				},
			},
			filter: ProjectsOrNamespacesFilter{
				filter: map[string]struct{}{
					"n1": struct{}{},
					"n2": struct{}{},
				},
				op: notEq,
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "granny-smith",
							"namespace": "n3",
						},
					},
				},
			},
		},
		{
			name: "filter by namespaces and projects negated",
			objects: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "fuji",
							"namespace": "n1",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "honeycrisp",
							"namespace": "n2",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "granny-smith",
							"namespace": "n3",
						},
					},
				},
			},
			filter: ProjectsOrNamespacesFilter{
				filter: map[string]struct{}{
					"n1":      struct{}{},
					"p-fghij": struct{}{},
				},
				op: notEq,
			},
			want: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "granny-smith",
							"namespace": "n3",
						},
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := FilterByProjectsAndNamespaces(test.objects, test.filter, mockNamespaceCache{})
			assert.Equal(t, test.want, got)
		})
	}
}

var namespaces = map[string]*corev1.Namespace{
	"n1": &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n1",
			Labels: map[string]string{
				"field.cattle.io/projectId": "p-abcde",
			},
		},
	},
	"n2": &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n2",
			Labels: map[string]string{
				"field.cattle.io/projectId": "p-fghij",
			},
		},
	},
	"n3": &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n3",
			Labels: map[string]string{
				"field.cattle.io/projectId": "p-klmno",
			},
		},
	},
	"n4": &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n4",
		},
	},
}

type mockNamespaceCache struct{}

func (m mockNamespaceCache) Get(name string) (*corev1.Namespace, error) {
	return namespaces[name], nil
}

func (m mockNamespaceCache) List(selector labels.Selector) ([]*corev1.Namespace, error) {
	panic("not implemented")
}
func (m mockNamespaceCache) AddIndexer(indexName string, indexer generic.Indexer[*corev1.Namespace]) {
	panic("not implemented")
}
func (m mockNamespaceCache) GetByIndex(indexName, key string) ([]*corev1.Namespace, error) {
	panic("not implemented")
}
