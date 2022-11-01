package listprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestFilterList(t *testing.T) {
	tests := []struct {
		name    string
		objects [][]unstructured.Unstructured
		filters []Filter
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
			filters: []Filter{
				{
					field: []string{"data", "color"},
					match: "pink",
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
			filters: []Filter{
				{
					field: []string{"data", "color"},
					match: "pink",
				},
				{
					field: []string{"metadata", "name"},
					match: "honey",
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
			filters: []Filter{
				{
					field: []string{"data", "color"},
					match: "purple",
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
			filters: []Filter{},
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
			filters: []Filter{
				{
					field: []string{"spec", "volumes"},
					match: "hostPath",
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
			filters: []Filter{
				{
					field: []string{"data", "productType"},
					match: "tablet",
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
			filters: []Filter{
				{
					field: []string{"data", "color", "shade"},
					match: "green",
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
			filters: []Filter{
				{
					field: []string{"data", "colors"},
					match: "yellow",
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
			filters: []Filter{
				{
					field: []string{"data", "varieties", "color"},
					match: "red",
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
			filters: []Filter{
				{
					field: []string{"data", "attributes"},
					match: "black",
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
			filters: []Filter{
				{
					field: []string{"data", "attributes", "green"},
					match: "plantain",
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
				field: []string{"metadata", "name"},
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
				field: []string{"metadata", "name"},
				order: DESC,
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
				field: []string{"data", "productType"},
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
