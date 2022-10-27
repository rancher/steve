package partition

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"testing"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/wrangler/pkg/schemas"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

func TestList(t *testing.T) {
	tests := []struct {
		name       string
		apiOps     []*types.APIRequest
		partitions []Partition
		objects    map[string]*unstructured.UnstructuredList
		want       []types.APIObjectList
	}{
		{
			name: "basic",
			apiOps: []*types.APIRequest{
				newRequest(""),
			},
			partitions: []Partition{
				mockPartition{
					name: "all",
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"all": {
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Objects: []types.APIObject{
						newApple("fuji").toObj(),
					},
				},
			},
		},
		{
			name: "limit and continue",
			apiOps: []*types.APIRequest{
				newRequest("limit=1"),
				newRequest(fmt.Sprintf("limit=1&continue=%s", base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"p":"all","c":"%s","l":1}`, base64.StdEncoding.EncodeToString([]byte("granny-smith"))))))),
				newRequest(fmt.Sprintf("limit=1&continue=%s", base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"p":"all","c":"%s","l":1}`, base64.StdEncoding.EncodeToString([]byte("crispin"))))))),
			},
			partitions: []Partition{
				mockPartition{
					name: "all",
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"all": {
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
						newApple("granny-smith").Unstructured,
						newApple("crispin").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Continue: base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"p":"all","c":"%s","l":1}`, base64.StdEncoding.EncodeToString([]byte("granny-smith"))))),
					Objects: []types.APIObject{
						newApple("fuji").toObj(),
					},
				},
				{
					Continue: base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"p":"all","c":"%s","l":1}`, base64.StdEncoding.EncodeToString([]byte("crispin"))))),
					Objects: []types.APIObject{
						newApple("granny-smith").toObj(),
					},
				},
				{
					Objects: []types.APIObject{
						newApple("crispin").toObj(),
					},
				},
			},
		},
		{
			name: "multi-partition",
			apiOps: []*types.APIRequest{
				newRequest(""),
			},
			partitions: []Partition{
				mockPartition{
					name: "green",
				},
				mockPartition{
					name: "yellow",
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"pink": {
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
					},
				},
				"green": {
					Items: []unstructured.Unstructured{
						newApple("granny-smith").Unstructured,
					},
				},
				"yellow": {
					Items: []unstructured.Unstructured{
						newApple("crispin").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Objects: []types.APIObject{
						newApple("granny-smith").toObj(),
						newApple("crispin").toObj(),
					},
				},
			},
		},
		{
			name: "multi-partition with limit and continue",
			apiOps: []*types.APIRequest{
				newRequest("limit=3"),
				newRequest(fmt.Sprintf("limit=3&continue=%s", base64.StdEncoding.EncodeToString([]byte(`{"p":"green","o":1,"l":3}`)))),
				newRequest(fmt.Sprintf("limit=3&continue=%s", base64.StdEncoding.EncodeToString([]byte(`{"p":"red","l":3}`)))),
			},
			partitions: []Partition{
				mockPartition{
					name: "pink",
				},
				mockPartition{
					name: "green",
				},
				mockPartition{
					name: "yellow",
				},
				mockPartition{
					name: "red",
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"pink": {
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
						newApple("honeycrisp").Unstructured,
					},
				},
				"green": {
					Items: []unstructured.Unstructured{
						newApple("granny-smith").Unstructured,
						newApple("bramley").Unstructured,
					},
				},
				"yellow": {
					Items: []unstructured.Unstructured{
						newApple("crispin").Unstructured,
						newApple("golden-delicious").Unstructured,
					},
				},
				"red": {
					Items: []unstructured.Unstructured{
						newApple("red-delicious").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Continue: base64.StdEncoding.EncodeToString([]byte(`{"p":"green","o":1,"l":3}`)),
					Objects: []types.APIObject{
						newApple("fuji").toObj(),
						newApple("honeycrisp").toObj(),
						newApple("granny-smith").toObj(),
					},
				},
				{
					Continue: base64.StdEncoding.EncodeToString([]byte(`{"p":"red","l":3}`)),
					Objects: []types.APIObject{
						newApple("bramley").toObj(),
						newApple("crispin").toObj(),
						newApple("golden-delicious").toObj(),
					},
				},
				{
					Objects: []types.APIObject{
						newApple("red-delicious").toObj(),
					},
				},
			},
		},
		{
			name: "with filters",
			apiOps: []*types.APIRequest{
				newRequest("filter=data.color=green"),
				newRequest("filter=data.color=green&filter=metadata.name=bramley"),
			},
			partitions: []Partition{
				mockPartition{
					name: "all",
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"all": {
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
						newApple("granny-smith").Unstructured,
						newApple("bramley").Unstructured,
						newApple("crispin").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Objects: []types.APIObject{
						newApple("granny-smith").toObj(),
						newApple("bramley").toObj(),
					},
				},
				{
					Objects: []types.APIObject{
						newApple("bramley").toObj(),
					},
				},
			},
		},
		{
			name: "multi-partition with filters",
			apiOps: []*types.APIRequest{
				newRequest("filter=data.category=baking"),
			},
			partitions: []Partition{
				mockPartition{
					name: "pink",
				},
				mockPartition{
					name: "green",
				},
				mockPartition{
					name: "yellow",
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"pink": {
					Items: []unstructured.Unstructured{
						newApple("fuji").with(map[string]string{"category": "eating"}).Unstructured,
						newApple("honeycrisp").with(map[string]string{"category": "eating,baking"}).Unstructured,
					},
				},
				"green": {
					Items: []unstructured.Unstructured{
						newApple("granny-smith").with(map[string]string{"category": "baking"}).Unstructured,
						newApple("bramley").with(map[string]string{"category": "eating"}).Unstructured,
					},
				},
				"yellow": {
					Items: []unstructured.Unstructured{
						newApple("crispin").with(map[string]string{"category": "baking"}).Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Objects: []types.APIObject{
						newApple("honeycrisp").with(map[string]string{"category": "eating,baking"}).toObj(),
						newApple("granny-smith").with(map[string]string{"category": "baking"}).toObj(),
						newApple("crispin").with(map[string]string{"category": "baking"}).toObj(),
					},
				},
			},
		},
		{
			name: "with sorting",
			apiOps: []*types.APIRequest{
				newRequest("sort=metadata.name"),
				newRequest("sort=-metadata.name"),
			},
			partitions: []Partition{
				mockPartition{
					name: "all",
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"all": {
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
						newApple("granny-smith").Unstructured,
						newApple("bramley").Unstructured,
						newApple("crispin").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Objects: []types.APIObject{
						newApple("bramley").toObj(),
						newApple("crispin").toObj(),
						newApple("fuji").toObj(),
						newApple("granny-smith").toObj(),
					},
				},
				{
					Objects: []types.APIObject{
						newApple("granny-smith").toObj(),
						newApple("fuji").toObj(),
						newApple("crispin").toObj(),
						newApple("bramley").toObj(),
					},
				},
			},
		},
		{
			name: "multi-partition sort=metadata.name",
			apiOps: []*types.APIRequest{
				newRequest("sort=metadata.name"),
			},
			partitions: []Partition{
				mockPartition{
					name: "green",
				},
				mockPartition{
					name: "yellow",
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"pink": {
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
					},
				},
				"green": {
					Items: []unstructured.Unstructured{
						newApple("granny-smith").Unstructured,
					},
				},
				"yellow": {
					Items: []unstructured.Unstructured{
						newApple("crispin").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Objects: []types.APIObject{
						newApple("crispin").toObj(),
						newApple("granny-smith").toObj(),
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema := &types.APISchema{Schema: &schemas.Schema{ID: "apple"}}
			stores := map[string]*mockStore{}
			for _, p := range test.partitions {
				stores[p.Name()] = &mockStore{
					contents: test.objects[p.Name()],
				}
			}
			store := Store{
				Partitioner: mockPartitioner{
					stores:     stores,
					partitions: test.partitions,
				},
			}
			for i, req := range test.apiOps {
				got, gotErr := store.List(req, schema)
				assert.Nil(t, gotErr)
				assert.Equal(t, test.want[i], got)
			}
		})
	}
}

type mockPartitioner struct {
	stores     map[string]*mockStore
	partitions []Partition
}

func (m mockPartitioner) Lookup(apiOp *types.APIRequest, schema *types.APISchema, verb, id string) (Partition, error) {
	panic("not implemented")
}

func (m mockPartitioner) All(apiOp *types.APIRequest, schema *types.APISchema, verb, id string) ([]Partition, error) {
	return m.partitions, nil
}

func (m mockPartitioner) Store(apiOp *types.APIRequest, partition Partition) (UnstructuredStore, error) {
	return m.stores[partition.Name()], nil
}

type mockPartition struct {
	name string
}

func (m mockPartition) Name() string {
	return m.name
}

type mockStore struct {
	contents  *unstructured.UnstructuredList
	partition mockPartition
}

func (m *mockStore) List(apiOp *types.APIRequest, schema *types.APISchema) (*unstructured.UnstructuredList, error) {
	query, _ := url.ParseQuery(apiOp.Request.URL.RawQuery)
	l := query.Get("limit")
	if l == "" {
		return m.contents, nil
	}
	i := 0
	if c := query.Get("continue"); c != "" {
		start, _ := base64.StdEncoding.DecodeString(c)
		for j, obj := range m.contents.Items {
			if string(start) == obj.GetName() {
				i = j
				break
			}
		}
	}
	lInt, _ := strconv.Atoi(l)
	contents := m.contents.DeepCopy()
	if len(contents.Items) > i+lInt {
		contents.SetContinue(base64.StdEncoding.EncodeToString([]byte(contents.Items[i+lInt].GetName())))
	}
	if i > len(contents.Items) {
		return contents, nil
	}
	if i+lInt > len(contents.Items) {
		contents.Items = contents.Items[i:]
		return contents, nil
	}
	contents.Items = contents.Items[i : i+lInt]
	return contents, nil
}

func (m *mockStore) ByID(apiOp *types.APIRequest, schema *types.APISchema, id string) (*unstructured.Unstructured, error) {
	panic("not implemented")
}

func (m *mockStore) Create(apiOp *types.APIRequest, schema *types.APISchema, data types.APIObject) (*unstructured.Unstructured, error) {
	panic("not implemented")
}

func (m *mockStore) Update(apiOp *types.APIRequest, schema *types.APISchema, data types.APIObject, id string) (*unstructured.Unstructured, error) {
	panic("not implemented")
}

func (m *mockStore) Delete(apiOp *types.APIRequest, schema *types.APISchema, id string) (*unstructured.Unstructured, error) {
	panic("not implemented")
}

func (m *mockStore) Watch(apiOp *types.APIRequest, schema *types.APISchema, w types.WatchRequest) (chan watch.Event, error) {
	panic("not implemented")
}

var colorMap = map[string]string{
	"fuji":             "pink",
	"honeycrisp":       "pink",
	"granny-smith":     "green",
	"bramley":          "green",
	"crispin":          "yellow",
	"golden-delicious": "yellow",
	"red-delicious":    "red",
}

func newRequest(query string) *types.APIRequest {
	return &types.APIRequest{
		Request: &http.Request{
			URL: &url.URL{
				Scheme:   "https",
				Host:     "rancher",
				Path:     "/apples",
				RawQuery: query,
			},
		},
	}
}

type apple struct {
	unstructured.Unstructured
}

func newApple(name string) apple {
	return apple{unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind": "apple",
			"metadata": map[string]interface{}{
				"name": name,
			},
			"data": map[string]interface{}{
				"color": colorMap[name],
			},
		},
	}}
}

func (a apple) toObj() types.APIObject {
	return types.APIObject{
		Type:   "apple",
		ID:     a.Object["metadata"].(map[string]interface{})["name"].(string),
		Object: &a.Unstructured,
	}
}

func (a apple) with(data map[string]string) apple {
	for k, v := range data {
		a.Object["data"].(map[string]interface{})[k] = v
	}
	return a
}
