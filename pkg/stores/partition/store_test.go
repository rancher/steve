package partition

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"github.com/rancher/steve/pkg/stores/partition/listprocessor"
	"net/http"
	"net/url"
	"strconv"
	"testing"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/wrangler/pkg/schemas"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
)

func TestList(t *testing.T) {
	tests := []struct {
		name          string
		apiOps        []*types.APIRequest
		access        []map[string]string
		partitions    map[string][]listprocessor.Partition
		objects       map[string]*unstructured.UnstructuredList
		want          []types.APIObjectList
		wantListCalls []map[string]int
	}{
		{
			name: "basic",
			apiOps: []*types.APIRequest{
				newRequest("", "user1"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
			},
			partitions: map[string][]listprocessor.Partition{
				"user1": {
					listprocessor.Partition{
						Namespace: "all",
						All:       true,
					},
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
					Count: 1,
					Objects: []types.APIObject{
						newApple("fuji").toObj(),
					},
				},
			},
		},
		{
			name: "multi-partition",
			apiOps: []*types.APIRequest{
				newRequest("", "user1"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
			},
			partitions: map[string][]listprocessor.Partition{
				"user1": {
					listprocessor.Partition{
						Namespace: "green",
						All:       true,
					},
					listprocessor.Partition{
						Namespace: "yellow",
						All:       true,
					},
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
					Count: 2,
					Objects: []types.APIObject{
						newApple("granny-smith").toObj(),
						newApple("crispin").toObj(),
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema := &types.APISchema{Schema: &schemas.Schema{ID: "apple"}}
			asl := &mockAccessSetLookup{userRoles: test.access}
			store := NewStore(mockPartitioner{
				store: &mockStore{
					contents: test.objects,
					called:   map[string]int{},
				},
				partitions: test.partitions,
			}, asl)
			for i, req := range test.apiOps {
				got, gotErr := store.List(req, schema)
				assert.Nil(t, gotErr)
				assert.Equal(t, test.want[i], got)
				if len(test.wantListCalls) > 0 {
					for name, called := range store.Partitioner.(mockPartitioner).store.(*mockStore).called {
						assert.Equal(t, test.wantListCalls[i][name], called)
					}
				}
			}
		})
	}
}

type mockPartitioner struct {
	store      UnstructuredStore
	partitions map[string][]listprocessor.Partition
}

func (m mockPartitioner) Lookup(apiOp *types.APIRequest, schema *types.APISchema, verb, id string) (listprocessor.Partition, error) {
	panic("not implemented")
}

func (m mockPartitioner) All(apiOp *types.APIRequest, schema *types.APISchema, verb, id string) ([]listprocessor.Partition, error) {
	user, _ := request.UserFrom(apiOp.Request.Context())
	return m.partitions[user.GetName()], nil
}

func (m mockPartitioner) Store() UnstructuredStore {
	return m.store
}

type mockStore struct {
	contents  map[string]*unstructured.UnstructuredList
	partition listprocessor.Partition
	called    map[string]int
}

func (m *mockStore) WatchByPartitions(apiOp *types.APIRequest, schema *types.APISchema, wr types.WatchRequest, partitions []listprocessor.Partition) (chan watch.Event, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockStore) ListByPartitions(apiOp *types.APIRequest, schema *types.APISchema, partitions []listprocessor.Partition) ([]unstructured.Unstructured, string, error) {
	list := []unstructured.Unstructured{}
	revision := ""
	for _, partition := range partitions {
		apiOp = apiOp.Clone()
		apiOp.Namespace = partition.Namespace
		partial, _, err := m.List(apiOp, schema)
		if err != nil {
			return nil, "", err
		}

		list = append(list, partial.Items...)
		revision = partial.GetResourceVersion()
	}
	return list, revision, nil
}

func (m *mockStore) List(apiOp *types.APIRequest, schema *types.APISchema) (*unstructured.UnstructuredList, []types.Warning, error) {
	n := apiOp.Namespace
	previous, ok := m.called[n]
	if !ok {
		m.called[n] = 1
	} else {
		m.called[n] = previous + 1
	}
	query, _ := url.ParseQuery(apiOp.Request.URL.RawQuery)
	l := query.Get("limit")
	if l == "" {
		return m.contents[n], nil, nil
	}
	i := 0
	if c := query.Get("continue"); c != "" {
		start, _ := base64.StdEncoding.DecodeString(c)
		for j, obj := range m.contents[n].Items {
			if string(start) == obj.GetName() {
				i = j
				break
			}
		}
	}
	lInt, _ := strconv.Atoi(l)
	contents := m.contents[n].DeepCopy()
	if len(contents.Items) > i+lInt {
		contents.SetContinue(base64.StdEncoding.EncodeToString([]byte(contents.Items[i+lInt].GetName())))
	}
	if i > len(contents.Items) {
		return contents, nil, nil
	}
	if i+lInt > len(contents.Items) {
		contents.Items = contents.Items[i:]
		return contents, nil, nil
	}
	contents.Items = contents.Items[i : i+lInt]
	return contents, nil, nil
}

func (m *mockStore) ByID(apiOp *types.APIRequest, schema *types.APISchema, id string) (*unstructured.Unstructured, []types.Warning, error) {
	panic("not implemented")
}

func (m *mockStore) Create(apiOp *types.APIRequest, schema *types.APISchema, data types.APIObject) (*unstructured.Unstructured, []types.Warning, error) {
	panic("not implemented")
}

func (m *mockStore) Update(apiOp *types.APIRequest, schema *types.APISchema, data types.APIObject, id string) (*unstructured.Unstructured, []types.Warning, error) {
	panic("not implemented")
}

func (m *mockStore) Delete(apiOp *types.APIRequest, schema *types.APISchema, id string) (*unstructured.Unstructured, []types.Warning, error) {
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

func newRequest(query, username string) *types.APIRequest {
	return &types.APIRequest{
		Request: (&http.Request{
			URL: &url.URL{
				Scheme:   "https",
				Host:     "rancher",
				Path:     "/apples",
				RawQuery: query,
			},
		}).WithContext(request.WithUser(context.Background(), &user.DefaultInfo{
			Name:   username,
			Groups: []string{"system:authenticated"},
		})),
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

type mockAccessSetLookup struct {
	accessID  string
	userRoles []map[string]string
}

func (m *mockAccessSetLookup) AccessFor(user user.Info) *accesscontrol.AccessSet {
	userName := user.GetName()
	access := getAccessID(userName, m.userRoles[0][userName])
	m.userRoles = m.userRoles[1:]
	return &accesscontrol.AccessSet{
		ID: access,
	}
}

func (m *mockAccessSetLookup) PurgeUserData(_ string) {
	panic("not implemented")
}

func getAccessID(user, role string) string {
	h := sha256.Sum256([]byte(user + role))
	return string(h[:])
}
