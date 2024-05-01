package partition_alpha

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/url"
	"strconv"
	"testing"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/lasso/pkg/cache/sql/partition"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/steve/pkg/stores/proxy_alpha"
	"github.com/rancher/wrangler/v2/pkg/generic"
	"github.com/rancher/wrangler/v2/pkg/schemas"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
)

// TODO: review these tests
func TestList(t *testing.T) {
	tests := []struct {
		name          string
		apiOps        []*types.APIRequest
		access        []map[string]string
		partitions    map[string][]partition.Partition
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
			},
			partitions: map[string][]partition.Partition{
				"user1": {
					partition.Partition{
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
			partitions: map[string][]partition.Partition{
				"user1": {
					partition.Partition{
						Namespace: "green",
						All:       true,
					},
					partition.Partition{
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
	store      proxy_alpha.Store
	partitions map[string][]partition.Partition
}

func (m mockPartitioner) Lookup(apiOp *types.APIRequest, schema *types.APISchema, verb, id string) (partition.Partition, error) {
	panic("not implemented")
}

func (m mockPartitioner) All(apiOp *types.APIRequest, schema *types.APISchema, verb, id string) ([]partition.Partition, error) {
	user, _ := request.UserFrom(apiOp.Request.Context())
	return m.partitions[user.GetName()], nil
}

func (m mockPartitioner) Store() proxy_alpha.Store {
	return m.store
}

type mockStore struct {
	contents  map[string]*unstructured.UnstructuredList
	partition partition.Partition
	called    map[string]int
}

func (m *mockStore) WatchByPartitions(apiOp *types.APIRequest, schema *types.APISchema, wr types.WatchRequest, partitions []partition.Partition) (chan watch.Event, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockStore) ListByPartitions(apiOp *types.APIRequest, schema *types.APISchema, partitions []partition.Partition) ([]unstructured.Unstructured, string, string, error) {
	list := []unstructured.Unstructured{}
	revision := ""
	for _, partition := range partitions {
		apiOp = apiOp.Clone()
		apiOp.Namespace = partition.Namespace
		partial, _, err := m.List(apiOp, schema)
		if err != nil {
			return nil, "", "", err
		}

		list = append(list, partial.Items...)
		revision = partial.GetResourceVersion()
	}
	return list, revision, "", nil
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
	meta := a.Object["metadata"].(map[string]interface{})
	id := meta["name"].(string)
	ns, ok := meta["namespace"]
	if ok {
		id = ns.(string) + "/" + id
	}
	return types.APIObject{
		Type:   "apple",
		ID:     id,
		Object: &a.Unstructured,
	}
}

func (a apple) with(data map[string]string) apple {
	for k, v := range data {
		a.Object["data"].(map[string]interface{})[k] = v
	}
	return a
}

func (a apple) withNamespace(namespace string) apple {
	a.Object["metadata"].(map[string]interface{})["namespace"] = namespace
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
