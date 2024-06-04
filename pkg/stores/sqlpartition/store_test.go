package sqlpartition

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/rancher/wrangler/v3/pkg/schemas"
	"github.com/stretchr/testify/assert"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/lasso/pkg/cache/sql/partition"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/steve/pkg/stores/sqlproxy"
	"github.com/rancher/wrangler/v3/pkg/generic"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
)

//go:generate mockgen --build_flags=--mod=mod -package sqlpartition -destination partition_mocks_test.go "github.com/rancher/steve/pkg/stores/sqlpartition" Partitioner,UnstructuredStore

func TestList(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}
	var tests []testCase
	tests = append(tests, testCase{
		description: "List() with no errors returned should returned no errors. Should have empty reivsion, count " +
			"should match number of items in list, and id should include namespace (if applicable) and name, separated" +
			" by a '/'.",
		test: func(t *testing.T) {
			p := NewMockPartitioner(gomock.NewController(t))
			us := NewMockUnstructuredStore(gomock.NewController(t))
			s := Store{
				Partitioner: p,
			}
			req := &types.APIRequest{}
			schema := &types.APISchema{
				Schema: &schemas.Schema{},
			}
			partitions := make([]partition.Partition, 0)
			uListToReturn := []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"kind": "apple",
						"metadata": map[string]interface{}{
							"name":      "fuji",
							"namespace": "fruitsnamespace",
						},
						"data": map[string]interface{}{
							"color": "pink",
						},
					},
				},
			}
			expectedAPIObjList := types.APIObjectList{
				Count:    1,
				Revision: "",
				Objects: []types.APIObject{
					{
						Object: &unstructured.Unstructured{
							Object: map[string]interface{}{
								"kind": "apple",
								"metadata": map[string]interface{}{
									"name":      "fuji",
									"namespace": "fruitsnamespace",
								},
								"data": map[string]interface{}{
									"color": "pink",
								},
							},
						},
						ID: "fruitsnamespace/fuji",
					},
				},
			}
			p.EXPECT().All(req, schema, "list", "").Return(partitions, nil)
			p.EXPECT().Store().Return(us)
			us.EXPECT().ListByPartitions(req, schema, partitions).Return(uListToReturn, "", nil)
			l, err := s.List(req, schema)
			assert.Nil(t, err)
			assert.Equal(t, expectedAPIObjList, l)
		},
	})
	tests = append(tests, testCase{
		description: "List() with partitioner All() error returned should returned an error.",
		test: func(t *testing.T) {
			p := NewMockPartitioner(gomock.NewController(t))
			s := Store{
				Partitioner: p,
			}
			req := &types.APIRequest{}
			schema := &types.APISchema{
				Schema: &schemas.Schema{},
			}
			p.EXPECT().All(req, schema, "list", "").Return(nil, fmt.Errorf("error"))
			_, err := s.List(req, schema)
			assert.NotNil(t, err)
		},
	})
	tests = append(tests, testCase{
		description: "List() with unstructured store ListByPartitions() error returned should returned an error.",
		test: func(t *testing.T) {
			p := NewMockPartitioner(gomock.NewController(t))
			us := NewMockUnstructuredStore(gomock.NewController(t))
			s := Store{
				Partitioner: p,
			}
			req := &types.APIRequest{}
			schema := &types.APISchema{
				Schema: &schemas.Schema{},
			}
			partitions := make([]partition.Partition, 0)
			p.EXPECT().All(req, schema, "list", "").Return(partitions, nil)
			p.EXPECT().Store().Return(us)
			us.EXPECT().ListByPartitions(req, schema, partitions).Return(nil, "", fmt.Errorf("error"))
			_, err := s.List(req, schema)
			assert.NotNil(t, err)
		},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

type mockPartitioner struct {
	store      sqlproxy.Store
	partitions map[string][]partition.Partition
}

func (m mockPartitioner) Lookup(apiOp *types.APIRequest, schema *types.APISchema, verb, id string) (partition.Partition, error) {
	panic("not implemented")
}

func (m mockPartitioner) All(apiOp *types.APIRequest, schema *types.APISchema, verb, id string) ([]partition.Partition, error) {
	user, _ := request.UserFrom(apiOp.Request.Context())
	return m.partitions[user.GetName()], nil
}

func (m mockPartitioner) Store() sqlproxy.Store {
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
