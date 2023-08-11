package cache

import (
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestGetCacheKey(t *testing.T) {
	opts := v1.ListOptions{
		LabelSelector:   "asd",
		ResourceVersion: "1234",
		FieldSelector:   "asdf",
		Watch:           true,
		Continue:        "abcdasdf",
	}
	resourcePath := "asdf"
	ns := "default"
	key := GetCacheKey(opts, resourcePath, ns)
	assert.Equal(t, CacheKey{listOptions: opts, resourcePath: resourcePath, namespace: ns}, key)
}

func TestString(t *testing.T) {
	key := CacheKey{listOptions: v1.ListOptions{
		LabelSelector:   "asd",
		ResourceVersion: "1234",
		FieldSelector:   "asdf",
		Watch:           true,
		Continue:        "abcdasdf",
	}, resourcePath: "clusters", namespace: "testns"}
	assert.Equal(t, "listOptions: &ListOptions{LabelSelector:asd,FieldSelector:asdf,Watch:true,ResourceVersion:1234,TimeoutSeconds:nil,Limit:0,Continue:abcdasdf,AllowWatchBookmarks:false,ResourceVersionMatch:,}, resourcePath: clusters, namespace: testns", key.String())
}

func TestAdd(t *testing.T) {
	type addRequest struct {
		objectList  *unstructured.UnstructuredList
		expectedErr bool
		key         CacheKey
	}
	tests := []struct {
		name        string
		sizeLimit   int
		addRequests []addRequest
	}{
		{
			name:      "test adding an entry that does not exceed limit",
			sizeLimit: 10000,
			addRequests: []addRequest{
				{
					objectList: &unstructured.UnstructuredList{
						Object: map[string]interface{}{
							"somefakeobj": "asdf",
						},
						Items: []unstructured.Unstructured{
							{
								Object: map[string]interface{}{
									"somefakeobj": "asd",
								},
							},
							{
								Object: map[string]interface{}{
									"somefakeobj2": "asaasdfd",
								},
							},
						},
					},
					key: CacheKey{
						resourcePath: "obj1",
						listOptions: v1.ListOptions{
							ResourceVersion: "1",
						},
					},
				},
			},
		},
		{
			name:      "update object already in cache", // this is not very likely to happen with a 30 minute expiration and always having a revision number
			sizeLimit: 10000,
			addRequests: []addRequest{
				{
					objectList: &unstructured.UnstructuredList{
						Object: map[string]interface{}{
							"somefakeobj": "asdf",
						},
						Items: []unstructured.Unstructured{
							{
								Object: map[string]interface{}{
									"somefakeobj": "asd",
								},
							},
							{
								Object: map[string]interface{}{
									"somefakeobj2": "asaasdfd",
								},
							},
						},
					},
					key: CacheKey{
						resourcePath: "obj1",
						listOptions: v1.ListOptions{
							ResourceVersion: "1",
						},
					},
				},
				{
					objectList: &unstructured.UnstructuredList{
						Object: map[string]interface{}{
							"somefakeobj": "asdf",
						},
						Items: []unstructured.Unstructured{
							{
								Object: map[string]interface{}{
									"somefakeobj": "asd",
								},
							},
							{
								Object: map[string]interface{}{
									"somefakeobj2": "asaasdfd",
								},
							},
							{
								Object: map[string]interface{}{
									"somefakeobj3": "asaasdfd",
								},
							},
						},
					},
					key: CacheKey{
						resourcePath: "obj1",
						listOptions: v1.ListOptions{
							ResourceVersion: "1",
						},
					},
				},
			},
		},
		{
			name:      "test adding an entry while above limit",
			sizeLimit: 10000,
			addRequests: []addRequest{
				{
					objectList: &unstructured.UnstructuredList{
						Object: map[string]interface{}{
							"somefakeobj": "asdf",
						},
						Items: []unstructured.Unstructured{
							{
								Object: map[string]interface{}{
									"somefakeobj": "asd",
								},
							},
							{
								Object: map[string]interface{}{
									"somefakeobj2": "asaasdfd",
								},
							},
						},
					},
					key: CacheKey{
						resourcePath: "obj1",
						listOptions: v1.ListOptions{
							ResourceVersion: "1",
						},
					},
				},
				{
					objectList: &unstructured.UnstructuredList{
						Object: map[string]interface{}{
							"somefakeobj": "asdf",
						},
						Items: []unstructured.Unstructured{
							{
								Object: map[string]interface{}{
									"somefakeobj": "asd",
								},
							},
							{
								Object: map[string]interface{}{
									"somefakeobj2": "asaasdfd",
								},
							},
							{
								Object: map[string]interface{}{
									"somefakeobj3": strings.Repeat("a", 10000),
								},
							},
						},
					},
					key: CacheKey{
						resourcePath: "obj2",
						listOptions: v1.ListOptions{
							ResourceVersion: "1",
						},
					},
					expectedErr: true,
				},
			},
		},
		{
			name:      "add obj that does not exceed limit because it replaces current entry",
			sizeLimit: 10000,
			addRequests: []addRequest{
				{
					objectList: &unstructured.UnstructuredList{
						Object: map[string]interface{}{
							"somefakeobj": "asdf",
						},
						Items: []unstructured.Unstructured{
							{
								Object: map[string]interface{}{
									"somefakeobj1": "asd",
								},
							},
							{
								Object: map[string]interface{}{
									"somefakeobj3": strings.Repeat("a", 7500),
								},
							},
						},
					},
					key: CacheKey{
						resourcePath: "obj1",
						listOptions: v1.ListOptions{
							ResourceVersion: "1",
						},
					},
				},
				{
					objectList: &unstructured.UnstructuredList{
						Object: map[string]interface{}{
							"somefakeobj": "asdf",
						},
						Items: []unstructured.Unstructured{
							{
								Object: map[string]interface{}{
									"somefakeobj1": "asd",
								},
							},
							{
								Object: map[string]interface{}{
									"somefakeobj2": "asaasdfd",
								},
							},
							{
								Object: map[string]interface{}{
									"somefakeobj3": strings.Repeat("a", 7500),
								},
							},
						},
					},
					key: CacheKey{
						resourcePath: "obj1",
						listOptions: v1.ListOptions{
							ResourceVersion: "1",
						},
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testCache := NewSizedRevisionCache(test.sizeLimit, 10)
			for _, ar := range test.addRequests {
				err := testCache.Add(ar.key, ar.objectList)
				obj, ok := testCache.listRevisionCache.Get(ar.key)
				cObj, _ := obj.(cacheObj)
				if !ar.expectedErr {
					assert.Nil(t, err)
					assert.True(t, ok)
					assert.Equal(t, ar.objectList, cObj.obj)
					continue
				}
				assert.NotNil(t, err)
				if !ok {
					continue
				}
				assert.NotEqual(t, ar.objectList, cObj.obj)

			}
		})
	}
}

func TestGet(t *testing.T) {
	c := NewSizedRevisionCache(10000, 10)
	key := GetCacheKey(v1.ListOptions{
		ResourceVersion: "1000",
	}, "something", "default")
	addedList := &unstructured.UnstructuredList{
		Object: map[string]interface{}{
			"somefakeobj": "asdf",
		},
		Items: []unstructured.Unstructured{
			{
				Object: map[string]interface{}{
					"somefakeobj": "asd",
				},
			},
		},
	}
	err := c.Add(key, addedList)
	assert.Nil(t, err)

	// get existing list
	list, err := c.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, addedList, list)

	// get existing list at different revision
	list, err = c.Get(GetCacheKey(v1.ListOptions{
		ResourceVersion: "900",
	}, "somethingelse", "default"))
	assert.Nil(t, list)
	assert.True(t, errors.Is(ErrNotFound, err))

	// get existing list with different ns
	list, err = c.Get(GetCacheKey(v1.ListOptions{
		ResourceVersion: "1000",
	}, "somethingelse", "asdf"))
	assert.Nil(t, list)
	assert.True(t, errors.Is(ErrNotFound, err))

	// get existing list with different cont
	list, err = c.Get(GetCacheKey(v1.ListOptions{
		ResourceVersion: "1000",
		Continue:        "asdf",
	}, "somethingelse", "default"))
	assert.Nil(t, list)
	assert.True(t, errors.Is(ErrNotFound, err))

	// get list that does not exist
	list, err = c.Get(GetCacheKey(v1.ListOptions{
		ResourceVersion: "1000",
	}, "somethingelse", "default"))
	assert.Nil(t, list)
	assert.True(t, errors.Is(ErrNotFound, err))
}
