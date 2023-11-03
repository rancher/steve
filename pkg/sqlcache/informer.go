package sqlcache

import (
	"context"
	"time"

	"github.com/rancher/steve/pkg/stores/partition/listprocessor"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
)

/*
type Informer interface {
	cache.SharedIndexInformer
	// ListByOptions returns objects according to the specified list options and partitions
	// see ListOptionIndexer.ListByOptions
	ListByOptions(lo *listprocessor.ListOptions, partitions []listprocessor.Partition, namespace string) (*unstructured.UnstructuredList, string, error)
}*/

// Informer is a SQLite-backed cache.SharedIndexInformer that can execute queries on listprocessor structs
type Informer struct {
	cache.SharedIndexInformer
	indexer *ListOptionIndexer
}

// NewInformer returns a new SQLite-backed Informer for the type specified by schema in unstructured.Unstructured form
// using the specified client
func NewInformer(client dynamic.ResourceInterface, gvk schema.GroupVersionKind, path string) (*Informer, error) {
	listWatcher := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return client.List(context.TODO(), options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return client.Watch(context.TODO(), options)
		},
	}

	example := &unstructured.Unstructured{}
	example.SetGroupVersionKind(gvk)

	// avoids the informer to periodically resync (re-list) its resources
	// currently it is a work hypothesis that, when interacting with the UI, this should not be needed
	resyncPeriod := time.Duration(0)

	sii := cache.NewSharedIndexInformer(listWatcher, example, resyncPeriod, cache.Indexers{})

	// temp: make a default selection of interesting indexedFields
	fields := [][]string{{"metadata", "creationTimestamp"}}

	name := gvk.Group + "_" + gvk.Version + "_" + gvk.Kind
	loi, err := NewListOptionIndexer(example, cache.DeletionHandlingMetaNamespaceKeyFunc, fields, name, path)
	if err != nil {
		return nil, err
	}

	// HACK: replace the default informer's indexer with the SQL based one
	UnsafeSet(sii, "indexer", loi)

	return &Informer{
		SharedIndexInformer: sii,
		indexer:             loi,
	}, nil
}

// ListByOptions returns objects according to the specified list options and partitions
// see ListOptionIndexer.ListByOptions
func (i *Informer) ListByOptions(lo *listprocessor.ListOptions, partitions []listprocessor.Partition, namespace string) (*unstructured.UnstructuredList, string, error) {
	return i.indexer.ListByOptions(lo, partitions, namespace)
}
