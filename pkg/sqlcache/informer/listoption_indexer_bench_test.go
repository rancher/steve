package informer

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/rancher/steve/pkg/sqlcache/db"
	"github.com/rancher/steve/pkg/sqlcache/encoding"
	"github.com/rancher/steve/pkg/sqlcache/encryption"
	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
	"github.com/rancher/steve/pkg/sqlcache/store"
	"github.com/rancher/wrangler/v3/pkg/unstructured"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

var random = rand.New(rand.NewSource(0))

func BenchmarkListOptionIndexerEncodings(b *testing.B) {
	// Cache will be populated with 5000 ConfigMaps, 10KB each
	const items = 5000
	testData := generateTestData(10000) // a single entry map with 10K zero characters should account for ~10KB ConfigMap size

	testCases := []struct {
		name     string
		encoding encoding.Encoding
	}{
		{name: "gob", encoding: encoding.EncodingGob},
		{name: "json", encoding: encoding.EncodingJSON},
		{name: "json+gzipped", encoding: encoding.EncodingGzipJSON},
	}
	for _, tt := range testCases {
		b.Run(fmt.Sprintf("%s", tt.name), func(b *testing.B) {
			if err := os.Setenv("CATTLE_SQL_CACHE_ENCODING", string(tt.encoding)); err != nil {
				b.Fatal(err)
			}
			defer os.Unsetenv("CATTLE_SQL_CACHE_ENCODING")

			informer, dbPath := initStore(b)

			b.Run("setup", func(b *testing.B) {
				if err := populateCache(informer, items, testData); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				b.ReportMetric(float64(getDirSize(b, dbPath)/1024), "KB-dbsize")
			})

			b.Run("list_all", func(b *testing.B) {
				for b.Loop() {
					partitions := []partition.Partition{{
						All: true,
					}}
					options := &sqltypes.ListOptions{}
					_, total, _, err := informer.ListByOptions(b.Context(), options, partitions, "testns")
					if err != nil {
						b.Fatal(err)
					} else if total != items {
						b.Errorf("unexpected total number of items, got %d, want %d", total, items)
					}
				}
			})
			b.Run("list_paginated", func(b *testing.B) {
				for b.Loop() {
					partitions := []partition.Partition{{
						All: true,
					}}
					options := &sqltypes.ListOptions{
						Pagination: sqltypes.Pagination{
							PageSize: 100,
						},
					}
					var count int
					for {
						list, total, _, err := informer.ListByOptions(b.Context(), options, partitions, "testns")
						if err != nil {
							b.Fatal(err)
						} else if total != items {
							b.Errorf("unexpected total number of items, got %d, want %d", total, items)
						}
						count += len(list.Items)
						if count < items {
							break
						}
						options.Pagination.Page += 1
					}
				}
			})

			b.Run("random_access_by_key", func(b *testing.B) {
				for b.Loop() {
					cmKey := fmt.Sprintf("testns/testcm-%06d", random.Int63()%items)
					_, exists, err := informer.GetByKey(cmKey)
					if err != nil {
						b.Fatal(err)
					} else if !exists {
						b.Errorf("key not found: %s", cmKey)
					}
				}
			})
		})
	}
}

func getDirSize(b *testing.B, path string) int64 {
	b.Helper()
	var size int64
	if err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	}); err != nil {
		b.Fatal(err)
	}
	return size
}

type informerInterface interface {
	cache.Store
	ByOptionsLister
}

func initStore(b *testing.B) (informerInterface, string) {
	b.Helper()
	ctx := b.Context()

	m, err := encryption.NewManager()
	if err != nil {
		b.Fatal(err)
	}
	dbClient, dbPath, err := db.NewClient(nil, m, m, true)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = os.RemoveAll(dbPath)
	})

	gvk := corev1.SchemeGroupVersion.WithKind("ConfigMap")
	storeName := gvk.Group + "_" + gvk.Version + "_" + gvk.Kind
	sqlstore, err := store.NewStore(ctx, &corev1.ConfigMap{}, cache.DeletionHandlingMetaNamespaceKeyFunc, dbClient, false, gvk, storeName, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	informer, err := NewListOptionIndexer(ctx, sqlstore, ListOptionIndexerOptions{
		IsNamespaced: true,
	})
	if err != nil {
		b.Fatal(err)
	}

	return informer, dbPath
}

// single-entry map, whose value is a randomly-generated string of n size
func generateTestData(n int) map[string]string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[random.Int63()%int64(len(letters))]
	}
	return map[string]string{
		"data": string(b),
	}
}

func populateCache(store cache.Store, count int, data map[string]string) error {
	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "testns",
		},
		Data: data,
	}
	for x := range count {
		cm.Name = fmt.Sprintf("testcm-%06d", x)
		uns, err := unstructured.ToUnstructured(cm)
		if err != nil {
			return err
		}
		if err := store.Add(uns); err != nil {
			return err
		}
	}
	return nil
}
