/*
Copyright 2023 SUSE LLC

Adapted from client-go, Copyright 2014 The Kubernetes Authors.
*/

package sqlcache

import (
	"github.com/stretchr/testify/assert"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"
)

// Test public interface
func doTestIndex(t *testing.T, indexer cache.Indexer) {
	mkObj := func(id string, val string) testStoreObject {
		return testStoreObject{Id: id, Val: val}
	}

	// Test Index (testStoreIndexFunc / "by_val")
	expected := map[string]sets.String{}
	expected["b"] = sets.NewString("a", "c")
	expected["f"] = sets.NewString("e")
	expected["h"] = sets.NewString("g")
	indexer.Add(mkObj("a", "b"))
	indexer.Add(mkObj("c", "b"))
	indexer.Add(mkObj("e", "f"))
	indexer.Add(mkObj("g", "h"))
	for k, v := range expected {
		found := sets.String{}
		indexResults, err := indexer.Index("by_val", mkObj("", k))
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
		for _, item := range indexResults {
			found.Insert(item.(testStoreObject).Id)
		}
		items := v.List()
		if !found.HasAll(items...) {
			t.Errorf("missing values, index %s, expected %v but found %v", k, items, found.List())
		}
		if len(found) != len(items) {
			t.Errorf("unexpected values, index %s, count %v instead of %v", k, len(found), len(items))
		}

		found = sets.String{}
		keys, err := indexer.IndexKeys("by_val", k)
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
		for _, item := range keys {
			found.Insert(item)
		}
		if !found.HasAll(items...) {
			t.Errorf("missing item keys, index %s, expected %v but found %v", k, items, found.List())
		}
		if len(found) != len(items) {
			t.Errorf("unexpected values, index %s, count %v instead of %v", k, len(found), len(items))
		}
	}

	// Test ListIndexFuncValues
	values := indexer.ListIndexFuncValues("by_val")
	found := sets.String{}
	for _, item := range values {
		found.Insert(item)
	}
	expectedValues := []string{"b", "f", "h"}
	if !found.HasAll(expectedValues...) {
		t.Errorf("missing values, index \"by_val\", expected %v but found %v", expectedValues, found.List())
	}
	if len(found) != len(expectedValues) {
		t.Errorf("unexpected values, index \"by_val\", count %v instead of %v", len(found), len(expectedValues))
	}

	// Test Index (testStoreIndexMultiFunc / "by_val_multi")
	expected = map[string]sets.String{}
	expected["a"] = sets.NewString("e", "g")
	expected["b"] = sets.NewString("a", "c")
	{
		for k, v := range expected {
			found := sets.String{}
			indexResults, err := indexer.Index("by_val_multi", mkObj("", k))
			if err != nil {
				t.Errorf("Unexpected error %v", err)
			}
			for _, item := range indexResults {
				found.Insert(item.(testStoreObject).Id)
			}
			items := v.List()
			if !found.HasAll(items...) {
				t.Errorf("missing values, index %s, expected %v but found %v", k, items, found.List())
			}
			if len(found) != len(items) {
				t.Errorf("unexpected values, index %s, count %v instead of %v", k, len(found), len(items))
			}
		}
	}

	// Test IndexKeys (testStoreIndexMultiFunc / "by_val_multi")
	expected = map[string]sets.String{}
	expected["b"] = sets.NewString("a", "c")
	expected["b_value"] = sets.NewString("a", "c")
	expected["non_b_value"] = sets.NewString("e", "g")
	{
		for k, v := range expected {
			keys, err := indexer.IndexKeys("by_val_multi", k)
			if err != nil {
				t.Errorf("Unexpected error %v", err)
			}
			found := sets.String{}
			for _, item := range keys {
				found.Insert(item)
			}
			items := v.List()
			if !found.HasAll(items...) {
				t.Errorf("missing item keys, index %s, expected %v but found %v", k, items, found.List())
			}
			if len(found) != len(keys) {
				t.Errorf("unexpected keys, index %s, count %v instead of %v", k, len(found), len(items))
			}
		}
	}

	err := indexer.Add(mkObj("g", "h2"))
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	keys, err := indexer.IndexKeys("by_val", "h2")
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	if len(keys) != 1 && keys[0] != "g" {
		t.Errorf("Unexpected indexed value after Update, got %v instead of [g]", keys)
	}
	keys, err = indexer.IndexKeys("by_val", "h")
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("Unexpected indexed value after Update, got %v instead of []", keys)
	}
}

func testStoreIndexFunc(obj interface{}) ([]string, error) {
	return []string{obj.(testStoreObject).Val}, nil
}

func testStoreIndexers() cache.Indexers {
	indexers := cache.Indexers{}
	indexers["by_val"] = testStoreIndexFunc
	indexers["by_val_multi"] = testStoreIndexMultiFunc
	return indexers
}

func testStoreIndexMultiFunc(obj interface{}) ([]string, error) {
	val := obj.(testStoreObject).Val
	if val == "b" {
		return []string{val, "b_value"}, nil
	}
	return []string{"non_b_value"}, nil
}

func TestIndexer(t *testing.T) {
	store, err := NewIndexer(testStoreObject{}, testStoreKeyFunc, TEST_DB_LOCATION, testStoreIndexers())
	if err != nil {
		t.Error(err)
	}
	doTestIndex(t, store)
	err = store.Close()
	if err != nil {
		return
	}
}

var currentVersion = 0

func testVersionFunc(obj any) (int, error) {
	currentVersion += 1
	return currentVersion, nil
}

func TestVersionedIndexer(t *testing.T) {
	store, err := NewVersionedIndexer(testStoreObject{}, testStoreKeyFunc, testVersionFunc, TEST_DB_LOCATION, testStoreIndexers())
	if err != nil {
		t.Error(err)
	}
	doTestIndex(t, store)

	assert := assert.New(t)

	item, found, err := store.GetByKeyAndVersion("g", 4)
	assert.Equal(true, found)
	assert.Equal("g", item.(testStoreObject).Id)
	assert.Equal("h", item.(testStoreObject).Val)

	item, found, err = store.GetByKeyAndVersion("g", 5)
	assert.Equal(true, found)
	assert.Equal("g", item.(testStoreObject).Id)
	assert.Equal("h2", item.(testStoreObject).Val)

	item, found, err = store.GetByKeyAndVersion("g", 6)
	assert.Equal(false, found)

	err = store.Close()
	if err != nil {
		return
	}
}
