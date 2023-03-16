/*
Copyright 2023 SUSE LLC

Adapted from client-go, Copyright 2014 The Kubernetes Authors.
*/

package sqlcache

import (
	"github.com/rancher/steve/pkg/stores/partition/listprocessor"
	u "github.com/rancher/wrangler/pkg/unstructured"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"
	"os"
	"strconv"
	"testing"
)

func TestListOptionIndexer(t *testing.T) {
	assert := assert.New(t)

	example, _ := u.ToUnstructured(&v1.Pod{})
	fields := [][]string{{"metadata", "labels", "Brand"}, {"metadata", "labels", "Color"}}

	os.RemoveAll(TEST_DB_LOCATION)
	l, err := NewListOptionIndexer(example, cache.DeletionHandlingMetaNamespaceKeyFunc, fields, "pods", TEST_DB_LOCATION)
	if err != nil {
		t.Error(err)
	}

	revision := 1
	red, err := u.ToUnstructured(&v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "testa rossa",
			ResourceVersion: strconv.Itoa(revision),
			Labels: map[string]string{
				"Brand": "ferrari",
				"Color": "red",
			},
		},
	})
	if err != nil {
		t.Error(err)
	}
	err = l.Add(red)
	failOnError(t, err)

	// add two v1.Pods and list with default options
	revision++
	blue, err := u.ToUnstructured(&v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "focus",
			ResourceVersion: strconv.Itoa(revision),
			Labels: map[string]string{
				"Brand": "ford",
				"Color": "blue",
			},
		},
	})
	failOnError(t, err)
	err = l.Add(blue)
	failOnError(t, err)

	passthroughPartitions := []listprocessor.Partition{
		{Passthrough: true},
	}

	lo := &listprocessor.ListOptions{
		Filters:    nil,
		Sort:       listprocessor.Sort{},
		Pagination: listprocessor.Pagination{},
		Revision:   "",
	}
	ul, r, _, err := l.ListByOptions(lo, passthroughPartitions, "")
	failOnError(t, err)
	assert.Len(ul.Items, 2)
	assert.Equal("2", r)

	// delete one and list again. Should be gone
	err = l.Delete(red)
	failOnError(t, err)
	ul, r, _, err = l.ListByOptions(lo, passthroughPartitions, "")
	failOnError(t, err)
	assert.Len(ul.Items, 1)
	assert.Equal(ul.Items[0].GetName(), "focus")
	assert.Equal("2", r)
	// gone also from most-recent store
	recent := l.List()
	assert.Len(recent, 1)
	assert.Equal(recent[0].(*unstructured.Unstructured).GetName(), "focus")

	// updating the Pod brings it back
	revision++
	red.SetResourceVersion(strconv.Itoa(revision))
	labels := red.GetLabels()
	labels["Wheels"] = "3"
	red.SetLabels(labels)
	err = l.Update(red)
	failOnError(t, err)
	recent = l.List()
	assert.Len(recent, 2)
	lo = &listprocessor.ListOptions{
		Filters:    []listprocessor.Filter{{Field: []string{"metadata", "labels", "Brand"}, Match: "ferrari"}},
		Sort:       listprocessor.Sort{},
		Pagination: listprocessor.Pagination{},
		Revision:   "",
	}
	ul, r, _, err = l.ListByOptions(lo, passthroughPartitions, "")
	failOnError(t, err)
	assert.Len(ul.Items, 1)
	assert.Equal(ul.Items[0].GetName(), "testa rossa")
	assert.Equal(ul.Items[0].GetResourceVersion(), "3")
	assert.Equal(ul.Items[0].GetLabels()["Wheels"], "3")
	assert.Equal("3", r)

	// historically, Pod still exists in version 1, gone in version 2, back in version 3
	lo = &listprocessor.ListOptions{
		Filters:    []listprocessor.Filter{{Field: []string{"metadata", "labels", "Brand"}, Match: "ferrari"}},
		Sort:       listprocessor.Sort{},
		Pagination: listprocessor.Pagination{},
		Revision:   "1",
	}
	ul, r, _, err = l.ListByOptions(lo, passthroughPartitions, "")
	failOnError(t, err)
	assert.Len(ul.Items, 1)
	assert.Equal("1", r)
	lo.Revision = "2"
	ul, r, _, err = l.ListByOptions(lo, passthroughPartitions, "")
	failOnError(t, err)
	assert.Len(ul.Items, 0)
	assert.Equal("2", r)
	lo.Revision = "3"
	ul, r, _, err = l.ListByOptions(lo, passthroughPartitions, "")
	failOnError(t, err)
	assert.Len(ul.Items, 1)
	assert.Equal("3", r)

	// add another Pod, test filter by substring and sorting
	revision++
	black, err := u.ToUnstructured(&v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "model 3",
			ResourceVersion: strconv.Itoa(revision),
			Labels: map[string]string{
				"Brand": "tesla",
				"Color": "black",
			},
		},
	})
	failOnError(t, err)
	err = l.Add(black)
	failOnError(t, err)
	lo = &listprocessor.ListOptions{
		Filters:    []listprocessor.Filter{{Field: []string{"metadata", "labels", "Brand"}, Match: "f"}}, // tesla filtered out
		Sort:       listprocessor.Sort{PrimaryField: []string{"metadata", "labels", "Color"}, PrimaryOrder: listprocessor.DESC},
		Pagination: listprocessor.Pagination{},
		Revision:   "",
	}
	ul, r, _, err = l.ListByOptions(lo, passthroughPartitions, "")
	failOnError(t, err)
	assert.Len(ul.Items, 2)
	assert.Equal(ul.Items[0].GetLabels()["Color"], "red")
	assert.Equal(ul.Items[1].GetLabels()["Color"], "blue")
	assert.Equal("4", r)

	// test Steve pagination
	lo = &listprocessor.ListOptions{
		Filters:    []listprocessor.Filter{},
		Sort:       listprocessor.Sort{PrimaryField: []string{"metadata", "labels", "Color"}},
		Pagination: listprocessor.Pagination{PageSize: 2},
		Revision:   "",
	}
	ul, r, _, err = l.ListByOptions(lo, passthroughPartitions, "")
	failOnError(t, err)
	assert.Len(ul.Items, 2)
	assert.Equal(ul.Items[0].GetLabels()["Color"], "black")
	assert.Equal(ul.Items[1].GetLabels()["Color"], "blue")
	assert.Equal("4", r)
	lo.Pagination.Page = 2
	ul, r, _, err = l.ListByOptions(lo, passthroughPartitions, "")
	failOnError(t, err)
	assert.Len(ul.Items, 1)
	assert.Equal(ul.Items[0].GetLabels()["Color"], "red")
	assert.Equal("4", r)

	// test k8s pagination
	lo = &listprocessor.ListOptions{
		Filters:   []listprocessor.Filter{},
		Sort:      listprocessor.Sort{PrimaryField: []string{"metadata", "labels", "Color"}},
		ChunkSize: 2,
		Revision:  "",
	}
	ul, r, continueToken, err := l.ListByOptions(lo, passthroughPartitions, "")
	failOnError(t, err)
	assert.Len(ul.Items, 2)
	assert.Equal(ul.Items[0].GetLabels()["Color"], "black")
	assert.Equal(ul.Items[1].GetLabels()["Color"], "blue")
	assert.Equal("4", r)
	lo.Resume = continueToken
	ul, r, _, err = l.ListByOptions(lo, passthroughPartitions, "")
	failOnError(t, err)
	assert.Len(ul.Items, 1)
	assert.Equal(ul.Items[0].GetLabels()["Color"], "red")
	assert.Equal("4", r)

	// test filtering by name
	lo = &listprocessor.ListOptions{
		Filters:    []listprocessor.Filter{},
		Sort:       listprocessor.Sort{PrimaryField: []string{"metadata", "labels", "Color"}},
		Pagination: listprocessor.Pagination{PageSize: 2},
		Revision:   "",
	}
	partitions := []listprocessor.Partition{
		{Passthrough: false, Namespace: "", All: false, Names: sets.NewString("model 3")},
	}
	ul, r, _, err = l.ListByOptions(lo, partitions, "")
	failOnError(t, err)
	assert.Len(ul.Items, 1)
	assert.Equal(ul.Items[0].GetLabels()["Color"], "black")
	assert.Equal("4", r)

	// test with empty name set
	partitions = []listprocessor.Partition{
		{Passthrough: false, Namespace: "", All: false, Names: sets.NewString()},
	}
	ul, r, _, err = l.ListByOptions(lo, partitions, "")
	failOnError(t, err)
	assert.Len(ul.Items, 0)
	assert.Equal("4", r)

	// test with different partition name
	partitions = []listprocessor.Partition{
		{Passthrough: false, Namespace: "", All: true},
	}
	ul, r, _, err = l.ListByOptions(lo, partitions, "parking lot")
	failOnError(t, err)
	assert.Len(ul.Items, 0)
	assert.Equal("4", r)

	err = l.Close()
	failOnError(t, err)
}

func TestListOptionIndexerWithPartitions(t *testing.T) {
	assert := assert.New(t)

	example, _ := u.ToUnstructured(&v1.Pod{})
	fields := [][]string{{"metadata", "labels", "Brand"}, {"metadata", "labels", "Color"}}

	os.RemoveAll(TEST_DB_LOCATION)
	l, err := NewListOptionIndexer(example, cache.DeletionHandlingMetaNamespaceKeyFunc, fields, "pods", TEST_DB_LOCATION)
	if err != nil {
		t.Error(err)
	}

	// garage namespace: three pods
	revision := 1
	red, err := u.ToUnstructured(&v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "garage",
			Name:            "testa rossa",
			ResourceVersion: strconv.Itoa(revision),
			Labels: map[string]string{
				"Brand": "ferrari",
				"Color": "red",
			},
		},
	})
	failOnError(t, err)
	err = l.Add(red)
	failOnError(t, err)

	revision++
	blue, err := u.ToUnstructured(&v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "garage",
			Name:            "focus",
			ResourceVersion: strconv.Itoa(revision),
			Labels: map[string]string{
				"Brand": "ford",
				"Color": "blue",
			},
		},
	})
	failOnError(t, err)
	err = l.Add(blue)
	failOnError(t, err)

	revision++
	black, err := u.ToUnstructured(&v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "garage",
			Name:            "model 3",
			ResourceVersion: strconv.Itoa(revision),
			Labels: map[string]string{
				"Brand": "tesla",
				"Color": "black",
			},
		},
	})
	failOnError(t, err)
	err = l.Add(black)
	failOnError(t, err)

	// yard namespace: one Pod
	revision++
	white, err := u.ToUnstructured(&v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "yard",
			Name:            "corolla",
			ResourceVersion: strconv.Itoa(revision),
			Labels: map[string]string{
				"Brand": "toyota",
				"Color": "white",
			},
		},
	})
	failOnError(t, err)
	err = l.Add(white)
	failOnError(t, err)

	// no namespace: one Pod
	revision++
	pink, err := u.ToUnstructured(&v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "",
			Name:            "minor",
			ResourceVersion: strconv.Itoa(revision),
			Labels: map[string]string{
				"Brand": "mini",
				"Color": "pink",
			},
		},
	})
	failOnError(t, err)
	err = l.Add(pink)
	failOnError(t, err)

	passthroughPartitions := []listprocessor.Partition{
		{Passthrough: true},
	}

	lo := &listprocessor.ListOptions{
		Filters:    nil,
		Sort:       listprocessor.Sort{PrimaryField: []string{"metadata", "name"}, PrimaryOrder: listprocessor.ASC},
		Pagination: listprocessor.Pagination{},
		Revision:   "",
	}

	// passthrough: get all 5 cars
	ul, r, _, err := l.ListByOptions(lo, passthroughPartitions, "*")
	failOnError(t, err)
	assert.Len(ul.Items, 5)
	assert.Equal("5", r)

	// no partitions: no cars
	ul, r, _, err = l.ListByOptions(lo, []listprocessor.Partition{}, "*")
	failOnError(t, err)
	assert.Len(ul.Items, 0)
	assert.Equal("5", r)

	// one partition on the garage namespace: 3 cars
	ul, r, _, err = l.ListByOptions(lo, []listprocessor.Partition{{Namespace: "garage", All: true}}, "garage")
	failOnError(t, err)
	assert.Len(ul.Items, 3)
	assert.Equal(ul.Items[0].GetName(), "focus")
	assert.Equal(ul.Items[1].GetName(), "model 3")
	assert.Equal(ul.Items[2].GetName(), "testa rossa")
	assert.Equal("5", r)

	// one partition on all namespaces: still 3 cars
	ul, r, _, err = l.ListByOptions(lo, []listprocessor.Partition{{Namespace: "garage", All: true}}, "*")
	failOnError(t, err)
	assert.Len(ul.Items, 3)
	assert.Equal(ul.Items[0].GetName(), "focus")
	assert.Equal(ul.Items[1].GetName(), "model 3")
	assert.Equal(ul.Items[2].GetName(), "testa rossa")
	assert.Equal("5", r)

	// one partition on the garage namespace but yard namespace requested: no cars
	ul, r, _, err = l.ListByOptions(lo, []listprocessor.Partition{}, "yard")
	failOnError(t, err)
	assert.Len(ul.Items, 0)
	assert.Equal("5", r)

	// two partition on the garage and yard namespaces: 4 cars
	ul, r, _, err = l.ListByOptions(lo, []listprocessor.Partition{
		{Namespace: "garage", All: true},
		{Namespace: "yard", All: true},
	}, "")
	failOnError(t, err)
	assert.Len(ul.Items, 4)
	assert.Equal(ul.Items[0].GetName(), "corolla")
	assert.Equal(ul.Items[1].GetName(), "focus")
	assert.Equal(ul.Items[2].GetName(), "model 3")
	assert.Equal(ul.Items[3].GetName(), "testa rossa")
	assert.Equal("5", r)

	// two partitions, one limited to one car in the garage, one with full permission on yard: 2 cars
	ul, r, _, err = l.ListByOptions(lo, []listprocessor.Partition{
		{Namespace: "garage", All: false, Names: sets.NewString("focus")},
		{Namespace: "yard", All: true},
	}, "")
	failOnError(t, err)
	assert.Len(ul.Items, 2)
	assert.Equal(ul.Items[0].GetName(), "corolla")
	assert.Equal(ul.Items[1].GetName(), "focus")
	assert.Equal("5", r)

	// two partitions, one limited to a car not in the set, one no specific permission: 0 cars
	ul, r, _, err = l.ListByOptions(lo, []listprocessor.Partition{
		{Namespace: "garage", All: false, Names: sets.NewString("viper")},
		{Namespace: "yard", All: false},
	}, "")
	failOnError(t, err)
	assert.Len(ul.Items, 0)
	assert.Equal("5", r)

	err = l.Close()
	failOnError(t, err)
}

func failOnError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("%+v", err)
	}
}
