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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"strconv"
	"testing"
)

func brandfunc(c any) any {
	return c.(*unstructured.Unstructured).GetLabels()["Brand"]
}

func colorfunc(c any) any {
	return c.(*unstructured.Unstructured).GetLabels()["Color"]
}

var fieldFunc = map[string]FieldFunc{
	"Brand": brandfunc,
	"Color": colorfunc,
}

func TestListOptionIndexer(t *testing.T) {
	assert := assert.New(t)

	podGVK := schema.GroupVersionKind{
		Version: "v1",
		Kind:    "Pod",
	}

	l, err := NewListOptionIndexer(podGVK, TEST_DB_LOCATION, fieldFunc)
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
	if err != nil {
		t.Error(err)
	}

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
	if err != nil {
		t.Error(err)
	}
	err = l.Add(blue)
	if err != nil {
		t.Error(err)
	}

	lo := listprocessor.ListOptions{
		Filters:    nil,
		Sort:       listprocessor.Sort{},
		Pagination: listprocessor.Pagination{},
		Revision:   "",
	}
	r, err := l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 2)

	// delete one and list again. Should be gone
	err = l.Delete(red)
	if err != nil {
		t.Error(err)
	}
	r, err = l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 1)
	assert.Equal(r[0].(*unstructured.Unstructured).GetName(), "focus")
	// gone also from most-recent store
	r = l.List()
	assert.Len(r, 1)
	assert.Equal(r[0].(*unstructured.Unstructured).GetName(), "focus")

	// updating the Pod brings it back
	revision++
	red.SetResourceVersion(strconv.Itoa(revision))
	labels := red.GetLabels()
	labels["Wheels"] = "3"
	red.SetLabels(labels)
	err = l.Update(red)
	if err != nil {
		t.Error(err)
	}
	r = l.List()
	assert.Len(r, 2)
	lo = listprocessor.ListOptions{
		Filters:    []listprocessor.Filter{{Field: []string{"Brand"}, Match: "ferrari"}},
		Sort:       listprocessor.Sort{},
		Pagination: listprocessor.Pagination{},
		Revision:   "",
	}
	r, err = l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 1)
	assert.Equal(r[0].(*unstructured.Unstructured).GetName(), "testa rossa")
	assert.Equal(r[0].(*unstructured.Unstructured).GetResourceVersion(), "3")
	assert.Equal(r[0].(*unstructured.Unstructured).GetLabels()["Wheels"], "3")

	// historically, Pod still exists in version 1, gone in version 2, back in version 3
	lo = listprocessor.ListOptions{
		Filters:    []listprocessor.Filter{{Field: []string{"Brand"}, Match: "ferrari"}},
		Sort:       listprocessor.Sort{},
		Pagination: listprocessor.Pagination{},
		Revision:   "1",
	}
	r, err = l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 1)
	lo.Revision = "2"
	r, err = l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 0)
	lo.Revision = "3"
	r, err = l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 1)

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
	if err != nil {
		t.Error(err)
	}
	err = l.Add(black)
	if err != nil {
		t.Error(err)
	}
	lo = listprocessor.ListOptions{
		Filters:    []listprocessor.Filter{{Field: []string{"Brand"}, Match: "f"}}, // tesla filtered out
		Sort:       listprocessor.Sort{PrimaryField: []string{"Color"}, PrimaryOrder: listprocessor.DESC},
		Pagination: listprocessor.Pagination{},
		Revision:   "",
	}
	r, err = l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 2)
	assert.Equal(r[0].(*unstructured.Unstructured).GetLabels()["Color"], "red")
	assert.Equal(r[1].(*unstructured.Unstructured).GetLabels()["Color"], "blue")

	// test pagination
	lo = listprocessor.ListOptions{
		Filters:    []listprocessor.Filter{},
		Sort:       listprocessor.Sort{PrimaryField: []string{"Color"}},
		Pagination: listprocessor.Pagination{PageSize: 2},
		Revision:   "",
	}
	r, err = l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 2)
	assert.Equal(r[0].(*unstructured.Unstructured).GetLabels()["Color"], "black")
	assert.Equal(r[1].(*unstructured.Unstructured).GetLabels()["Color"], "blue")
	lo.Pagination.Page = 2
	r, err = l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 1)
	assert.Equal(r[0].(*unstructured.Unstructured).GetLabels()["Color"], "red")

	err = l.Close()
	if err != nil {
		t.Error(err)
	}
}
