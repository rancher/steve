/*
Copyright 2023 SUSE LLC

Adapted from client-go, Copyright 2014 The Kubernetes Authors.
*/

package sqlcache

import (
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strconv"
	"testing"
)

func brandfunc(c any) any {
	return c.(*v1.Pod).Labels["Brand"]
}

func colorfunc(c any) any {
	return c.(*v1.Pod).Labels["Color"]
}

var fieldFunc = map[string]FieldFunc{
	"Brand": brandfunc,
	"Color": colorfunc,
}

func TestListOptionIndexer(t *testing.T) {
	assert := assert.New(t)

	l, err := NewListOptionIndexer(&v1.Pod{}, TEST_DB_LOCATION, fieldFunc)
	if err != nil {
		t.Error(err)
	}

	revision := 1
	red := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "testa rossa",
			ResourceVersion: strconv.Itoa(revision),
			Labels: map[string]string{
				"Brand": "ferrari",
				"Color": "red",
			},
		},
	}
	err = l.Add(red)
	if err != nil {
		t.Error(err)
	}

	// add two v1.Pods and list with default options
	revision++
	blue := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "focus",
			ResourceVersion: strconv.Itoa(revision),
			Labels: map[string]string{
				"Brand": "ford",
				"Color": "blue",
			},
		},
	}
	err = l.Add(blue)
	if err != nil {
		t.Error(err)
	}

	lo := ListOptions{
		Filters:    nil,
		Sort:       Sort{},
		Pagination: Pagination{},
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
	assert.Equal(r[0].(*v1.Pod).Name, "focus")
	// gone also from most-recent store
	r = l.List()
	assert.Len(r, 1)
	assert.Equal(r[0].(*v1.Pod).Name, "focus")

	// updating the v1.Pod brings it back
	revision++
	red.ResourceVersion = strconv.Itoa(revision)
	red.Labels["Wheels"] = "3"
	err = l.Update(red)
	if err != nil {
		t.Error(err)
	}
	r = l.List()
	assert.Len(r, 2)
	lo = ListOptions{
		Filters:    []Filter{{field: []string{"Brand"}, match: "ferrari"}},
		Sort:       Sort{},
		Pagination: Pagination{},
		Revision:   "",
	}
	r, err = l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 1)
	assert.Equal(r[0].(*v1.Pod).Name, "testa rossa")
	assert.Equal(r[0].(*v1.Pod).ResourceVersion, "3")
	assert.Equal(r[0].(*v1.Pod).Labels["Wheels"], "3")

	// historically, v1.Pod still exists in version 1, gone in version 2, back in version 3
	lo = ListOptions{
		Filters:    []Filter{{field: []string{"Brand"}, match: "ferrari"}},
		Sort:       Sort{},
		Pagination: Pagination{},
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

	// add another v1.Pod, test filter by substring and sorting
	revision++
	black := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "model 3",
			ResourceVersion: strconv.Itoa(revision),
			Labels: map[string]string{
				"Brand": "tesla",
				"Color": "black",
			},
		},
	}
	err = l.Add(black)
	if err != nil {
		t.Error(err)
	}
	lo = ListOptions{
		Filters:    []Filter{{field: []string{"Brand"}, match: "f"}}, // tesla filtered out
		Sort:       Sort{primaryField: []string{"Color"}, primaryOrder: DESC},
		Pagination: Pagination{},
		Revision:   "",
	}
	r, err = l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 2)
	assert.Equal(r[0].(*v1.Pod).Labels["Color"], "red")
	assert.Equal(r[1].(*v1.Pod).Labels["Color"], "blue")

	// test pagination
	lo = ListOptions{
		Filters:    []Filter{},
		Sort:       Sort{primaryField: []string{"Color"}},
		Pagination: Pagination{pageSize: 2},
		Revision:   "",
	}
	r, err = l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 2)
	assert.Equal(r[0].(*v1.Pod).Labels["Color"], "black")
	assert.Equal(r[1].(*v1.Pod).Labels["Color"], "blue")
	lo.Pagination.page = 2
	r, err = l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 1)
	assert.Equal(r[0].(*v1.Pod).Labels["Color"], "red")

	err = l.Close()
	if err != nil {
		t.Error(err)
	}
}
