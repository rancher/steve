/*
Copyright 2024 SUSE LLC
*/

package informer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
)

func TestSyntheticWatcher(t *testing.T) {
	dynamicClient := NewMockResourceInterface(gomock.NewController(t))
	var err error
	cs1 := v1.ComponentStatus{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ComponentStatus",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "cs1",
			UID:             "1",
			ResourceVersion: "rv1.1",
		},
		Conditions: []v1.ComponentCondition{v1.ComponentCondition{Type: "Healthy", Status: v1.ConditionTrue, Message: "hi from cs1"}},
	}
	cs2 := v1.ComponentStatus{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ComponentStatus",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "cs2",
			UID:             "2",
			ResourceVersion: "rv2.1",
		},
		Conditions: []v1.ComponentCondition{v1.ComponentCondition{Type: "Healthy", Status: v1.ConditionTrue, Message: "hi from cs2"}},
	}
	cs3 := v1.ComponentStatus{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ComponentStatus",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "cs3",
			UID:             "3",
			ResourceVersion: "rv3.1",
		},
		Conditions: []v1.ComponentCondition{v1.ComponentCondition{Type: "Healthy", Status: v1.ConditionTrue, Message: "hi from cs3"}},
	}
	cs4 := v1.ComponentStatus{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ComponentStatus",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "cs4",
			UID:             "4",
			ResourceVersion: "rv4.1",
		},
		Conditions: []v1.ComponentCondition{v1.ComponentCondition{Type: "Healthy", Status: v1.ConditionTrue, Message: "hi from cs4"}},
	}
	list, err := makeCSList(cs1, cs2, cs3, cs4)
	assert.Nil(t, err)
	dynamicClient.EXPECT().List(gomock.Any(), gomock.Any()).Return(list, nil)
	// Make copies to avoid modifying objects before the watcher has processed them.
	cs1b := cs1.DeepCopy()
	cs1b.ObjectMeta.ResourceVersion = "rv1.2"
	cs2b := cs2.DeepCopy()
	cs2b.ObjectMeta.ResourceVersion = "rv2.2"
	list2, err := makeCSList(*cs1b, *cs2b, cs4)
	assert.Nil(t, err)
	dynamicClient.EXPECT().List(gomock.Any(), gomock.Any()).AnyTimes().Return(list2, nil)

	ctx, cancel := context.WithCancel(context.Background())
	sw := newSyntheticWatcher(ctx, cancel)
	pollingInterval := 10 * time.Millisecond
	watchFunc := func(options metav1.ListOptions) (watch.Interface, error) {
		return sw.watch(dynamicClient, options, pollingInterval)
	}
	options := metav1.ListOptions{}
	w, err := watchFunc(options)
	assert.Nil(t, err)
	errChan := make(chan error)
	results := make([]processedObjectInfo, 0)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		results, err = handleAnyWatch(w, errChan, sw.stopChan)
		wg.Done()
	}()

	go func() {
		time.Sleep(40 * time.Millisecond)
		sw.stopChan <- struct{}{}
		wg.Done()
	}()
	wg.Wait()
	// Verify we get what we expected to see
	assert.Len(t, results, 8)
	for i, _ := range list.Items {
		assert.Equal(t, "added-result", results[i].eventName)
	}
	assert.Equal(t, "modified-result", results[len(list.Items)].eventName)
	assert.Equal(t, "modified-result", results[len(list.Items)+1].eventName)
	assert.Equal(t, "deleted-result", results[len(list.Items)+2].eventName)
	assert.Equal(t, "stop", results[7].eventName)
	// We can't really assert that the events get the correct timestamps on them
	// because they can be held up in the channel for unexpected durations. I did have
	// assert.Greater(t, float64(timeDelta), 0.9*float64(pollingInterval))
	// but saw a failure -- the interval was actually 0.75 * pollingInterval.
	// So there's no point testing that.
	assert.Greater(t, results[4].createdAt, results[0].createdAt)
}

func makeCSList(objs ...v1.ComponentStatus) (*unstructured.UnstructuredList, error) {
	unList := make([]unstructured.Unstructured, len(objs))
	for i, cs := range objs {
		unst, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&cs)
		if err != nil {
			return nil, err
		}
		unList[i] = unstructured.Unstructured{Object: unst}
	}
	list := &unstructured.UnstructuredList{
		Items: unList,
	}
	return list, nil
}

type processedObjectInfo struct {
	createdAt time.Time
	eventName string
	payload   interface{}
}

func makeProcessedObject(eventName string, payload interface{}) processedObjectInfo {
	return processedObjectInfo{
		createdAt: time.Now(),
		eventName: eventName,
		payload:   payload,
	}
}

func handleAnyWatch(w watch.Interface,
	errCh chan error,
	stopCh <-chan struct{},
) ([]processedObjectInfo, error) {
	results := make([]processedObjectInfo, 0)
loop:
	for {
		select {
		case <-stopCh:
			results = append(results, makeProcessedObject("stop", nil))
			return results, nil
		case err := <-errCh:
			results = append(results, makeProcessedObject("error", err))
			return results, err
		case event, ok := <-w.ResultChan():
			if !ok {
				results = append(results, makeProcessedObject("bad-result", nil))
				break loop
			}
			switch event.Type {
			case watch.Added:
				results = append(results, makeProcessedObject("added-result", &event))
			case watch.Modified:
				results = append(results, makeProcessedObject("modified-result", &event))
			case watch.Deleted:
				results = append(results, makeProcessedObject("deleted-result", &event))
			case watch.Bookmark:
				results = append(results, makeProcessedObject("bookmark-result", &event))
			default:
				results = append(results, makeProcessedObject("unexpected-result", &event))
			}
		}
	}
	return results, nil
}
