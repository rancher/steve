package informer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
)

type SyntheticWatcher struct {
	resultChan   chan watch.Event
	stopChan     chan struct{}
	doneChan     chan struct{}
	stopChanLock sync.Mutex
}

func newSyntheticWatcher() *SyntheticWatcher {
	return &SyntheticWatcher{
		stopChan:   make(chan struct{}),
		doneChan:   make(chan struct{}),
		resultChan: make(chan watch.Event, 0),
	}
}

func (rw *SyntheticWatcher) watch(client dynamic.ResourceInterface, options metav1.ListOptions, interval time.Duration) (*SyntheticWatcher, error) {
	go rw.receive(client, options, interval)
	return rw, nil
}

type objectHolder struct {
	version            string
	unstructuredObject *unstructured.Unstructured
}

// receive periodically calls client.List(), and converts the returned items into Watch Events
func (rw *SyntheticWatcher) receive(client dynamic.ResourceInterface, options metav1.ListOptions, interval time.Duration) { //
	go func() {
		defer close(rw.doneChan)
		defer close(rw.resultChan)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		previousState := make(map[string]objectHolder)
		ticker := time.NewTicker(interval)

		for {
			select {
			case <-ticker.C:
				list, err := client.List(ctx, options)
				if err != nil {
					logrus.Errorf("synthetic watcher: client.List => error: %s", err)
					continue
				}
				newObjects := make(map[string]objectHolder)
				for _, uItem := range list.Items {
					namespace := uItem.GetNamespace()
					name := uItem.GetName()
					key := name
					if namespace != "" {
						key = fmt.Sprintf("%s/%s", namespace, name)
					}
					version := uItem.GetResourceVersion()
					newObjects[key] = objectHolder{version: version, unstructuredObject: &uItem}
				}
				// Now determine whether items were added, deleted, or modified
				currentState := make(map[string]objectHolder)
				for key, newObject := range newObjects {
					currentState[key] = newObject
					if oldItem, ok := previousState[key]; !ok {
						w, err := createWatchEvent(watch.Added, newObject.unstructuredObject)
						if err != nil {
							logrus.Errorf("can't convert unstructured obj into runtime: %s", err)
							continue
						}
						rw.resultChan <- w
					} else {
						delete(previousState, key)
						if oldItem.version != newObject.version {
							w, err := createWatchEvent(watch.Modified, oldItem.unstructuredObject)
							if err != nil {
								logrus.Errorf("can't convert unstructured obj into runtime: %s", err)
								continue
							}
							rw.resultChan <- w
						}
					}
				}
				// And anything left  in the previousState didn't show up in currentState and can be deleted.
				for _, item := range previousState {
					w, err := createWatchEvent(watch.Deleted, item.unstructuredObject)
					if err != nil {
						logrus.Errorf("can't convert unstructured obj into runtime: %s", err)
						continue
					}
					rw.resultChan <- w
				}
				previousState = currentState

			case <-rw.stopChan:
				cancel()
				return

			case <-ctx.Done():
				return
			}
		}
	}()
}

/*
func getRuntimeObject(u *unstructured.Unstructured) (*runtime.Object, error) {
	obj, ok := u.(runtime.Object)
	if !ok {
		return nil, fmt.Errorf("can't convert an unstructured object into a runtime.Object")
	}
	return obj, nil
}
*/

func createWatchEvent(event watch.EventType, u *unstructured.Unstructured) (watch.Event, error) {
	/*
		obj, err := getRuntimeObject(u)
		if err != nil {
			return watch.Event{}, err
		}
		return watch.Event{Type: event, Object: *obj}, nil
	*/
	return watch.Event{Type: event, Object: u}, nil
}

// ResultChan implements Interface.
func (rw *SyntheticWatcher) ResultChan() <-chan watch.Event {
	return rw.resultChan
}

// Stop implements Interface.
func (rw *SyntheticWatcher) Stop() {
	rw.stopChanLock.Lock()
	defer rw.stopChanLock.Unlock()

	// Prevent closing an already closed channel to prevent a panic
	select {
	case <-rw.stopChan:
	default:
		close(rw.stopChan)
	}
}

// Done allows the caller to be notified when Retry watcher stops.
func (rw *SyntheticWatcher) Done() <-chan struct{} {
	return rw.doneChan
}
