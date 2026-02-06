/*
Copyright 2023 SUSE LLC

Adapted from client-go, Copyright 2014 The Kubernetes Authors.
*/

package informer

import (
	"context"
	"fmt"
	"github.com/rancher/steve/pkg/sqlcache/db"
	"github.com/rancher/steve/pkg/sqlcache/encryption"
	"github.com/rancher/steve/pkg/sqlcache/store"
	"go.uber.org/mock/gomock"
	"os"
	"testing"
	"time"

	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

var emptyNamespaceList = &unstructured.UnstructuredList{Object: map[string]any{"items": []any{}}, Items: []unstructured.Unstructured{}}

func makeListOptionIndexer(ctx context.Context, gvk schema.GroupVersionKind, opts ListOptionIndexerOptions, shouldEncrypt bool, nsList *unstructured.UnstructuredList) (*ListOptionIndexer, string, error) {
	m, err := encryption.NewManager()
	if err != nil {
		return nil, "", err
	}

	db, dbPath, err := db.NewClient(ctx, nil, m, m, true)
	if err != nil {
		return nil, "", err
	}
	// First create a namespace table so the projectsornamespaces query succeeds
	nsGVK := schema.GroupVersionKind{
		Group:   "",
		Version: "v1",
		Kind:    "Namespace",
	}
	example := &unstructured.Unstructured{}
	example.SetGroupVersionKind(nsGVK)
	name := informerNameFromGVK(nsGVK)
	s, err := store.NewStore(ctx, example, cache.DeletionHandlingMetaNamespaceKeyFunc, db, shouldEncrypt, nsGVK, name, nil, nil)
	if err != nil {
		return nil, "", err
	}
	ns_opts := ListOptionIndexerOptions{
		Fields:       [][]string{},
		IsNamespaced: false,
	}
	listOptionIndexer, err := NewListOptionIndexer(ctx, s, ns_opts)
	if err != nil {
		return nil, "", err
	}
	if nsList != nil {
		for _, item := range nsList.Items {
			err = listOptionIndexer.Add(&item)
			if err != nil {
				return nil, "", err
			}
		}
	}

	example = &unstructured.Unstructured{}
	example.SetGroupVersionKind(gvk)
	name = informerNameFromGVK(gvk)

	s, err = store.NewStore(ctx, example, cache.DeletionHandlingMetaNamespaceKeyFunc, db, shouldEncrypt, gvk, name, nil, nil)
	if err != nil {
		return nil, "", err
	}
	if opts.IsNamespaced {
		// Can't use slices.Compare because []string doesn't implement comparable
		idEntry := []string{"id"}
		if opts.Fields == nil {
			opts.Fields = [][]string{idEntry}
		} else {
			opts.Fields = append(opts.Fields, idEntry)
		}
	}

	listOptionIndexer, err = NewListOptionIndexer(ctx, s, opts)
	if err != nil {
		return nil, "", err
	}

	return listOptionIndexer, dbPath, nil
}

func cleanTempFiles(basePath string) {
	os.Remove(basePath)
	os.Remove(basePath + "-shm")
	os.Remove(basePath + "-wal")
}

func TestNewListOptionIndexer(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase
	tests = append(tests, testCase{description: "NewListOptionIndexer() with no errors returned, should return no error", test: func(t *testing.T) {
		ctrl := gomock.NewController(t)
		txClient := NewMockTxClient(ctrl)
		store := NewMockStore(ctrl)
		stmt := NewMockStmt(ctrl)
		fields := [][]string{{"something"}}
		id := "somename"
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})
		store.EXPECT().RegisterAfterAdd(gomock.Any())
		store.EXPECT().RegisterAfterUpdate(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterAdd(gomock.Any()).Times(3)
		store.EXPECT().RegisterAfterUpdate(gomock.Any()).Times(3)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(1)
		store.EXPECT().RegisterAfterDeleteAll(gomock.Any()).Times(2)
		store.EXPECT().RegisterBeforeDropAll(gomock.Any()).AnyTimes()

		// create field table
		txClient.EXPECT().Exec(fmt.Sprintf(dropFieldsFmt, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsTableFmt, id, id, `"metadata.name" TEXT, "metadata.creationTimestamp" TEXT, "metadata.namespace" TEXT, "something" INT`)).Return(nil, nil)
		// create field table indexes
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.name", id, "metadata.name")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.namespace", id, "metadata.namespace")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.creationTimestamp", id, "metadata.creationTimestamp")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, fields[0][0], id, fields[0][0])).Return(nil, nil)
		// create labels table
		txClient.EXPECT().Exec(fmt.Sprintf(dropLabelsStmtFmt, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createLabelsTableFmt, id, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createLabelsTableIndexFmt, id, id)).Return(nil, nil)
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})

		opts := ListOptionIndexerOptions{
			Fields:       fields,
			TypeGuidance: map[string]string{"something": "INT"},
			IsNamespaced: true,
		}
		loi, err := NewListOptionIndexer(context.Background(), store, opts)
		assert.Nil(t, err)
		assert.NotNil(t, loi)
	}})
	tests = append(tests, testCase{description: "NewListOptionIndexer() with error returned from NewIndexer(), should return an error", test: func(t *testing.T) {
		txClient := NewMockTxClient(gomock.NewController(t))
		store := NewMockStore(gomock.NewController(t))
		fields := [][]string{{"something"}}
		id := "somename"
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})

		opts := ListOptionIndexerOptions{
			Fields: fields,
		}
		_, err := NewListOptionIndexer(context.Background(), store, opts)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewListOptionIndexer() with error returned from Begin(), should return an error", test: func(t *testing.T) {
		ctrl := gomock.NewController(t)
		txClient := NewMockTxClient(ctrl)
		store := NewMockStore(ctrl)
		stmt := NewMockStmt(ctrl)
		fields := [][]string{{"something"}}
		id := "somename"
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})
		store.EXPECT().RegisterAfterAdd(gomock.Any())
		store.EXPECT().RegisterAfterUpdate(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterAdd(gomock.Any()).Times(3)
		store.EXPECT().RegisterAfterUpdate(gomock.Any()).Times(3)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(1)
		store.EXPECT().RegisterAfterDeleteAll(gomock.Any()).Times(2)
		store.EXPECT().RegisterBeforeDropAll(gomock.Any()).AnyTimes()

		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error"))

		opts := ListOptionIndexerOptions{
			Fields: fields,
		}
		_, err := NewListOptionIndexer(context.Background(), store, opts)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewListOptionIndexer() with error from Exec() when creating fields table, should return an error", test: func(t *testing.T) {
		ctrl := gomock.NewController(t)
		txClient := NewMockTxClient(ctrl)
		store := NewMockStore(ctrl)
		stmt := NewMockStmt(ctrl)
		fields := [][]string{{"something"}}
		id := "somename"
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})
		store.EXPECT().RegisterAfterAdd(gomock.Any())
		store.EXPECT().RegisterAfterUpdate(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterAdd(gomock.Any()).Times(3)
		store.EXPECT().RegisterAfterUpdate(gomock.Any()).Times(3)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(1)
		store.EXPECT().RegisterAfterDeleteAll(gomock.Any()).Times(2)
		store.EXPECT().RegisterBeforeDropAll(gomock.Any()).AnyTimes()

		txClient.EXPECT().Exec(fmt.Sprintf(dropFieldsFmt, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsTableFmt, id, id, `"metadata.name" TEXT, "metadata.creationTimestamp" TEXT, "metadata.namespace" TEXT, "something" TEXT`)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.name", id, "metadata.name")).Return(nil, fmt.Errorf("error"))
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err == nil {
					t.Fail()
				}
			})

		opts := ListOptionIndexerOptions{
			Fields:       fields,
			IsNamespaced: true,
		}
		_, err := NewListOptionIndexer(context.Background(), store, opts)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewListOptionIndexer() with error from create-labels, should return an error", test: func(t *testing.T) {
		ctrl := gomock.NewController(t)
		txClient := NewMockTxClient(ctrl)
		store := NewMockStore(ctrl)
		stmt := NewMockStmt(ctrl)
		fields := [][]string{{"something"}}
		id := "somename"
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})
		store.EXPECT().RegisterAfterAdd(gomock.Any())
		store.EXPECT().RegisterAfterUpdate(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterAdd(gomock.Any()).Times(3)
		store.EXPECT().RegisterAfterUpdate(gomock.Any()).Times(3)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(1)
		store.EXPECT().RegisterAfterDeleteAll(gomock.Any()).Times(2)
		store.EXPECT().RegisterBeforeDropAll(gomock.Any()).AnyTimes()

		txClient.EXPECT().Exec(fmt.Sprintf(dropFieldsFmt, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsTableFmt, id, id, `"metadata.name" TEXT, "metadata.creationTimestamp" TEXT, "metadata.namespace" TEXT, "something" TEXT`)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.name", id, "metadata.name")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.namespace", id, "metadata.namespace")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.creationTimestamp", id, "metadata.creationTimestamp")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, fields[0][0], id, fields[0][0])).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(dropLabelsStmtFmt, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createLabelsTableFmt, id, id)).Return(nil, fmt.Errorf("error"))
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err == nil {
					t.Fail()
				}
			})

		opts := ListOptionIndexerOptions{
			Fields:       fields,
			IsNamespaced: true,
		}
		_, err := NewListOptionIndexer(context.Background(), store, opts)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewListOptionIndexer() with error from Commit(), should return an error", test: func(t *testing.T) {
		ctrl := gomock.NewController(t)
		txClient := NewMockTxClient(ctrl)
		store := NewMockStore(ctrl)
		stmt := NewMockStmt(ctrl)
		fields := [][]string{{"something"}}
		id := "somename"
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})
		store.EXPECT().RegisterAfterAdd(gomock.Any())
		store.EXPECT().RegisterAfterUpdate(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterAdd(gomock.Any()).Times(3)
		store.EXPECT().RegisterAfterUpdate(gomock.Any()).Times(3)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(1)
		store.EXPECT().RegisterAfterDeleteAll(gomock.Any()).Times(2)
		store.EXPECT().RegisterBeforeDropAll(gomock.Any()).AnyTimes()

		txClient.EXPECT().Exec(fmt.Sprintf(dropFieldsFmt, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsTableFmt, id, id, `"metadata.name" TEXT, "metadata.creationTimestamp" TEXT, "metadata.namespace" TEXT, "something" TEXT`)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.name", id, "metadata.name")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.namespace", id, "metadata.namespace")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.creationTimestamp", id, "metadata.creationTimestamp")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, fields[0][0], id, fields[0][0])).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(dropLabelsStmtFmt, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createLabelsTableFmt, id, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createLabelsTableIndexFmt, id, id)).Return(nil, nil)
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})

		opts := ListOptionIndexerOptions{
			Fields:       fields,
			IsNamespaced: true,
		}
		_, err := NewListOptionIndexer(context.Background(), store, opts)
		assert.NotNil(t, err)
	}})

	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestDropAll(t *testing.T) {
	ctx := t.Context()

	gvk := corev1.SchemeGroupVersion.WithKind("ConfigMap")
	opts := ListOptionIndexerOptions{
		IsNamespaced: true,
	}
	loi, dbPath, err := makeListOptionIndexer(ctx, gvk, opts, false, nil)
	defer cleanTempFiles(dbPath)
	assert.NoError(t, err)

	obj1 := &unstructured.Unstructured{
		Object: map[string]any{
			"metadata": map[string]any{
				"name": "obj1",
			},
		},
	}
	obj1.SetGroupVersionKind(gvk)
	err = loi.Add(obj1)
	assert.NoError(t, err)

	_, _, _, _, err = loi.ListByOptions(ctx, &sqltypes.ListOptions{}, []partition.Partition{{All: true}}, "")
	assert.NoError(t, err)

	loi.DropAll(ctx)

	_, _, _, _, err = loi.ListByOptions(ctx, &sqltypes.ListOptions{}, []partition.Partition{{All: true}}, "")
	assert.Error(t, err)
}

func TestWatchEncryption(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	gvk := corev1.SchemeGroupVersion.WithKind("Pod")

	opts := ListOptionIndexerOptions{
		Fields: [][]string{
			{"metadata", "somefield"},
			{"spec", "replicas"},
			{"spec", "minReplicas"},
		},
		IsNamespaced: true,
	}
	// shouldEncrypt = true to ensure we can write + read from encrypted events
	loi, dbPath, err := makeListOptionIndexer(ctx, gvk, opts, true, emptyNamespaceList)
	defer cleanTempFiles(dbPath)
	assert.NoError(t, err)

	foo := &unstructured.Unstructured{
		Object: map[string]any{
			"metadata": map[string]any{
				"name": "foo",
			},
			"spec": map[string]any{
				"replicas": int64(1),
			},
		},
	}
	foo.SetGroupVersionKind(gvk)
	foo.SetResourceVersion("100")
	foo2 := foo.DeepCopy()
	foo2.SetResourceVersion("120")

	startWatcher := func(ctx context.Context) (chan watch.Event, chan error) {
		errCh := make(chan error, 1)
		eventsCh := make(chan watch.Event, 100)
		go func() {
			watchErr := loi.Watch(ctx, WatchOptions{
				// Make a watch request to this specific resource version to be sure we go get from SQL database
				ResourceVersion: "100",
			}, eventsCh)
			errCh <- watchErr
		}()
		time.Sleep(100 * time.Millisecond)
		return eventsCh, errCh
	}

	waitStopWatcher := func(errCh chan error) error {
		select {
		case <-time.After(time.Second * 5):
			return fmt.Errorf("not finished in time")
		case err := <-errCh:
			return err
		}
	}

	receiveEvents := func(eventsCh chan watch.Event) []watch.Event {
		timer := time.NewTimer(time.Millisecond * 50)
		var events []watch.Event
		for {
			select {
			case <-timer.C:
				return events
			case ev := <-eventsCh:
				events = append(events, ev)
			}
		}
	}

	err = loi.Add(foo)
	assert.NoError(t, err)
	err = loi.Update(foo2)
	assert.NoError(t, err)

	watcher1, errCh1 := startWatcher(ctx)
	events := receiveEvents(watcher1)
	assert.Len(t, events, 1)
	assert.Equal(t, []watch.Event{
		{
			Type:   watch.Modified,
			Object: foo2,
		},
	}, events)

	cancel()

	err = waitStopWatcher(errCh1)
	assert.NoError(t, err)
}

func TestWatchMany(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	gvk := corev1.SchemeGroupVersion.WithKind("Pod")

	opts := ListOptionIndexerOptions{
		Fields: [][]string{
			{"metadata", "somefield"},
			{"spec", "replicas"},
			{"spec", "minReplicas"},
		},
		IsNamespaced: true,
	}
	loi, dbPath, err := makeListOptionIndexer(ctx, gvk, opts, false, emptyNamespaceList)
	defer cleanTempFiles(dbPath)
	assert.NoError(t, err)

	startWatcher := func(ctx context.Context) (chan watch.Event, chan error) {
		errCh := make(chan error, 1)
		eventsCh := make(chan watch.Event, 100)
		go func() {
			watchErr := loi.Watch(ctx, WatchOptions{}, eventsCh)
			errCh <- watchErr
		}()
		time.Sleep(100 * time.Millisecond)
		return eventsCh, errCh
	}

	waitStopWatcher := func(errCh chan error) error {
		select {
		case <-time.After(time.Second * 5):
			return fmt.Errorf("not finished in time")
		case err := <-errCh:
			return err
		}
	}

	receiveEvents := func(eventsCh chan watch.Event) []watch.Event {
		timer := time.NewTimer(time.Millisecond * 50)
		var events []watch.Event
		for {
			select {
			case <-timer.C:
				return events
			case ev := <-eventsCh:
				events = append(events, ev)
			}
		}
	}
	watcher1, errCh1 := startWatcher(ctx)
	events := receiveEvents(watcher1)
	assert.Len(t, events, 0)

	foo := &unstructured.Unstructured{
		Object: map[string]any{
			"metadata": map[string]any{
				"name": "foo",
			},
			"spec": map[string]any{
				"replicas":    int64(1),
				"minReplicas": float64(1.0),
			},
		},
	}
	foo.SetGroupVersionKind(gvk)
	foo.SetResourceVersion("100")
	foo2 := foo.DeepCopy()
	foo2.SetResourceVersion("120")
	foo2.SetLabels(map[string]string{
		"hello": "world",
	})
	foo3 := foo.DeepCopy()
	foo3.SetResourceVersion("140")
	foo4 := foo2.DeepCopy()
	foo4.SetResourceVersion("160")

	err = loi.Add(foo)
	assert.NoError(t, err)

	events = receiveEvents(watcher1)
	assert.Equal(t, []watch.Event{{Type: watch.Added, Object: foo}}, events)

	ctx2, cancel2 := context.WithCancel(context.Background())
	watcher2, errCh2 := startWatcher(ctx2)

	err = loi.Update(foo2)
	assert.NoError(t, err)

	events = receiveEvents(watcher1)
	assert.Equal(t, []watch.Event{{Type: watch.Modified, Object: foo2}}, events)

	events = receiveEvents(watcher2)
	assert.Equal(t, []watch.Event{{Type: watch.Modified, Object: foo2}}, events)

	watcher3, errCh3 := startWatcher(ctx)

	cancel2()
	err = waitStopWatcher(errCh2)
	assert.NoError(t, err)

	err = loi.Delete(foo2)
	assert.NoError(t, err)
	err = loi.Add(foo3)
	assert.NoError(t, err)
	err = loi.Update(foo4)
	assert.NoError(t, err)

	events = receiveEvents(watcher3)
	assert.Equal(t, []watch.Event{
		{Type: watch.Deleted, Object: foo2},
		{Type: watch.Added, Object: foo3},
		{Type: watch.Modified, Object: foo4},
	}, events)

	// Verify cancelled watcher don't receive anything anymore
	events = receiveEvents(watcher2)
	assert.Len(t, events, 0)

	events = receiveEvents(watcher1)
	assert.Equal(t, []watch.Event{
		{Type: watch.Deleted, Object: foo2},
		{Type: watch.Added, Object: foo3},
		{Type: watch.Modified, Object: foo4},
	}, events)

	cancel()
	err = waitStopWatcher(errCh1)
	assert.NoError(t, err)

	err = waitStopWatcher(errCh3)
	assert.NoError(t, err)
}

func TestWatchFilter(t *testing.T) {
	gvk := corev1.SchemeGroupVersion.WithKind("TestKind")
	startWatcher := func(ctx context.Context, loi *ListOptionIndexer, filter WatchFilter) (chan watch.Event, chan error) {
		errCh := make(chan error, 1)
		eventsCh := make(chan watch.Event, 100)
		go func() {
			watchErr := loi.Watch(ctx, WatchOptions{Filter: filter}, eventsCh)
			errCh <- watchErr
		}()
		time.Sleep(100 * time.Millisecond)
		return eventsCh, errCh
	}

	waitStopWatcher := func(errCh chan error) error {
		select {
		case <-time.After(time.Second * 5):
			return fmt.Errorf("not finished in time")
		case err := <-errCh:
			return err
		}
	}

	receiveEvents := func(eventsCh chan watch.Event) []watch.Event {
		timer := time.NewTimer(time.Millisecond * 50)
		var events []watch.Event
		for {
			select {
			case <-timer.C:
				return events
			case ev := <-eventsCh:
				events = append(events, ev)
			}
		}
	}

	foo := &unstructured.Unstructured{}
	foo.SetGroupVersionKind(gvk)
	foo.SetName("foo")
	foo.SetNamespace("foo")
	foo.SetLabels(map[string]string{
		"app": "foo",
	})

	fooUpdated := foo.DeepCopy()
	fooUpdated.SetLabels(map[string]string{
		"app": "changed",
	})

	bar := &unstructured.Unstructured{}
	bar.SetGroupVersionKind(gvk)
	bar.SetName("bar")
	bar.SetNamespace("bar")
	bar.SetLabels(map[string]string{
		"app": "bar",
	})

	appSelector, err := labels.Parse("app=foo")
	assert.NoError(t, err)

	tests := []struct {
		name           string
		filter         WatchFilter
		setupStore     func(store cache.Store) error
		expectedEvents []watch.Event
	}{
		{
			name:   "namespace filter",
			filter: WatchFilter{Namespace: "foo"},
			setupStore: func(store cache.Store) error {
				err := store.Add(foo)
				if err != nil {
					return err
				}
				err = store.Add(bar)
				if err != nil {
					return err
				}
				return nil
			},
			expectedEvents: []watch.Event{{Type: watch.Added, Object: foo}},
		},
		{
			name:   "selector filter",
			filter: WatchFilter{Selector: appSelector},
			setupStore: func(store cache.Store) error {
				err := store.Add(foo)
				if err != nil {
					return err
				}
				err = store.Add(bar)
				if err != nil {
					return err
				}
				err = store.Update(fooUpdated)
				if err != nil {
					return err
				}
				return nil
			},
			expectedEvents: []watch.Event{
				{Type: watch.Added, Object: foo},
				{Type: watch.Modified, Object: fooUpdated},
			},
		},
		{
			name:   "id filter",
			filter: WatchFilter{ID: "foo"},
			setupStore: func(store cache.Store) error {
				err := store.Add(foo)
				if err != nil {
					return err
				}
				err = store.Add(bar)
				if err != nil {
					return err
				}
				err = store.Update(fooUpdated)
				if err != nil {
					return err
				}
				err = store.Update(foo)
				if err != nil {
					return err
				}
				return nil
			},
			expectedEvents: []watch.Event{
				{Type: watch.Added, Object: foo},
				{Type: watch.Modified, Object: fooUpdated},
				{Type: watch.Modified, Object: foo},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())

			opts := ListOptionIndexerOptions{
				Fields:       [][]string{{"metadata", "somefield"}},
				IsNamespaced: true,
			}
			loi, dbPath, err := makeListOptionIndexer(ctx, gvk, opts, false, emptyNamespaceList)
			defer cleanTempFiles(dbPath)
			assert.NoError(t, err)

			wCh, errCh := startWatcher(ctx, loi, WatchFilter{
				Namespace: "foo",
			})

			if test.setupStore != nil {
				err = test.setupStore(loi)
				assert.NoError(t, err)
			}

			events := receiveEvents(wCh)
			assert.Equal(t, test.expectedEvents, events)

			cancel()
			err = waitStopWatcher(errCh)
			assert.NoError(t, err)

		})
	}

}

func TestWatchResourceVersion(t *testing.T) {
	gvk := corev1.SchemeGroupVersion.WithKind("ConfigMap")
	startWatcher := func(ctx context.Context, loi *ListOptionIndexer, rv string) (chan watch.Event, chan error) {
		errCh := make(chan error, 1)
		eventsCh := make(chan watch.Event, 100)
		go func() {
			watchErr := loi.Watch(ctx, WatchOptions{ResourceVersion: rv}, eventsCh)
			errCh <- watchErr
		}()
		time.Sleep(100 * time.Millisecond)
		return eventsCh, errCh
	}

	waitStopWatcher := func(errCh chan error) error {
		select {
		case <-time.After(time.Second * 5):
			return fmt.Errorf("not finished in time")
		case err := <-errCh:
			return err
		}
	}

	receiveEvents := func(eventsCh chan watch.Event) []watch.Event {
		timer := time.NewTimer(time.Millisecond * 50)
		var events []watch.Event
		for {
			select {
			case <-timer.C:
				return events
			case ev := <-eventsCh:
				events = append(events, ev)
			}
		}
	}

	foo := &unstructured.Unstructured{}
	foo.SetGroupVersionKind(gvk)
	foo.SetResourceVersion("100")
	foo.SetName("foo")
	foo.SetNamespace("foo")
	foo.Object["id"] = "foo/foo"
	foo.SetLabels(map[string]string{
		"app": "foo",
	})

	fooUpdated := foo.DeepCopy()
	fooUpdated.SetResourceVersion("120")
	fooUpdated.SetLabels(map[string]string{
		"app": "changed",
	})

	bar := &unstructured.Unstructured{}
	bar.SetGroupVersionKind(gvk)
	bar.SetResourceVersion("150")
	bar.SetName("bar")
	bar.SetNamespace("bar")
	bar.Object["id"] = "bar/bar"
	bar.SetLabels(map[string]string{
		"app": "bar",
	})

	barDeleted := bar.DeepCopy()
	barDeleted.SetResourceVersion("160")

	barNew := bar.DeepCopy()
	barNew.SetResourceVersion("170")

	parentCtx := context.Background()

	opts := ListOptionIndexerOptions{
		IsNamespaced: true,
	}
	loi, dbPath, err := makeListOptionIndexer(parentCtx, gvk, opts, false, emptyNamespaceList)
	defer cleanTempFiles(dbPath)
	assert.NoError(t, err)

	getRV := func(t *testing.T) string {
		t.Helper()
		list, _, _, _, err := loi.ListByOptions(parentCtx, &sqltypes.ListOptions{}, []partition.Partition{{All: true}}, "")
		assert.NoError(t, err)
		return list.GetResourceVersion()
	}

	err = loi.Add(foo)
	assert.NoError(t, err)
	rv1 := getRV(t)

	err = loi.Update(fooUpdated)
	assert.NoError(t, err)
	rv2 := getRV(t)

	err = loi.Add(bar)
	assert.NoError(t, err)
	rv3 := getRV(t)

	err = loi.Delete(barDeleted)
	assert.NoError(t, err)
	rv4 := getRV(t)

	err = loi.Add(barNew)
	assert.NoError(t, err)
	rv5 := getRV(t)

	tests := []struct {
		rv             string
		expectedEvents []watch.Event
		expectedErr    error
	}{
		{
			rv: "",
		},
		{
			rv: rv1,
			expectedEvents: []watch.Event{
				{Type: watch.Modified, Object: fooUpdated},
				{Type: watch.Added, Object: bar},
				{Type: watch.Deleted, Object: barDeleted},
				{Type: watch.Added, Object: barNew},
			},
		},
		{
			rv: rv2,
			expectedEvents: []watch.Event{
				{Type: watch.Added, Object: bar},
				{Type: watch.Deleted, Object: barDeleted},
				{Type: watch.Added, Object: barNew},
			},
		},
		{
			rv: rv3,
			expectedEvents: []watch.Event{
				{Type: watch.Deleted, Object: barDeleted},
				{Type: watch.Added, Object: barNew},
			},
		},
		{
			rv: rv4,
			expectedEvents: []watch.Event{
				{Type: watch.Added, Object: barNew},
			},
		},
		{
			rv:             rv5,
			expectedEvents: nil,
		},
		{
			rv:          "unknown",
			expectedErr: ErrTooOld,
		},
	}

	for _, test := range tests {
		t.Run(test.rv, func(t *testing.T) {
			ctx, cancel := context.WithCancel(parentCtx)
			watcherCh, errCh := startWatcher(ctx, loi, test.rv)
			gotEvents := receiveEvents(watcherCh)

			cancel()
			err := waitStopWatcher(errCh)
			if test.expectedErr != nil {
				assert.ErrorIs(t, err, ErrTooOld)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.expectedEvents, gotEvents)
			}
		})
	}
}

func TestNonNumberResourceVersion(t *testing.T) {
	ctx := context.Background()
	gvk := corev1.SchemeGroupVersion.WithKind("TestKind")

	opts := ListOptionIndexerOptions{
		Fields:       [][]string{{"metadata", "somefield"}},
		IsNamespaced: true,
	}
	loi, dbPath, err := makeListOptionIndexer(ctx, gvk, opts, false, emptyNamespaceList)
	defer cleanTempFiles(dbPath)
	assert.NoError(t, err)

	foo := &unstructured.Unstructured{
		Object: map[string]any{
			"metadata": map[string]any{
				"name": "foo",
			},
			"id": "/foo",
		},
	}
	foo.SetGroupVersionKind(gvk)
	foo.SetResourceVersion("a")
	foo2 := foo.DeepCopy()
	foo2.SetResourceVersion("b")
	foo2.SetLabels(map[string]string{
		"hello": "world",
	})
	bar := &unstructured.Unstructured{
		Object: map[string]any{
			"metadata": map[string]any{
				"name": "bar",
			},
			"id": "/bar",
		},
	}
	bar.SetGroupVersionKind(gvk)
	bar.SetResourceVersion("c")
	err = loi.Add(foo)
	assert.NoError(t, err)
	err = loi.Update(foo2)
	assert.NoError(t, err)
	err = loi.Add(bar)
	assert.NoError(t, err)

	expectedUnstructured := &unstructured.Unstructured{
		Object: map[string]any{
			"items": []any{bar.Object, foo2.Object},
		},
	}
	expectedList, err := expectedUnstructured.ToList()
	require.NoError(t, err)

	list, _, _, _, err := loi.ListByOptions(ctx, &sqltypes.ListOptions{}, []partition.Partition{{All: true}}, "")
	require.NoError(t, err)
	assert.Equal(t, expectedList.Items, list.Items)
}

// Test that we don't panic in case the transaction fails but stil manages to add a watcher
func TestWatchCancel(t *testing.T) {
	gvk := corev1.SchemeGroupVersion.WithKind("TestKind")
	startWatcher := func(ctx context.Context, loi *ListOptionIndexer, rv string) (chan watch.Event, chan error) {
		eventsCh := make(chan watch.Event, 1)
		errCh := make(chan error, 1)
		go func() {
			watchErr := loi.Watch(ctx, WatchOptions{ResourceVersion: rv}, eventsCh)
			errCh <- watchErr
			close(eventsCh)
		}()
		time.Sleep(100 * time.Millisecond)
		return eventsCh, errCh
	}

	ctx := context.Background()

	opts := ListOptionIndexerOptions{
		Fields:       [][]string{{"metadata", "somefield"}},
		IsNamespaced: true,
	}
	loi, dbPath, err := makeListOptionIndexer(ctx, gvk, opts, false, emptyNamespaceList)
	defer cleanTempFiles(dbPath)
	assert.NoError(t, err)

	foo := &unstructured.Unstructured{
		Object: map[string]any{
			"metadata": map[string]any{
				"name": "foo",
			},
		},
	}
	foo.SetGroupVersionKind(gvk)
	foo.SetResourceVersion("100")

	foo2 := foo.DeepCopy()
	foo2.SetResourceVersion("200")

	foo3 := foo.DeepCopy()
	foo3.SetResourceVersion("300")

	err = loi.Add(foo)
	assert.NoError(t, err)
	loi.Add(foo2)
	assert.NoError(t, err)
	loi.Add(foo3)
	assert.NoError(t, err)

	watchCtx, watchCancel := context.WithCancel(ctx)

	eventsCh, errCh := startWatcher(watchCtx, loi, "100")

	<-eventsCh

	watchCancel()

	<-eventsCh

	go func() {
		foo4 := foo.DeepCopy()
		foo4.SetResourceVersion("400")
		loi.Add(foo4)
	}()
	<-errCh
	time.Sleep(1 * time.Second)
}
