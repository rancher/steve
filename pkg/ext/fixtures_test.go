//+k8s:openapi-gen=true
//+k8s:openapi-model-package=io.ok.ok

package ext

import (
	"context"
	"fmt"
	"sort"
	"sync"

	testapisv1 "github.com/rancher/steve/pkg/ext/testapis/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/registry/rest"
)

var (
	schemeBuilderTest = runtime.NewSchemeBuilder(addKnownTypesTest)
	addToSchemeTest   = schemeBuilderTest.AddToScheme
)

func addKnownTypesTest(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(testTypeGV,
		&testapisv1.TestType{},
		&testapisv1.TestTypeList{},
		&testapisv1.TestTypeOther{},
		&testapisv1.TestTypeOtherList{},
	)
	metav1.AddToGroupVersion(scheme, testTypeGV)
	return nil
}

var (
	testTypeResource = "testtypes"

	testTypeGV = schema.GroupVersion{
		Group:   "ext.cattle.io",
		Version: "v1",
	}

	testTypeGVR = schema.GroupVersionResource{
		Group:    testTypeGV.Group,
		Version:  testTypeGV.Version,
		Resource: testTypeResource,
	}

	testTypeListFixture = testapisv1.TestTypeList{
		TypeMeta: metav1.TypeMeta{
			Kind:       "TestTypeList",
			APIVersion: testTypeGV.String(),
		},
		Items: []testapisv1.TestType{
			testTypeFixture,
		},
	}

	testTypeFixture = testapisv1.TestType{
		TypeMeta: metav1.TypeMeta{
			Kind:       "TestType",
			APIVersion: testTypeGV.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo",
		},
	}

	testTypeOtherListFixture = testapisv1.TestTypeOtherList{
		TypeMeta: metav1.TypeMeta{
			Kind:       "TestTypeOtherList",
			APIVersion: testTypeGV.String(),
		},
		Items: []testapisv1.TestTypeOther{
			testTypeOtherFixture,
		},
	}

	testTypeOtherFixture = testapisv1.TestTypeOther{
		TypeMeta: metav1.TypeMeta{
			Kind:       "TestTypeOther",
			APIVersion: testTypeGV.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "bar",
		},
	}
)

var _ rest.Storage = (*testStore[*testapisv1.TestType, *testapisv1.TestTypeList])(nil)
var _ rest.Lister = (*testStore[*testapisv1.TestType, *testapisv1.TestTypeList])(nil)
var _ rest.GracefulDeleter = (*testStore[*testapisv1.TestType, *testapisv1.TestTypeList])(nil)
var _ rest.Creater = (*testStore[*testapisv1.TestType, *testapisv1.TestTypeList])(nil)
var _ rest.Updater = (*testStore[*testapisv1.TestType, *testapisv1.TestTypeList])(nil)
var _ rest.Getter = (*testStore[*testapisv1.TestType, *testapisv1.TestTypeList])(nil)

type testStore[T runtime.Object, TList runtime.Object] struct {
	singular string
	objT     T
	objListT TList
	gvk      schema.GroupVersionKind
	gvr      schema.GroupVersionResource

	// lock protects both items and watcher
	lock    sync.Mutex
	items   map[string]*testapisv1.TestType
	watcher *watcher
}

func newDefaultTestStore() *testStore[*testapisv1.TestType, *testapisv1.TestTypeList] {
	return &testStore[*testapisv1.TestType, *testapisv1.TestTypeList]{
		singular: "testtype",
		objT:     &testapisv1.TestType{},
		objListT: &testapisv1.TestTypeList{},
		gvk:      testTypeGV.WithKind("TestType"),
		gvr:      testTypeGVR,
		items: map[string]*testapisv1.TestType{
			testTypeFixture.Name: &testTypeFixture,
		},
	}
}

// New implements [rest.Storage]
func (t *testStore[T, TList]) New() runtime.Object {
	obj := t.objT.DeepCopyObject()
	obj.GetObjectKind().SetGroupVersionKind(t.gvk)
	return obj
}

// GetSingularName implements [rest.SingularNameProvider]
func (t *testStore[T, TList]) GetSingularName() string {
	return t.singular
}

// NamespaceScoped implements [rest.Scoper]
func (t *testStore[T, TList]) NamespaceScoped() bool {
	return false
}

// GroupVersionKind implements [rest.GroupVersionKindProvider]
func (t *testStore[T, TList]) GroupVersionKind(_ schema.GroupVersion) schema.GroupVersionKind {
	return t.gvk
}

// Destroy implements [rest.Storage]
func (t *testStore[T, TList]) Destroy() {
}

// Get implements [rest.Getter]
func (t *testStore[T, TList]) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.get(ctx, name, options)
}

// Create implements [rest.Creater]
func (t *testStore[T, TList]) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if createValidation != nil {
		err := createValidation(ctx, obj)
		if err != nil {
			return obj, err
		}
	}

	objT, ok := obj.(*testapisv1.TestType)
	if !ok {
		var zeroT T
		return nil, convertError(fmt.Errorf("expected %T but got %T", zeroT, obj))
	}

	return t.create(ctx, objT, options)
}

// Update implements [rest.Updater]
func (t *testStore[T, TList]) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	return CreateOrUpdate(ctx, name, objInfo, createValidation, updateValidation, forceAllowCreate, options, t.get, t.create, t.update)
}

func (t *testStore[T, TList]) get(_ context.Context, name string, _ *metav1.GetOptions) (*testapisv1.TestType, error) {
	obj, found := t.items[name]
	if !found {
		return nil, apierrors.NewNotFound(t.gvr.GroupResource(), name)
	}
	return obj, nil
}

func (t *testStore[T, TList]) create(_ context.Context, obj *testapisv1.TestType, _ *metav1.CreateOptions) (*testapisv1.TestType, error) {
	if _, found := t.items[obj.Name]; found {
		return nil, apierrors.NewAlreadyExists(t.gvr.GroupResource(), obj.Name)
	}
	t.items[obj.Name] = obj
	t.addEventLocked(watch.Event{
		Type:   watch.Added,
		Object: obj,
	})
	return obj, nil
}

func (t *testStore[T, TList]) update(_ context.Context, obj *testapisv1.TestType, _ *metav1.UpdateOptions) (*testapisv1.TestType, error) {
	if _, found := t.items[obj.Name]; !found {
		return nil, apierrors.NewNotFound(t.gvr.GroupResource(), obj.Name)
	}
	obj.ManagedFields = []metav1.ManagedFieldsEntry{}
	t.items[obj.Name] = obj
	t.addEventLocked(watch.Event{
		Type:   watch.Modified,
		Object: obj,
	})
	return obj, nil
}

// NewList implements [rest.Lister]
func (t *testStore[T, TList]) NewList() runtime.Object {
	objList := t.objListT.DeepCopyObject()
	objList.GetObjectKind().SetGroupVersionKind(t.gvk)
	return objList
}

// List implements [rest.Lister]
func (t *testStore[T, TList]) List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	items := []testapisv1.TestType{}
	for _, obj := range t.items {
		items = append(items, *obj)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Name > items[j].Name
	})
	list := &testapisv1.TestTypeList{
		Items: items,
	}
	return list, nil
}

// ConvertToTable implements [rest.Lister]
func (t *testStore[T, TList]) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return ConvertToTableDefault[T](ctx, object, tableOptions, t.gvr.GroupResource())
}

// Watch implements [rest.Watcher]
func (t *testStore[T, TList]) Watch(ctx context.Context, internaloptions *metainternalversion.ListOptions) (watch.Interface, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	w := &watcher{
		ch: make(chan watch.Event, 100),
	}
	t.watcher = w
	return w, nil
}

func (t *testStore[T, TList]) addEventLocked(event watch.Event) {
	if t.watcher != nil {
		t.watcher.addEvent(event)
	}
}

// Delete implements [rest.GracefulDeleter]
func (t *testStore[T, TList]) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	obj, found := t.items[name]
	if !found {
		return nil, false, apierrors.NewNotFound(t.gvr.GroupResource(), name)
	}

	if deleteValidation != nil {
		err := deleteValidation(ctx, obj)
		if err != nil {
			return nil, false, err
		}
	}

	delete(t.items, name)
	t.addEventLocked(watch.Event{
		Type:   watch.Deleted,
		Object: obj,
	})
	return obj, true, nil
}

type watcher struct {
	closedLock sync.RWMutex
	closed     bool
	ch         chan watch.Event
}

// Stop implements [watch.Interface]
//
// As documented, Stop must only be called by the consumer (the k8s library) not the producer (our store)
func (w *watcher) Stop() {
	w.closedLock.Lock()
	defer w.closedLock.Unlock()
	if !w.closed {
		close(w.ch)
		w.closed = true
	}
}

// ResultChan implements [watch.Interface]
func (w *watcher) ResultChan() <-chan watch.Event {
	return w.ch
}

func (w *watcher) addEvent(event watch.Event) bool {
	w.closedLock.RLock()
	defer w.closedLock.RUnlock()
	if w.closed {
		return false
	}

	w.ch <- event
	return true
}
