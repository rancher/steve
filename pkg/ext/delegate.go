package ext

import (
	"context"
	"errors"
	"fmt"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
)

var (
	errMissingUserInfo = errors.New("missing user info")
)

// Delegate is the bridge between k8s.io/apiserver's [rest.Storage] interface and
// our own Store interface we want developers to use
//
// It currently supports non-namespaced stores only because Store[T, TList] doesn't
// expose namespaces anywhere. When needed we'll add support to namespaced resources.
type Delegate[T runtime.Object, TList runtime.Object] struct {
	scheme *runtime.Scheme
	// t is the resource of the delegate (eg: *Token) and must be non-nil.
	t T
	// tList is the resource list of the delegate (eg: *TokenList) and must be non-nil.
	tList        TList
	gvk          schema.GroupVersionKind
	gvr          schema.GroupVersionResource
	singularName string
	authorizer   authorizer.Authorizer
}

// New implements [rest.Storage]
//
// It uses generics to create the resource and set its GVK.
func (d *Delegate[T, TList]) New() runtime.Object {
	t := d.t.DeepCopyObject()
	t.GetObjectKind().SetGroupVersionKind(d.gvk)
	return t
}

// Destroy cleans up its resources on shutdown.
// Destroy has to be implemented in thread-safe way and be prepared
// for being called more than once.
//
// It is NOT meant to delete resources from the backing storage. It is meant to
// stop clients, runners, etc that could be running for the store when the extension
// API server gracefully shutdowns/exits.
func (d *Delegate[T, TList]) Destroy() {
}

// NewList implements [rest.Lister]
//
// It uses generics to create the resource and set its GVK.
func (d *Delegate[T, TList]) NewList() runtime.Object {
	tList := d.tList.DeepCopyObject()
	tList.GetObjectKind().SetGroupVersionKind(d.gvk)
	return tList
}

// List implements [rest.Lister]
func (d *Delegate[T, TList]) List(parentCtx context.Context, internaloptions *metainternalversion.ListOptions, listFn ListFunc[TList]) (runtime.Object, error) {
	result, err := d.list(parentCtx, internaloptions, listFn)
	if err != nil {
		return nil, convertError(err)
	}
	return result, nil
}

func (d *Delegate[T, TList]) list(parentCtx context.Context, internaloptions *metainternalversion.ListOptions, listFn ListFunc[TList]) (runtime.Object, error) {
	ctx, err := d.makeContext(parentCtx)
	if err != nil {
		return nil, err
	}

	options, err := d.convertListOptions(internaloptions)
	if err != nil {
		return nil, err
	}

	return listFn(ctx, options)
}

// ConvertToTable implements [rest.Lister]
//
// It converts an object or a list of objects to a table, which is used by kubectl
// (and Rancher UI) to display a table of the items.
//
// Currently, we use the default table convertor which will show two columns: Name and Created At.
func (d *Delegate[T, TList]) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	result, err := d.convertToTable(ctx, object, tableOptions)
	if err != nil {
		return nil, convertError(err)
	}
	return result, nil
}

func (d *Delegate[T, TList]) convertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	defaultTableConverter := rest.NewDefaultTableConvertor(d.gvr.GroupResource())
	return defaultTableConverter.ConvertToTable(ctx, object, tableOptions)
}

// Get implements [rest.Getter]
func (d *Delegate[T, TList]) Get(parentCtx context.Context, name string, options *metav1.GetOptions, getFn GetFunc[T]) (runtime.Object, error) {
	result, err := d.get(parentCtx, name, options, getFn)
	if err != nil {
		return nil, convertError(err)
	}
	return result, nil
}

func (d *Delegate[T, TList]) get(parentCtx context.Context, name string, options *metav1.GetOptions, getFn GetFunc[T]) (runtime.Object, error) {
	ctx, err := d.makeContext(parentCtx)
	if err != nil {
		return nil, err
	}

	return getFn(ctx, name, options)
}

// Delete implements [rest.GracefulDeleter]
//
// deleteValidation is used to do some validation on the object before deleting
// it in the store. For example, running mutating/validating webhooks, though we're not using these yet.
func (d *Delegate[T, TList]) Delete(parentCtx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions, getFn GetFunc[T], deleteFn DeleteFunc[T]) (runtime.Object, bool, error) {
	result, completed, err := d.delete(parentCtx, name, deleteValidation, options, getFn, deleteFn)
	if err != nil {
		return nil, false, convertError(err)
	}
	return result, completed, nil
}

func (d *Delegate[T, TList]) delete(parentCtx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions, getFn GetFunc[T], deleteFn DeleteFunc[T]) (runtime.Object, bool, error) {
	ctx, err := d.makeContext(parentCtx)
	if err != nil {
		return nil, false, err
	}

	oldObj, err := getFn(ctx, name, &metav1.GetOptions{})
	if err != nil {
		return nil, false, err
	}

	if deleteValidation != nil {
		if err = deleteValidation(ctx, oldObj); err != nil {
			return nil, false, err
		}
	}

	err = deleteFn(ctx, name, options)
	return oldObj, true, err
}

// Create implements [rest.Creater]
//
// createValidation is used to do some validation on the object before creating
// it in the store. For example, running mutating/validating webhooks, though we're not using these yet.
//
//nolint:misspell
func (d *Delegate[T, TList]) Create(parentCtx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions, createFn CreateFunc[T]) (runtime.Object, error) {
	result, err := d.create(parentCtx, obj, createValidation, options, createFn)
	if err != nil {
		return nil, convertError(err)
	}
	return result, nil
}

func (d *Delegate[T, TList]) create(parentCtx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions, createFn CreateFunc[T]) (runtime.Object, error) {
	ctx, err := d.makeContext(parentCtx)
	if err != nil {
		return nil, err
	}

	if createValidation != nil {
		err := createValidation(ctx, obj)
		if err != nil {
			return obj, err
		}
	}

	tObj, ok := obj.(T)
	if !ok {
		return nil, fmt.Errorf("object was of type %T, not of expected type %T", obj, d.t)
	}

	return createFn(ctx, tObj, options)
}

// Update implements [rest.Updater]
//
// createValidation is used to do some validation on the object before creating
// it in the store. For example, it will do an authorization check for "create"
// verb if the object needs to be created.
// See here for details: https://github.com/kubernetes/apiserver/blob/70ed6fdbea9eb37bd1d7558e90c20cfe888955e8/pkg/endpoints/handlers/update.go#L190-L201
// Another example is running mutating/validating webhooks, though we're not using these yet.
//
// updateValidation is used to do some validation on the object before updating it in the store.
// One example is running mutating/validating webhooks, though we're not using these yet.
func (d *Delegate[T, TList]) Update(
	parentCtx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	options *metav1.UpdateOptions,
	getFn GetFunc[T],
	createFn CreateFunc[T],
	updateFn UpdateFunc[T],
) (runtime.Object, bool, error) {
	result, created, err := d.update(parentCtx, name, objInfo, createValidation, updateValidation, forceAllowCreate, options, getFn, createFn, updateFn)
	if err != nil {
		return nil, false, convertError(err)
	}
	return result, created, nil
}

func (d *Delegate[T, TList]) update(
	parentCtx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	options *metav1.UpdateOptions,
	getFn GetFunc[T],
	createFn CreateFunc[T],
	updateFn UpdateFunc[T],
) (runtime.Object, bool, error) {
	ctx, err := d.makeContext(parentCtx)
	if err != nil {
		return nil, false, err
	}

	oldObj, err := getFn(ctx, name, &metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, false, err
		}

		obj, err := objInfo.UpdatedObject(ctx, nil)
		if err != nil {
			return nil, false, err
		}

		if err = createValidation(ctx, obj); err != nil {
			return nil, false, err
		}

		tObj, ok := obj.(T)
		if !ok {
			return nil, false, fmt.Errorf("object was of type %T, not of expected type %T", obj, d.t)
		}

		newObj, err := createFn(ctx, tObj, &metav1.CreateOptions{})
		if err != nil {
			return nil, false, err
		}
		return newObj, true, err
	}

	newObj, err := objInfo.UpdatedObject(ctx, oldObj)
	if err != nil {
		return nil, false, err
	}

	newT, ok := newObj.(T)
	if !ok {
		return nil, false, fmt.Errorf("object was of type %T, not of expected type %T", newObj, d.t)
	}

	if updateValidation != nil {
		err = updateValidation(ctx, newT, oldObj)
		if err != nil {
			return nil, false, err
		}
	}

	newT, err = updateFn(ctx, newT, options)
	if err != nil {
		return nil, false, err
	}

	return newT, false, nil
}

type watcher struct {
	closedLock sync.RWMutex
	closed     bool
	ch         chan watch.Event
}

func (w *watcher) Stop() {
	w.closedLock.Lock()
	defer w.closedLock.Unlock()
	if !w.closed {
		close(w.ch)
		w.closed = true
	}
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

func (w *watcher) ResultChan() <-chan watch.Event {
	return w.ch
}

func (d *Delegate[T, TList]) Watch(parentCtx context.Context, internaloptions *metainternalversion.ListOptions, watchFn WatchFunc[T]) (watch.Interface, error) {
	result, err := d.watch(parentCtx, internaloptions, watchFn)
	if err != nil {
		return nil, convertError(err)
	}
	return result, nil
}

func (d *Delegate[T, TList]) watch(parentCtx context.Context, internaloptions *metainternalversion.ListOptions, watchFn WatchFunc[T]) (watch.Interface, error) {
	ctx, err := d.makeContext(parentCtx)
	if err != nil {
		return nil, err
	}

	options, err := d.convertListOptions(internaloptions)
	if err != nil {
		return nil, err
	}

	w := &watcher{
		ch: make(chan watch.Event),
	}
	go func() {
		// Not much point continuing the watch if the store stopped its watch.
		// Double stopping here is fine.
		defer w.Stop()

		// Closing eventCh is the responsibility of the store.Watch method
		// to avoid the store panicking while trying to send to a close channel
		eventCh, err := watchFn(ctx, options)
		if err != nil {
			return
		}

		for event := range eventCh {
			added := w.addEvent(watch.Event{
				Type:   event.Event,
				Object: event.Object,
			})
			if !added {
				break
			}
		}
	}()

	return w, nil
}

// GroupVersionKind implements rest.GroupVersionKind
//
// This is used to generate the data for the Discovery API
func (d *Delegate[T, TList]) GroupVersionKind(_ schema.GroupVersion) schema.GroupVersionKind {
	return d.gvk
}

// NamespaceScoped implements rest.Scoper
//
// The delegate is used for non-namespaced resources so it always returns false
func (d *Delegate[T, TList]) NamespaceScoped() bool {
	return false
}

// Kind implements rest.KindProvider
//
// XXX: Example where / how this is used
func (d *Delegate[T, TList]) Kind() string {
	return d.gvk.Kind
}

// GetSingularName implements rest.SingularNameProvider
//
// This is used by a variety of things such as kubectl to map singular name to
// resource name. (eg: token => tokens)
func (d *Delegate[T, TList]) GetSingularName() string {
	return d.singularName
}

func (d *Delegate[T, TList]) makeContext(parentCtx context.Context) (Context, error) {
	userInfo, ok := request.UserFrom(parentCtx)
	if !ok {
		return Context{}, errMissingUserInfo
	}

	ctx := Context{
		Context:              parentCtx,
		User:                 userInfo,
		Authorizer:           d.authorizer,
		GroupVersionResource: d.gvr,
	}
	return ctx, nil
}

func (d *Delegate[T, TList]) convertListOptions(options *metainternalversion.ListOptions) (*metav1.ListOptions, error) {
	var out metav1.ListOptions
	err := d.scheme.Convert(options, &out, nil)
	if err != nil {
		return nil, fmt.Errorf("convert list options: %w", err)
	}

	return &out, nil
}

func convertError(err error) error {
	if _, ok := err.(apierrors.APIStatus); ok {
		return err
	}

	return apierrors.NewInternalError(err)
}
