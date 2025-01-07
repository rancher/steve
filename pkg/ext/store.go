package ext

import (
	"context"
	"fmt"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/registry/rest"
)

// ConvertFunc will convert a object to a list of cell in a metav1.Table (think kubectl get table output)
type ConvertFunc[T runtime.Object] func(obj T) []string

// ConvertToTable implements [rest.Lister]
//
// It converts an object or a list of objects to a table, which is used by kubectl
// (and Rancher UI) to display a table of the items.
//
// Currently, we use the default table convertor which will show two columns: Name and Created At.
func ConvertToTable[T runtime.Object](ctx context.Context, object runtime.Object, tableOptions runtime.Object, groupResource schema.GroupResource, columnDefs []metav1.TableColumnDefinition, convertFn ConvertFunc[T]) (*metav1.Table, error) {
	result, err := convertToTable(ctx, object, tableOptions, groupResource, columnDefs, convertFn)
	if err != nil {
		return nil, convertError(err)
	}
	return result, nil
}

func ConvertToTableDefault[T runtime.Object](ctx context.Context, object runtime.Object, tableOptions runtime.Object, groupResource schema.GroupResource) (*metav1.Table, error) {
	return ConvertToTable[T](ctx, object, tableOptions, groupResource, nil, nil)
}

func convertToTable[T runtime.Object](ctx context.Context, object runtime.Object, tableOptions runtime.Object, groupResource schema.GroupResource, columnDefs []metav1.TableColumnDefinition, convertFn ConvertFunc[T]) (*metav1.Table, error) {
	defaultTableConverter := rest.NewDefaultTableConvertor(groupResource)
	table, err := defaultTableConverter.ConvertToTable(ctx, object, tableOptions)
	if err != nil {
		return nil, err
	}

	if columnDefs == nil {
		return table, nil
	}

	// Override only if there were definitions before (to respect the NoHeader option)
	if len(table.ColumnDefinitions) > 0 {
		table.ColumnDefinitions = columnDefs
	}
	table.Rows = []metav1.TableRow{}
	fn := func(obj runtime.Object) error {
		objT, ok := obj.(T)
		if !ok {
			var zeroT T
			return fmt.Errorf("expected %T but got %T", zeroT, obj)
		}
		cells := convertFn(objT)
		if len(cells) != len(columnDefs) {
			return fmt.Errorf("defined %d columns but got %d cells", len(columnDefs), len(cells))
		}

		table.Rows = append(table.Rows, metav1.TableRow{
			Cells:  cellStringToCellAny(cells),
			Object: runtime.RawExtension{Object: obj},
		})
		return nil
	}
	switch {
	case meta.IsListType(object):
		if err := meta.EachListItem(object, fn); err != nil {
			return nil, err
		}
	default:
		if err := fn(object); err != nil {
			return nil, err
		}
	}

	return table, nil
}

func cellStringToCellAny(cells []string) []any {
	res := []any{}
	for _, cell := range cells {
		res = append(res, cell)
	}
	return res
}

// CreateOrUpdate helps implement [rest.Updater] by handling most of the logic.
//
// createValidation is used to do some validation on the object before creating
// it in the store. For example, it will do an authorization check for "create"
// verb if the object needs to be created.
// See here for details: https://github.com/kubernetes/apiserver/blob/70ed6fdbea9eb37bd1d7558e90c20cfe888955e8/pkg/endpoints/handlers/update.go#L190-L201
// Another example is running mutating/validating webhooks, though we're not using these yet.
//
// updateValidation is used to do some validation on the object before updating it in the store.
// One example is running mutating/validating webhooks, though we're not using these yet.
func CreateOrUpdate[T runtime.Object](
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	options *metav1.UpdateOptions,
	getFn func(ctx context.Context, name string, opts *metav1.GetOptions) (T, error),
	createFn func(ctx context.Context, obj T, opts *metav1.CreateOptions) (T, error),
	updateFn func(ctx context.Context, obj T, opts *metav1.UpdateOptions) (T, error),
) (runtime.Object, bool, error) {
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
			var zeroT T
			return nil, false, fmt.Errorf("object was of type %T, not of expected type %T", obj, zeroT)
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
		var zeroT T
		return nil, false, fmt.Errorf("object was of type %T, not of expected type %T", newObj, zeroT)
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

type WatchEvent[T runtime.Object] struct {
	Event  watch.EventType
	Object T
}

func doWatch[T runtime.Object](
	ctx context.Context,
	scheme *runtime.Scheme,
	internaloptions *metainternalversion.ListOptions,
	watchFn func(context.Context, *metav1.ListOptions) (chan WatchEvent[T], error),
) (watch.Interface, error) {
	options, err := convertListOptions(internaloptions, scheme)
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

func convertListOptions(options *metainternalversion.ListOptions, scheme *runtime.Scheme) (*metav1.ListOptions, error) {
	var out metav1.ListOptions
	err := scheme.Convert(options, &out, nil)
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
