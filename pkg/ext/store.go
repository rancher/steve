package ext

import (
	"context"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/registry/rest"
)

// Context wraps a context.Context and adds a few fields that will be useful for
// each requests handled by a Store.
//
// It will allow us to add more such fields without breaking Store implementation.
type Context struct {
	context.Context

	// User is the user making the request
	User user.Info
	// Authorizer helps you determines if a user is authorized to perform
	// actions to specific resources.
	Authorizer authorizer.Authorizer
	// GroupVersionResource is the GVR of the request.
	// It makes it easy to create errors such as in:
	//     apierrors.NewNotFound(ctx.GroupVersionResource.GroupResource(), name)
	GroupVersionResource schema.GroupVersionResource
}

type CreateFunc[T runtime.Object] func(ctx Context, obj T, opts *metav1.CreateOptions) (T, error)
type UpdateFunc[T runtime.Object] func(ctx Context, obj T, opts *metav1.UpdateOptions) (T, error)
type GetFunc[T runtime.Object] func(ctx Context, name string, opts *metav1.GetOptions) (T, error)
type ListFunc[TList runtime.Object] func(ctx Context, opts *metav1.ListOptions) (TList, error)
type WatchFunc[T runtime.Object] func(ctx Context, opts *metav1.ListOptions) (<-chan WatchEvent[T], error)
type DeleteFunc[T runtime.Object] func(ctx Context, name string, opts *metav1.DeleteOptions) error

// ConvertFunc will convert a object to a list of cell in a metav1.Table (think kubectl get table output)
type ConvertFunc[T runtime.Object] func(obj T) []string

type Storage[T runtime.Object, TList runtime.Object] interface {
	InjectDelegate(*Delegate[T, TList])

	rest.Storage
	rest.Scoper
	rest.KindProvider
	rest.SingularNameProvider
}

// Store should provide all required operations to serve a given resource. A
// resource is defined by the resource itself (T) and a list type for the resource (TList).
// For example, Store[*Token, *TokenList] is a store that allows CRUD operations on *Token
// objects and allows listing tokens in a *TokenList object.
//
// Store does not define the backing storage for a resource. The storage is
// up to the implementer. For example, resources could be stored in another ETCD
// database, in a SQLite database, in another built-in resource such as Secrets.
// It is also possible to have no storage at all.
//
// Errors returned by the Store should use errors from k8s.io/apimachinery/pkg/api/errors. This
// will ensure that the right error will be returned to the clients (eg: kubectl, client-go) so
// they can react accordingly. For example, if an object is not found, store should
// return the following error:
//
//	apierrors.NewNotFound(ctx.GroupVersionResource.GroupResource(), name)
//
// Stores should make use of the various metav1.*Options as best as possible.
// Those options are the same options coming from client-go or kubectl, generally
// meant to control the behavior of the stores. Note: We currently don't have
// field-manager enabled.
type Store[T runtime.Object, TList runtime.Object] interface {
	// Create should store the resource to some backing storage.
	//
	// It can apply modifications as necessary before storing it. It must
	// return a resource of the type of the store, but can
	// create/update/delete arbitrary objects in Kubernetes without
	// returning them to the user.
	//
	// It is called either when a request creates a resource, or when a
	// request updates a resource that doesn't exist.
	Create(ctx Context, obj T, opts *metav1.CreateOptions) (T, error)
	// Update should overwrite a resource that is present in the backing storage.
	//
	// It can apply modifications as necessary before storing it. It must
	// return a resource of the type of the store, but can
	// create/update/delete arbitrary objects in Kubernetes without
	// returning them to the user.
	//
	// It is called when a request updates a resource (eg: through a patch or update request)
	Update(ctx Context, obj T, opts *metav1.UpdateOptions) (T, error)
	// Get retrieves the resource with the given name from the backing storage.
	//
	// Get is called for the following requests:
	// - get requests: The object must be returned.
	// - update requests: The object is needed to apply a JSON patch and to make some validation on the change.
	// - delete requests: The object is needed to make some validation on it.
	Get(ctx Context, name string, opts *metav1.GetOptions) (T, error)
	// List retrieves all resources matching the given ListOptions from the backing storage.
	List(ctx Context, opts *metav1.ListOptions) (TList, error)
	// Watch sends change events to a returned channel.
	//
	// The store is responsible for closing the channel.
	Watch(ctx Context, opts *metav1.ListOptions) (<-chan WatchEvent[T], error)
	// Delete deletes the resource of the given name from the backing storage.
	Delete(ctx Context, name string, opts *metav1.DeleteOptions) error
}

type TableConvertor[T runtime.Object] interface {
	ConvertToTable() ([]metav1.TableColumnDefinition, func(obj T) []string)
}

type WatchEvent[T runtime.Object] struct {
	Event  watch.EventType
	Object T
}

type StandardStorage[
	T runtime.Object,
	TList runtime.Object,
] struct {
	*Delegate[T, TList]
	store Store[T, TList]
}

func (s *StandardStorage[T, TList]) InjectDelegate(delegate *Delegate[T, TList]) {
	s.Delegate = delegate
}

func (s *StandardStorage[T, TList]) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	return s.Delegate.Create(ctx, obj, createValidation, options, s.store.Create)
}

func (s *StandardStorage[T, TList]) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	return s.Delegate.Update(ctx, name, objInfo, createValidation, updateValidation, forceAllowCreate, options, s.store.Get, s.store.Create, s.store.Update)
}

func (s *StandardStorage[T, TList]) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return s.Delegate.Get(ctx, name, options, s.store.Get)
}

func (s *StandardStorage[T, TList]) NewList() runtime.Object {
	return s.Delegate.NewList()
}

func (s *StandardStorage[T, TList]) List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
	return s.Delegate.List(ctx, options, s.store.List)
}

func (s *StandardStorage[T, TList]) Watch(ctx context.Context, options *metainternalversion.ListOptions) (watch.Interface, error) {
	return s.Delegate.Watch(ctx, options, s.store.Watch)
}

func (s *StandardStorage[T, TList]) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	return s.Delegate.Delete(ctx, name, deleteValidation, options, s.store.Get, s.store.Delete)
}

func (s *StandardStorage[T, TList]) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	if converter, ok := s.store.(TableConvertor[T]); ok {
		columnDefs, convertFn := converter.ConvertToTable()
		return s.Delegate.ConvertToTable(ctx, object, tableOptions, columnDefs, convertFn)
	}
	return s.Delegate.ConvertToDefaultTable(ctx, object, tableOptions)
}
