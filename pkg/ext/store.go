package ext

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

type WatchEvent[T runtime.Object] struct {
	Event  watch.EventType
	Object T
}

type Context struct {
	context.Context

	// User is the user making the request
	User user.Info
	// Authorizer helps you determines if a user is authorized to perform
	// actions to specific resources
	// XXX: Could be our own interface ?
	Authorizer authorizer.Authorizer
}

type Store[T runtime.Object, TList runtime.Object] interface {
	Create(ctx Context, obj T, opts *metav1.CreateOptions) (T, error)
	Update(ctx Context, obj T, opts *metav1.UpdateOptions) (T, error)
	Get(ctx Context, name string, opts *metav1.GetOptions) (T, error)
	List(ctx Context, opts *metav1.ListOptions) (TList, error)
	Watch(ctx Context, opts *metav1.ListOptions) (<-chan WatchEvent[T], error)
	Delete(ctx Context, name string, opts *metav1.DeleteOptions) error
}
