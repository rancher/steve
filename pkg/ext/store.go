package ext

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
)

type WatchEvent[T runtime.Object] struct {
	Event  watch.EventType
	Object T
}

type Store[T runtime.Object, TList runtime.Object] interface {
	Create(ctx context.Context, userInfo user.Info, obj T, opts *metav1.CreateOptions) (T, error)
	Update(ctx context.Context, userInfo user.Info, obj T, opts *metav1.UpdateOptions) (T, error)
	Get(ctx context.Context, userInfo user.Info, name string, opts *metav1.GetOptions) (T, error)
	List(ctx context.Context, userInfo user.Info, opts *metav1.ListOptions) (TList, error)
	Watch(ctx context.Context, userInfo user.Info, opts *metav1.ListOptions) (<-chan WatchEvent[T], error)
	Delete(ctx context.Context, userInfo user.Info, name string, opts *metav1.DeleteOptions) error
}
