package informer

import "sync"

type listeners struct {
	lock      sync.Mutex
	listeners map[Listener]struct{}
	// count is incremented everytime Notify is called
	count int
}

func newlisteners() *listeners {
	return &listeners{
		listeners: make(map[Listener]struct{}),
	}
}

func (w *listeners) Notify(revision string, oldObj any, newObj any) {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.count += 1

	for listener := range w.listeners {
		listener.Notify(revision, oldObj, newObj)
	}
}

func (w *listeners) AddListener(listener Listener) int {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.listeners[listener] = struct{}{}
	return w.count
}

func (w *listeners) RemoveListener(listener Listener) {
	w.lock.Lock()
	defer w.lock.Unlock()

	delete(w.listeners, listener)
}

func (w *listeners) Count() int {
	w.lock.Lock()
	defer w.lock.Unlock()

	return w.count
}

type Listener interface {
	Notify(revision string, oldObj any, newObj any)
}
