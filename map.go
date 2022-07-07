package scheduler

import "sync"

type AtomicMap[U comparable, V any] struct {
	mu    sync.RWMutex
	value map[U]V
}

func NewAtomicMap[U comparable, V any]() *AtomicMap[U, V] {
	return &AtomicMap[U, V]{
		value: make(map[U]V),
	}
}

func (a *AtomicMap[U, V]) Range(fn func(key U, val V) bool) {
	a.mu.RLock()
	for k, v := range a.value {
		k, v := k, v
		if !fn(k, v) {
			break
		}
	}
	a.mu.RUnlock()
}

func (a *AtomicMap[U, V]) Set(key U, value V) {
	a.mu.Lock()
	a.value[key] = value
	a.mu.Unlock()
}

func (a *AtomicMap[U, V]) Add(key U, value V) bool {
	a.mu.Lock()
	if _, ok := a.value[key]; ok {
		a.mu.Unlock()

		return false
	}

	a.value[key] = value
	a.mu.Unlock()

	return true
}

func (a *AtomicMap[U, V]) Remove(key U) {
	a.mu.Lock()
	delete(a.value, key)
	a.mu.Unlock()
}

func (a *AtomicMap[U, V]) Get(key U) (val V, found bool) {
	a.mu.RLock()
	val, found = a.value[key]
	a.mu.RUnlock()

	return
}
