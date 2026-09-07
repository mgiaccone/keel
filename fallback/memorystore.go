package fallback

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
)

// MemoryStore is a process-local [Store]: a map guarded by a mutex, with the
// least recently used key evicted beyond a bound. It is a per-process tier:
// n instances of a fleet hold n independent copies, each judged by the
// [Policy] independently, and [Reader.Invalidate] on one instance reaches
// only that instance's copy. Use [fallback/redistore] for a fleet-wide fast
// source instead.
type MemoryStore[K comparable, V any] struct {
	maxKeys int

	mu      sync.Mutex
	records map[K]*memRecord[K, V]
	lru     *list.List // front = most recently used
}

type memRecord[K comparable, V any] struct {
	key   K
	value V
	elem  *list.Element
}

// MemoryOption configures a [MemoryStore].
type MemoryOption func(*memoryConfig) error

type memoryConfig struct {
	maxKeys int
}

// WithMaxKeys bounds the number of keys kept; the least recently used is
// evicted beyond it. Default 1024. Size it for the keys active at once, not
// the total ever seen.
func WithMaxKeys(n int) MemoryOption {
	return func(c *memoryConfig) error {
		if n < 1 {
			return fmt.Errorf("%w: WithMaxKeys(%d): must be at least 1", ErrInvalidOption, n)
		}
		c.maxKeys = n
		return nil
	}
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore[K comparable, V any](opts ...MemoryOption) (*MemoryStore[K, V], error) {
	cfg := &memoryConfig{maxKeys: 1024}
	var errs []error
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return &MemoryStore[K, V]{
		maxKeys: cfg.maxKeys,
		records: make(map[K]*memRecord[K, V]),
		lru:     list.New(),
	}, nil
}

// Get implements [Source].
func (m *MemoryStore[K, V]) Get(_ context.Context, key K) (V, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.records[key]
	if !ok {
		var zero V
		return zero, false, nil
	}
	m.lru.MoveToFront(r.elem)
	return r.value, true, nil
}

// Set implements [Store].
func (m *MemoryStore[K, V]) Set(_ context.Context, key K, v V) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if r, ok := m.records[key]; ok {
		r.value = v
		m.lru.MoveToFront(r.elem)
		return nil
	}

	r := &memRecord[K, V]{key: key, value: v}
	r.elem = m.lru.PushFront(r)
	m.records[key] = r

	if len(m.records) > m.maxKeys {
		back := m.lru.Back()
		m.lru.Remove(back)
		delete(m.records, back.Value.(*memRecord[K, V]).key)
	}
	return nil
}

// Delete implements [Store].
func (m *MemoryStore[K, V]) Delete(_ context.Context, key K) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.records[key]; ok {
		m.lru.Remove(r.elem)
		delete(m.records, key)
	}
	return nil
}

// Len reports how many keys are currently held.
func (m *MemoryStore[K, V]) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.records)
}
