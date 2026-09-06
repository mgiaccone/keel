package ratelimit

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// MemoryStore is a process-local [Store]: a map of records guarded by a
// mutex, with idle records expiring on their TTL and the least recently used
// evicted beyond a bound. It also implements [Updater], so a limiter on it
// applies each step under the lock and never sees a version conflict. It is
// what to use when each instance may enforce its own limit; use the Redis
// store for a fleet-wide one.
type MemoryStore struct {
	now     func() time.Time
	maxKeys int

	mu      sync.Mutex
	records map[string]*memRecord
	lru     *list.List // front = most recently used
	version uint64     // store-wide, so a version is never reused across keys
}

type memRecord struct {
	key     string
	state   State
	version uint64
	expires time.Time
	elem    *list.Element
}

// MemoryOption configures a [MemoryStore].
type MemoryOption func(*MemoryStore) error

// WithMaxKeys bounds the number of records kept; the least recently used is
// evicted beyond it, and an evicted key returns as if never seen. Default
// 1024. Size it for the keys active at once, not the total ever seen.
func WithMaxKeys(n int) MemoryOption {
	return func(m *MemoryStore) error {
		if n < 1 {
			return fmt.Errorf("%w: WithMaxKeys(%d): must be at least 1", ErrInvalidOption, n)
		}
		m.maxKeys = n
		return nil
	}
}

// WithClock sets the store's clock. Default time.Now.
func WithClock(now func() time.Time) MemoryOption {
	return func(m *MemoryStore) error {
		if now == nil {
			return fmt.Errorf("%w: WithClock(nil)", ErrInvalidOption)
		}
		m.now = now
		return nil
	}
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore(opts ...MemoryOption) (*MemoryStore, error) {
	m := &MemoryStore{now: time.Now, maxKeys: 1024, records: make(map[string]*memRecord), lru: list.New()}
	var errs []error
	for _, opt := range opts {
		if err := opt(m); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return m, nil
}

// Get implements [Store].
func (m *MemoryStore) Get(_ context.Context, key string) (Record, time.Time, error) {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.lookup(key, now)
	if r == nil {
		return Record{}, now, nil
	}

	m.lru.MoveToFront(r.elem)
	return Record{State: r.state, Version: r.version}, now, nil
}

// CompareAndSet implements [Store].
func (m *MemoryStore) CompareAndSet(_ context.Context, key string, expect uint64, state State, ttl time.Duration) (bool, error) {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.lookup(key, now)

	var current uint64
	if r != nil {
		current = r.version
	}

	if current != expect {
		return false, nil
	}

	m.put(r, key, state, now.Add(ttl))
	return true, nil
}

// Update implements [Updater]: the step runs under the store's lock, so no
// two callers ever race on a key.
func (m *MemoryStore) Update(_ context.Context, key string, algorithm Algorithm) (Decision, error) {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.lookup(key, now)

	var current State
	if r != nil {
		current = r.state
	}

	next, d := algorithm.Step(current, now)
	if next == current {
		if r != nil {
			m.lru.MoveToFront(r.elem)
		}
		return d, nil
	}

	m.put(r, key, next, now.Add(algorithm.TTL()))
	return d, nil
}

// lookup returns the key's live record, or nil, dropping it first if it has
// expired. The caller holds the lock.
func (m *MemoryStore) lookup(key string, now time.Time) *memRecord {
	r, ok := m.records[key]
	if !ok {
		return nil
	}
	if !r.expires.After(now) {
		m.remove(r)
		return nil
	}
	return r
}

// put stamps state on r, or on a new record for key when r is nil, gives it
// the next version, marks it most recently used and evicts past maxKeys. The
// caller holds the lock.
func (m *MemoryStore) put(r *memRecord, key string, state State, expires time.Time) {
	m.version++
	if r == nil {
		r = &memRecord{key: key}
		r.elem = m.lru.PushFront(r)
		m.records[key] = r
		if m.lru.Len() > m.maxKeys {
			m.remove(m.lru.Back().Value.(*memRecord))
		}
	} else {
		m.lru.MoveToFront(r.elem)
	}

	r.state, r.version, r.expires = state, m.version, expires
}

// Keys implements [KeyCounter].
func (m *MemoryStore) Keys() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lru.Len()
}

func (m *MemoryStore) remove(r *memRecord) {
	m.lru.Remove(r.elem)
	delete(m.records, r.key)
}
