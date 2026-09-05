package ratelimit

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Store keeps one [State] per key and updates it atomically. It does not
// interpret the state. A store also owns the clock: Get returns the time the
// limiter must compute against, so a fleet sharing a Redis store agrees on
// time even with skewed local clocks.
//
// Versions make updates atomic without locks across processes: Get returns
// the record's version, 0 when the key is absent, and CompareAndSet writes
// only if the version is still what the caller saw. Two limiters racing on a
// key see one succeed and one retry.
type Store interface {
	// Get returns the key's record, the zero Record when absent or expired,
	// and the store's current time.
	Get(ctx context.Context, key string) (Record, time.Time, error)
	// CompareAndSet writes state as the key's record if its current version
	// is expect (0 for "must be absent"), and arranges for the record to
	// expire after ttl of inactivity. It reports whether the write happened.
	CompareAndSet(ctx context.Context, key string, expect uint64, state State, ttl time.Duration) (bool, error)
}

// Record is a key's state with the version the store gave it.
type Record struct {
	State   State
	Version uint64 // 0 when absent
}

// KeyCounter is optionally implemented by stores that can say how many keys
// they hold; the limiter reports it as Stats.Keys and a gauge.
type KeyCounter interface {
	Keys() int
}

// Updater is optionally implemented by stores that can apply a step
// atomically themselves, typically under a local lock. The limiter prefers it
// to the Get/CompareAndSet round, which removes version conflicts entirely.
// A distributed store cannot offer it, since fn cannot run inside Redis;
// that is what CompareAndSet is for.
//
// Update calls fn once with the key's current state (zero if absent or
// expired) and the store's time, writes the returned state if it differs, and
// returns fn's decision.
type Updater interface {
	Update(ctx context.Context, key string, ttl time.Duration, fn func(State, time.Time) (State, Decision)) (Decision, error)
}

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
	r, ok := m.records[key]
	if !ok {
		return Record{}, now, nil
	}
	if !r.expires.After(now) {
		m.remove(r)
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
	r, ok := m.records[key]
	if ok && !r.expires.After(now) {
		m.remove(r)
		r, ok = nil, false
	}
	var current uint64
	if ok {
		current = r.version
	}
	if current != expect {
		return false, nil
	}
	m.version++
	if !ok {
		r = &memRecord{key: key}
		r.elem = m.lru.PushFront(r)
		m.records[key] = r
		if m.lru.Len() > m.maxKeys {
			m.remove(m.lru.Back().Value.(*memRecord))
		}
	} else {
		m.lru.MoveToFront(r.elem)
	}
	r.state, r.version, r.expires = state, m.version, now.Add(ttl)
	return true, nil
}

// Update implements [Updater]: the step runs under the store's lock, so no
// two callers ever race on a key.
func (m *MemoryStore) Update(_ context.Context, key string, ttl time.Duration, fn func(State, time.Time) (State, Decision)) (Decision, error) {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.records[key]
	if ok && !r.expires.After(now) {
		m.remove(r)
		r, ok = nil, false
	}
	var current State
	if ok {
		current = r.state
	}
	next, d := fn(current, now)
	if next == current {
		if ok {
			m.lru.MoveToFront(r.elem)
		}
		return d, nil
	}
	m.version++
	if !ok {
		r = &memRecord{key: key}
		r.elem = m.lru.PushFront(r)
		m.records[key] = r
		if m.lru.Len() > m.maxKeys {
			m.remove(m.lru.Back().Value.(*memRecord))
		}
	} else {
		m.lru.MoveToFront(r.elem)
	}
	r.state, r.version, r.expires = next, m.version, now.Add(ttl)
	return d, nil
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
