package ratelimit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMemoryStoreEvictsLRU(t *testing.T) {
	h := newHarness(t, GCRA(1, 1), WithMaxKeys(2))
	h.allow(t, "a")
	h.allow(t, "b")
	h.allow(t, "a") // a most recent
	h.allow(t, "c") // evicts b
	if h.store.Keys() != 2 {
		t.Fatalf("keys = %d", h.store.Keys())
	}

	if d := h.allow(t, "b"); !d.Allowed {
		t.Fatal("evicted key should return as fresh")
	}

	if d := h.allow(t, "c"); d.Allowed {
		t.Fatal("c was kept and should still be exhausted")
	}
}

func TestMemoryStoreInvalidOptions(t *testing.T) {
	if _, err := NewMemoryStore(WithMaxKeys(0)); !errors.Is(err, ErrInvalidOption) {
		t.Fatal(err)
	}
	if _, err := NewMemoryStore(WithClock(nil)); !errors.Is(err, ErrInvalidOption) {
		t.Fatal(err)
	}
}

func TestMemoryStoreNeverConflicts(t *testing.T) {
	h := newHarness(t, GCRA(1000, 5))
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for range 50 {
				h.allow(t, "hot")
			}
		})
	}
	wg.Wait()
	if s := h.Stats(); s.Conflicts != 0 || s.Errors != 0 || s.Allowed != 5 || s.Limited != 32*50-5 {
		t.Fatalf("stats = %+v", s)
	}
}

// casOnly hides a store's Updater so the limiter takes the compare-and-set path.
type casOnly struct{ s Store }

func (c casOnly) Get(ctx context.Context, key string) (Record, time.Time, error) {
	return c.s.Get(ctx, key)
}
func (c casOnly) CompareAndSet(ctx context.Context, key string, expect uint64, st State, ttl time.Duration) (bool, error) {
	return c.s.CompareAndSet(ctx, key, expect, st, ttl)
}

// conflictingStore wraps a CAS-only store and fails the first n compare-and-sets.
type conflictingStore struct {
	Store
	remaining atomic.Int32
}

func (c *conflictingStore) CompareAndSet(ctx context.Context, key string, expect uint64, st State, ttl time.Duration) (bool, error) {
	if c.remaining.Add(-1) >= 0 {
		return false, nil
	}
	return c.Store.CompareAndSet(ctx, key, expect, st, ttl)
}
