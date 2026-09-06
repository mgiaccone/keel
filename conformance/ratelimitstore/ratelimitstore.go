// Package ratelimitstore is the contract every [ratelimit.Store] must
// satisfy. A store implementation runs it from its own tests:
//
//	func TestStore(t *testing.T) {
//		ratelimitstore.Run(t, func(t *testing.T) ratelimit.Store { return newStoreForTest(t) })
//	}
package ratelimitstore

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mgiaccone/keel/ratelimit"
)

// Run exercises a store: absent keys, create, version conflicts, expiry, and
// atomicity under concurrent compare-and-set. newStore is called per subtest
// and must return an empty store, or one whose keys do not collide with
// other runs (a shared Redis with a unique prefix is fine).
func Run(t *testing.T, newStore func(t *testing.T) ratelimit.Store) {
	ctx := context.Background()

	t.Run("absent key is the zero record", func(t *testing.T) {
		s := newStore(t)
		rec, now, err := s.Get(ctx, "missing")
		if err != nil || rec != (ratelimit.Record{}) || now.IsZero() {
			t.Fatalf("Get = %+v, %v, %v", rec, now, err)
		}
	})

	t.Run("create requires expect 0 and returns the state with a version", func(t *testing.T) {
		s := newStore(t)

		if ok, err := s.CompareAndSet(ctx, "k", 7, ratelimit.State{A: 1}, time.Minute); err != nil || ok {
			t.Fatalf("create with wrong expect: %v %v", ok, err)
		}

		if ok, err := s.CompareAndSet(ctx, "k", 0, ratelimit.State{A: 1, B: 2, C: 3}, time.Minute); err != nil || !ok {
			t.Fatalf("create: %v %v", ok, err)
		}

		rec, _, err := s.Get(ctx, "k")
		if err != nil || rec.Version == 0 || rec.State != (ratelimit.State{A: 1, B: 2, C: 3}) {
			t.Fatalf("Get after create = %+v %v", rec, err)
		}
	})

	t.Run("update requires the current version and bumps it", func(t *testing.T) {
		s := newStore(t)
		s.CompareAndSet(ctx, "k", 0, ratelimit.State{A: 1}, time.Minute)
		rec, _, _ := s.Get(ctx, "k")

		if ok, _ := s.CompareAndSet(ctx, "k", rec.Version+1, ratelimit.State{A: 2}, time.Minute); ok {
			t.Fatal("stale/future version accepted")
		}
		if ok, _ := s.CompareAndSet(ctx, "k", 0, ratelimit.State{A: 2}, time.Minute); ok {
			t.Fatal("expect 0 accepted for an existing key")
		}

		if ok, err := s.CompareAndSet(ctx, "k", rec.Version, ratelimit.State{A: 2}, time.Minute); err != nil || !ok {
			t.Fatalf("update: %v %v", ok, err)
		}
		rec2, _, _ := s.Get(ctx, "k")
		if rec2.State.A != 2 || rec2.Version == rec.Version {
			t.Fatalf("after update: %+v (before %+v)", rec2, rec)
		}

		if ok, _ := s.CompareAndSet(ctx, "k", rec.Version, ratelimit.State{A: 3}, time.Minute); ok {
			t.Fatal("old version accepted after update")
		}
	})

	t.Run("negative values round-trip", func(t *testing.T) {
		s := newStore(t)
		want := ratelimit.State{A: -1, B: 1 << 62, C: -(1 << 62)}
		s.CompareAndSet(ctx, "k", 0, want, time.Minute)

		if rec, _, _ := s.Get(ctx, "k"); rec.State != want {
			t.Fatalf("got %+v", rec.State)
		}
	})

	t.Run("records expire after the ttl", func(t *testing.T) {
		s := newStore(t)
		s.CompareAndSet(ctx, "k", 0, ratelimit.State{A: 1}, 60*time.Millisecond)

		deadline := time.Now().Add(5 * time.Second)
		for {
			rec, _, err := s.Get(ctx, "k")
			if err != nil {
				t.Fatal(err)
			}
			if rec == (ratelimit.Record{}) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("record did not expire: %+v", rec)
			}
			time.Sleep(20 * time.Millisecond)
		}

		// An expired key is created again with expect 0.
		if ok, err := s.CompareAndSet(ctx, "k", 0, ratelimit.State{A: 2}, time.Minute); err != nil || !ok {
			t.Fatalf("recreate after expiry: %v %v", ok, err)
		}
	})

	t.Run("time moves forward", func(t *testing.T) {
		s := newStore(t)
		_, t1, _ := s.Get(ctx, "k")
		time.Sleep(5 * time.Millisecond)
		_, t2, _ := s.Get(ctx, "k")

		if !t2.After(t1) {
			t.Fatalf("time did not advance: %v then %v", t1, t2)
		}
	})

	t.Run("concurrent compare-and-set is atomic", func(t *testing.T) {
		s := newStore(t)
		const writers, each = 16, 25
		var wg sync.WaitGroup

		for range writers {
			wg.Go(func() {
				for range each {
					for {
						rec, _, err := s.Get(ctx, "counter")
						if err != nil {
							t.Error(err)
							return
						}
						ok, err := s.CompareAndSet(ctx, "counter", rec.Version, ratelimit.State{A: rec.State.A + 1}, time.Minute)
						if err != nil {
							t.Error(err)
							return
						}
						if ok {
							break
						}
					}
				}
			})
		}
		wg.Wait()

		rec, _, _ := s.Get(ctx, "counter")
		if rec.State.A != writers*each {
			t.Fatalf("lost updates: counter = %d, want %d", rec.State.A, writers*each)
		}
	})
}
