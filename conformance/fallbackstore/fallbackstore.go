// Package fallbackstore is the contract every [fallback.Store] must satisfy.
// A store implementation runs it from its own tests:
//
//	func TestStore(t *testing.T) {
//		fallbackstore.Run(t, func(t *testing.T) fallback.Store[string, int] { return newStoreForTest(t) },
//			"k1", "k2", 1, 2)
//	}
package fallbackstore

import (
	"testing"

	"github.com/mgiaccone/keel/fallback"
)

// Run exercises a store: absence, write-back, overwrite, eviction, and
// independence between keys. newStore is called per subtest and must
// return an empty store, or one whose keys do not collide with other runs
// (a shared backend with a unique prefix is fine). k1 and k2 must be
// distinct keys; v1 and v2 must be distinct values.
//
// Store carries no atomicity contract of its own — unlike [ratelimit.Store],
// there is no compare-and-set, so nothing here promises what happens when
// two callers write the same key concurrently. A caller wanting that composes
// it outside the store. This suite therefore has no concurrent-writers test
// the way [ratelimitstore.Run] does; a store's own mutex, if it has one, is
// exercised by its own tests instead.
func Run[K comparable, V comparable](t *testing.T, newStore func(t *testing.T) fallback.Store[K, V], k1, k2 K, v1, v2 V) {
	var zero V

	t.Run("absent key returns found=false and no error", func(t *testing.T) {
		ctx := t.Context()
		s := newStore(t)
		if v, found, err := s.Get(ctx, k1); err != nil || found || v != zero {
			t.Fatalf("Get(absent) = %v, %v, %v, want %v, false, nil", v, found, err, zero)
		}
	})

	t.Run("set then get returns the value", func(t *testing.T) {
		ctx := t.Context()
		s := newStore(t)
		if err := s.Set(ctx, k1, v1); err != nil {
			t.Fatalf("Set: %v", err)
		}
		if v, found, err := s.Get(ctx, k1); err != nil || !found || v != v1 {
			t.Fatalf("Get = %v, %v, %v, want %v, true, nil", v, found, err, v1)
		}
	})

	t.Run("set again overwrites", func(t *testing.T) {
		ctx := t.Context()
		s := newStore(t)
		must(t, s.Set(ctx, k1, v1))
		must(t, s.Set(ctx, k1, v2))
		if v, found, err := s.Get(ctx, k1); err != nil || !found || v != v2 {
			t.Fatalf("Get after overwrite = %v, %v, %v, want %v, true, nil", v, found, err, v2)
		}
	})

	t.Run("delete removes the key", func(t *testing.T) {
		ctx := t.Context()
		s := newStore(t)
		must(t, s.Set(ctx, k1, v1))
		if err := s.Delete(ctx, k1); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if v, found, err := s.Get(ctx, k1); err != nil || found || v != zero {
			t.Fatalf("Get after Delete = %v, %v, %v, want %v, false, nil", v, found, err, zero)
		}
	})

	t.Run("delete on an absent key is a no-op", func(t *testing.T) {
		ctx := t.Context()
		s := newStore(t)
		if err := s.Delete(ctx, k1); err != nil {
			t.Fatalf("Delete(absent) = %v, want nil", err)
		}
	})

	t.Run("keys are independent", func(t *testing.T) {
		ctx := t.Context()
		s := newStore(t)
		must(t, s.Set(ctx, k1, v1))
		must(t, s.Set(ctx, k2, v2))

		if v, found, err := s.Get(ctx, k1); err != nil || !found || v != v1 {
			t.Fatalf("Get(k1) = %v, %v, %v, want %v, true, nil", v, found, err, v1)
		}
		if v, found, err := s.Get(ctx, k2); err != nil || !found || v != v2 {
			t.Fatalf("Get(k2) = %v, %v, %v, want %v, true, nil", v, found, err, v2)
		}

		must(t, s.Delete(ctx, k1))
		if v, found, err := s.Get(ctx, k2); err != nil || !found || v != v2 {
			t.Fatalf("Get(k2) after Delete(k1) = %v, %v, %v, want %v, true, nil (unaffected)", v, found, err, v2)
		}
	})
}

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
