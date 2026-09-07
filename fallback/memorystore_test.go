package fallback_test

import (
	"testing"

	"github.com/mgiaccone/keel/conformance/fallbackstore"
	"github.com/mgiaccone/keel/fallback"
)

// TestMemoryStoreSatisfiesTheStoreContract runs the shared conformance suite
// every fallback.Store implementation must pass; MemoryStore-specific
// behavior (LRU eviction, WithMaxKeys validation) is covered separately
// below, since the contract itself says nothing about a bound on size.
func TestMemoryStoreSatisfiesTheStoreContract(t *testing.T) {
	fallbackstore.Run(t, func(t *testing.T) fallback.Store[string, int] {
		s, err := fallback.NewMemoryStore[string, int]()
		if err != nil {
			t.Fatalf("NewMemoryStore: %v", err)
		}
		return s
	}, "k1", "k2", 1, 2)
}

func TestMemoryStoreEvictsLeastRecentlyUsed(t *testing.T) {
	ctx := t.Context()
	s, err := fallback.NewMemoryStore[string, int](fallback.WithMaxKeys(2))
	if err != nil {
		t.Fatalf("NewMemoryStore: %v", err)
	}

	_ = s.Set(ctx, "a", 1)
	_ = s.Set(ctx, "b", 2)
	if _, _, err := s.Get(ctx, "a"); err != nil { // touch a, so b becomes LRU
		t.Fatalf("Get: %v", err)
	}
	_ = s.Set(ctx, "c", 3) // evicts b

	if _, found, _ := s.Get(ctx, "b"); found {
		t.Fatal("b should have been evicted")
	}
	if _, found, _ := s.Get(ctx, "a"); !found {
		t.Fatal("a should still be present")
	}
	if _, found, _ := s.Get(ctx, "c"); !found {
		t.Fatal("c should be present")
	}
	if got := s.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
}

func TestWithMaxKeysRejectsNonPositive(t *testing.T) {
	if _, err := fallback.NewMemoryStore[string, int](fallback.WithMaxKeys(0)); err == nil {
		t.Fatal("WithMaxKeys(0) should be rejected")
	}
}
