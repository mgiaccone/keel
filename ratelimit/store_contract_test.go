package ratelimit_test

import (
	"testing"

	"github.com/mgiaccone/keel/ratelimit"
	"github.com/mgiaccone/keel/ratelimit/storetest"
)

// TestMemoryStoreContract runs the contract every Store must satisfy against
// the memory store. It lives in an external test package because storetest
// imports ratelimit.
func TestMemoryStoreContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) ratelimit.Store {
		s, err := ratelimit.NewMemoryStore()
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}
