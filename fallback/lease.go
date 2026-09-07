package fallback

import (
	"context"
	"time"
)

// Leaser bounds a background refresh (started by [ServeAndRefresh]) to one
// holder across a fleet, via [WithLease]. It is never inferred from a
// [Store]'s dynamic type — a capability a caller did not know to ask for is
// a capability a decorator can silently drop, which is exactly the failure
// this package avoids by requiring Leaser to be configured explicitly.
type Leaser[K comparable] interface {
	// Acquire attempts to hold key for ttl. ok is false, with a nil err,
	// when another holder already has it — the ordinary, expected way
	// to lose a race, not a failure.
	Acquire(ctx context.Context, key K, ttl time.Duration) (token string, ok bool, err error)
	// Release gives up a lease acquired with token before ttl expires.
	// A failure here is observed, never returned: the refresh it guarded
	// has already run, and the lease will still expire on its own.
	Release(ctx context.Context, key K, token string) error
}
