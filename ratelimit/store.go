package ratelimit

import (
	"context"
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
//
// A new implementation should satisfy [conformance/ratelimitstore.Run] from
// its own tests; [MemoryStore] does.
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
// they hold; the limiter reports it as Stats.Keys, and the keys gauge reads
// it at scrape time.
type KeyCounter interface {
	Keys() int
}

// Updater is optionally implemented by stores that can apply a step
// atomically themselves, typically under a local lock. The limiter prefers it
// to the Get/CompareAndSet round, which removes version conflicts entirely.
// A distributed store cannot offer it, since fn cannot run inside Redis;
// that is what CompareAndSet is for.
//
// Update steps the algorithm once against the key's current state (zero if
// absent or expired) at the store's time, writes the new state if it differs
// with the algorithm's TTL, and returns the decision.
type Updater interface {
	Update(ctx context.Context, key string, algorithm Algorithm) (Decision, error)
}
