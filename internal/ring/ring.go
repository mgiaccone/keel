// Package ring is the trailing-window ring the keel packages share: a fixed
// number of equal-width buckets rotated lazily against a clock, with no timer
// and no goroutine of its own.
package ring

import "time"

// Ring is a trailing window split into buckets of equal width, aligned to
// multiples of that width since the Unix epoch, with the newest bucket at the
// head. It holds only the per-bucket payloads; a caller that wants a running
// total over the window keeps it itself and subtracts what [Ring.Rotate]
// evicts, which is what makes reading that total free.
//
// Nothing here is safe for concurrent use: a Ring is meant to live inside
// whatever already serialises access to it, a state goroutine or a mutex.
type Ring[T any] struct {
	width    time.Duration
	buckets  []T
	head     int
	slot     int64 // the newest bucket's slot: its start divided by width
	anchored bool  // slot is meaningful; false for a fresh or reset ring
}

// New returns a ring of n buckets spanning window. The caller is responsible
// for n >= 1 and for window being at least n nanoseconds wide: a bucket
// narrower than a nanosecond rounds to zero and every rotation would divide
// by it.
func New[T any](window time.Duration, n int) *Ring[T] {
	return &Ring[T]{width: window / time.Duration(n), buckets: make([]T, n)}
}

// Rotate advances the ring to now, calling evict on each bucket that has left
// the window before zeroing it. That callback is the only chance a caller has
// to subtract the bucket from a running total, so it is required rather than
// optional: a rotation whose evictions go unnoticed silently inflates one. A
// gap of a whole window evicts every bucket. The first rotation of a fresh or
// reset ring anchors it at now, whatever now is: no slot value is a sentinel,
// so a clock before the epoch works like any other.
func (r *Ring[T]) Rotate(now time.Time, evict func(*T)) {
	slot := now.UnixNano() / int64(r.width)
	if !r.anchored {
		r.slot, r.anchored = slot, true
		return
	}

	k := slot - r.slot
	if k <= 0 {
		return
	}

	var zero T
	if k >= int64(len(r.buckets)) {
		for i := range r.buckets {
			evict(&r.buckets[i])
			r.buckets[i] = zero
		}
		r.head = 0
	} else {
		for range k {
			r.head = (r.head + 1) % len(r.buckets)
			b := &r.buckets[r.head]
			evict(b)
			*b = zero
		}
	}

	r.slot = slot
}

// Head is the newest bucket, for a caller to record into. It does not rotate:
// call [Ring.Rotate] with the same instant first, or the record lands in a
// bucket that should already have aged out.
func (r *Ring[T]) Head() *T { return &r.buckets[r.head] }

// Reset empties every bucket and unanchors the ring, so the next [Ring.Rotate]
// starts a fresh window wherever the clock then is.
func (r *Ring[T]) Reset() {
	clear(r.buckets)
	r.head, r.slot, r.anchored = 0, 0, false
}
