package ring

import (
	"testing"
	"time"
)

type counter struct{ n int }

// sum is the running total a caller keeps beside the ring, kept honest by
// Rotate's evict callback; every test here checks it against the ring.
type sum struct {
	ring  *Ring[counter]
	total int
}

func newSum(window time.Duration, n int) *sum {
	s := &sum{}
	s.ring = New[counter](window, n)
	return s
}

func (s *sum) add(now time.Time, n int) {
	s.rotate(now)
	s.ring.Head().n += n
	s.total += n
}

func (s *sum) rotate(now time.Time) {
	s.ring.Rotate(now, func(b *counter) { s.total -= b.n })
}

var _epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestRecordsAgeOutOneBucketAtATime(t *testing.T) {
	s := newSum(10*time.Second, 10) // one-second buckets
	for i := range 10 {
		s.add(_epoch.Add(time.Duration(i)*time.Second), 1)
	}
	if s.total != 10 {
		t.Fatalf("total = %d, want 10 with the window exactly full", s.total)
	}

	// Each further second drops exactly the bucket that has left the window.
	for i := range 5 {
		s.rotate(_epoch.Add(time.Duration(10+i) * time.Second))
		if want := 9 - i; s.total != want {
			t.Errorf("after %ds: total = %d, want %d", 10+i, s.total, want)
		}
	}
}

func TestAGapOfAWholeWindowEvictsEverything(t *testing.T) {
	s := newSum(time.Second, 4)
	s.add(_epoch, 7)
	s.rotate(_epoch.Add(time.Minute))
	if s.total != 0 {
		t.Fatalf("total = %d, want 0 after a gap far wider than the window", s.total)
	}

	s.add(_epoch.Add(time.Minute), 3)
	if s.total != 3 {
		t.Fatalf("total = %d, want 3: the ring must still be usable after a full eviction", s.total)
	}
}

func TestAFreshRingAnchorsWhereverTheClockIs(t *testing.T) {
	// A clock before the Unix epoch would break any implementation using zero
	// as a "not started" sentinel for the slot.
	before := time.Date(1969, 7, 20, 20, 17, 0, 0, time.UTC)
	s := newSum(time.Second, 4)
	s.add(before, 5)
	s.rotate(before.Add(100 * time.Millisecond))
	if s.total != 5 {
		t.Fatalf("total = %d, want 5: a fraction of a bucket must not rotate", s.total)
	}
}

func TestRotatingBackwardsChangesNothing(t *testing.T) {
	s := newSum(time.Second, 4)
	s.add(_epoch.Add(time.Second), 2)
	s.rotate(_epoch)
	if s.total != 2 {
		t.Fatalf("total = %d, want 2: a clock that went backwards must not drop records", s.total)
	}
}

func TestResetUnanchorsTheRing(t *testing.T) {
	s := newSum(time.Second, 4)
	s.add(_epoch, 9)
	s.ring.Reset()
	s.total = 0

	// Unanchored, the next rotation re-anchors instead of evicting, so a
	// record made at a far later instant survives.
	s.add(_epoch.Add(time.Hour), 4)
	if s.total != 4 {
		t.Fatalf("total = %d, want 4 after a reset and a record an hour later", s.total)
	}
}
