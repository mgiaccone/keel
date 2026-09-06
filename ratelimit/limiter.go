package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
)

// Limiter applies an [Algorithm] to records in a [Store]. Each Allow reads
// the key's record, steps the algorithm, and writes the new state back with a
// compare-and-set, retrying on a version conflict; on a store that implements
// [Updater] the step runs inside the store instead and never conflicts. A
// refusal that leaves the state unchanged is not written, so refused calls
// cost one read.
//
// Limiter has no goroutine, needs no teardown and is safe for concurrent
// use; its counters are atomics.
type Limiter struct {
	name        string
	algorithm   Algorithm
	store       Store
	maxAttempts int
	metrics     *metrics

	allowed, limited, errors, conflicts atomic.Uint64
}

// Option configures a [Limiter].
type Option func(*Limiter) error

// WithMaxAttempts bounds how many times Allow retries a compare-and-set that
// lost a race before returning [ErrContention]. Default 8. It only applies to
// stores without [Updater], such as Redis, where a hot key shared by a busy
// fleet occasionally conflicts.
func WithMaxAttempts(n int) Option {
	return func(l *Limiter) error {
		if n < 1 {
			return fmt.Errorf("%w: WithMaxAttempts(%d): must be at least 1", ErrInvalidOption, n)
		}
		l.maxAttempts = n
		return nil
	}
}

// New returns a limiter enforcing algorithm on the records in store. name
// identifies it in [Stats] and metrics.
func New(name string, algorithm Algorithm, store Store, opts ...Option) (*Limiter, error) {
	l := &Limiter{name: name, algorithm: algorithm, store: store, maxAttempts: 8}
	var errs []error
	if name == "" {
		errs = append(errs, fmt.Errorf("%w: New: name must not be empty", ErrInvalidOption))
	}
	if algorithm == nil {
		errs = append(errs, fmt.Errorf("%w: New: algorithm must not be nil", ErrInvalidOption))
	} else if err := algorithm.Validate(); err != nil {
		errs = append(errs, err)
	}
	if store == nil {
		errs = append(errs, fmt.Errorf("%w: New: store must not be nil", ErrInvalidOption))
	}
	for _, opt := range opts {
		if err := opt(l); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	l.metrics = newMetrics(name, algorithm.Name())
	l.metrics.started()
	if _, ok := store.(KeyCounter); ok {
		_keys.add(l) // the keys gauge asks the store at scrape time
	}
	return l, nil
}

// Allow implements [Allower].
func (l *Limiter) Allow(ctx context.Context, key string) (Decision, error) {
	if u, ok := l.store.(Updater); ok {
		d, err := u.Update(ctx, key, l.algorithm)
		if err != nil {
			return l.fail(fmt.Errorf("ratelimit: store update: %w", err))
		}
		return l.decided(d), nil
	}
	for attempt := 0; attempt < l.maxAttempts; attempt++ {
		rec, now, err := l.store.Get(ctx, key)
		if err != nil {
			return l.fail(fmt.Errorf("ratelimit: store get: %w", err))
		}
		next, d := l.algorithm.Step(rec.State, now)
		if next == rec.State {
			return l.decided(d), nil // nothing to write
		}
		ok, err := l.store.CompareAndSet(ctx, key, rec.Version, next, l.algorithm.TTL())
		if err != nil {
			return l.fail(fmt.Errorf("ratelimit: store compare-and-set: %w", err))
		}
		if ok {
			return l.decided(d), nil
		}
		l.conflicts.Add(1)
		l.metrics.conflicts.Inc()
	}
	return l.fail(fmt.Errorf("%w: %q after %d attempts", ErrContention, key, l.maxAttempts))
}

func (l *Limiter) decided(d Decision) Decision {
	if d.Allowed {
		l.allowed.Add(1)
		l.metrics.allowed.Inc()
	} else {
		l.limited.Add(1)
		l.metrics.limited.Inc()
	}
	return d
}

func (l *Limiter) fail(err error) (Decision, error) {
	l.errors.Add(1)
	l.metrics.errors.Inc()
	return Decision{Remaining: -1}, err
}

// Stats is a snapshot of a [Limiter].
type Stats struct {
	// Name and Algorithm identify the limiter.
	Name, Algorithm string
	// Keys is the number of records the store holds, if it can say
	// ([KeyCounter]); 0 otherwise.
	Keys int
	// Allowed and Limited are cumulative decisions. Errors counts calls the
	// limiter could not decide. Conflicts counts compare-and-set retries.
	Allowed, Limited, Errors, Conflicts uint64
}

// String renders the snapshot as one log line:
//
//	ratelimit: name=public-api algorithm=gcra keys=3 allowed=812 limited=41 errors=0 conflicts=2
func (s Stats) String() string {
	return fmt.Sprintf("ratelimit: name=%s algorithm=%s keys=%d allowed=%d limited=%d errors=%d conflicts=%d",
		s.Name, s.Algorithm, s.Keys, s.Allowed, s.Limited, s.Errors, s.Conflicts)
}

// Stats returns a snapshot.
func (l *Limiter) Stats() Stats {
	s := Stats{
		Name: l.name, Algorithm: l.algorithm.Name(),
		Allowed: l.allowed.Load(), Limited: l.limited.Load(), Errors: l.errors.Load(), Conflicts: l.conflicts.Load(),
	}
	if kc, ok := l.store.(KeyCounter); ok {
		s.Keys = kc.Keys()
	}
	return s
}
