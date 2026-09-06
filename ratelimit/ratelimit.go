// Package ratelimit bounds how many calls start per unit of time. It stands
// alone: use it in an HTTP middleware to hold API clients to their quotas, in
// a worker to pace calls to a third party, or in front of any operation that
// must not exceed a rate. Package breaker, which bounds what happens to calls
// once they start, composes with it through [Admission] but is not required.
//
// # Architecture
//
// A [Limiter] is an [Algorithm] applied to records in a [Store]:
//
//	Algorithm   the rule, a pure function: Step(state, now) → (state', decision)
//	Store       where a key's state lives and how it is updated atomically:
//	            Get(key) → (state, version, now); CompareAndSet(key, version, state', ttl)
//	Limiter     Get, Step, CompareAndSet; retry on a version conflict
//
// Algorithms are written once, in Go, and work against every store. Stores
// know nothing about rates; they keep a few integers per key and swap them
// atomically. [NewMemoryStore] is process-local; github.com/mgiaccone/keel/ratelimit/redistore
// shares one limit across a fleet through Redis. Adding an algorithm is one
// pure function; adding a store is one Get and one CompareAndSet.
//
// Limits are keyed. The key names what is being limited: an API key or tenant
// ID for per-client quotas, a route for per-endpoint limits, a client IP for
// abuse control. For one limit on the whole thing the key is "": [KeyGlobal]
// inbound, [AdmissionGlobal] outbound.
//
// # Choosing an algorithm
//
//	GCRA           a token bucket: bursts up to burst, long-run average at rate.
//	               The default for protecting a backend or pacing a client.
//	FixedWindow    at most limit calls per aligned window. Simplest to state
//	               as a quota; admits up to 2×limit across a boundary.
//	SlidingWindow  at most about limit calls in any window, estimated from two
//	               aligned windows. No boundary burst; a few percent off under
//	               uneven traffic.
//
// Inbound, a refusal is 429 Too Many Requests: the client exceeded a quota
// that is theirs. Outbound, through a breaker, the same refusal is your own
// capacity and becomes 503 at your boundary. The limiter does not know which.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Decision is a limiter's answer for one call.
type Decision struct {
	// Allowed is whether the call may proceed.
	Allowed bool
	// RetryAfter is how long until a call for this key would be allowed;
	// zero when Allowed. It is what a Retry-After header should carry.
	RetryAfter time.Duration
	// Remaining is how many more calls the key can make right now; -1 if
	// unknown.
	Remaining int
}

// Allower is what [Middleware], [Admission] and [FailOpen] accept: anything
// that can decide whether a call for a key may start. [*Limiter] is the
// implementation in this package; the interface exists so callers can wrap
// or fake it. The error return means "could not decide", which is different
// from "not allowed"; the caller chooses whether to fail open or closed.
type Allower interface {
	Allow(ctx context.Context, key string) (Decision, error)
}

var (
	// ErrLimited is matched by every refusal returned through [Admission];
	// the concrete value is a [*LimitedError].
	ErrLimited = errors.New("ratelimit: limit exceeded")
	// ErrInvalidOption is wrapped by every error a constructor returns for a
	// value that cannot be meant.
	ErrInvalidOption = errors.New("ratelimit: invalid option")
	// ErrContention is returned by [Limiter.Allow] when the store's
	// CompareAndSet kept failing: too many writers on one key for the retry
	// budget. Treat it like any other "could not decide" error.
	ErrContention = errors.New("ratelimit: too much contention on key")
)

// LimitedError is a refusal with the key and the wait attached.
type LimitedError struct {
	Key        string
	RetryAfter time.Duration
}

func (e *LimitedError) Error() string {
	if e.Key == "" {
		return fmt.Sprintf("ratelimit: limit exceeded, retry after %s", e.RetryAfter)
	}
	return fmt.Sprintf("ratelimit: limit exceeded for %q, retry after %s", e.Key, e.RetryAfter)
}

// Is makes errors.Is(err, ErrLimited) true.
func (e *LimitedError) Is(target error) bool { return target == ErrLimited }

// Retryable reports true: a refusal is "not yet", not "no". It is the
// Retryable() bool contract package retry looks for, so a retrier stops on a
// breaker refusal but waits on a limiter one, with no import between the
// packages.
func (e *LimitedError) Retryable() bool { return true }

// RetryDelay returns RetryAfter. It is the RetryDelay() time.Duration
// contract package retry looks for: a retrier waits at least this long before
// the next attempt, unless the delay exceeds its cap, in which case the
// refusal is terminal. It is a method because a field and a method cannot
// share the name RetryAfter.
func (e *LimitedError) RetryDelay() time.Duration { return e.RetryAfter }

// Admission adapts a limiter to a veto function: nil when the call may
// proceed, a [*LimitedError] when it is refused, and the limiter's own error
// when it could not decide, which is fail closed; wrap with [FailOpen] to
// invert that. It is shaped for breaker.WithAdmission, with a key per breaker
// when several share one limiter:
//
//	b, err := breaker.New("db-fallback", breaker.WithAdmission(ratelimit.Admission(limiter, "db-fallback")))
//
// There, the veto runs after the circuit has admitted the call, so an open
// circuit still answers breaker.ErrOpen, a call the circuit refuses never
// consumes quota, and a refused call is recorded by the breaker as denied.
// This package does not depend on the breaker. For one limit on the whole
// thing, with no per-key distinction, use [AdmissionGlobal].
func Admission(l Allower, key string) func(context.Context) error {
	return func(ctx context.Context) error {
		d, err := l.Allow(ctx, key)
		if err != nil {
			return err
		}
		if !d.Allowed {
			return &LimitedError{Key: key, RetryAfter: d.RetryAfter}
		}
		return nil
	}
}

// AdmissionGlobal is [Admission] with the key "": one limit on the whole
// thing, with no per-key distinction. It is the [KeyGlobal] of the Admission
// family. Use [Admission] with a key when several budgets share one limiter.
func AdmissionGlobal(l Allower) func(context.Context) error {
	return Admission(l, "")
}

// FailOpen wraps an Allower so that an error from it, such as the store being
// unreachable, allows the call instead of refusing it. onError, if not nil,
// is told about each error; the limiter's metrics already count them. Use it
// when the dependency behind the limit matters more than the limit.
func FailOpen(l Allower, onError func(error)) Allower {
	return failOpen{l: l, onError: onError}
}

type failOpen struct {
	l       Allower
	onError func(error)
}

func (f failOpen) Allow(ctx context.Context, key string) (Decision, error) {
	d, err := f.l.Allow(ctx, key)
	if err != nil {
		if f.onError != nil {
			f.onError(err)
		}
		return Decision{Allowed: true, Remaining: -1}, nil
	}
	return d, nil
}
