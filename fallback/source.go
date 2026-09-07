// Package fallback answers a read from two sources: a fast one that may be
// wrong, and an authoritative one behind per-key single-flight. A [Policy]
// decides whether the fast answer is good enough; the package has no opinion
// on what makes it so.
package fallback

import "context"

// Source answers reads. found reports whether key exists; err reports that
// the source itself failed. The two must never be conflated: a nil err with
// found false is the ordinary, expected shape of "not here", and folding it
// into an error is what makes a guard wrapping this source miscount an empty
// key space as an outage (see [GuardedSource]).
type Source[K comparable, V any] interface {
	Get(ctx context.Context, key K) (V, bool, error)
}

// SourceFunc adapts a function to [Source]. A one-off read needs no named
// type: fallback.SourceFunc[string, Product](repo.Find) is a Source.
type SourceFunc[K comparable, V any] func(ctx context.Context, key K) (V, bool, error)

// Get implements [Source].
func (f SourceFunc[K, V]) Get(ctx context.Context, key K) (V, bool, error) { return f(ctx, key) }

// Store is the fast source: a [Source] the [Reader] may fill and evict. Get
// must answer the same way [Source.Get] does — found false and a nil error
// for an absent key — or a guard wrapping it will count a cold store's misses
// as failures and never let it warm up.
//
// A new implementation should satisfy [conformance/fallbackstore.Run] from
// its own tests; [MemoryStore] does.
type Store[K comparable, V any] interface {
	Source[K, V]
	// Set stores v for key. A failure here is observed, never returned to
	// a caller of [Reader.Get] — the answer it is caching was already
	// produced, and refusing to hand it over because the cache is unwell
	// would make this store a liability rather than a fast path.
	Set(ctx context.Context, key K, v V) error
	// Delete removes key. Called by [Reader.Invalidate] and by [Reader.Get]
	// itself when the origin reports a key gone (see the doc on
	// [ReadOnly] for what happens when this is a no-op).
	Delete(ctx context.Context, key K) error
}

// ReadOnly gives a [Source] the [Store] methods as no-ops, for a fast source
// someone else fills and evicts — a store already populated by a write path
// or a batch job, where this reader must not touch it directly.
//
// Embedding it costs three things for as long as it is embedded, all
// permanent, none an error: a load's write-back never reaches this source,
// so it never learns an answer this reader produced; an origin reporting a
// key gone is not reflected here, so a deleted entity keeps being served
// from this source until whatever else owns its writes removes it; and
// [Reader.Invalidate] itself becomes a no-op against this source too, since
// it calls the same Delete — Invalidate still works to poison an in-flight
// load's write-back, but has nothing here to evict. Embed this because the
// trade is the right one for a source you do not own, not by default — a
// name that reads wrong at a declaration site where it does not belong is
// the point.
type ReadOnly[K comparable, V any] struct{}

// Set implements [Store] as a no-op.
func (ReadOnly[K, V]) Set(context.Context, K, V) error { return nil }

// Delete implements [Store] as a no-op.
func (ReadOnly[K, V]) Delete(context.Context, K) error { return nil }

// Guard wraps one call the way [breaker.Breaker.Do] would, if Do were not
// generic. A *breaker.Breaker cannot satisfy Guard directly — its own doc
// comment says as much: Do is a generic method and cannot be part of an
// interface. See example_test.go in this package for the adapter it asks
// for, over exactly this shape — fallback must not import breaker to
// provide one itself.
type Guard func(ctx context.Context, fn func(context.Context) error) error

// GuardedSource wraps s so every Get runs through g, typically a circuit
// breaker's Do via the adapter shown in example_test.go. Passing the result
// of GuardedSource where a [Store] is expected — including as [New]'s fast
// argument — is a compile error, since Source lacks Set and Delete; that is
// the mistake this split with [GuardedStore] exists to catch. The mistake it
// cannot catch is a hand-written Store wrapper that forwards Get through
// GuardedSource but wires Set and Delete to the unguarded store directly —
// nothing stops that from compiling, and it silently leaves write-back and
// eviction unguarded.
func GuardedSource[K comparable, V any](s Source[K, V], g Guard) Source[K, V] {
	return guardedSource[K, V]{s: s, g: g}
}

// GuardedStore wraps s so every Get, Set and Delete runs through g.
func GuardedStore[K comparable, V any](s Store[K, V], g Guard) Store[K, V] {
	return guardedStore[K, V]{s: s, g: g}
}

type guardedSource[K comparable, V any] struct {
	s Source[K, V]
	g Guard
}

func (w guardedSource[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var v V
	var found bool
	err := w.g(ctx, func(ctx context.Context) error {
		var err error
		v, found, err = w.s.Get(ctx, key)
		return err
	})
	return v, found, err
}

type guardedStore[K comparable, V any] struct {
	s Store[K, V]
	g Guard
}

func (w guardedStore[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	return guardedSource[K, V]{s: w.s, g: w.g}.Get(ctx, key)
}

func (w guardedStore[K, V]) Set(ctx context.Context, key K, v V) error {
	return w.g(ctx, func(ctx context.Context) error { return w.s.Set(ctx, key, v) })
}

func (w guardedStore[K, V]) Delete(ctx context.Context, key K) error {
	return w.g(ctx, func(ctx context.Context) error { return w.s.Delete(ctx, key) })
}
