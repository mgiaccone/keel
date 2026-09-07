package fallback

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// ErrInvalidOption is wrapped by every error [New] returns for an option
// that cannot be meant. New reports every invalid option, not just the
// first.
var ErrInvalidOption = errors.New("fallback: invalid option")

// config collects what every Option sets, before [New] type-asserts the
// generic-typed fields (policy, lease) against the Reader's own K and V.
// Option itself cannot be generic over K and V: a caller writing
// fallback.WithFastTimeout(d) supplies nothing Go could infer either type
// parameter from, and spelling both out on every option call this package
// has (most of which touch neither K nor V) is the ergonomics this design
// avoids. The cost, paid once in New rather than never, is that a Policy or
// Leaser built for the wrong K/V pair fails at construction with
// [ErrInvalidOption] instead of at compile time — state this in review, it
// is the one place this package's options behave differently from every
// other keel package's.
type config struct {
	policy        any // Policy[V]
	lease         any // Leaser[K]
	leaseTTL      time.Duration
	fastTimeout   time.Duration
	loadTimeout   time.Duration
	refreshJitter time.Duration
	observers     []Observer
	now           func() time.Time
	seed          [2]uint64
}

// Option configures a [Reader].
type Option func(*config) error

// WithPolicy sets the policy judging the fast source's answer. Default: a
// nil Policy, meaning always [Serve] — the fast source is trusted until
// [Reader.Invalidate] removes an entry.
func WithPolicy[V any](p Policy[V]) Option {
	return func(c *config) error {
		if p == nil {
			return fmt.Errorf("%w: WithPolicy(nil)", ErrInvalidOption)
		}
		c.policy = p
		return nil
	}
}

// WithLease configures fleet-wide coalescing of background refreshes
// started by [ServeAndRefresh]: a refresh acquires l for ttl before running
// and releases it on completion; a loser skips its refresh entirely. Without
// this option, [ServeAndRefresh] dedupes only within one process. If a
// leaseholder dies mid-refresh, ttl is what recovers the key — the next
// stale hit anywhere in the fleet acquires the expired lease — so the real
// bound this option delivers is one refresh per key per lease interval, not
// absolutely one, ever.
func WithLease[K comparable](l Leaser[K], ttl time.Duration) Option {
	return func(c *config) error {
		if l == nil {
			return fmt.Errorf("%w: WithLease(nil, %s)", ErrInvalidOption, ttl)
		}
		if ttl <= 0 {
			return fmt.Errorf("%w: WithLease(l, %s): ttl must be positive", ErrInvalidOption, ttl)
		}
		c.lease = l
		c.leaseTTL = ttl
		return nil
	}
}

// WithFastTimeout bounds every call to the fast source. Default 0: no bound
// beyond ctx. Without it, a fast source that hangs costs every Get the
// entire hang before falling back to the origin — set this whenever the
// fast source is a network call.
func WithFastTimeout(d time.Duration) Option {
	return func(c *config) error {
		if d <= 0 {
			return fmt.Errorf("%w: WithFastTimeout(%s): must be positive", ErrInvalidOption, d)
		}
		c.fastTimeout = d
		return nil
	}
}

// WithLoadTimeout bounds every call to the origin, for a blocking load and
// for a background refresh alike. Default 10s. A load runs on a context
// detached from any caller's cancellation — this is the only bound on how
// long it can run, and the only defense against a loader that ignores
// cancellation leaking a goroutine forever.
func WithLoadTimeout(d time.Duration) Option {
	return func(c *config) error {
		if d <= 0 {
			return fmt.Errorf("%w: WithLoadTimeout(%s): must be positive", ErrInvalidOption, d)
		}
		c.loadTimeout = d
		return nil
	}
}

// WithRefreshJitter spreads background refreshes started by
// [ServeAndRefresh] over [0, d) before they run, so values that crossed a
// policy's threshold in the same burst do not refresh in the same instant.
// Default 0: no jitter. A [Policy] cannot do this itself — drawing
// randomness inside one would make it impure and prone to flapping between
// verdicts on consecutive reads of the same value.
func WithRefreshJitter(d time.Duration) Option {
	return func(c *config) error {
		if d < 0 {
			return fmt.Errorf("%w: WithRefreshJitter(%s): must not be negative", ErrInvalidOption, d)
		}
		c.refreshJitter = d
		return nil
	}
}

// WithObserver attaches an observer; see [Observer]. It may be given more
// than once, and observers are notified in the order they were attached.
func WithObserver(o Observer) Option {
	return func(c *config) error {
		if o == nil {
			return fmt.Errorf("%w: WithObserver(nil)", ErrInvalidOption)
		}
		c.observers = append(c.observers, o)
		return nil
	}
}

// WithClock sets the clock a [Policy] is judged against. Default time.Now.
func WithClock(now func() time.Time) Option {
	return func(c *config) error {
		if now == nil {
			return fmt.Errorf("%w: WithClock(nil)", ErrInvalidOption)
		}
		c.now = now
		return nil
	}
}

// WithSeed seeds the refresh-jitter RNG. The default, (0, 0), seeds it from
// the runtime; any other pair makes jitter draws reproducible.
func WithSeed(a, b uint64) Option {
	return func(c *config) error {
		c.seed = [2]uint64{a, b}
		return nil
	}
}

// call is one in-flight load — blocking or a background refresh — shared by
// every caller waiting on the same key.
type call[V any] struct {
	done  chan struct{}
	value V
	found bool
	err   error

	poisoned atomic.Bool // set by Invalidate: skip this call's write-back

	// waiters counts blocking joiners (via awaitLoad), guarded by Reader.mu
	// rather than its own atomic: a background refresh that loses its lease
	// race must decide whether to skip or to run a real load by checking
	// this under the same lock a joiner increments it under, or a joiner
	// arriving in the gap between the check and the decision could be left
	// waiting on a call that was never going to produce a real answer.
	waiters int
}

// Reader answers Get from a fast [Store] and an authoritative [Source],
// with per-key single-flight on every load. See the package doc and
// [Policy] for what decides which source answers.
type Reader[K comparable, V any] struct {
	name   string
	fast   Store[K, V]
	origin Source[K, V]
	policy Policy[V]
	now    func() time.Time

	lease         Leaser[K]
	leaseTTL      time.Duration
	fastTimeout   time.Duration
	loadTimeout   time.Duration
	refreshJitter time.Duration
	observers     []Observer

	rngMu sync.Mutex // guards rng, which is not safe for concurrent use
	rng   *rand.Rand

	mu       sync.Mutex
	inflight map[K]*call[V]
	wg       sync.WaitGroup // in-flight loads and refreshes; drained by Close

	metrics *metrics

	gets, served, loaded, degraded, failed, aborted atomic.Uint64
	misses, refreshes, loadFailures, fastErrors     atomic.Uint64
	writeBackFailures, leaseFailures                atomic.Uint64
}

// New returns a reader over fast and origin. name identifies it in [Stats]
// and metrics.
func New[K comparable, V any](name string, fast Store[K, V], origin Source[K, V], opts ...Option) (*Reader[K, V], error) {
	cfg := &config{now: time.Now, loadTimeout: 10 * time.Second}

	var errs []error
	if name == "" {
		errs = append(errs, fmt.Errorf("%w: New: name must not be empty", ErrInvalidOption))
	}
	if fast == nil {
		errs = append(errs, fmt.Errorf("%w: New: fast must not be nil", ErrInvalidOption))
	}
	if origin == nil {
		errs = append(errs, fmt.Errorf("%w: New: origin must not be nil", ErrInvalidOption))
	}
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			errs = append(errs, err)
		}
	}

	var policy Policy[V]
	if cfg.policy != nil {
		p, ok := cfg.policy.(Policy[V])
		if !ok {
			errs = append(errs, fmt.Errorf("%w: WithPolicy: built for a different value type than this Reader's", ErrInvalidOption))
		} else {
			policy = p
		}
	}
	var lease Leaser[K]
	if cfg.lease != nil {
		l, ok := cfg.lease.(Leaser[K])
		if !ok {
			errs = append(errs, fmt.Errorf("%w: WithLease: built for a different key type than this Reader's", ErrInvalidOption))
		} else {
			lease = l
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	if cfg.seed == ([2]uint64{}) {
		cfg.seed = [2]uint64{rand.Uint64(), rand.Uint64()}
	}
	m := newMetrics(name)
	m.started()
	return &Reader[K, V]{
		name:          name,
		fast:          fast,
		origin:        origin,
		policy:        policy,
		now:           cfg.now,
		lease:         lease,
		leaseTTL:      cfg.leaseTTL,
		fastTimeout:   cfg.fastTimeout,
		loadTimeout:   cfg.loadTimeout,
		refreshJitter: cfg.refreshJitter,
		observers:     cfg.observers,
		rng:           rand.New(rand.NewPCG(cfg.seed[0], cfg.seed[1])),
		inflight:      make(map[K]*call[V]),
		metrics:       m,
	}, nil
}

// Get is the whole package: consult the fast source, judge it with the
// policy, load from the origin when the policy or the fast source's absence
// requires it, and write back on success. found is false only when the
// origin is authoritative that key does not exist; every other failure to
// produce a value is reported through err.
func (r *Reader[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	r.gets.Add(1)
	var zero V

	if err := ctx.Err(); err != nil {
		r.countGet(Aborted)
		return zero, false, err
	}

	v, found, ferr := r.getFast(ctx, key)
	switch {
	case ferr != nil:
		r.countFastError(ferr)
		return r.resolveViaLoad(ctx, key, false)
	case !found:
		return r.resolveViaLoad(ctx, key, true)
	}

	verdict := Serve
	if r.policy != nil {
		verdict = r.policy(v, r.now())
	}

	switch verdict {
	case ServeAndRefresh:
		r.startRefresh(key)
		fallthrough
	case Serve:
		r.countGet(Served)
		return v, true, nil
	case LoadOrServe:
		nv, nfound, nerr, abortedLocally := r.awaitLoad(ctx, key)
		switch {
		case abortedLocally:
			r.countGet(Aborted)
			return zero, false, nerr
		case nerr != nil:
			// The load failed; the stale value beats an error, which is the
			// entire point of LoadOrServe. Row 4 of the decision table.
			r.countGet(Degraded)
			return v, true, nil
		default:
			r.countLoaded(nfound)
			return nv, nfound, nil
		}
	default: // Load
		return r.resolveViaLoad(ctx, key, false)
	}
}

// resolveViaLoad joins or starts a blocking load for key and counts the Get
// it resolves as Aborted, Failed or Loaded. countMiss is true when the fast
// source was already known absent, so Misses is counted regardless of what
// the load itself finds — a load that also comes back not-found must not
// count a second time.
func (r *Reader[K, V]) resolveViaLoad(ctx context.Context, key K, countMiss bool) (V, bool, error) {
	var zero V
	v, found, err, abortedLocally := r.awaitLoad(ctx, key)
	if abortedLocally {
		r.countGet(Aborted)
		return zero, false, err
	}
	if countMiss {
		// The fast source was already known absent — that is the Miss,
		// independent of whatever the load itself goes on to do.
		r.countMiss()
	}
	if err != nil {
		r.countGet(Failed)
		return zero, false, err
	}
	if !found && !countMiss {
		// The fast source had nothing to say either way (it errored) or
		// was never consulted for this reason; the load itself is what
		// found the origin has none. Row 12's Miss, not row 7/8's.
		r.countMiss()
	}
	r.countGet(Loaded)
	return v, found, nil
}

// countLoaded records a successful blocking load's outcome for a Get that
// does its own dispatch (LoadOrServe's success path), rather than going
// through resolveViaLoad.
func (r *Reader[K, V]) countLoaded(found bool) {
	if !found {
		r.countMiss()
	}
	r.countGet(Loaded)
}

// awaitLoad joins the in-flight load for key, starting one if none is
// running, and waits for it or for ctx, whichever ends first.
// abortedLocally is true only when ctx ended first — the load itself
// continues for any other waiter, per the package's single-flight
// invariant: no individual caller's cancellation can affect another's.
func (r *Reader[K, V]) awaitLoad(ctx context.Context, key K) (v V, found bool, err error, abortedLocally bool) {
	c := r.joinOrStartLoad(key)
	select {
	case <-c.done:
		return c.value, c.found, c.err, false
	case <-ctx.Done():
		var zero V
		return zero, false, ctx.Err(), true
	}
}

func (r *Reader[K, V]) joinOrStartLoad(key K) *call[V] {
	r.mu.Lock()
	if c, ok := r.inflight[key]; ok {
		c.waiters++ // this caller will block on c.done; see abandonRefresh
		r.mu.Unlock()
		return c
	}
	c := &call[V]{done: make(chan struct{}), waiters: 1}
	r.inflight[key] = c
	r.mu.Unlock()

	r.wg.Add(1)
	go r.runLoad(key, c, false)
	return c
}

// startRefresh begins a background refresh for key if none is already
// running for it — a stale hit that arrives while one is in flight joins
// it rather than starting a second.
func (r *Reader[K, V]) startRefresh(key K) {
	r.mu.Lock()
	if _, ok := r.inflight[key]; ok {
		r.mu.Unlock()
		return
	}
	c := &call[V]{done: make(chan struct{})}
	r.inflight[key] = c
	r.mu.Unlock()

	r.wg.Add(1)
	go r.runRefresh(key, c)
}

// runRefresh applies jitter and an optional lease before handing off to
// runLoad, which owns clearing r.inflight, closing c.done and r.wg.Done in
// every case — except the two lease-loss paths, which hand off to
// abandonRefresh instead and only reach runLoad if a blocking caller is
// depending on this call for a real answer (see abandonRefresh).
func (r *Reader[K, V]) runRefresh(key K, c *call[V]) {
	if r.refreshJitter > 0 {
		timer := time.NewTimer(r.jitter(r.refreshJitter))
		<-timer.C // detached by design: no ctx of any caller applies here
		timer.Stop()
	}

	if r.lease == nil {
		r.runLoad(key, c, true)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.leaseTTL)
	token, ok, err := r.lease.Acquire(ctx, key, r.leaseTTL)
	cancel()
	if err != nil {
		r.countLeaseFailure(err) // a lease failure is observed, not a load failure
		r.abandonRefresh(key, c)
		return
	}
	if !ok {
		r.abandonRefresh(key, c) // held elsewhere: skip, no polling — unless someone is waiting
		return
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), r.leaseTTL)
		defer cancel()
		if err := r.lease.Release(ctx, key, token); err != nil {
			r.countLeaseFailure(err)
		}
	}()
	r.runLoad(key, c, true)
}

// abandonRefresh gives up on a refresh whose lease was not acquired —
// unless a blocking Get has already joined c, in which case abandoning it
// would leave that caller with a fabricated "not found": indistinguishable
// from the origin's own authoritative answer (see [Source]'s doc comment on
// found/err), but actually just this instance losing a lease race. A lease
// outcome must never produce that lie, so a real load runs instead whenever
// c.waiters is nonzero. The check and the decision happen under the same
// lock a joiner increments waiters under, so no joiner can arrive in the
// gap between them: it either joins before this runs (already counted) or
// after c is already gone from r.inflight (and starts a fresh call of its
// own, never this one).
func (r *Reader[K, V]) abandonRefresh(key K, c *call[V]) {
	r.mu.Lock()
	if c.waiters > 0 {
		r.mu.Unlock()
		r.runLoad(key, c, true)
		return
	}
	delete(r.inflight, key)
	r.mu.Unlock()
	close(c.done)
	r.wg.Done()
}

// runLoad calls the origin, writes back on success (unless c has been
// poisoned by a concurrent Invalidate), and always clears r.inflight,
// closes c.done and calls r.wg.Done — exactly once, regardless of outcome.
func (r *Reader[K, V]) runLoad(key K, c *call[V], background bool) {
	defer r.wg.Done()
	defer func() {
		r.mu.Lock()
		delete(r.inflight, key)
		r.mu.Unlock()
		close(c.done)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), r.loadTimeout)
	defer cancel()

	v, found, err := r.callOrigin(ctx, key)
	c.value, c.found, c.err = v, found, err

	if c.poisoned.Load() {
		return // Invalidate ran during this load: do not resurrect the key
	}
	if err != nil {
		if background {
			r.countLoadFailure(err)
		}
		return
	}

	wctx := context.Background()
	if found {
		if serr := r.fast.Set(wctx, key, v); serr != nil {
			r.countWriteBackFailure(serr)
		}
	} else if derr := r.fast.Delete(wctx, key); derr != nil {
		r.countWriteBackFailure(derr)
	}
	if background {
		r.countRefresh()
	}
}

// callOrigin recovers a panic in origin.Get as a [PanicError] rather than
// letting it cross into whichever goroutine happens to be running the load
// — the load's goroutine belongs to the Reader, not to any caller of Get.
func (r *Reader[K, V]) callOrigin(ctx context.Context, key K) (v V, found bool, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = &PanicError{Value: p, Stack: debug.Stack()}
		}
	}()
	return r.origin.Get(ctx, key)
}

func (r *Reader[K, V]) getFast(ctx context.Context, key K) (V, bool, error) {
	if r.fastTimeout <= 0 {
		return r.fast.Get(ctx, key)
	}
	fctx, cancel := context.WithTimeout(ctx, r.fastTimeout)
	defer cancel()
	return r.fast.Get(fctx, key)
}

func (r *Reader[K, V]) jitter(max time.Duration) time.Duration {
	r.rngMu.Lock()
	defer r.rngMu.Unlock()
	return time.Duration(r.rng.Int64N(int64(max)))
}

// Invalidate evicts key from the fast source. Use this rather than calling
// Delete on the fast source directly: doing so can race a load already in
// flight for key, whose write-back would resurrect it right after this call
// removed it. Invalidate poisons that flight instead, so its write-back is
// dropped; its waiters still receive its result.
//
// This closes the race against a load already running when Invalidate is
// called, but not one that starts in the instant between Invalidate reading
// r.inflight and its own call to fast.Delete: that load is not poisoned, and
// if its write-back lands after this Delete, the key comes back. Callers
// for whom this narrow window matters should pair Invalidate with their own
// higher-level exclusion (e.g. not accepting reads for key during a write).
func (r *Reader[K, V]) Invalidate(ctx context.Context, key K) error {
	r.mu.Lock()
	if c, ok := r.inflight[key]; ok {
		c.poisoned.Store(true)
	}
	r.mu.Unlock()
	return r.fast.Delete(ctx, key)
}

// Close waits for every in-flight load and background refresh to finish, or
// for ctx to end, whichever comes first. A background refresh runs on a
// context detached from any caller, so without draining it a process can
// exit mid-refresh and lose a write-back — including one racing an
// Invalidate that just ran, which is the case that makes an abandoned
// refresh worse than a merely wasted one.
//
// The caller must stop issuing Get before calling Close, the same
// precondition [sync.WaitGroup.Wait] documents for Add: a Get that starts a
// new load concurrently with Close can race the internal WaitGroup this
// method waits on.
func (r *Reader[K, V]) Close(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stats returns a snapshot of the reader's counters.
func (r *Reader[K, V]) Stats() Stats {
	return Stats{
		Name:              r.name,
		Gets:              r.gets.Load(),
		Served:            r.served.Load(),
		Loaded:            r.loaded.Load(),
		Degraded:          r.degraded.Load(),
		Failed:            r.failed.Load(),
		Aborted:           r.aborted.Load(),
		Misses:            r.misses.Load(),
		Refreshes:         r.refreshes.Load(),
		LoadFailures:      r.loadFailures.Load(),
		FastErrors:        r.fastErrors.Load(),
		WriteBackFailures: r.writeBackFailures.Load(),
		LeaseFailures:     r.leaseFailures.Load(),
	}
}

// countGet records one Get's outcome: the atomic in [Stats], the matching
// Prometheus series and the [Observer] notification, in one place — so
// none of the three can drift from the other two the way a scattered
// Add/notify pair once did.
func (r *Reader[K, V]) countGet(o Outcome) {
	switch o {
	case Served:
		r.served.Add(1)
	case Loaded:
		r.loaded.Add(1)
	case Degraded:
		r.degraded.Add(1)
	case Failed:
		r.failed.Add(1)
	case Aborted:
		r.aborted.Add(1)
	}
	r.metrics.gets[o].Inc()
	for _, ob := range r.observers {
		ob.Get(o)
	}
}

func (r *Reader[K, V]) countMiss() {
	r.misses.Add(1)
	r.metrics.misses.Inc()
}

func (r *Reader[K, V]) countRefresh() {
	r.refreshes.Add(1)
	r.metrics.refreshes.Inc()
	for _, ob := range r.observers {
		ob.Refreshed()
	}
}

func (r *Reader[K, V]) countLoadFailure(err error) {
	r.loadFailures.Add(1)
	r.metrics.loadFailures.Inc()
	for _, ob := range r.observers {
		ob.LoadFailed(err)
	}
}

func (r *Reader[K, V]) countFastError(err error) {
	r.fastErrors.Add(1)
	r.metrics.fastErrors.Inc()
	for _, ob := range r.observers {
		ob.FastError(err)
	}
}

func (r *Reader[K, V]) countWriteBackFailure(err error) {
	r.writeBackFailures.Add(1)
	r.metrics.writeBackFailures.Inc()
	for _, ob := range r.observers {
		ob.WriteBackFailed(err)
	}
}

func (r *Reader[K, V]) countLeaseFailure(err error) {
	r.leaseFailures.Add(1)
	r.metrics.leaseFailures.Inc()
	for _, ob := range r.observers {
		ob.LeaseFailed(err)
	}
}
