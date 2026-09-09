// Package overload keeps a server serving when more work arrives than it can
// do. It measures how long its own requests take, computes from that how many
// may run at once, and refuses the rest by priority — the least important work
// first — so that a process under more load than it can absorb degrades into
// serving less rather than into serving everything slowly and nothing well.
//
// It stands alone, and it is the inbound counterpart to the other keel
// packages rather than a replacement for any of them:
//
//	ratelimit  a quota, per caller: "this client may make 100 calls a second".
//	           A rule about who, decided from a number you chose.
//	breaker    concurrency and failure, per dependency: "at most 30 calls in
//	           flight to the payments API, and none at all while it is down".
//	overload   saturation, per process: "this server can run 47 requests at
//	           once right now, and 47 is a number it worked out itself".
//
// A limiter is not a global thing. Wire one [Limiter] per group of routes that
// share a latency profile, exactly as a breaker gets one instance per
// dependency: one shared capacity number across a 2ms endpoint and a 2s report
// tells you nothing about either, and sheds the fast routes for the slow one's
// sake. docs/overload.md works that example through.
//
// # Architecture
//
//	Algorithm  the rule, a pure function: Step(state, signal, now) → (state', decision)
//	Limiter    admission: hold the count in flight under the decision's capacity,
//	           divided between priorities by the shares given to New
//	Priority   which work is given up first when there is not enough capacity
//
// [Middleware] is the primary placement, at the front of a request, where the
// work a shed request would have done has not been started yet. [Admission] is
// the same decision as a veto function for breaker.WithAdmission or
// retry.WithBudget, for consistency with the rest of keel; it is honestly the
// lesser use, since by the time an outbound call is being vetoed the inbound
// request is already on the path.
//
// Inbound, a refusal is 503 Service Unavailable: your own capacity, not the
// client's quota, which is what makes it different from ratelimit's 429.
package overload

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/mgiaccone/keel/internal/ring"
)

var (
	// ErrOverloaded is matched by every refusal [Acquire] and [Admission]
	// return; the concrete value is a [*ShedError].
	ErrOverloaded = errors.New("overload: capacity exhausted")
	// ErrInvalidOption is wrapped by every error a constructor returns for a
	// value that cannot be meant. [New] reports every invalid option, not just
	// the first.
	ErrInvalidOption = errors.New("overload: invalid option")
)

// Priority is how much a request is worth keeping when there is not enough
// capacity for all of it. The shares given to [New] decide how much of
// capacity each band may occupy, so a band is refused while more important
// work is still being served.
//
// There is deliberately no band that is never shed. [Critical] is admitted up
// to the whole computed capacity and refused once that is gone, like every
// other band. A route that must answer under any load — a health check whose
// failure gets the instance killed by an orchestrator that cannot tell
// "overloaded" from "dead" — belongs outside [Middleware]'s subtree entirely:
// a handler that never calls [Acquire] can never be shed, which is a stronger
// guarantee than a value that is merely supposed to always win.
type Priority uint8

const (
	// Sheddable is work whose loss a user does not notice: prefetches,
	// analytics beacons, background sync, anything a client will retry later
	// without a person waiting on it.
	Sheddable Priority = iota
	// Default is ordinary request traffic, and what [Middleware] assigns when
	// [WithPriority] is not given.
	Default
	// Critical is work whose loss is itself an outage: a payment being
	// confirmed, a session being written, the last step of a flow the user
	// cannot restart.
	Critical
)

// String returns "sheddable", "default" or "critical".
func (p Priority) String() string {
	switch p {
	case Sheddable:
		return "sheddable"
	case Default:
		return "default"
	case Critical:
		return "critical"
	default:
		return fmt.Sprintf("Priority(%d)", uint8(p))
	}
}

// ShedError is a refusal, with the priority that was refused attached. It is
// what [Acquire] and [Admission] return when there was no capacity for the
// request.
type ShedError struct {
	// Priority is the priority the refused request asked for.
	Priority Priority
}

func (e *ShedError) Error() string {
	return fmt.Sprintf("overload: capacity exhausted, %s request shed", e.Priority)
}

// Is makes errors.Is(err, ErrOverloaded) true.
func (e *ShedError) Is(target error) bool { return target == ErrOverloaded }

// Retryable reports false, and deliberately not what ratelimit's refusal
// reports. A rate limit says "not yet"; this says "this process has more work
// than it can do", and retrying it inside that same process adds to exactly
// the load that caused it. This is the Retryable() bool contract package retry
// looks for, so an enclosing retry.Do stops here immediately, with no import
// between the packages.
//
// It says nothing about the client on the far side of an HTTP boundary: that
// client sees 503, builds a retry.StatusError from it, and is told true, which
// is correct — its retry lands on some other instance, or on this one after it
// has recovered.
func (e *ShedError) Retryable() bool { return false }

const (
	// Buckets in the trailing window the give-up rule reads. More buckets means
	// a wait under target ages out of it closer to exactly one interval after
	// it was observed.
	_waitBuckets = 10
	// The Retry-After a shed HTTP response carries, before jitter.
	_retryAfter = time.Second
	// The fraction by which that value is randomised, matching
	// breaker.WithOpenJitter's default.
	_retryAfterJitter = 0.2
)

// config collects what every [Option] sets, before [New] validates the lot.
type config struct {
	maxWait      time.Duration
	waitTarget   time.Duration
	waitInterval time.Duration
	now          func() time.Time
	seed         [2]uint64
	observers    []Observer
}

// Option configures a [Limiter].
type Option func(*config) error

// WithMaxWait gives requests that find no capacity a bounded wait instead of
// an immediate refusal: d is the hard ceiling on that wait, and target and
// interval are the queue's own give-up rule — once no waiter has been admitted
// within target for a continuous interval, the queue starts shedding waiters
// rather than letting them ride out the full d. Default (0, 0, 0): no queue,
// [Acquire] refuses at once, which is the common case and the right default.
//
// Queueing is worth it when overload is a brief burst: it turns a refusal into
// a slightly late success, and a burst is exactly what a capacity estimate is
// slowest to react to. It stops helping, and starts hurting, the moment
// overload is sustained instead of momentary — every waiter holds its
// goroutine, its connection and whatever the request has already allocated for
// up to d, which is memory spent on work that is going to be refused anyway.
// That is what the give-up rule is for, and it is why d belongs at a small
// fraction of what the caller upstream will wait, never at a substitute for a
// timeout.
//
// A freed slot goes to the oldest waiter in the highest-priority band that has
// one, not to the oldest waiter overall. Passing (0, 0, 0) explicitly switches
// the queue back off.
func WithMaxWait(d, target, interval time.Duration) Option {
	return func(c *config) error {
		switch {
		case d == 0 && target == 0 && interval == 0:
		case d <= 0:
			return fmt.Errorf("%w: WithMaxWait(%s, %s, %s): d must be positive, or all three zero to switch the queue off", ErrInvalidOption, d, target, interval)
		case target <= 0 || interval < time.Duration(_waitBuckets):
			return fmt.Errorf("%w: WithMaxWait(%s, %s, %s): need target > 0 and interval >= %dns", ErrInvalidOption, d, target, interval, _waitBuckets)
		}
		c.maxWait, c.waitTarget, c.waitInterval = d, target, interval
		return nil
	}
}

// WithClock sets the clock every duration the limiter measures is read from:
// service time, sojourn, and the instant handed to [Algorithm.Step]. Default
// time.Now. The bounded wait under [WithMaxWait] is a real timer regardless —
// a fake clock cannot wake a blocked goroutine — so a test that freezes the
// clock freezes what the limiter measures, not how long it blocks.
func WithClock(now func() time.Time) Option {
	return func(c *config) error {
		if now == nil {
			return fmt.Errorf("%w: WithClock(nil)", ErrInvalidOption)
		}
		c.now = now
		return nil
	}
}

// WithSeed seeds the Retry-After jitter RNG. The default, (0, 0), seeds it
// from the runtime; any other pair makes the jittered values reproducible.
func WithSeed(a, b uint64) Option {
	return func(c *config) error {
		c.seed = [2]uint64{a, b}
		return nil
	}
}

// WithObserver attaches an observer; see [Observer]. It may be given more than
// once, and observers are notified in the order they were attached.
func WithObserver(o Observer) Option {
	return func(c *config) error {
		if o == nil {
			return fmt.Errorf("%w: WithObserver(nil)", ErrInvalidOption)
		}
		c.observers = append(c.observers, o)
		return nil
	}
}

// waiter is one request blocked in the queue under [WithMaxWait]. ch carries
// the queue's answer exactly once — true when a slot has been reserved for
// this waiter, false when the give-up rule dropped it — and is buffered, so
// the goroutine holding the lock never blocks handing it over.
type waiter struct {
	p      Priority
	since  time.Time
	ch     chan bool
	queued bool // guarded by Limiter.mu
}

// sojournBucket counts waits observed in one slice of the give-up rule's
// window, split by whether they came in under target.
type sojournBucket struct{ good, bad int }

// Limiter is an admission gate in front of a server's own work: it holds the
// number of requests running at once under a capacity its [Algorithm] computes
// from how long those requests take, and divides that capacity between
// priorities. Create one with [New]; the zero value is not usable.
//
// It has no goroutine and nothing to close, and is safe for concurrent use.
// Everything mutable lives under one mutex held for the length of an admission
// decision and never across a caller's work.
type Limiter struct {
	name         string
	algorithm    Algorithm
	shares       [3]float64
	maxWait      time.Duration
	waitTarget   time.Duration
	waitInterval time.Duration
	now          func() time.Time
	observers    []Observer
	metrics      *metrics

	mu        sync.Mutex
	rng       *rand.Rand
	state     State
	capacity  int
	shedBelow Priority
	inFlight  int
	queue     [3][]*waiter
	sojourns  *ring.Ring[sojournBucket]
	good, bad int // sums over sojourns

	requests, admitted, shed           uint64
	shedByPriority                     [3]uint64
	waited, waitedAdmitted, waitedShed uint64
}

// New returns a limiter that admits at most the capacity algorithm computes,
// with sheddableShare and defaultShare as the fractions of that capacity above
// which [Sheddable] and [Default] are refused; [Critical] is refused only once
// capacity is gone entirely. name identifies it in [Stats] and metrics.
//
// The algorithm and both shares are required rather than defaulted, because
// neither has an answer that is right for every service. An algorithm needs
// real parameters — the range your server's concurrency can plausibly sit in —
// and the shares decide whether a whole class of your traffic is served at
// all under load, which is not a thing to inherit silently. Sensible starting
// points are 0.6 and 0.85 with [Gradient]; docs/overload.md sizes all of them.
//
// Shares are fractions of the current capacity, so they tighten as capacity
// falls: at capacity 40 a 0.6 sheddable share is 24 slots, at capacity 4 it is
// 2, and at capacity 1 it is none at all — the point where only Critical is
// still being served. New returns an error wrapping [ErrInvalidOption]
// describing every problem it found if algorithm is nil or invalid, if name is
// empty, if the shares are not 0 < sheddableShare < defaultShare <= 1, or if
// any option is invalid.
func New(name string, algorithm Algorithm, sheddableShare, defaultShare float64, opts ...Option) (*Limiter, error) {
	cfg := &config{now: time.Now}

	var errs []error
	if name == "" {
		errs = append(errs, fmt.Errorf("%w: New: name must not be empty", ErrInvalidOption))
	}
	if algorithm == nil {
		errs = append(errs, fmt.Errorf("%w: New: algorithm must not be nil", ErrInvalidOption))
	} else if err := algorithm.Validate(); err != nil {
		errs = append(errs, err)
	}
	if !(sheddableShare > 0 && sheddableShare < defaultShare && defaultShare <= 1) { // !(…) also rejects NaN
		errs = append(errs, fmt.Errorf("%w: New(%v, %v): need 0 < sheddableShare < defaultShare <= 1", ErrInvalidOption, sheddableShare, defaultShare))
	}
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	if cfg.seed == ([2]uint64{}) {
		// The global generator is read exactly here, once, to seed this
		// limiter's own; nothing after construction touches it again.
		cfg.seed = [2]uint64{rand.Uint64(), rand.Uint64()}
	}
	l := &Limiter{
		name:         name,
		algorithm:    algorithm,
		shares:       [3]float64{Sheddable: sheddableShare, Default: defaultShare, Critical: 1},
		maxWait:      cfg.maxWait,
		waitTarget:   cfg.waitTarget,
		waitInterval: cfg.waitInterval,
		now:          cfg.now,
		observers:    cfg.observers,
		metrics:      newMetrics(name),
		rng:          rand.New(rand.NewPCG(cfg.seed[0], cfg.seed[1])),
	}
	if l.maxWait > 0 {
		l.sojourns = ring.New[sojournBucket](cfg.waitInterval, _waitBuckets)
	}

	// One step of a zero State is how an algorithm states its opening
	// capacity; there is no request to report yet, so the signal is empty.
	state, d := algorithm.Step(State{}, Signal{}, l.now())
	l.state, l.capacity, l.shedBelow = state, max(d.Capacity, 0), min(d.ShedBelow, Critical)

	l.metrics.started()
	l.metrics.capacity.Set(float64(l.capacity))
	return l, nil
}

// Acquire asks for one slot at priority p. On admission it returns a release
// function and a nil error; release must be called exactly once, with the
// outcome of the work (nil on success), or the slot is held forever and
// capacity walks down to nothing — defer it on the line after the error check.
// Calling it more than once is ignored rather than punished.
//
// On refusal the release function is nil and the error is a [*ShedError],
// which errors.Is matches against [ErrOverloaded]. Under [WithMaxWait] a
// refused request first waits for a slot, and Acquire then returns ctx's error
// if the context ends first — counted in [Stats] as shed, since the request
// was not served either way. A priority outside the three constants is refused
// with an error wrapping [ErrInvalidOption] and is not counted at all.
func (l *Limiter) Acquire(ctx context.Context, p Priority) (release func(err error), err error) {
	if p > Critical {
		return nil, fmt.Errorf("%w: Acquire: priority %d is not one of Sheddable, Default or Critical", ErrInvalidOption, p)
	}

	l.mu.Lock()
	now := l.now()

	// Waiters get first refusal on whatever is free, and the give-up rule gets
	// a chance to run: arrivals are the only thing that happens at all under
	// an overload where nothing is completing.
	l.pump(now)

	l.requests++
	if l.admissible(p) {
		l.inFlight++
		l.accept(p, false)
		l.mu.Unlock()
		return l.releaser(now, false, 0), nil
	}
	if l.maxWait <= 0 {
		l.refuse(p, false)
		l.mu.Unlock()
		return nil, &ShedError{Priority: p}
	}

	w := &waiter{p: p, since: now, ch: make(chan bool, 1), queued: true}
	l.queue[p] = append(l.queue[p], w)
	l.waited++
	for _, o := range l.observers {
		o.Waited(p)
	}
	l.mu.Unlock()

	timer := time.NewTimer(l.maxWait)
	defer timer.Stop()

	select {
	case granted := <-w.ch:
		return l.resolve(w, granted)
	case <-timer.C:
		return l.abandon(w, nil)
	case <-ctx.Done():
		return l.abandon(w, ctx.Err())
	}
}

// resolve finishes a wait the queue itself ended, either way.
func (l *Limiter) resolve(w *waiter, granted bool) (func(error), error) {
	l.mu.Lock()
	now := l.now()
	sojourn := now.Sub(w.since)
	if !granted {
		l.refuse(w.p, true)
		l.mu.Unlock()
		l.metrics.wait.Observe(sojourn.Seconds())
		return nil, &ShedError{Priority: w.p}
	}

	l.accept(w.p, true) // the slot was reserved for this waiter as it was dequeued
	l.mu.Unlock()
	l.metrics.wait.Observe(sojourn.Seconds())
	return l.releaser(now, true, sojourn), nil
}

// abandon finishes a wait the waiter's own side ended: the ceiling d elapsed,
// or its context did. It has to cope with the queue having resolved the waiter
// in the same instant, which is why it re-checks under the lock rather than
// trusting the select that woke it.
func (l *Limiter) abandon(w *waiter, cause error) (func(error), error) {
	l.mu.Lock()
	now := l.now()
	sojourn := now.Sub(w.since)
	if w.queued {
		l.remove(w)
		l.refuse(w.p, true)
		l.mu.Unlock()
		l.metrics.wait.Observe(sojourn.Seconds())
		if cause != nil {
			return nil, cause
		}
		return nil, &ShedError{Priority: w.p}
	}

	// Not queued means the queue resolved this waiter under the same lock
	// before this call took it, so the send has already happened and the
	// receive cannot block.
	granted := <-w.ch
	switch {
	case granted && cause == nil:
		// The ceiling and the grant landed together: taking the slot beats
		// shedding a request that already has one waiting for it.
		l.accept(w.p, true)
		l.mu.Unlock()
		l.metrics.wait.Observe(sojourn.Seconds())
		return l.releaser(now, true, sojourn), nil
	case granted:
		l.inFlight-- // hand the reserved slot straight on; this caller is gone
		l.metrics.inFlight.Set(float64(l.inFlight))
		l.refuse(w.p, true)
		l.pump(now)
	default:
		l.refuse(w.p, true)
	}
	l.mu.Unlock()

	l.metrics.wait.Observe(sojourn.Seconds())
	if cause != nil {
		return nil, cause
	}
	return nil, &ShedError{Priority: w.p}
}

// releaser builds the release function for an admitted request. start is when
// the request was admitted, not when Acquire was called: the queue wait is
// reported as Sojourn and deliberately kept out of Duration.
func (l *Limiter) releaser(start time.Time, waited bool, sojourn time.Duration) func(error) {
	// released is read and written under the limiter's own lock, so a second
	// release — from anywhere, including another goroutine — is dropped rather
	// than freeing a slot the request never held twice.
	released := false
	return func(err error) {
		l.mu.Lock()
		if released {
			l.mu.Unlock()
			return
		}
		released = true

		now := l.now()
		l.inFlight--
		sig := Signal{Waited: waited, Sojourn: sojourn, Duration: now.Sub(start), Err: err}
		l.step(sig, now)
		l.pump(now)
		l.metrics.inFlight.Set(float64(l.inFlight))
		l.mu.Unlock()

		l.metrics.service.Observe(sig.Duration.Seconds())
	}
}

// admissible reports whether one more request at p fits, under both the band's
// share of capacity and the algorithm's own floor on which bands it will serve
// at all.
func (l *Limiter) admissible(p Priority) bool {
	return p >= l.shedBelow && l.inFlight < l.bandLimit(p)
}

// bandLimit is how many slots of the current capacity band p may occupy.
func (l *Limiter) bandLimit(p Priority) int {
	if p == Critical {
		return l.capacity
	}
	return int(float64(l.capacity) * l.shares[p])
}

func (l *Limiter) accept(p Priority, waited bool) {
	l.admitted++
	if waited {
		l.waitedAdmitted++
	}
	l.metrics.admitted[p].Inc()
	l.metrics.inFlight.Set(float64(l.inFlight))
	for _, o := range l.observers {
		o.Admitted(p)
	}
}

func (l *Limiter) refuse(p Priority, waited bool) {
	l.shed++
	l.shedByPriority[p]++
	if waited {
		l.waitedShed++
	}
	l.metrics.shed[p].Inc()
	for _, o := range l.observers {
		o.Shed(p)
	}
}

// step runs the algorithm over one completed request and publishes a capacity
// change.
func (l *Limiter) step(sig Signal, now time.Time) {
	prev := l.capacity

	state, d := l.algorithm.Step(l.state, sig, now)
	l.state, l.capacity, l.shedBelow = state, max(d.Capacity, 0), min(d.ShedBelow, Critical)

	if l.capacity != prev {
		l.metrics.capacity.Set(float64(l.capacity))
		for _, o := range l.observers {
			o.CapacityChanged(l.capacity)
		}
	}
}

// pump hands freed capacity to waiters and applies the give-up rule. It runs
// under the lock after anything that could have changed how much room there
// is: a release, a capacity change, a slot handed back.
func (l *Limiter) pump(now time.Time) {
	if l.maxWait <= 0 {
		return
	}

	for {
		p, w := l.head()
		if w == nil || !l.admissible(p) {
			break
		}
		l.remove(w)
		l.inFlight++ // reserved for w, which counts it as admitted when it wakes
		w.ch <- true
	}

	// The give-up rule is fed from exactly one place: the wait of the oldest
	// waiter left in the queue, whichever band it is in. That is the queue's
	// own worst case, and it is the only sample that answers the question the
	// rule asks. Sampling waiters as they are granted instead would let a
	// briskly-served Critical band supply an endless run of short waits while
	// a Sheddable waiter behind it starves — the band the rule exists to drop
	// would be the one band it could never reach.
	l.observeSojourn(now)

	if l.sustained(now) {
		// Shed from the least important band, oldest first: the queue is
		// priority-aware on the way out as well as on the way in.
		if _, w := l.lowest(); w != nil {
			l.remove(w)
			w.ch <- false
		}
	}
}

// head is the oldest waiter in the highest-priority non-empty band: the one a
// freed slot belongs to.
func (l *Limiter) head() (Priority, *waiter) {
	for p := len(l.queue) - 1; p >= 0; p-- {
		if len(l.queue[p]) > 0 {
			return Priority(p), l.queue[p][0]
		}
	}
	return 0, nil
}

// lowest is the oldest waiter in the lowest-priority non-empty band: the one
// the give-up rule drops first.
func (l *Limiter) lowest() (Priority, *waiter) {
	for p := range l.queue {
		if len(l.queue[p]) > 0 {
			return Priority(p), l.queue[p][0]
		}
	}
	return 0, nil
}

// oldest is the longest-waiting waiter in the whole queue, across every band.
// Each band is in arrival order, so only the three fronts can hold it.
func (l *Limiter) oldest() *waiter {
	var found *waiter
	for p := range l.queue {
		if len(l.queue[p]) == 0 {
			continue
		}
		if w := l.queue[p][0]; found == nil || w.since.Before(found.since) {
			found = w
		}
	}
	return found
}

func (l *Limiter) remove(w *waiter) {
	q := l.queue[w.p]
	for i, other := range q {
		if other == w {
			l.queue[w.p] = append(q[:i], q[i+1:]...)
			w.queued = false
			return
		}
	}
}

// observeSojourn enters the current worst wait in the give-up rule's window.
// An empty queue enters nothing, so the window ages out on its own and a queue
// that has been quiet for an interval starts its next run from scratch.
func (l *Limiter) observeSojourn(now time.Time) {
	l.rotate(now)

	w := l.oldest()
	if w == nil {
		return
	}

	b := l.sojourns.Head()
	if now.Sub(w.since) > l.waitTarget {
		b.bad++
		l.bad++
	} else {
		b.good++
		l.good++
	}
}

func (l *Limiter) rotate(now time.Time) {
	l.sojourns.Rotate(now, func(b *sojournBucket) {
		l.good -= b.good
		l.bad -= b.bad
	})
}

// sustained reports whether the queue has been failing its target long enough
// to start giving up on waiters. Two conditions, and each rules out a case the
// other would get wrong. The window says nothing has been observed under target
// lately and something has been observed over it — one wait under target, or a
// whole interval with nobody queued, clears that, so a queue keeping up is
// never given up on however long it has existed. The waiter's own age then says
// the queue has genuinely been backed up for a full interval, which the window
// alone cannot: how many samples it holds depends on how often arrivals and
// releases happened to look at the queue, and this rule is about elapsed time,
// not about sampling rate. Nothing here is remembered between runs.
func (l *Limiter) sustained(now time.Time) bool {
	l.rotate(now)
	if l.bad == 0 || l.good > 0 {
		return false
	}

	w := l.oldest()
	return w != nil && now.Sub(w.since) >= l.waitInterval
}

// retryAfter is the Retry-After a shed HTTP response carries: one second,
// randomised by ±20% from this limiter's own seeded generator, reusing
// breaker.WithOpenJitter's own fraction rather than inventing a second one. Be
// clear about how much this buys: the header is whole seconds, so a value in
// [0.8s, 1.2s] rounds up to one or two, and a refused fleet returns in two
// waves rather than one. That is worth having over a constant, which returns
// them in one, but it is not the fine-grained spread jittering an open interval
// gives — a client that needs that has to add its own.
func (l *Limiter) retryAfter() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	return time.Duration(float64(_retryAfter) * (1 + _retryAfterJitter*(2*l.rng.Float64()-1)))
}

// Stats returns a snapshot.
func (l *Limiter) Stats() Stats {
	l.mu.Lock()
	defer l.mu.Unlock()
	return Stats{
		Name: l.name, Algorithm: l.algorithm.Name(),
		Requests: l.requests, Admitted: l.admitted, Shed: l.shed,
		ShedByPriority:     l.shedByPriority,
		Waited:             l.waited,
		WaitedThenAdmitted: l.waitedAdmitted,
		WaitedThenShed:     l.waitedShed,
		Capacity:           l.capacity,
		InFlight:           l.inFlight,
	}
}

// Admission adapts a limiter to a veto function: nil when a request at p would
// be admitted right now, a [*ShedError] when it would not. It is shaped for
// breaker.WithAdmission and retry.WithBudget:
//
//	b, err := breaker.New("payments", breaker.WithAdmission(overload.Admission(l, overload.Default)))
//
// It holds no slot, and it cannot: a veto has no release to pair with, so
// there is nowhere to report the work's service time from. That has two
// consequences worth knowing before reaching for it. The vetoed work never
// counts towards in-flight and never feeds the algorithm a measurement, so a
// limiter reached only this way never learns anything and its capacity never
// moves. And composed into a breaker, the veto runs after the circuit and the
// bulkhead have already admitted the call, by which point the inbound request
// this package exists to shed is already on the path.
//
// [Middleware] is the placement that makes the package work. Use Admission for
// a narrower outbound veto that shares one process-wide capacity number, and
// only alongside a Middleware that is doing the real measuring. Requests
// through it are counted in [Stats] and metrics like any other.
func Admission(l *Limiter, p Priority) func(context.Context) error {
	return func(context.Context) error {
		if p > Critical {
			return fmt.Errorf("%w: Admission: priority %d is not one of Sheddable, Default or Critical", ErrInvalidOption, p)
		}

		l.mu.Lock()
		defer l.mu.Unlock()
		l.requests++
		if !l.admissible(p) {
			l.refuse(p, false)
			return &ShedError{Priority: p}
		}
		l.accept(p, false)
		return nil
	}
}
