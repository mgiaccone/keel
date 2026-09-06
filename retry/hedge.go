package retry

import (
	"context"
	"time"
)

// outcome is what an attempt goroutine reports back to the call.
type outcome[T any] struct {
	v        T
	err      error
	attempt  int
	hedge    bool
	panicked any
}

// hedged is do under [WithHedge]. The caller's goroutine owns the counters,
// the budget, the observers and the hedge timer; attempts run in goroutines
// of their own and report on results, which is buffered for every attempt
// the cap allows so no attempt ever blocks on it after the call has
// returned. Nothing here needs locking beyond the RNG's.
//
// Every attempt runs on a context derived from the caller's. The attempt
// whose result the call returns, the winner or the last failure, keeps its
// context, since the value it produced may keep using it after Do returns,
// an HTTP body for one; every other attempt's is cancelled when the call
// ends, or when a later failure supersedes it. The kept context ends with the
// caller's, which is why hedging wants a per-call context rather than a
// service-lifetime one.
func hedged[T any](r *Retrier, ctx context.Context, fn func(context.Context) (T, error), discard func(T, error)) (T, error) {
	var zero T
	if ctx.Err() != nil {
		r.end(Canceled, 0)
		return zero, ctx.Err()
	}
	var (
		results  = make(chan outcome[T], r.cfg.maxAttempts) // one slot per attempt the cap allows
		cancels  []context.CancelFunc
		attempts int
		inFlight int
		pending  *outcome[T]   // the last failed result, not yet returned or discarded
		previous time.Duration // the schedule's last delay, for Decorrelated
		denied   bool          // the budget refused this call: nothing more starts
		timer    chan error    // the armed hedge timer; nil when none is armed
		tcancel  = func() {}
	)
	start := func(hedge bool) {
		attempts++
		r.attempts.Add(1)
		for _, o := range r.cfg.observers {
			o.Attempt(attempts)
		}
		if hedge {
			r.hedges.Add(1)
			for _, o := range r.cfg.observers {
				o.Hedge(attempts)
			}
		}
		actx, cancel := context.WithCancel(ctx)
		cancels = append(cancels, cancel)
		inFlight++
		go func(n int) {
			defer func() {
				if p := recover(); p != nil {
					results <- outcome[T]{attempt: n, hedge: hedge, panicked: p}
				}
			}()
			v, err := fn(actx)
			results <- outcome[T]{v: v, err: err, attempt: n, hedge: hedge}
		}(attempts)
	}
	// arm starts the hedge timer through the configured sleep, unless nothing
	// more may start. The previous timer is dropped first, so a fire it
	// buffered before being cancelled can never be read.
	arm := func() {
		tcancel()
		timer = nil
		if attempts >= r.cfg.maxAttempts || denied {
			return
		}
		var tctx context.Context
		tctx, tcancel = context.WithCancel(ctx)
		ch := make(chan error, 1)
		timer = ch
		go func() { ch <- r.cfg.sleep(tctx, r.cfg.hedge) }()
	}
	// leave ends the call: it stops the timer, cancels every attempt but the
	// one whose result is returned, and drains the results of the attempts still in flight, into
	// the discard hook when there is one. A loser that panics after the call
	// has returned panics there rather than being swallowed.
	leave := func(returned int) {
		tcancel()
		for i, c := range cancels {
			if i+1 != returned {
				c()
			}
		}
		if inFlight > 0 {
			go func(n int) {
				for range n {
					o := <-results
					if o.panicked != nil {
						panic(o.panicked)
					}
					if discard != nil {
						discard(o.v, o.err)
					}
				}
			}(inFlight)
		}
	}
	// supersede discards the pending failed result, which will not be
	// returned, and ends its attempt's context.
	supersede := func() {
		if pending != nil {
			if discard != nil {
				discard(pending.v, pending.err)
			}
			cancels[pending.attempt-1]()
		}
		pending = nil
	}

	start(false)
	arm()
	for {
		select {
		case o := <-results:
			inFlight--
			if o.panicked != nil {
				leave(0)
				supersede()
				panic(o.panicked)
			}
			if o.err == nil {
				leave(o.attempt) // the losers' contexts end first, so a pending body drains fast
				supersede()
				r.end(Success, attempts)
				if o.hedge {
					r.hedgeWins.Add(1)
					r.metrics.hedgeWon()
				}
				return o.v, nil
			}
			supersede()
			pending = &o
			if ctx.Err() != nil {
				leave(o.attempt)
				r.end(Canceled, attempts)
				return o.v, unwrapPermanent(o.err)
			}
			retryable, floor := r.classify(o.err)
			if !retryable || floor > r.cfg.maxRetryAfter {
				leave(o.attempt)
				r.end(Aborted, attempts)
				return o.v, unwrapPermanent(o.err)
			}
			if inFlight > 0 {
				continue // nothing starts because of a failure; the timer keeps running
			}
			// Every attempt has answered and failed: the ordinary retry path.
			// The last failure's context stays alive until it is returned or
			// superseded, so what it produced is usable either way.
			leave(o.attempt)
			switch {
			case attempts >= r.cfg.maxAttempts:
				r.end(Exhausted, attempts)
				return o.v, unwrapPermanent(o.err)
			case denied:
				r.end(Budget, attempts)
				return o.v, unwrapPermanent(o.err)
			}
			if r.cfg.budget != nil {
				if err := r.cfg.budget(ctx); err != nil {
					if r.cfg.onRetry != nil {
						r.cfg.onRetry(attempts, err, 0)
					}
					r.end(Budget, attempts)
					return o.v, unwrapPermanent(o.err)
				}
			}
			previous = r.cfg.backoff.Delay(attempts, previous, r.uniform())
			wait := floor + previous
			if r.cfg.onRetry != nil {
				r.cfg.onRetry(attempts, o.err, wait)
			}
			for _, ob := range r.cfg.observers {
				ob.Wait(attempts, wait)
			}
			r.waited.Add(int64(wait))
			if err := r.cfg.sleep(ctx, wait); err != nil || ctx.Err() != nil {
				r.end(Canceled, attempts)
				return o.v, unwrapPermanent(o.err)
			}
			supersede() // superseded by the retry about to start
			start(false)
			arm()
		case err := <-timer:
			timer = nil // a channel fires once; a nil one never
			if err != nil || ctx.Err() != nil || attempts >= r.cfg.maxAttempts || denied {
				continue // the caller's context is done, or nothing more may start: not re-armed
			}
			if r.cfg.budget != nil {
				if err := r.cfg.budget(ctx); err != nil {
					denied = true
					if r.cfg.onRetry != nil {
						r.cfg.onRetry(attempts, err, 0)
					}
					continue
				}
			}
			start(true)
			arm()
		}
	}
}
