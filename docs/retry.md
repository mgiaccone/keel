# Retrier

Package `github.com/mgiaccone/keel/retry`. A retrier for a single call, with
four jittered schedules, a retry budget, a cap on how long a `Retry-After` may
ask for, and an `http.RoundTripper` that retries what `net/http` itself would
replay.

## Quick start

```go
r, err := retry.New("catalogue",
    retry.Exponential(50*time.Millisecond, 2*time.Second),
    retry.WithMaxAttempts(4),
    retry.WithBudget(ratelimit.AdmissionGlobal(budget)),  // at most so many retries per second, fleet-wide with Redis
    retry.WithRetryIf(func(err error) bool {              // a not-found is an answer, not a failure
        return !errors.Is(err, ErrNotFound)
    }),
)
if err != nil {
    return err // wraps retry.ErrInvalidOption and lists every invalid option
}

row, err := r.Do(ctx, func(ctx context.Context) (Row, error) {
    return b.Do(ctx, func(ctx context.Context) (Row, error) { // b is a breaker; its refusals are never retried
        return db.Get(ctx, key)
    })
})

client := &http.Client{Transport: transport} // transport, err := retry.NewTransport(r, nil)
```

`retry/example_test.go` contains a retried read through a breaker with a
limiter as the budget, and an `http.Client` on the transport, both with
verified output.

## When to use it

A retry absorbs a blip: a connection reset, a request that landed on one bad
replica, a limiter that said "not yet". It amplifies an outage: when the
dependency is down, every retry is one more call it cannot serve, from every
instance at the same time, and the retries can be the load that keeps it
down. Three bounds separate the two cases, and this package has all three:

- **The schedule** spreads retries in time. Every schedule but `Constant` is
  jittered, so a fleet that failed together does not retry together.
- **The attempt cap and the caller's context** bound one call. A cap of 4
  against a dependency that hangs costs four times the attempt's timeout, so
  the attempt needs its own timeout: put a breaker with `WithTimeout` inside
  the retrier.
- **The budget** bounds the fleet. It is a rate limit on retries, so
  `ratelimit` provides it, per process with a memory store or across every
  instance with Redis. A budget of a tenth of normal traffic means that
  during an outage retries add a tenth, not a multiple. This is the bound
  that holds when every instance is retrying and the only one that does.

Whether a call may be retried at all is the caller's contract. A retried
write that is not idempotent is applied twice; the retrier cannot know. Mark
such errors `Permanent`, or exclude them in `WithRetryIf`. The HTTP transport
applies the rule `net/http` applies to its own replays.

A hedge is the opposite kind of tool: a retry helps when an attempt fails, a
hedge helps when an attempt is slow. `WithHedge(after)` starts another
attempt when the ones in flight have not answered within `after`; the first
success wins and the rest are cancelled. It cuts the latency tail that a few
slow replicas cause. Against a dependency that is uniformly slow it doubles
the load and helps nobody, and during an outage it is amplification on
purpose, so hedge only with a budget, and only where the tail comes from
replicas rather than from the request itself. Hedges need idempotency even
more than retries: two attempts may both run to completion, and cancelling
the loser is best effort, not a guarantee — see Hedging below for what that
means for a caller.

## How it works

### The attempt loop

`Do` runs `fn` and, after each failure, decides in this order. The first
rule that applies ends the call, and `Do` returns what the last attempt
returned.

1. The caller's context is done: the call is **canceled**.
2. The error is classified. One that implements `Retryable` decides for
   itself; `Permanent(err)` is such an error, and so are the breaker's
   refusals and a limiter's refusal. Any other error is asked of the
   `WithRetryIf` predicate. Not retryable: the call is **aborted**.
3. The error implements `Delayed` and its delay exceeds `WithMaxRetryAfter`:
   **aborted**.
4. The attempt cap is reached: **exhausted**.
5. `WithBudget` is set and refuses: **budget**. The budget is never asked
   before the first attempt, and not asked when the cap has already ended the
   call.
6. The wait is computed: the schedule's delay for this retry, plus the
   error's delay if it had one. The `WithOnRetry` hook and the observers
   see it, then the retrier sleeps. A context that ends during the wait
   makes the call **canceled**.

A context that is already done before the first attempt ends the call as
canceled with `ctx.Err()`, the only case where `Do` returns an error `fn` did
not.

### Hedging

`WithHedge(after)` runs attempts concurrently. Each gets its own context
derived from the caller's, and `fn` is called concurrently with itself,
which is a requirement of the option.

**`fn` must be safe to run more than once concurrently with itself.** Hedging
can have two attempts in flight at the same time, so a non-idempotent handler
can double-apply *without either attempt having failed* — unlike a sequential
retry, where a second attempt only ever follows a first that has already
ended. Mark the operation safe with an idempotency key the caller generates
once per operation and sends on every attempt; the HTTP transport already
treats `Idempotency-Key` and `X-Idempotency-Key` as a replay signal. This
package neither generates nor stores such a key — that is the caller's and
the server's contract.

Cancelling a loser is **best effort, not a guarantee**: it stops `fn` from
starting more work on that attempt, but it cannot unsend a request already
on the wire, so a losing attempt's write may still reach and be applied by
the dependency after `Do` has returned a different attempt's answer. A loser
that itself succeeds is not distinguished from one that failed or was
cancelled — see rule 3 below — so the caller has no way to learn that a
losing write landed.

The rules:

1. Attempt 1 starts and a timer is armed for `after`.
2. The timer fires with attempts in flight. If the cap allows and the budget
   has not refused this call, the budget is asked; refused, the `WithOnRetry`
   hook sees the veto's error, no further attempt of any kind starts for
   this call, and the running ones finish it. Allowed, a hedge starts,
   `Attempt` and then `Hedge` reach the observers, and the timer is
   re-armed.
3. A success ends the call at once: every other attempt is cancelled and
   `Do` does not wait for them. Their results are discarded — including one
   that itself succeeded but arrived after the winner: its value is dropped
   and, on the HTTP transport, its response body is drained and closed
   unread, so the caller cannot tell that this attempt's write reached the
   server and was applied. The winner's context is left alive, since the
   value it produced may keep using it after `Do` returns, an HTTP body for
   one; it ends with the caller's. The same holds for the attempt whose
   failure `Do` returns: what it produced, a 503 with a body, is usable, and
   its context ends with the caller's. Every other attempt's context ends
   with the call.
4. A failure while other attempts are running is classified. Not retryable,
   or asking for a delay above `WithMaxRetryAfter`: the others are cancelled
   and the call is **aborted** with that error. The caller's context is done:
   **canceled**, with that error. Retryable: nothing starts because of it,
   the timer keeps running, and the error is what `Do` returns if nothing
   better arrives.
5. A failure with nothing else in flight takes the ordinary path of the
   attempt loop: the cap, the budget (a refusal earlier in the call still
   holds), the schedule's wait plus any delay floor, the hook and the
   observers, the sleep. The next attempt is a retry, not a hedge, and the
   timer is re-armed for it. The hedge delay does not floor the schedule's
   wait; the two are independent. The schedule is asked for the wait after
   that many failed attempts, hedges included, so two attempts failing
   together back off as far as two failing in turn.
6. The timer fires after the caller's context has ended: nothing starts and
   the timer is not re-armed; the attempts have been cancelled through their
   contexts and rule 4 ends the call when one answers.
7. A panic in any attempt is recovered on its goroutine, the other attempts
   are cancelled, and the panic is raised again on the caller's goroutine. A
   loser that panics after the call has returned panics on its own
   goroutine: a panic in `fn` is a bug and is not swallowed.
8. `Do` returns what the last attempt to answer returned, with `Permanent`
   unwrapped as always. Hedges count against the attempt cap together with
   retries. `Stats.Hedged` counts hedges started and `Stats.HedgeWon` calls a
   hedge won; a hedge that did not win was load for nothing, and the ratio
   is how to judge `after`.

The results a call does not return, the failed result a retry supersedes
and the losers of a hedge race, go to an internal hook the transport uses to
close response bodies the caller never sees. Without hedging, `Do` is the
sequential loop above and allocates nothing on success.

Hedge under a per-call context. Every attempt's context is derived from the
caller's; the context of the attempt whose result `Do` returns, the winner
or the last failure, ends only with the caller's, every other attempt's with
the call, so under a service-lifetime cancellable context each returned
attempt stays registered in it until it ends.

### Schedules

A schedule is a `Backoff`: a pure function from the retry number, the
previous wait and a uniform draw to the next wait. The retrier supplies the
draw from its own seeded generator, so schedules keep no state and are tested
with explicit inputs.

| Constructor | Wait before retry n | Jitter |
|---|---|---|
| `Constant(d)` | `d` | none |
| `Exponential(base, max)` | uniform in [0, min(max, base × 2ⁿ⁻¹)) | full |
| `Decorrelated(base, max)` | uniform in [base, min(max, 3 × previous)), first from [base, 3 × base) | by construction |
| `Fibonacci(base, max)` | uniform in [0, min(max, base × fib(n))) | full |

Full jitter's lower bound of zero is deliberate: it spreads a fleet better
than any scheme that keeps a floor, at the cost of an occasional immediate
retry. Decorrelated jitter keeps a floor of `base` and makes each wait depend
on the last, which breaks a fleet's lockstep faster under sustained failure;
prefer it for a busy fleet against one dependency. Fibonacci climbs more
gently than exponential. Constant is for a local pacing decision or a test,
never for a fleet.

Growth saturates at `max` without overflow at any retry number.

### Delayed errors

An error carrying a delay says when a retry may succeed. A local limiter's
refusal is exact: a token exists at that instant and nobody competes for it.
A remote `Retry-After` is advisory, may be long, and was sent to every client
at once. One rule covers both:

- The delay is honoured when it fits under `WithMaxRetryAfter`, 30s by
  default, and inside the caller's deadline. Above the cap the call is
  aborted rather than holding a goroutine and a request's memory for an
  hour.
- The delay is a floor, not the wait. The schedule's own jittered delay is
  added on top, so a fleet told the same `Retry-After` still retries spread
  out.

### The budget

`WithBudget` takes a `func(context.Context) error`, the shape
`ratelimit.Admission` returns and `breaker.WithAdmission` takes. Before each
retry the retrier asks it; an error ends the call as **budget** and `Do`
returns the last error from `fn`. The budget's own error goes to the hook and
the observers, where it can be logged, but a caller sees the dependency's
last error, which is the one that explains the failure.

The packages do not import each other.

### Composition order

Each attempt is a breaker call, so the breaker goes inside the retrier: it
gets the breaker's per-attempt timeout and bulkhead and is counted by the
breaker as a call. A breaker refusal, `ErrOpen`, `ErrProbeLimit`,
`ErrBulkhead` or `ErrStopped`, answers false to `Retryable`, so the retrier
stops on it: a retry would be refused again or take the probe slot recovery
depends on. A limiter refusal from `WithAdmission` answers true and carries
its delay, so the retrier waits for it. See [Composing](composing.md) for the
full stack, including where the inbound rate limiter and a fallback reader
sit and what breaks under the wrong nesting.

### The HTTP replay rule

`NewTransport` returns an `http.RoundTripper`. It retries a request only when
`net/http` itself would replay it on a broken connection: the method is GET,
HEAD, OPTIONS or TRACE, or one added with `WithIdempotentMethods`, or the
request carries an `Idempotency-Key` or `X-Idempotency-Key` header; and the
body is nil or `http.NoBody`, or the request has `GetBody`, which
`http.NewRequest` sets for `bytes` and `strings` readers. Every other request
goes straight to the next transport, untouched and uncounted, so a POST with
a streaming body is never replayed by accident.

PUT and DELETE are idempotent by RFC 9110 and still not in the default set,
because handlers often are not: a PUT that appends, a DELETE that fails on
the second call. Add them for an API where they are.

A retried response's body is read into memory when it is small, freeing the
connection during the wait, and drained before the next attempt otherwise.
Once retries end the last response is returned with its body open and a nil
error, whatever ended them, so the caller sees the 503 rather than an error
the retrier made up. A transport error that ends the retries is returned as
is.

Under `WithHedge` the same replay rule decides whether a request may be
hedged: a request the transport would not retry is not hedged either, and
goes through once. A losing attempt's response is drained and closed
whenever it arrives, before or after the winner has been returned to the
caller; the transport never leaks a body.

## Configuration

### Retrier

`New(name, backoff, opts...)`. The name identifies the retrier in `Stats`
and the `retrier` metric label. An option given a value that cannot be meant
makes `New` return an error wrapping `ErrInvalidOption` that lists every such
option.

| Option | Default | Effect |
|---|---|---|
| `WithMaxAttempts(n)` | 3 | Attempts per call, the first included, hedges included. 1 disables retries. |
| `WithHedge(after)` | off | Starts another attempt when the ones in flight have not answered after `after`; see Hedging. |
| `WithRetryIf(fn)` | retry every error | Predicate for errors that do not implement `Retryable`. |
| `WithMaxRetryAfter(d)` | 30s | Longest delay a `Delayed` error may ask for; longer aborts the call. |
| `WithBudget(fn)` | none | Veto asked before every retry. |
| `WithOnRetry(fn)` | none | Called before every wait with the attempt, its error and the delay. |
| `WithObserver(o)` | none | Receives every event; see Composing. |
| `WithClock(fn)` | `time.Now` | Clock for turning an HTTP-date `Retry-After` into a delay. |
| `WithSleep(fn)` | a timer | How the retrier waits; tests record and return. |
| `WithSeed(a, b)` | random | Seed for the jitter generator; `(0, 0)`, the default, seeds from the runtime. |

### `WithRetryIf`

The default retries every error that does not decide for itself. It must be
replaced for any real dependency, because it retries answers as well as
failures: a validation rejection or a not-found is retried until the cap and
fails the same way, slower, while spending budget. The predicate should
return true only for errors a fresh attempt could change: connection
failures, timeouts, 5xx. `Permanent(err)` is the alternative when the
decision is made where the error is produced.

### Transport

`NewTransport(retrier, next, opts...)`; `next` nil means
`http.DefaultTransport`.

| Option | Default | Effect |
|---|---|---|
| `WithIdempotentMethods(m...)` | GET, HEAD, OPTIONS, TRACE | Adds methods retried without an idempotency header. |
| `WithRetryResponse(fn)` | status ∈ 408, 429, 502, 503, 504 | Which responses are retried. The predicate must not consume the body. |

A 500 is not retried by default: it is as often a bug as an outage, and a bug
does not go away on retry.

## Behaviour

### Errors from `Do`

| Result | `Do` returns |
|---|---|
| success | the value and nil |
| exhausted | the last attempt's value and error |
| aborted | the last attempt's value and error, with a `Permanent` wrapper removed when it is the outermost |
| canceled | the last attempt's value and error, or `ctx.Err()` if `fn` never ran |
| budget | the last attempt's value and error |

The retrier never rewrites `fn`'s error. Which bound ended a call is in
`Stats`, the observer, the hook and the `result` label, not in the error.
[Composing](composing.md)'s "What each layer refuses" has the full
cross-package error reference, including `Permanent`, `StatusError` and
every other package's own errors.

### The cross-package contract

Two method signatures, exported here as `Retryable` and `Delayed`, let errors
from other packages steer the retrier without an import:

```go
type Retryable interface{ Retryable() bool }
type Delayed   interface{ RetryDelay() time.Duration }
```

`breaker.ErrOpen`, `ErrProbeLimit`, `ErrBulkhead` and `ErrStopped` report
`Retryable() == false`. `*ratelimit.LimitedError` reports true and its
`RetryAfter` through `RetryDelay`. `*retry.StatusError`, which the transport
creates for a retried response, reports true and the `Retry-After` header.
`Permanent(err)` wraps any error so that it reports false.

### `Stats`

`Stats` returns the name, schedule and hedge delay (`HedgeAfter`, 0 when
off), the cumulative counters `Calls`, `Attempts`, `Succeeded`, `Exhausted`,
`Aborted`, `Canceled`, `BudgetDenied`, `Hedged` and `HedgeWon`, plus
`Waited`, the total time the retrier asked to wait, hedge delays excluded.
`Calls` is the sum of the five result counters, so

```
Succeeded + Exhausted + Aborted + Canceled + BudgetDenied == Calls
```

holds at every observation, and

```
Attempts >= Calls
Hedged <= Attempts - Calls
HedgeWon <= min(Hedged, Succeeded)
```

hold exactly for a retrier with no call in flight. The counters are
independent atomics read one after another rather than a locked snapshot,
the price of an attempt path with no lock, so with calls in flight each of
the three may be off by the calls that ended while `Stats` was reading. A
`Stats` line is a diagnostic, not a ledger. `Stats.String` renders one line:

```
retry: name=catalogue backoff=exponential calls=812 attempts=901 ok=800 exhausted=5 aborted=7 canceled=0 budget=0 waited=1m2s hedged=40 hedge_won=31
```

`hedged` and `hedge_won` appear only under `WithHedge`.

### Guarantees

- `Do` returns what the last attempt returned, or `ctx.Err()` when nothing
  ran.
- The budget is asked once per retry and once per hedge, never for a first
  attempt; a hedge never starts without its consent.
- A `Delayed` error is waited for at least its delay or not at all.
- The transport never sends a request `net/http` would not replay, never
  returns a response whose body it has consumed, and closes every response
  the caller does not receive.
- The retrier itself allocates nothing on the success path without
  `WithHedge`; a closure passed to `Do` that captures variables is
  heap-allocated by the caller, see Benchmarks.

## Composing

See [Composing](composing.md) for where this package sits in the full stack —
rate limit, retry, breaker, the call — and why.

### With a circuit breaker

Put the breaker inside the retrier, so each attempt is a breaker call:

```go
v, err := r.Do(ctx, func(ctx context.Context) (V, error) {
    return b.Do(ctx, fn)
})
```

The breaker's refusals are never retried, by the `Retryable` contract. Its
`WithTimeout` bounds each attempt, which the retrier needs and cannot do
itself.

### With a rate limiter as the budget

```go
budget, err := ratelimit.New("catalogue-retries", ratelimit.GCRA(10, 20), store)
r, err := retry.New("catalogue", retry.Exponential(50*time.Millisecond, 2*time.Second),
    retry.WithBudget(ratelimit.AdmissionGlobal(budget)),
)
```

With a Redis store the budget is fleet-wide. Size it as a fraction of normal
traffic: ten retries per second against a dependency that sees a hundred
calls per second means retries add a tenth during an outage. A refused
retry ends the call as budget and returns the dependency's last error.

### With other telemetry

`WithObserver` attaches an `Observer` that receives `Started`, `Attempt`,
`Hedge`, `Wait` and `Call`, in order, on the goroutine running the call. The
Prometheus metrics are one implementation of this interface. Observers must
return promptly.

`Hedge` was added in v0.5.0 with `WithHedge`; an observer written before it
needs the method, an empty one if hedges are of no interest. It follows the
`Attempt` of an attempt a hedge started and is never delivered otherwise.
`Call` cannot say which attempt won, so hedge wins are read from `Stats`.
`Wait`'s first argument is the number of attempts that have failed so far,
which is the retry number without hedging.

## Observability

### Metrics

`retry.Register(reg)` registers the package's metrics once, under
`<namespace>_retry_` with namespace `go` by default;
`retry.WithNamespace("inventory")` changes it. Every retrier reports under
its name as the `retrier` label. Its series exist from the moment `New`
returns and persist for the life of the process.

| Metric | Type | Labels |
|---|---|---|
| `go_retry_calls_total` | counter | `retrier`, `backoff`, `result` ∈ success, exhausted, aborted, canceled, budget |
| `go_retry_attempts_total` | counter, first attempts included | `retrier`, `backoff` |
| `go_retry_wait_seconds_total` | counter | `retrier`, `backoff` |
| `go_retry_hedges_total` | counter, included in attempts | `retrier`, `backoff` |
| `go_retry_hedge_wins_total` | counter | `retrier`, `backoff` |

Common queries:

```promql
(rate(go_retry_attempts_total[5m]) - rate(go_retry_calls_total[5m]))
  / rate(go_retry_calls_total[5m])                                     # retries per call, hedges included
rate(go_retry_calls_total{result="exhausted"}[5m])
  / rate(go_retry_calls_total[5m])                                     # share of calls that gave up
rate(go_retry_calls_total{result="budget"}[5m])                        # retries and hedges the budget refused
rate(go_retry_wait_seconds_total[5m])                                  # goroutine-seconds per second spent waiting
rate(go_retry_hedge_wins_total[5m]) / rate(go_retry_hedges_total[5m])  # hedges that were worth it; low means after is too short
```

### Alerting

`contrib/prometheus/alerts.yaml` contains the rules below in a `retry`
group.

| Rule | Severity | Condition |
|---|---|---|
| `retry:retries_per_call` (recording) | | retries ÷ calls, per retrier |
| `RetryAmplifying` | ticket | `retry:retries_per_call > 0.5` for 10m |
| `RetryExhausting` | page | exhausted calls exceed a tenth of all calls for 5m |

Retries per call above one half means most calls are being retried: the
dependency is degraded and the retries are now a significant share of its
load, or `WithRetryIf` is retrying answers. It is a ticket because the cap
and the budget bound the damage. Exhaustion is the user-facing symptom: calls
that failed after every attempt, which is the dependency being down for
longer than the schedule spans. A budget denial rate that is not zero during
an incident is the budget doing its job and needs no alert.

### Dashboard

`contrib/grafana/keel.json` includes a collapsed row for retriers: retries
per call, calls by result with hedges started as a dashed series, and time
spent waiting.

## Testing

Schedules are tested as pure functions with explicit draws, covering each
shape, the cap, and saturation at a huge retry number. The retrier runs on a
fake clock with a recording sleep that returns at once: every bound, the
classification order, the delay floor and cap, the budget, context
cancellation before an attempt and during a wait, the `Stats` identities
under concurrency, the observer sequence, and seed reproducibility. The
composition tests import `breaker` and `ratelimit` from the test package
only: a tripped breaker's refusal is not retried, a limiter's refusal is
waited for, and a limiter as budget denies past its burst. The transport is
tested against `httptest`: the status set, the method rule with both
idempotency headers and the opt-in, body replay, pass-through of a
non-replayable body, `Retry-After` in both forms, the cap, release of retried
bodies, transport errors, and the last response being returned open.

Hedging is tested with a gated `fn` whose attempts block until the test
releases them and a hedge timer that fires only when the test says so, so
every ordering in the rules above is reproduced exactly: a hedge
overtaking a slow attempt and the loser's context being cancelled, a
hedge's failure not ending the call, a permanent failure aborting it, every
attempt failing into the retry path, the budget before each hedge and its
refusal holding, the cap, the caller cancelling, a panic reaching the
caller, and the internal discard hook seeing every superseded result and
nothing else. A chaos test with random latencies, outcomes and
cancellations checks the `Stats` identities under the race detector. The
transport is tested with a stalled first request, a hedge answering 503
with a body too large to buffer, and the stalled request winning: the 503
body must end up closed and the winner's open. A second test exhausts the
cap on such 503s and reads the returned body to the end.

## Benchmarks

`go test -bench . -benchmem ./retry/` on an Apple M5 Max (18 cores), Go
1.27.1:

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `Do`, success first attempt | 6.5 | 0 | 0 |
| `Do`, success first attempt, `WithHedge` set | 1380 | 1176 | 17 |
| `Do`, two retries, no-op sleep | 128 | 88 | 6 |

The success path is a closure call and three atomic increments. The retry
path adds classification through `errors.As`, the schedule and the
observers; the allocations are the interface conversions in classification
plus the benchmark's own closure and the counter it captures. That closure
is heap-allocated since v0.5.0: `Do` may hand `fn` to other goroutines under
`WithHedge`, so a closure that captures variables escapes whether or not
hedging is on. A function value that captures nothing still costs nothing.
The hedged path pays for the attempt goroutine, its context, the results
channel and the timer goroutine on every call, even when no hedge starts. A
real retry waits for the schedule, which dwarfs everything above.
