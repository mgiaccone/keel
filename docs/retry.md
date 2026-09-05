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
    retry.WithBudget(ratelimit.Admission(budget, "")),   // at most so many retries per second, fleet-wide with Redis
    retry.WithRetryIf(func(err error) bool {             // a not-found is an answer, not a failure
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

The retrier does not hedge. Sending a duplicate request before the first has
failed is a different tool with different failure modes.

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

The stack is retry → breaker → limiter. Each attempt is a breaker call, so it
gets the breaker's per-attempt timeout and bulkhead and is counted by the
breaker as a call. A breaker refusal, `ErrOpen`, `ErrProbeLimit`,
`ErrBulkhead` or `ErrStopped`, answers false to `Retryable`, so the retrier
stops on it: a retry would be refused again or take the probe slot recovery
depends on. A limiter refusal from `WithAdmission` answers true and carries
its delay, so the retrier waits for it.

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

## Configuration

### Retrier

`New(name, backoff, opts...)`. The name identifies the retrier in `Stats`
and the `retrier` metric label. An option given a value that cannot be meant
makes `New` return an error wrapping `ErrInvalidOption` that lists every such
option.

| Option | Default | Effect |
|---|---|---|
| `WithMaxAttempts(n)` | 3 | Attempts per call, the first included. 1 disables retries. |
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

`Stats` returns the name, schedule, and the cumulative counters `Calls`,
`Attempts`, `Succeeded`, `Exhausted`, `Aborted`, `Canceled` and
`BudgetDenied`, plus `Waited`, the total time the retrier asked to wait.
`Calls` is the sum of the five result counters, so

```
Succeeded + Exhausted + Aborted + Canceled + BudgetDenied == Calls
Attempts >= Calls
```

hold at every observation. `Stats.String` renders one line:

```
retry: name=catalogue backoff=exponential calls=812 attempts=901 ok=800 exhausted=5 aborted=7 canceled=0 budget=0 waited=1m2s
```

### Guarantees

- `Do` returns what the last attempt returned, or `ctx.Err()` when nothing
  ran.
- The budget is asked once per retry, never for a first attempt.
- A `Delayed` error is waited for at least its delay or not at all.
- The transport never sends a request `net/http` would not replay, and never
  returns a response whose body it has consumed.
- The success path allocates nothing.

## Composing

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
    retry.WithBudget(ratelimit.Admission(budget, "")),
)
```

With a Redis store the budget is fleet-wide. Size it as a fraction of normal
traffic: ten retries per second against a dependency that sees a hundred
calls per second means retries add a tenth during an outage. A refused
retry ends the call as budget and returns the dependency's last error.

### With other telemetry

`WithObserver` attaches an `Observer` that receives `Started`, `Attempt`,
`Wait` and `Call`, in order, on the goroutine running the call. The
Prometheus metrics are one implementation of this interface. Observers must
return promptly.

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

Common queries:

```promql
(rate(go_retry_attempts_total[5m]) - rate(go_retry_calls_total[5m]))
  / rate(go_retry_calls_total[5m])                                     # retries per call
rate(go_retry_calls_total{result="exhausted"}[5m])
  / rate(go_retry_calls_total[5m])                                     # share of calls that gave up
rate(go_retry_calls_total{result="budget"}[5m])                        # retries the budget refused
rate(go_retry_wait_seconds_total[5m])                                  # goroutine-seconds per second spent waiting
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
per call, calls by result, and time spent waiting.

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

## Benchmarks

`go test -bench . -benchmem ./retry/` on an Apple M5 Max (18 cores), Go
1.27.1:

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `Do`, success first attempt | 6.3 | 0 | 0 |
| `Do`, two retries, no-op sleep | 114 | 64 | 4 |

The success path is a closure call and three atomic increments. The retry
path adds classification through `errors.As`, the schedule and the
observers; the allocations are the benchmark's own closure and the
interface conversions in classification. A real retry waits for the
schedule, which dwarfs everything above.
