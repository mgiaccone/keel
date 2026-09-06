# Circuit Breaker

Package `github.com/mgiaccone/keel/breaker`. A circuit breaker for a single
dependency, tripped by consecutive failures or by an error rate over a
window, with a concurrency limit (bulkhead), a per-call timeout, a recovery
ramp and a pluggable admission veto.

## Quick start

```go
b, err := breaker.New("db-fallback",
    breaker.WithIsFailure(func(err error) bool {
        // The database answering "no such row" is a successful call.
        return err != nil && !errors.Is(err, sql.ErrNoRows) && !errors.Is(err, context.Canceled)
    }),
    breaker.WithTimeout(2*time.Second),
    breaker.WithMaxInFlight(16),
    breaker.WithRecoveryRamp(2, 8),
)
if err != nil {
    return err // wraps breaker.ErrInvalidOption and lists every invalid option
}

row, err := b.Do(ctx, func(ctx context.Context) (Row, error) {
    return db.Get(ctx, key)
})
switch {
case errors.Is(err, breaker.ErrOpen), errors.Is(err, breaker.ErrProbeLimit), errors.Is(err, breaker.ErrBulkhead):
    // The call did not run. Return an "unavailable" error; do not retry.
}
```

`breaker/example_test.go` contains a complete repository that reads from a
cache and falls back to a database through a breaker, with verified output.

## When to use it

A breaker refuses every call to a dependency that has shown itself down,
until a probe succeeds. Two rules decide "down", and the circuit opens when
either fires.

Consecutive failures suit a path where call volume is low and errors are
otherwise rare: a fallback taken only on a cache miss, a call made only on a
slow path, a dependency with a small number of callers. There a run of five
failures means the dependency is down.

On a high-volume path where scattered errors are normal, a run of five is a
coincidence rather than an outage, and a real 30% error rate rarely produces
five in a row. `WithErrorRate` trips on the share of calls that failed over a
trailing window, once enough calls have been seen for the share to mean
something; the consecutive rule is then set high, or switched off with
`WithFailureThreshold(0)`.

Bounding the requests a service itself accepts, by an adaptive concurrency
limit with load shedding, is inbound admission and outside this package.

The breaker's bulkhead addresses a different failure: a dependency that is
slow but not failing. Slow calls never trip the circuit, but each one holds a
goroutine, a connection and a request's memory. The bulkhead bounds how many
calls can be in flight at once. The two limits are independent and both are
usually wanted.

Bounding how many calls *start* per second is rate limiting, which is the
[`ratelimit`](ratelimit.md) package.

## How it works

### States

| State | Calls are | Leaves when |
|---|---|---|
| closed | admitted; outcomes counted | `FailureThreshold` consecutive failures, or the error-rate rule → open |
| open | refused with `ErrOpen` | the open interval has elapsed → half-open |
| half-open | admitted up to `MaxProbes` at a time; further calls refused with `ErrProbeLimit` | `SuccessThreshold` consecutive successes → closed; one failure → open |

A success while closed resets the failure count. A failure while half-open
reopens the circuit regardless of how many successes preceded it.

### The error-rate window

`WithErrorRate(threshold, window, minCalls)` adds the second trip rule. Every
call settled while closed, except a cancellation, enters a ring of
`WithErrorRateBuckets` buckets (default 10) that together span `window`; each
bucket counts the successes and failures that landed in it. After each
outcome, if the ring holds at least `minCalls` calls and the share that
failed is at least `threshold`, the circuit opens. The consecutive rule is
checked first on the same outcome; either opens the circuit.

The ring is rotated lazily. Buckets are aligned to multiples of
`window/buckets` on the clock. When an outcome or a `Stats` call arrives, the
buckets that have left the window are zeroed, and a gap of a whole window
empties the ring; nothing runs between calls. An outcome therefore leaves
the window together with its bucket, between `window − window/buckets` and
`window` after it was recorded, and the rate in `Stats` decays to zero on an
idle breaker as inspections roll the outcomes out.

Only closed-state outcomes enter the window: half-open keeps its probe rules,
and a stale outcome (see Generations) is counted in the totals but not in the
window. The ring is cleared when the circuit closes, so a recovery starts
with no history and a second trip needs `minCalls` fresh calls. While open or
half-open the ring keeps what it had, decaying, which is what
`Stats.ErrorRate` shows during an outage.

Worked example: `WithErrorRate(0.5, 10*time.Second, 20)` with the default ten
buckets, so each bucket spans one second. At 5 calls per second the window
holds about 50 calls; the circuit opens once 20 or more of the last ten
seconds' calls have settled and half of them failed, and 3 failures among 47
successes move the rate to 0.06 and nothing else. At 1 call per second the
window never reaches 20 calls and the rule never fires; the consecutive rule
is what protects that path.

### The open interval

The interval starts at `OpenBase` on the first trip and doubles on each
consecutive trip (a trip from half-open that did not lead to a close), capped
at `OpenMax`. It is then multiplied by a random factor in
[1 − `OpenJitter`, 1 + `OpenJitter`]. The value is drawn once when the circuit
opens and fixed for that open period. The consecutive-trip count resets when
the circuit closes.

The transition from open to half-open is evaluated when the next call or
`Stats` request arrives and the deadline has passed, not by a timer. A breaker
with no traffic stays open indefinitely; there is nothing to recover for.

### The state goroutine

All mutable state belongs to one goroutine started by `New`. A call to `Do`
sends an admission request over an unbuffered channel and receives the
decision on a per-call reply channel; after `fn` returns, it sends the outcome
and waits for the goroutine to acknowledge that the outcome has been applied.
`Stats` and `State` are answered by the same goroutine. The core has no
mutexes, atomics or timers; the goroutine reads an injectable clock when a
message arrives. `WithTimeout` adds a timer per call through
`context.WithTimeout`, and the Prometheus counters use atomics.

The acknowledgement on settle gives `Do` its main guarantee: when `Do`
returns, the outcome has been applied. A `Stats` call made afterwards reflects
it, any transition it caused has already been reported, and the open deadline
it may have started was computed against the clock as it read before `Do`
returned.

### Generations

Every transition increments a generation counter, and every admitted call
carries the generation it was admitted under. An outcome arriving with an old
generation is counted in the totals but does not drive the state machine. A
slow call admitted while closed that succeeds after the circuit has tripped
does not close it; a stale probe does not free a probe slot.

### Admission order

For each call, the checks run in this order, and the first that fails
determines the error:

1. Circuit state: open → `ErrOpen`; half-open at `MaxProbes` → `ErrProbeLimit`.
2. Bulkhead: at the in-flight cap → `ErrBulkhead`.
3. Admission veto (`WithAdmission`), run on the caller's goroutine: its error
   is returned as is, and the call is recorded as denied.
4. Timeout (`WithTimeout`): the context passed to `fn` is derived from the
   caller's with the configured deadline.
5. `fn` runs.

The order puts the most informative refusal first. A call the circuit refuses
never reaches the veto, so it consumes no rate-limit quota; a call the veto
refuses never runs.

### Lifecycle

`New` starts the goroutine. Nothing needs to be called to stop it: the
goroutine holds only the breaker's internal state, never the value returned
by `New`, and a runtime cleanup on that value stops the goroutine once the
value is unreachable. A breaker created at startup runs for the life of the
process; one created for a shorter purpose is dropped like any other value.
`Stop` exists for deterministic teardown, for example in tests, or to remove
the breaker's metric series immediately rather than after the next garbage
collection. After `Stop`, `Do` returns `ErrStopped` and `Stats` the zero
value.

Either form of teardown waits for the goroutine to exit, and the goroutine
runs the state-change hook and the observers. One that blocks delays `Stop`
and, for a dropped breaker, holds up the runtime's cleanup goroutine, which
every cleanup in the process shares.

## Configuration

`New` takes the breaker's name and functional options. The name identifies
the dependency in `Stats`, the log line and the `dependency` metric label.
Each name should belong to one live breaker at a time: two with the same name
merge their metrics, and stopping either removes the series of both. An
option given a value that cannot be meant makes `New` return an error
wrapping `ErrInvalidOption` that lists every such option.

| Option | Default | Effect |
|---|---|---|
| `WithFailureThreshold(n)` | 5 | Consecutive failures while closed that open the circuit; 0 switches the rule off, with `WithErrorRate` set. |
| `WithErrorRate(threshold, window, minCalls)` | off | Opens the circuit when at least `minCalls` calls settled while closed in the trailing `window` and a share of `threshold` or more failed. |
| `WithErrorRateBuckets(n)` | 10 | Buckets the window is split into; the rate moves in steps of `window/n`. |
| `WithSuccessThreshold(n)` | 2 | Consecutive successful probes while half-open that close it. |
| `WithMaxProbes(n)` | 1 | Probes in flight at once while half-open. |
| `WithOpenInterval(base, max)` | 5s, 60s | First open interval and its cap. |
| `WithOpenJitter(f)` | 0.2 | Random factor applied to each open interval; 0 disables. |
| `WithTimeout(d)` | none | Deadline for `fn`, derived from the caller's context. |
| `WithMaxInFlight(n)` | unlimited | Bulkhead: calls in flight at once, in any state. |
| `WithAdaptiveInFlight(min, max, target)` | off | Bulkhead whose cap moves between `min` and `max` by AIMD. Exclusive with `WithMaxInFlight`. |
| `WithRecoveryRamp(start, end)` | off | After a close, the cap starts at `start` and grows by one per success until `end`. |
| `WithAdmission(fn)` | none | Veto run after the circuit admits a call and before `fn`. |
| `WithIsFailure(fn)` | see below | Which errors count as failures. |
| `WithOnStateChange(fn)` | none | Called on every transition, on the state goroutine. |
| `WithObserver(o)` | none | Receives every event; see Composing. |
| `WithClock(fn)` | `time.Now` | Clock for open deadlines and call durations. |
| `WithSeed(a, b)` | random | Seed for the jitter generator; `(0, 0)`, the default, seeds from the runtime. |

### `WithIsFailure`

The default classifies every non-nil error as a failure except
`context.Canceled`. The predicate receives every admitted call's error, nil
included, and must return false for nil. It must be replaced for any real
dependency, because the default counts errors that are correct answers:
`sql.ErrNoRows`, an HTTP 404, a validation rejection. Those mean the
dependency answered; counting them as failures opens the circuit on healthy
traffic, and no other option compensates. The predicate should return true
only for errors that indicate the dependency itself is unhealthy: timeouts,
connection failures, 5xx responses.

`context.DeadlineExceeded` is a failure by default. A dependency too slow to
answer within its deadline is what the breaker exists to detect.

An error for which the predicate returns false and which wraps
`context.Canceled` is neutral: it is counted as canceled, neither extends nor
resets the failure run, and frees a probe slot without counting as a probe
result. A caller giving up says nothing about the dependency.

### `WithSuccessThreshold`

The default is 2. With 1, a single successful probe against a dependency that
is still failing restores full traffic; the traffic fails, the circuit
reopens with a doubled interval, and the sequence repeats. Requiring two
consecutive successes prevents that oscillation at the cost of one extra
probe per recovery.

### `WithErrorRate`

Off by default; "The error-rate window" above has the mechanics. `threshold`
is in (0, 1], `window` is positive, `minCalls` is at least 1, and `New`
rejects a window too short to give each bucket a nanosecond. Nothing else is
rejected: a short window with a low `minCalls` can be meant, in a test.

Sizing:

- `window`: about ten times the p99 latency of the call, so one slow burst
  cannot fill the window on its own and the rate reflects many independent
  calls.
- `minCalls`: about the calls one window sees at the lowest traffic you still
  want the rule to protect. Below it the rule is silent, which is the point:
  two failures out of three at night are not an outage.
- `threshold`: 0.3 to 0.5 for a dependency whose scattered errors are normal,
  higher only if `IsFailure` is already strict. At 1 the rule fires only when
  every call in the window failed.
- `WithErrorRateBuckets`: rarely worth changing. With 10 buckets the rate
  moves in tenths of the window, finer than the open interval that follows a
  trip. More buckets suit a window that is long relative to how quickly the
  rate should fall after a burst of failures.

`WithFailureThreshold(0)` switches the consecutive rule off for a path that
should trip only on the rate; it is an error without `WithErrorRate`.
`ExampleWithErrorRate` in `breaker/example_test.go` shows a path tripping on
a 40% rate with the consecutive rule off.

### Bulkhead

`WithMaxInFlight(n)` refuses calls with `ErrBulkhead` when `n` are already in
flight. Nothing queues. The refusal is not a failure and does not affect the
circuit.

The cap protects the caller's resources, not the dependency. Size it from the
path's own numbers: in-flight ≈ rate × latency, taken at the p99, with a
multiplier of three to five so the cap is not reached in normal operation.
Keep it at or below the connection pool size, otherwise calls queue inside
the driver instead. The worst case is `n` × `WithTimeout` of dependency time
per instance. The `go_breaker_in_flight` gauge shows the actual peak; a
reasonable procedure is to run without a cap, observe the peak over a week,
and set the cap at several times that.

`WithAdaptiveInFlight(min, max, target)` moves the cap instead of fixing it:
each successful call that completes within `target` raises it by one, each
failure or slower call halves it, cancellations leave it unchanged, and it
stays within [`min`, `max`]. The time measured is `fn`'s own: it starts once
the admission veto, if any, has returned, so a slow limiter never reads as a
slow backend. The cap starts at `max`. This is additive
increase, multiplicative decrease, the rule TCP uses for congestion control;
its state is one integer and its history is visible on the
`go_breaker_in_flight_limit` gauge. It needs enough calls to distinguish a
slow dependency from a single slow call. On a path with a few calls per
second, one slow query halves the cap, so prefer the static cap there.

### Recovery ramp

When the circuit closes, every caller that was refused during the open period
may retry at once, against a dependency that has just handled one probe at a
time. `WithRecoveryRamp(start, end)` sets the in-flight cap to `start` on
close and raises it by one per successful call until it reaches `end`, after
which the steady cap applies (`WithMaxInFlight`, or unlimited). Failures do
not move the ramp; enough of them reopen the circuit, and the ramp restarts
on the next close. Nothing happens at startup.

Under `WithAdaptiveInFlight`, the ramp only sets the cap at close; AIMD grows
it from there, and the ramp is over when the cap reaches `end` or AIMD lowers
it.

`Stats.Ramping` and the `go_breaker_ramping` gauge report a ramp in progress.
A low cap during a ramp is the intended recovery; the same cap under AIMD
means the dependency is slow. The bulkhead alert excludes ramps for this
reason.

### `WithOpenJitter`

Every instance of a service trips on the same outage within moments of each
other. Without jitter they finish the same open interval together and probe
together; the recovering dependency receives a synchronised burst, fails, and
every instance reopens for twice the interval. The default of 0.2 spreads the
probes over ±20% of the interval.

### `WithTimeout`

The breaker assumes every admitted call eventually settles. A call that hangs
while the circuit is half-open holds the only probe slot and nothing can
free it. `WithTimeout` derives the context passed to `fn` from the caller's
with an added deadline, so a hung dependency becomes
`context.DeadlineExceeded`, which is a failure, and the circuit trips. The
timeout starts once the call is admitted and the admission veto, if any, has
returned; the veto runs against the caller's context. `fn` must honour its
context; the breaker cannot abort `fn`, and `Do` returns only when `fn` does.

## Behaviour

### Errors from `Do`

| Error | Meaning | `fn` ran |
|---|---|---|
| `ErrOpen` | circuit open | no |
| `ErrProbeLimit` | half-open, `MaxProbes` probes in flight | no |
| `ErrBulkhead` | in-flight cap reached | no |
| the veto's error | `WithAdmission` refused | no |
| `ctx.Err()` | caller's context done before admission | no |
| `ErrStopped` | after `Stop` | no |
| `fn`'s error | returned unchanged | yes |

The breaker never rewrites `fn`'s error. Every other error means the call did
not run and has no side effects. Callers should translate the three refusals
into their own "unavailable" error at the boundary, return it to their
clients as `503 Service Unavailable` with a `Retry-After` (for `ErrOpen`,
`Stats.NextProbeIn`), and not retry: a retry is refused again or takes the
probe slot recovery depends on. The refusals answer false to the `Retryable`
contract, so a [`retry`](retry.md) retrier stops on them without either
package importing the other.

### Results

Every call that reaches the breaker ends in exactly one result:

| Result | `fn` ran | Effect on the circuit |
|---|---|---|
| success | yes | resets the failure run and enters the error-rate window; counts toward closing while half-open |
| failure | yes | extends the failure run and enters the error-rate window; reopens while half-open |
| canceled | yes | none |
| rejected | no | none (`ErrOpen`, `ErrProbeLimit`) |
| shed | no | none (`ErrBulkhead`) |
| denied | no | none (admission veto) |

### `Stats`

`Stats` returns a snapshot with the state, `ConsecutiveTrips`, `NextProbeIn`
(zero unless open), `InFlight`, `InFlightLimit` (0 when unlimited),
`Ramping`, the error-rate window as `Window` (0 when the rule is off),
`WindowCalls` and `ErrorRate`, and the cumulative counters `Calls`,
`Rejected`, `Shed`, `Denied`, `Admitted`, `Successes`, `Failures`, `Canceled`
and `Trips`. Three identities hold at every observation:

```
Admitted + Rejected + Shed + Denied + InFlight == Calls
Successes + Failures + Canceled == Admitted
WindowCalls <= Successes + Failures
```

`Admitted` counts calls whose `fn` has returned; a running call is in
`InFlight`. Every cumulative counter is monotonic. `WindowCalls` and
`ErrorRate` are computed against the clock at inspection, so they fall on an
idle breaker as the window rolls.

`Stats.String` renders one line:

```
breaker: name=db-fallback state=open trips=3(consecutive=2) calls=812 rejected=41 shed=2 denied=0 ok=760 fail=9 canceled=0 in_flight=3/64 error_rate=0.12(120) next_probe_in=23s
```

The cap after the slash appears only when one is set, `(ramping)` follows it
during a ramp, `error_rate` with `WindowCalls` in parentheses appears only
under `WithErrorRate`, and `next_probe_in` appears only while open.

### Guarantees

- When `Do` returns, the outcome has been applied.
- A stale outcome is counted but never drives the state machine.
- Open → half-open is evaluated lazily, and so is the error-rate window; no
  goroutine wakes for an idle breaker.
- `Do` allocates nothing on the admitted or refused paths (a per-call channel
  is recycled through a free list). `WithTimeout` adds the allocations of
  `context.WithTimeout`.
- A panic in `fn` is recorded as a failure, frees the call's slots, and
  propagates.

## Composing

### With a rate limiter

`WithAdmission` takes a `func(context.Context) error`. `ratelimit.Admission`
returns one, so a limiter is attached with:

```go
b, err := breaker.New("db-fallback",
    breaker.WithAdmission(ratelimit.Admission(limiter, "")),
)
```

The veto runs after the circuit has admitted the call, so a call the circuit
refuses consumes no quota, and a call the limiter refuses is recorded by the
breaker as denied. The two packages do not import each other.

### With a retrier

The breaker goes inside the retrier, so each attempt is a breaker call:

```go
v, err := r.Do(ctx, func(ctx context.Context) (V, error) {
    return b.Do(ctx, fn)
})
```

`ErrOpen`, `ErrProbeLimit`, `ErrBulkhead` and `ErrStopped` implement
`Retryable() bool` and answer false, so the retrier stops on them. The
breaker's `WithTimeout` bounds each attempt, which the retrier needs and
cannot do itself. See [`retry`](retry.md).

### With other telemetry

`WithObserver` attaches an `Observer` that receives `Started`, `Call(Result)`,
`Transition(from, to, consecutiveTrips, openUntil)`, `Load(inFlight, limit,
ramping)`, `Window(rate, calls)` and `Stopped`, in order, on the state
goroutine. The Prometheus metrics are one implementation of this interface.
Observers must return promptly and must not call back into the breaker.

`Window` was added in v0.4.0 with `WithErrorRate`; an observer written
before it needs the method, an empty one if the window is of no interest. It
is delivered whenever the window changes: after an outcome enters it, after
an inspection rolls outcomes out of it, and as `(0, 0)` when a close clears
it. Without `WithErrorRate` it is never delivered.

## Observability

### Metrics

`breaker.Register(reg)` registers the package's metrics once, under
`<namespace>_breaker_` with namespace `go` by default;
`breaker.WithNamespace("inventory")` changes it. Every breaker reports under
its name as the `dependency` label. A breaker's series exist from the moment
`New` returns and are removed when it is stopped or collected.

| Metric | Type | Labels |
|---|---|---|
| `go_breaker_state` | gauge, one-hot | `dependency`, `state` ∈ closed, open, half-open |
| `go_breaker_open_until_timestamp_seconds` | gauge, Unix time, 0 unless open | `dependency` |
| `go_breaker_consecutive_trips` | gauge | `dependency` |
| `go_breaker_in_flight` | gauge | `dependency` |
| `go_breaker_in_flight_limit` | gauge, +Inf when unlimited | `dependency` |
| `go_breaker_ramping` | gauge, 0 or 1 | `dependency` |
| `go_breaker_error_rate` | gauge, 0 to 1; 0 when the rule is off | `dependency` |
| `go_breaker_window_calls` | gauge | `dependency` |
| `go_breaker_calls_total` | counter | `dependency`, `result` ∈ success, failure, canceled, rejected, shed, denied |
| `go_breaker_trips_total` | counter | `dependency` |

The state gauge is one-hot: exactly one of the three `state` series is 1. This
allows `sum by (state)` across a fleet and reads as text in queries.

Because open → half-open is evaluated lazily, an idle breaker reports
`state="open"` past its deadline. `go_breaker_open_until_timestamp_seconds -
time()` is the time until the next probe would be admitted; a negative value
means the deadline has passed and the next call will move the circuit to
half-open.

Common queries:

```promql
go_breaker_state{state="open"} == 1                         # open now
rate(go_breaker_calls_total{result=~"rejected|shed"}[5m])   # calls refused per second
go_breaker_in_flight / go_breaker_in_flight_limit           # bulkhead utilisation
go_breaker_error_rate > 0.3                                 # the window's failed share, per instance
increase(go_breaker_trips_total[10m])                       # trips in the last ten minutes
```

The window gauges follow `Stats`: they are updated when an outcome enters the
window and when an inspection rolls outcomes out, so on an idle instance they
hold their last value until something reads `Stats`, the way the state gauge
holds `open` past its deadline.

### Alerting

`contrib/prometheus/alerts.yaml` contains the rules below in a `breaker`
group, for `promtool check rules`. They aggregate across instances so that
one instance's state does not page on its own, and every alert carries the
`dependency` label for routing.

| Rule | Severity | Condition |
|---|---|---|
| `breaker:open_fraction` (recording) | | instances open ÷ instances, per dependency |
| `BreakerOpenFleetWide` | page | `breaker:open_fraction > 0.5` for 2m |
| `BreakerFlapping` | ticket | more than 5 trips in 30m |
| `BreakerSheddingMajority` | page | refused calls exceed half of all calls for 5m |
| `BreakerBulkheadSaturated` | ticket | in-flight at the cap for 5m, outside a recovery ramp |
| `BreakerMetricsAbsent` | ticket | no series for an expected dependency for 10m |

`BreakerOpenFleetWide` uses `for: 2m` because the first open interval is 5s
by default and a single trip that recovers on its first probe should not
page. `BreakerFlapping` indicates either a dependency that is partially
recovering or an `IsFailure` predicate that counts correct answers as
failures. `BreakerMetricsAbsent` catches a missing `Register` call or a
breaker that was never created, both of which otherwise look like a healthy
dependency.

`go_breaker_consecutive_trips`, `go_breaker_open_until_timestamp_seconds`
and `go_breaker_error_rate` are for dashboards rather than alerts; a negative
countdown is the lazy transition, not a fault, and a trip by the rate rule is
a trip like any other, which `BreakerOpenFleetWide` covers.

A CPU-throttled container hits its own timeouts, and `DeadlineExceeded` is a
failure, so the circuit opens because of the container rather than the
dependency. Plotting `container_cpu_cfs_throttled_periods_total` next to
`breaker:open_fraction` distinguishes the two.

When `BreakerOpenFleetWide` fires: check the dependency's own health, then
read one instance's `Stats` line from its health endpoint, which gives the
current state, how many times it has tripped, and when the next probe is due.

### Dashboard

`contrib/grafana/keel.json` is an importable Grafana dashboard covering these
metrics, provided as a starting point. The state gauge is best shown as a
state timeline after collapsing the one-hot series into one ordinal:
`go_breaker_state{state="half-open"} * 1 + go_breaker_state{state="open"} * 2`.
The "Error rate" panel plots the worst instance's `go_breaker_error_rate`
per dependency; a flat zero is a healthy window or a breaker without the
rule.

## Testing

Beyond one test per documented property, the suite has three layers:

- **A reference model.** `TestModel` drives the breaker and an independent
  single-threaded implementation of the documented rules through the same
  random sequences of admissions, out-of-order settles, admission vetoes and
  clock advances, comparing `Stats` after every step. Half the seeds enable
  the error-rate rule; the model keeps a list of outcomes with their bucket
  slots where the breaker keeps a ring, so the two formulations must agree
  on `WindowCalls` and `ErrorRate` at every inspection. A divergence prints
  the seed.
- **Chaos with invariants.** `TestChaosInvariants` runs many goroutines with
  random outcomes, cancellations, vetoes, clock advances and inspections,
  checks the `Stats` identities and counter monotonicity at every observation,
  and afterwards checks that every transition the hook saw was a legal edge.
- **Regression pins.** The settle-before-apply clock race, zero allocations
  per call, probe-slot release on panic, goroutine exit for a dropped breaker,
  and free-list overflow each have a dedicated test.

Every blocking wait in the suite has a deadline, so a deadlock fails at a
named line. `-short` reduces the model and chaos iterations. The tests use an
injected clock and a fixed jitter seed; the only synchronisation primitive in
the test code is the fake clock's atomic.

## Benchmarks

`go test -bench . -benchmem ./breaker/` on an Apple M5 Max (18 cores), Go
1.27.1:

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| baseline, `fn` called directly | 1.6 | 0 | 0 |
| `Do`, closed, serial, metrics fed | 1004 | 0 | 0 |
| `Do`, closed, serial, `WithErrorRate` set | 1077 | 0 | 0 |
| `Do`, closed, serial, `WithTimeout` set | 1287 | 272 | 4 |
| `Do`, open (refused) | 446 | 0 | 0 |
| `Do`, closed, 18 goroutines | 3608 | 0 | 0 |
| `Stats` | 555 | 272 | 2 |

A `Do` on the admitted path costs two channel round trips, one for admission
and one for the acknowledged settle; the refused path costs one. The
error-rate rule adds a clock read and a ring update per settle. Channel
handoff time is dominated by scheduler wakeups and is roughly double on a
single core. The parallel figure is per call across all goroutines: a single
state goroutine serialises every admission, so contention appears as latency.
