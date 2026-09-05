# breaker

The circuit breaker package: `github.com/mgiaccone/keel/breaker`.

```go
b, err := breaker.New("db-fallback",
    breaker.WithIsFailure(func(err error) bool {
        // "the backend answered, and the answer was no" is a success
        return err != nil && !errors.Is(err, sql.ErrNoRows) && !errors.Is(err, context.Canceled)
    }),
    breaker.WithOnStateChange(func(from, to breaker.State) {
        slog.Warn("circuit state change", "dependency", "db-fallback", "from", from, "to", to)
    }),
    breaker.WithTimeout(2*time.Second),  // a hung call must become a failure, not a held probe slot
    breaker.WithMaxInFlight(16),         // a slow backend must not absorb every goroutine
    breaker.WithRecoveryRamp(2, 8),      // after a recovery, let traffic back in gradually
)
if err != nil {
    return err // wraps breaker.ErrInvalidOption; lists every bad option
}

row, err := b.Do(ctx, func(ctx context.Context) (Row, error) {
    return db.Get(ctx, key)
})
if errors.Is(err, breaker.ErrOpen) || errors.Is(err, breaker.ErrProbeLimit) || errors.Is(err, breaker.ErrBulkhead) {
    // fail fast, do not retry
}
```

See `breaker/example_test.go` for a complete cache-then-database repository.

## When a breaker is the right tool

A breaker counts consecutive failures and, past a threshold, rejects every
call outright until a probe succeeds. That is a blunt instrument, and it is
the right one under two conditions:

- **The guarded path is low volume by construction.** The canonical case is a
  fallback: a call made only on a cache miss, only when a primary is
  unreachable, only for the long tail. At low request rates
  consecutive-failure counting is well behaved; a handful of consecutive
  failures really does mean the backend is down, and the circuit does not
  flip-flop the way it would on a high-rps path where scattered errors are
  normal.
- **Legibility matters.** "Circuit open on db-fallback, next probe in
  23s" is something an on-call engineer acts on immediately. `Stats.String`
  is designed to be that line, and `OnStateChange` is the hook for logging
  every transition.

Use **adaptive throttling** (client-side rate limiting keyed on the observed
success ratio) instead when:

- the path carries high volume, so an error-rate window has enough samples
  to be meaningful and a binary open/closed switch would be either too
  trigger-happy or too slow; or
- the backend degrades partially rather than failing outright, and you want to
  shed a *proportion* of load rather than all of it.

The two compose: throttle the main read path, break the fallback path.

## Knobs

`New` takes the breaker's name first: the dependency it guards, which appears
in `Stats`, the log line and the Prometheus `dependency` label. Every other
setting is a functional option. A value that cannot be
meant (a threshold below 1, a negative interval) makes `New` return an error
wrapping `ErrInvalidOption` that names every offending option, so
misconfiguration is caught at construction rather than at the first outage.

| Option | Default | Meaning |
|---|---|---|
| `WithFailureThreshold(n)` | 5 | Consecutive failures while closed that trip the circuit. A success resets the run. |
| `WithSuccessThreshold(n)` | 2 | Consecutive successful probes while half-open needed to close. See below. |
| `WithMaxProbes(n)` | 1 | Concurrent probes admitted while half-open; the rest get `ErrProbeLimit`. |
| `WithOpenInterval(base, max)` | 5s, 60s | Open interval after the first trip, and its cap. |
| `WithTimeout(d)` | none | Deadline for each admitted call, derived from the caller's context. Set it: a hung call while half-open holds the probe slot forever. |
| `WithMaxInFlight(n)` | unlimited | Bulkhead: calls at the cap get `ErrBulkhead` immediately, nothing queues. See below. |
| `WithAdaptiveInFlight(min, max, target)` | off | Bulkhead whose cap moves by AIMD between the bounds. Opt-in; needs volume. See below. |
| `WithRecoveryRamp(start, end)` | off | After a recovery, the cap starts at `start` and grows by one per success to `end`, then the steady limit resumes. See below. |
| `WithAdmission(fn)` | none | A veto run on the caller's goroutine after the circuit admits a call; an error means fn does not run and the call is `denied`. The seam for rate limiters and anything else that may block to decide. |
| `WithOpenJitter(f)` | 0.2 | ±fraction applied to each open interval. 0 disables. See below. |
| `WithIsFailure(fn)` | `err != nil && !errors.Is(err, context.Canceled)` | Which errors count against the circuit. **Set this.** |
| `WithOnStateChange(fn)` | none | Called synchronously on every transition. Log here. Must not call back into the breaker. |
| `WithClock(now)` | `time.Now` | Clock; inject a fake in tests. |
| `WithSeed(a, b)` | from runtime | Jitter RNG seed; fix it in tests. |
| `WithObserver(o)` | none | Receives every event synchronously; how the Prometheus metrics are fed and the hook for other backends. |

### `WithIsFailure` is the setting that matters

The default counts every error except a caller-side cancellation. That is
wrong for almost every real backend, because it counts "the backend answered
correctly and the answer was no": `sql.ErrNoRows`, an HTTP 404, a validation
rejection. Those are successes as far as the circuit is concerned. Counting
them as failures is the most common way to make a breaker trip on healthy
traffic, and no other knob can compensate. Return `true` only for errors that
mean the backend itself is unhealthy: timeouts, connection failures, 5xx.

A `context.Canceled` that `IsFailure` does not classify as a failure is
*neutral*: it counts neither for nor against the circuit, since a caller
giving up says nothing about the backend. `context.DeadlineExceeded` is a
failure by default, deliberately: a backend too slow to answer is what the
breaker exists to detect.

### Why `WithSuccessThreshold` defaults to 2

With a threshold of 1, a single lucky probe against a still-sick backend
restores full traffic. The traffic fails, the circuit reopens, the interval
doubles, and the cycle repeats: the classic flap, each iteration hitting a
recovering backend with a burst it cannot yet absorb. Requiring two
consecutive successes is cheap insurance. A single failed probe reopens the
circuit regardless of how many successes preceded it.

### Bulkhead: what it protects and how to size it

The bulkhead protects your service, not the backend. A backend that is slow
but not failing never trips the circuit, and every in-flight call holds a
goroutine, a connection and whatever the request allocated. Without a cap
those grow until requests that never needed the backend fail too. With one,
the worst case is `maxInFlight` slots lost and everything else keeps working.

Size it from Little's law, in-flight = rate × latency, at the p99 and with
headroom so it never bites in normal operation:

```
steady in-flight = misses/sec × p99 latency      e.g. 50/s × 40ms = 2
cap              = 3–5 × steady                  e.g. 10
```

Keep the cap below the connection pool size, or the pile-up moves into the
driver where you cannot see it. Worst-case exposure is `cap × WithTimeout` of
backend time per instance. Ship without a cap first, watch
`max_over_time(go_breaker_in_flight[7d])`, then set it at several times the
peak.

The bulkhead is always on when configured. While half-open, `MaxProbes` is the
tighter limit and applies first; while open nothing is admitted anyway. An
open circuit reports `ErrOpen`, never `ErrBulkhead`, so the responder sees the
more informative error.

`WithAdaptiveInFlight` moves the cap by additive increase, multiplicative
decrease (AIMD), TCP's congestion rule: each success within `target` raises
the cap by one, each failure or slow call halves it, cancellations leave it
alone. It starts at `max` and only tightens on evidence. Its whole state is
one integer, so the sawtooth on the `in_flight_limit` gauge explains every
decision. It is opt-in because it needs volume: on a path doing a few calls a
second, one slow query is indistinguishable from saturation and the cap will
jitter. Prefer the static cap there.

### Recovery ramp

Closing the circuit after two probe successes proves the backend can handle
one call at a time; it says nothing about the backlog that built up while the
circuit was open. `WithRecoveryRamp(start, end)` releases that backlog
gradually: on close the in-flight cap is `start`, each success raises it by
one, and at `end` the steady limit resumes, the static cap or unlimited.
Failures do not move it; enough of them reopen the circuit and the ramp
restarts on the next close. Nothing happens at startup.

Under `WithAdaptiveInFlight` the ramp only seeds the cap; AIMD grows it from
there, and the ramp is over when the cap reaches `end` or AIMD lowers it,
which means backoff rather than recovery.

`Stats.Ramping`, the `(ramping)` suffix on the log line and the
`go_breaker_ramping` gauge say a ramp is in progress. That matters because a
cap of 3 during a ramp is recovery working as designed, and the same cap
under AIMD is the backend struggling; the saturation alert in [Observability](#observability) excludes
ramps for exactly this reason.

Tell consumers one thing about `ErrBulkhead`, and about `ErrOpen`: the request
never ran, so there are no side effects to reconcile. Return it as
`503 Service Unavailable` with a `Retry-After`, never as a 4xx, and ask them to
fail fast rather than retry immediately.

### Why jitter, and why it matters more here

The open interval doubles per consecutive trip from `OpenBase`, capped at
`OpenMax`, so a backend that stays down is probed less and less often.
`ConsecutiveTrips` resets when the circuit closes.

Every instance of the service trips on the same backend outage within moments
of each other. Without jitter they all finish the same open interval together
and probe in lockstep. The recovering backend takes a synchronised burst, fails
under it, and every instance reopens together, for twice as long. Jitter of
±20% spreads the instances' probes over a window of seconds, so recovery is
gradual. The jittered deadline is drawn once on entering Open and fixed for
that open period; `Stats.NextProbeIn` counts down to it.

## Guarantees

- When `Do` returns, the outcome has been applied. A subsequent `Stats`
  reflects it and any transition it caused has already been reported through
  `OnStateChange`. There is no barrier to call.
- `Admitted + Rejected + Shed + Denied + InFlight == Calls` and
  `Successes + Failures + Canceled == Admitted` hold at every observation, and
  every cumulative counter is monotonic.
- A stale outcome, from a call admitted before the most recent transition, is
  counted in the totals but never drives the state machine. A slow success
  from before a trip cannot close a circuit that has since opened.
- Open → half-open is evaluated lazily when a call or `Stats` arrives. With no
  traffic there is nothing to recover for, and no goroutine is woken.
- Nothing to stop or close. The state goroutine lives while the breaker is
  reachable and stops itself once it is not, like a `time.Timer`; a breaker
  created at bootstrap simply lives for the process. `Stop` exists for
  deterministic teardown and is optional.

## Observability

### Metrics

The package owns one set of metric vectors. Every breaker feeds them from its
state goroutine under its name; `Register` publishes them
under `<namespace>_breaker_`, with namespace `go` by default.
Register once at bootstrap, then create as many breakers as you like:

```go
if err := breaker.Register(prometheus.DefaultRegisterer); err != nil { ... }
// or, so each service's breakers carry its own name:
if err := breaker.Register(prometheus.DefaultRegisterer, breaker.WithNamespace("inventory")); err != nil { ... }

db, err  := breaker.New("db-fallback", ...)
sms, err := breaker.New("sms-gateway", ...)
```

| Metric | Type | Labels |
|---|---|---|
| `go_breaker_state` | gauge, one-hot | `dependency`, `state` |
| `go_breaker_open_until_timestamp_seconds` | gauge, unix time, 0 unless open | `dependency` |
| `go_breaker_consecutive_trips` | gauge | `dependency` |
| `go_breaker_in_flight` | gauge | `dependency` |
| `go_breaker_in_flight_limit` | gauge, +Inf when unlimited | `dependency` |
| `go_breaker_ramping` | gauge, 1 during a recovery ramp | `dependency` |
| `go_breaker_calls_total` | counter | `dependency`, `result` ∈ success, failure, canceled, rejected, shed, denied |
| `go_breaker_trips_total` | counter | `dependency` |

The names below assume the default namespace; with `WithNamespace` the
`go_` prefix changes accordingly, and so must the alert rules and dashboard
further down. A breaker's series exist at zero from the moment `New` returns
and are deleted when it is stopped. Counters are incremented by the same goroutine
that updates `Stats`, so the two always agree. Because open → half-open is
evaluated lazily, an idle breaker keeps reporting `state="open"` past its
deadline; `go_breaker_open_until_timestamp_seconds - time()` going negative is
how you see that. `go_breaker_state{state="open"} == 1` is the alert and
`increase(go_breaker_trips_total[10m])` catches flapping.

Custom instrumentation, such as OpenTelemetry, attaches through the same
`Observer` interface the metrics use, via `WithObserver`.

### Alerting

Alert on what the on-call engineer can act on, and aggregate across instances
so a single flapping pod does not page. Every alert carries the `dependency`
label, so Alertmanager can route each dependency to the team that owns it.

The rules live in `contrib/prometheus/alerts.yaml`, ready for
`promtool check rules` and for dropping into your rule files. In outline:

| Rule | Severity | Fires when |
|---|---|---|
| `breaker:open_fraction` (recording) | | share of instances whose circuit is open, per dependency |
| `BreakerOpenFleetWide` | page | more than half the instances are open for 2m: the dependency is down |
| `BreakerFlapping` | ticket | more than 5 trips in 30m: half-recovering backend, or `IsFailure` counting healthy answers |
| `BreakerSheddingMajority` | page | more than half of calls refused, by circuit or bulkhead, for 5m |
| `BreakerBulkheadSaturated` | ticket | bulkhead at its cap for 5m outside a recovery ramp: the backend is slow, not down |
| `BreakerMetricsAbsent` | ticket | no series for an expected dependency: a forgotten `Register` or a breaker never created |


What not to alert on:

- **`go_breaker_consecutive_trips`** is for the dashboard. `max by (dependency)`
  of it shows how deep into backoff the fleet is.
- **`go_breaker_open_until_timestamp_seconds`** is for the dashboard too.
  `… - time()` is the countdown to the next probe. Negative means the
  breaker is idle with its deadline passed, waiting for the next call to go
  half-open. That is lazy expiry working as designed, not a fault.

Put the container's CPU throttling counter
(`container_cpu_cfs_throttled_periods_total`) on the same dashboard as
`breaker:open_fraction`. A pod starved of CPU hits its own timeouts, and
`DeadlineExceeded` is a failure by default, so the circuit trips because of
the pod rather than the dependency. Seeing both side by side makes the false
attribution obvious.

Runbook for `BreakerOpenFleetWide`: check the dependency's own health first,
then read one instance's `Stats` line from its health endpoint. It says
`name=db-fallback state=open trips=3(consecutive=3) … next_probe_in=18s`,
which gives how long it has been failing and when the next probe happens
without another graph.

### Dashboard

`contrib/grafana/keel.json` is an importable Grafana dashboard
(Grafana 10+, Prometheus datasource). Import it via Dashboards → New → Import,
pick your Prometheus datasource when prompted, and select dependencies with
the `Dependency` variable.


| Panel | Type | Shows |
|---|---|---|
| Open now | bar gauge | The same fraction, current value. |
| Load shed | time series | Rejected and shed calls as a share of all calls: the user-facing symptom. |
| In flight vs limit | time series | Bulkhead headroom per dependency; a line pinned to the limit is a slow backend, unless the limit is climbing after a recovery, which is the ramp. |
| Trips | bars | Trips per interval, stacked by dependency. A comb is flapping. |
| Open circuits | table | One row per open breaker with the countdown to the next probe and its consecutive trips. A negative countdown means idle past the deadline, waiting for a call. |
| Per instance (collapsed) | state timeline | One row per breaker, Closed / Half-open / Open, for drill-down. |

| Rate limiters (collapsed row) | time series, bar gauge | Decisions per second by result per limiter, the refused share, limiter errors, and per-key records kept. Filtered by the `Limiter` variable. |

The per-instance panel collapses the one-hot `go_breaker_state` gauge into a
single ordinal, which is the shape a state timeline wants:

```promql
go_breaker_state{state="half-open"} * 1 + go_breaker_state{state="open"} * 2
```

with value mappings `0 → Closed`, `1 → Half-open`, `2 → Open`. Two things
not to do: plotting the raw `go_breaker_state` series on a time-series panel
gives three overlapping 0/1 lines per breaker, and plotting
`go_breaker_open_until_timestamp_seconds` directly plots a Unix timestamp; it
is only useful as the derived countdown `… - time()`.

The same dashboard carries a collapsed row for rate limiters; see the
`ratelimit` document.

## Testing

The suite has three layers beyond the per-property tests. Every blocking wait
in it carries a deadline, so a deadlock fails at a named line rather than
hanging until the `go test` timeout; `-short` trims the model and chaos
iterations.

- **A reference model.** `TestModel` drives the breaker and an independent
  single-threaded model of the documented rules through the same random
  sequences of admissions, out-of-order settles and clock advances, and
  compares `Stats` after every step. A divergence prints the seed.
- **Chaos with invariants.** `TestChaosInvariants` hammers one breaker from
  many goroutines with random outcomes, cancellations, clock advances and
  inspections, checks the accounting invariants at every observation, and
  verifies afterwards that every transition the hook saw was a legal edge.
- **Regression pins.** The settle-before-apply clock race, zero allocations
  per call, probe-slot release on panic, goroutine release on `Stop`, and
  free-list overflow each have a dedicated test.

## Benchmarks

Apple M5 Max, 18 cores, Go 1.27.1, `go test -bench . -benchmem`:

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Baseline (no breaker) | 1.6 | 0 | 0 |
| `Do` closed, serial (Prometheus metrics fed) | 1007 | 0 | 0 |
| `Do` closed, serial, `WithTimeout` set | 1252 | 272 | 4 |
| `Do` open (rejection) | 453 | 0 | 0 |
| `Do` closed, `RunParallel` (18 goroutines) | 3530 | 0 | 0 |
| `Stats` | 528 | 240 | 2 |

Each call talks to the loop on one pointer-free `chan uint64`: the loop
replies on it with the admission decision and later acknowledges the settle on
it. Tokens are recycled through a buffered channel used as a free list, so in
steady state `Do` allocates nothing. `Stats` allocates its reply channel, two objects now that the snapshot carries
a name; it is a scrape path, not a request path. `WithTimeout` costs the standard
`context.WithTimeout` allocations per call, which is the price of a bounded
network call anywhere in Go.

The parallel figure is per call across all goroutines: a single actor
serialises every admission and settle, so contention shows up as latency, not
as a data race. That is acceptable on a path that is low volume by
construction and would not be for a hot read path.

Channel handoff cost is dominated by scheduler wakeups and is substantially
worse on a single core; expect roughly double the latency on a 1-vCPU box.
Rejection is cheaper than admission because it makes one channel round trip
instead of two.
