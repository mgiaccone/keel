# Load Shedding

Package `github.com/mgiaccone/keel/overload`. An inbound admission gate that measures its own
requests, works out from that how many may run at once, and refuses the rest by priority — the
least important work first.

## Quick start

```go
l, err := overload.New("api", overload.Gradient(4, 200), 0.6, 0.85)  // min, max in flight; sheddable and default shares

mux.Handle("/v1/", overload.MustMiddleware(l,
    overload.WithPriority(func(r *http.Request) overload.Priority {
        switch {
        case strings.HasPrefix(r.URL.Path, "/v1/beacon"):
            return overload.Sheddable                                 // nobody is waiting on it
        case strings.HasPrefix(r.URL.Path, "/v1/pay"):
            return overload.Critical
        default:
            return overload.Default
        }
    }),
)(api))

mux.HandleFunc("/healthz", health)                                    // deliberately outside: never shed
```

`overload/example_test.go` contains the same wiring plus the direct `Acquire`/release and
`Admission` forms, with verified output.

## When to use it

A server has one number it cannot exceed and usually does not know: how many requests it can run
concurrently before adding one more makes every one of them slower without finishing any sooner.
Past that point a queue builds inside the process — goroutines, connections, buffers, request
allocations — and the symptom is not that some requests fail, it is that *all* of them get slower,
including the ones that would have succeeded, until latency runs past every client's timeout and
the work is thrown away after being paid for. That is the failure this package exists to prevent:
it puts a hard ceiling on concurrent work and gives up the least important part of it, so that
what is left is served properly.

That is a different question from the ones the other keel packages answer, and the three do not
substitute for each other:

| | Question | Scope | Number comes from |
|---|---|---|---|
| `ratelimit` | May this caller start a request? | per caller or tenant | a quota you chose and owe them |
| `breaker` | May this call to *that* dependency start? | per dependency | a cap you sized, or its own AIMD |
| `overload` | Can this process run one more request at all? | the whole process | measured, continuously, from your own latency |

The distinction that matters in review: a rate limit is a **promise to a caller** — exceed it and
the caller is at fault, which is why it answers 429. A shed is **your own saturation** — the caller
did nothing wrong, which is why it answers 503. A quota cannot protect you from saturation, because
every client can be inside its quota while the sum of them is more than you can serve; and
saturation shedding cannot enforce a quota, because it has no idea who anyone is. A busy service
generally wants both, in that order — see [Composing](composing.md).

Reach for it when: you serve inbound traffic whose volume you do not control; your tail latency
degrades under load rather than your error rate; or you have work of genuinely different value
sharing one process — a payment confirmation and an analytics beacon are not equally worth serving
in the last 5% of capacity.

Do not reach for it when: your bottleneck is a single downstream dependency (that is `breaker`'s
bulkhead, which acts closer to the problem); your traffic is a handful of requests per second (any
adaptive limit needs volume to distinguish a slow request from saturation — see the sizing notes
below); or what you actually want is a fixed concurrency cap you already know, which
`overload.AIMD(n, n, …)` will give you but a semaphore gives you more cheaply.

## How it works

A `Limiter` is an `Algorithm` and a count:

```
Algorithm  the rule, a pure function: Step(state, signal, now) → (state', decision)
Limiter    hold in-flight requests under decision.Capacity; divide that capacity
           between priority bands by the shares given to New
Priority   which band is given up first
```

`Acquire(ctx, p)` decides admission and hands back a `release` function; `release(err)` reports
how the request went. That completed request becomes one `Signal` — its service time and its
outcome — and one `Step` of the algorithm, which returns the capacity to enforce from then on.
There is no goroutine, no timer and no background sampling: capacity moves only when a request
finishes, which is the only moment there is anything new to learn.

The signal is deliberately end-to-end. `Signal.Duration` is measured from admission to release,
so every dependency the request touched is inside it. That is the point, not a defect: a slow
database genuinely bounds how much concurrent inbound work this process can hold without its
memory and goroutine count growing without limit. It does mean `overload` and a `breaker`'s
bulkhead will react to the same underlying slowdown from two vantage points — process-wide and
per-dependency. They reinforce each other, but size them knowing both will move.

### Priority and the shares

```go
overload.New(name, algorithm, sheddableShare, defaultShare)
```

The shares are fractions of the **current** capacity above which a band is refused, so they
tighten automatically as capacity falls:

| Band | Refused when in-flight reaches | At capacity 40, shares 0.6/0.85 | At capacity 4 |
|---|---|---|---|
| `Sheddable` | `sheddableShare × capacity` | 24 | 2 |
| `Default` | `defaultShare × capacity` | 34 | 3 |
| `Critical` | `capacity` | 40 | 4 |

There is no band that is never shed. `Critical` is admitted up to the whole computed capacity and
refused past it, exactly like the others. The obvious objection is the health check: shed one and a
merely-overloaded instance looks dead to an orchestrator that cannot tell the difference, and gets
killed for being busy. The fix is structural, not a priority value — **register that route outside
the middleware's subtree**, as the quick start does with `/healthz`. A handler that never calls
`Acquire` cannot be shed by this package at all, which is a stronger guarantee than a band that is
merely supposed to always win, and it costs nothing to state in the mux.

Derive the priority from something the client cannot set. A header any caller can send is a header
every caller will eventually send as `Critical`.

### The algorithms

`Gradient(min, max)` is the recommended default and needs no latency target. It keeps the best
service time it has itself observed as a moving baseline and moves capacity as the current service
time drifts from it: a request at twice the baseline is a queue building, wherever that baseline
happens to sit, so the same algorithm works unchanged on a 2ms endpoint and a 2s one. Capacity
starts at `max`, shrinks on drift and on errors, and climbs back through a `√capacity` headroom
probe — without which a limiter that only ever shrinks would never discover that the load had
passed.

Its failure mode, stated plainly because every self-calibrating limiter has it: the baseline is
only as good as the best request it has seen, so a service that has been degraded since the process
started calibrates to degraded and calls it normal. It recalibrates the moment one genuinely fast
request lands. Conversely, a service that is permanently slower now will have its new speed adopted
as the baseline over a few hundred requests and capacity will come back up around it — wanted, but
worth knowing. Pair it with a breaker, which judges outcomes rather than drift, if that window
matters.

`AIMD(min, max, target, interval)` is the deterministic alternative: every request within `target`
raises the limit by one, and requests that are slower than `target` or that fail halve it — the
rule `breaker.WithAdaptiveInFlight` already uses outbound. `interval` is the hysteresis that makes
it usable inbound: the halving fires only once requests have been bad *continuously* for a whole
`interval`, never on one slow request, and a single good request ends the run. Without it a
low-volume path collapses its own limit on one unlucky query. Choose it when you know your target
latency and want a limit you can predict by hand.

Both are pure functions over an opaque three-integer `State`, and both read only `Signal.Duration`
and `Signal.Err` — never `Signal.Sojourn`. That is what makes them run identically whether or not
the wait queue is on: if the queue's wait fed the capacity estimate, queueing would look like
slowness and the limiter would shrink capacity for having queued.

Once either has backed all the way down to its `min`, it also sets `Decision.ShedBelow` to
`Default`, refusing `Sheddable` outright rather than leaving it whatever the share allows. A
limiter with `min == max` is a fixed cap, not a limiter that has backed down, so it never does
this.

### The wait queue

Off by default. `WithMaxWait(d, target, interval)` turns it on: a request that finds no capacity
blocks, on its own goroutine, in a `select` against its context, a hard ceiling `d`, and the
queue's own give-up rule. The package owns no background timer and no goroutine of its own.

The give-up rule is CoDel's one genuinely useful idea, repurposed. It is *not* a competing
`Algorithm` — capacity is `Gradient`/`AIMD`'s job — it answers the separate question of how long a
request may wait for a slot: once no waiter has come in under `target` for a continuous `interval`,
the queue starts shedding waiters instead of letting them all ride out the full `d`. One wait under
target, or a whole `interval` with nobody queued, clears the run.

Two details of how that is measured, because both are places a plausible implementation gets it
wrong. The wait it reads is always the **oldest waiter in the whole queue**, never whichever waiter
happens to be granted a slot — otherwise a briskly-served `Critical` band would supply an endless
run of short waits while the `Sheddable` waiter behind it starved, and the band the rule exists to
drop first would be the one band it could never reach. And the `interval` is elapsed time, not a
count of observations: the queue is only looked at when a request arrives or releases, so a rule
reading how full a trailing window is would answer differently for a busy service and a quiet one,
and on the quiet one would never fire at all.

The queue is priority-aware in both directions. A freed slot goes to the oldest waiter in the
**highest-priority** non-empty band, not to the oldest waiter overall; the give-up rule drops from
the **lowest**-priority band first.

The trade-off, which belongs in the review conversation and not only here: queueing converts a
would-be rejection into a slightly late success, which is exactly right when overload is a brief
burst — the case a capacity estimate is slowest to react to. It stops helping, and starts hurting,
once overload is sustained rather than momentary, because every waiter holds a goroutine, a
connection and whatever the request already allocated for up to `d` — memory spent on work that is
going to be refused anyway. Size `d` as a small fraction of what the caller upstream is willing to
wait, and never as a substitute for a timeout. `Stats.WaitedThenShed` is the number to watch: if it
is not near zero, the queue is buying delay rather than success.

## Configuration

### `New`

```go
func New(name string, algorithm Algorithm, sheddableShare, defaultShare float64, opts ...Option) (*Limiter, error)
```

`algorithm` and both shares are required positional arguments, not defaulted options — the same
shape as `ratelimit.New(name, algorithm, store, …)`. Neither has an answer that is right for every
service: an algorithm needs the concurrency range your server can plausibly sit in, and the shares
decide whether a whole class of your traffic is served at all under load, which is not a thing to
inherit silently. `New` returns an error wrapping `ErrInvalidOption` describing *every* problem it
found — an empty name, a nil or invalid algorithm, shares outside
`0 < sheddableShare < defaultShare ≤ 1`, or any invalid option.

Sizing:

- **`max`** — the concurrency you are confident the process can sustain in good health. Little's
  law gives the starting point: `in-flight = peak request rate × p50 latency`, then double or
  triple it so the limit does not bite in normal operation. It is a ceiling, not a target; the
  algorithm starts here and only ever moves down from it on evidence.
- **`min`** — the floor you would rather serve than fall to zero. Never 1 unless you mean it:
  `Sheddable`'s share of a capacity of 1 is no slots at all, so a limiter pinned at `min = 1` serves
  only `Critical`. Something like 5–10% of `max`, and at least 2, is a reasonable floor.
- **`sheddableShare`, `defaultShare`** — 0.6 and 0.85 are sensible starting points with `Gradient`.
  The gap between them is your warning band: between 60% and 85% utilisation only sheddable work is
  being refused, which is the interval you want to be alerted in rather than at 100%. Widen the gap
  if the shed share alarms too late, narrow it if sheddable traffic is being refused during ordinary
  peaks.
- **`WithMaxWait(d, target, interval)`** — `d` a small fraction of the upstream caller's own
  timeout (tens of milliseconds against a one-second client budget, not hundreds); `target` around
  the wait you would consider acceptable, typically a fraction of `d`; `interval` several times
  `target`, long enough that one slow burst rides through it. Leaving it off is the right default.

### Options

| Option | Default | What it does |
|---|---|---|
| `WithMaxWait(d, target, interval)` | `(0, 0, 0)` — no queue | Bounded, priority-ordered wait with a CoDel-style give-up rule. `(0, 0, 0)` explicitly switches it back off. |
| `WithClock(now)` | `time.Now` | The clock every measured duration is read from. The bounded wait is a real timer regardless; a fake clock freezes what is measured, not how long a goroutine blocks. |
| `WithSeed(a, b)` | seeded from the runtime | Makes the `Retry-After` jitter reproducible. |
| `WithObserver(o)` | none | Streams admissions, sheds, waits and capacity changes. May be given more than once. |

### Middleware options

| Option | Default | What it does |
|---|---|---|
| `WithPriority(f)` | everything `Default` | Derives the band from the request. Without it nothing is preferentially shed. |
| `WithShedHandler(h)` | 503 with a plain-text body | What a refused request gets. `Retry-After` is set before it runs either way. |

`Middleware` reports the handler's outcome to the algorithm: a 5xx response releases with an error,
so a burst of server errors counts as evidence of overload rather than as a set of very fast
requests. Reading the status means wrapping the `http.ResponseWriter`; the wrapper implements
`Unwrap`, so `http.ResponseController` reaches the real writer for flushing, hijacking and
deadlines — but a handler that type-asserts the writer directly to some other interface will not
find it.

## Behaviour

### Errors from `Acquire`

| Returned | When | `release` | Counted as |
|---|---|---|---|
| `nil` | admitted, immediately or after queueing | non-nil, call it exactly once | `Admitted` |
| `*ShedError` | no capacity for this band, or the queue gave up | `nil` | `Shed` |
| `ctx.Err()` | the caller's context ended while queued | `nil` | `Shed` |
| `ErrInvalidOption` | a `Priority` outside the three constants | `nil` | nothing |

`errors.Is(err, ErrOverloaded)` matches every `*ShedError`. `(*ShedError).Retryable()` answers
**false**, matching `breaker`'s refusals rather than `ratelimit`'s: retrying your own overload
signal inside the process that is the one overloaded is amplification, not relief, so an enclosing
`retry.Do` aborts on it immediately with no import between the packages. That says nothing about
the client on the far side of an HTTP boundary — it sees 503, builds a `retry.StatusError` from it,
and is told `true`, which is correct, because its retry lands somewhere else or later.

A context ending while queued is counted as `Shed` rather than as a fourth outcome. The request was
not served either way, and the alternative breaks the `Admitted + Shed == Requests` identity for a
distinction the metric label does not draw either.

`release` must be called exactly once — `defer` it on the line after the error check — or the slot
is never freed and capacity walks down to nothing. Calling it a second time is ignored, not
punished.

### `Stats`

```
overload: name=api algorithm=gradient requests=8120 admitted=7940 shed=180 sheddable=171 default=9 critical=0 waited=204 waited_admitted=190 waited_shed=14 capacity=48 in_flight=31
```

Identity: `Admitted + Shed == Requests`. `Waited`, `WaitedThenAdmitted` and `WaitedThenShed`
annotate the subset of those requests that queued rather than adding terms to it, the same
relationship `fallback.Stats`'s `Misses` has to its `Gets`. `Capacity` and `InFlight` are live
values, not cumulative.

### `Retry-After`

A shed HTTP response carries `Retry-After: 1`, jittered ±20% and rounded up to whole seconds as the
header requires. The fraction and the mechanism are `breaker.WithOpenJitter`'s, reused rather than
reinvented, and the draw comes from the limiter's own seeded generator. Be clear about how much
that buys: whole-second rounding maps `[0.8s, 1.2s]` onto one or two, so a refused fleet returns in
two waves rather than one. Better than the single wave a constant produces, and not the same thing
as the fine-grained spread jitter gives an open interval — a client that needs that has to add its
own.

## Composing

See [Composing](composing.md) for the full stack. `overload` sits **outermost**, above the inbound
rate limiter: a request shed for load should not first spend a client's quota.

```go
mux.Handle("/v1/", overload.MustMiddleware(shedder, overload.WithPriority(byRoute))(
    ratelimit.MustMiddleware(quota, ratelimit.KeyByHeader("X-API-Key"))(api),
))
```

`Admission(l, p)` returns the `func(context.Context) error` veto shape `breaker.WithAdmission` and
`retry.WithBudget` both take. Read its honest limits before reaching for it: the veto holds no slot,
because there is no release to pair it with, so vetoed work never counts towards in-flight and never
feeds the algorithm a measurement — a limiter reached only this way never learns anything and its
capacity never moves. Composed into a breaker it also runs *after* the circuit and bulkhead have
admitted the call, by which point the inbound request this package exists to shed is already on the
request path. `Middleware` is the placement that makes the package work; `Admission` is for a
narrower outbound veto sharing one process-wide capacity number, alongside a `Middleware` that is
doing the real measuring.

### One limiter per latency profile

This is the mistake the package most invites, so it gets a worked example — the same shape
[`docs/fallback.md`](fallback.md) uses for its two breakers, and for the same reason.

Wrong: one limiter around the whole mux.

```go
l, err := overload.New("app", overload.Gradient(4, 200), 0.6, 0.85)
mux.Handle("/", overload.MustMiddleware(l)(everything))          // don't
```

A search endpoint answering in 3ms and a report endpoint answering in 2s now share one baseline and
one capacity number. Ten people asking for the expensive report is not overload — it is ten people
asking for the expensive report — but under one limiter it drags the shared baseline out, collapses
the shared capacity, and starts shedding the 3ms search traffic that was never in any difficulty.
Run the same thing the other way and it is worse: the fast traffic dominates the baseline, so the
report endpoint's perfectly normal 2s never registers as drift at all and the limiter never protects
anything.

Right: one limiter per class of work, sized for that class.

```go
// Fast routes: high concurrency, a floor that still serves real traffic.
fast, err := overload.New("api.fast", overload.Gradient(20, 400), 0.6, 0.85)

// One known-slow endpoint, on its own: low concurrency, and no sheddable
// traffic to give up, so both shares sit high.
reports, err := overload.New("api.reports", overload.Gradient(2, 12), 0.8, 0.9)

mux.Handle("/v1/", overload.MustMiddleware(fast, overload.WithPriority(byRoute))(api))
mux.Handle("/v1/reports/", overload.MustMiddleware(reports)(reportAPI))
mux.HandleFunc("/healthz", health)                                // neither limiter wraps it
```

Two capacity numbers, each measured against traffic that is actually comparable, and two rows in
the dashboard that mean something on their own. The rule is the one `breaker` already follows —
one instance per thing whose behaviour you are actually tracking — and the same naming rule applies:
one live limiter per name, because two sharing a name silently merge their metric series.

## Observability

### Metrics

`overload.Register(reg)` registers the package's metrics once, under `<namespace>_overload_` with
namespace `go` by default; `overload.WithNamespace("inventory")` changes it. Every limiter reports
under its name as the `name` label.

| Metric | Type | Labels |
|---|---|---|
| `go_overload_in_flight` | gauge | `name` |
| `go_overload_capacity` | gauge | `name` |
| `go_overload_requests_total` | counter | `name`, `priority` ∈ sheddable, default, critical, `result` ∈ admitted, shed |
| `go_overload_wait_seconds` | histogram | `name` — no observations without `WithMaxWait` |
| `go_overload_service_seconds` | histogram | `name` |

Common queries:

```promql
go_overload_in_flight / go_overload_capacity                                  # headroom; 1 means saturated
rate(go_overload_requests_total{result="shed"}[5m])
  / rate(go_overload_requests_total[5m])                                      # shed share
rate(go_overload_requests_total{result="shed",priority="critical"}[5m]) > 0   # shedding work that matters
histogram_quantile(0.99, rate(go_overload_wait_seconds_bucket[5m]))           # what the queue is costing
```

The pair to watch is `in_flight` against `capacity`. A limiter riding its capacity is at its limit;
a `capacity` falling away from a flat `in_flight` is the algorithm deciding the work has got slower.

### Alerting

`contrib/prometheus/alerts.yaml` contains the rule below in an `overload` group.

| Rule | Severity | Condition |
|---|---|---|
| `OverloadSheddingSustained` | page | shed share above 0.1 for 5m |

A tenth of traffic refused for five minutes is not a burst any more; either the capacity estimate
has collapsed onto something genuinely slow, or the traffic is beyond what this fleet can serve and
it needs more instances. The 5-minute sustain window is the one every other page-severity rule in
the file uses.

Know what the ratio counts before you page on it: `Admission` vetoes carry the same `name` and
`result` labels as `Middleware` requests, and a veto refuses far more freely than the inbound gate,
because it holds no slot and so fires whenever in-flight is already at the band limit. A limiter
used both ways can cross the threshold on its outbound vetoes while inbound shedding is negligible.
Give the veto its own limiter name if you want this rule to be about inbound shedding alone.

Two more refinements worth adding per service rather than shipping by default: a rule on
`priority="critical"` sheds, which should be zero and means capacity is gone entirely when it is
not; and an `absent()` guard for a limiter you expect to see, since a forgotten `Register` looks
exactly like everything being fine.

### Dashboard

`contrib/grafana/keel.json` includes a collapsed "Load shedding" row, filtered by a `Shedder`
template variable: in-flight against capacity, the shed share stacked by priority, wait time
against service time, and request rate stacked by priority and result. It is provided as a
starting point.

## Testing

`overload_test.go` wraps the limiter in a `harness` on a fake clock and covers the documented
properties one at a time: the share rule per band, `ShedBelow`, the priority ordering the queue
grants in, the ceiling, the give-up rule firing only after a full interval and clearing on one good
wait, a context ending mid-wait, a double release, and the veto holding no slot. Beyond those there
are two tests that have to survive any rearrangement of the internals:
`TestAdmissionMatchesAReferenceModel` replays four thousand random acquire/release steps against a
hand-written model of the admission rule and requires agreement on every decision, and
`TestStatsIdentitiesUnderChaos` runs sixteen goroutines with random priorities, outcomes, timings,
cancellations and a queue in play, then checks every identity `Stats` documents — including that the
observer saw exactly what the counters recorded. `algorithm_test.go` adds a chaos test that drives
both algorithms with everything a `Signal` can carry and requires only what both promise: a capacity
inside `[min, max]`. `-short` trims the chaos iterations.

## Benchmarks

`go test -bench . -benchmem ./overload/` on an Apple M5 Max (18 cores), Go 1.27.1:

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `Acquire` + release, admitted | 86 | 65 | 2 |
| `Acquire` + release, 18 goroutines | 343 | 65 | 2 |
| `Acquire`, refused | 32 | 1 | 1 |
| `Gradient.Step` | 2.9 | 0 | 0 |
| `AIMD.Step` | 1.8 | 0 | 0 |

The admitted path is one mutex acquisition each way, the algorithm step, and the release closure —
the two allocations. The parallel figure is contention on that single mutex; it is the cost of an
admission decision being one shared count, which is what the package is. The refusal path allocates
only the `*ShedError`. Both algorithms are pure integer and float arithmetic over three words and
allocate nothing.
