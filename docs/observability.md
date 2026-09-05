# Observability

Both packages publish Prometheus metrics through one `Register` call each,
render a one-line `Stats` for logs and health endpoints, and ship alert rules
and a Grafana dashboard under `contrib/`.

## Breaker metrics

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

## Rate limiter metrics

Metrics, registered once at bootstrap alongside the breaker's and namespaced
the same way:

```go
breaker.Register(prometheus.DefaultRegisterer)
ratelimit.Register(prometheus.DefaultRegisterer)   // go_ratelimit_*
```

| Metric | Type | Labels |
|---|---|---|
| `go_ratelimit_decisions_total` | counter | `limiter`, `algorithm`, `result` ∈ allowed, limited, error |
| `go_ratelimit_cas_conflicts_total` | counter | `limiter`, `algorithm` |
| `go_ratelimit_keys` | gauge, when the store can report it | `limiter`, `algorithm` |

Keys are deliberately not a label: per-tenant or per-IP keys would be
unbounded cardinality. Aggregate per limiter, and use the breaker's
`result="denied"` for the per-dependency view.

## Alerting

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
| `RateLimitRefusingMajority` | ticket | more than half of decisions refused for 10m |
| `RateLimitErrors` | page | a limiter could not decide: its store is unreachable, and the middleware fails open |


  # Bulkhead saturation without an open circuit: the backend is slow, not
  # down. Usually a ticket; the timeout and the cap bound the damage. A
  # saturated cap during a recovery ramp is the ramp doing its job, so those
  # are excluded.
  - alert: BreakerBulkheadSaturated
    expr: |
      max by (dependency) (
        (go_breaker_in_flight / go_breaker_in_flight_limit >= 1)
          unless on (dependency, instance) go_breaker_ramping == 1
      )
    for: 5m
    labels: { severity: ticket }

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

## Dashboard

`contrib/grafana/keel.json` is an importable Grafana dashboard
(Grafana 10+, Prometheus datasource). Import it via Dashboards → New → Import,
pick your Prometheus datasource when prompted, and select dependencies with
the `Dependency` variable.

| Panel | Type | Shows |
|---|---|---|
| Circuit open, fraction of instances | state timeline | One row per dependency, coloured by the share of instances open. Red across the row is the dependency being down; a sliver is one pod. |
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
