# Rate Limiter

Package `github.com/mgiaccone/keel/ratelimit`, with a Redis store in
`github.com/mgiaccone/keel/ratelimit/redistore`. A keyed rate limiter built from
an algorithm applied to records in a store, with a `net/http` middleware.

## Quick start

```go
store, err := ratelimit.NewMemoryStore()      // per process; redistore.NewStore(client) for one quota across instances
limiter, err := ratelimit.New("public-api", ratelimit.GCRA(100, 20), store)   // 100 per second per key, bursts of 20

// As middleware: allowed requests get X-RateLimit-Remaining; refused ones get
// Retry-After and 429 Too Many Requests.
mux.Handle("/v1/", ratelimit.MustMiddleware(limiter, ratelimit.KeyByHeader("X-API-Key"))(api))

// Or directly.
d, err := limiter.Allow(ctx, key)   // d.Allowed, d.Remaining, d.RetryAfter
```

`ratelimit/example_test.go` contains a middleware example and a breaker
composition example, both with verified output.

## When to use it

A rate limiter bounds how many calls start per unit of time, regardless of
how long they take. It applies in two directions:

- **Inbound**, to hold callers to a quota: so many requests per second per
  API key, tenant or client address. A refusal is `429 Too Many Requests`;
  the caller exceeded a limit that applies to them.
- **Outbound**, to pace calls to a dependency that has a quota or a capacity
  you must not exceed. A refusal here is your own capacity decision, and
  becomes `503 Service Unavailable` at your boundary.

It does not bound how many calls are in progress at once; that is the
breaker's bulkhead. Fast calls pass through a small concurrency cap at a high
rate, and slow calls fill it at a low one. The two limits are independent.

## How it works

A `Limiter` is an `Algorithm` applied to records in a `Store`.

```
Algorithm   the rule, as a pure function:   Step(state, now) → (state', decision)
Store       where a key's state lives:      Get(key) → (state, version, now)
                                             CompareAndSet(key, version, state', ttl)
Limiter     Get, Step, CompareAndSet; retry on a version conflict
```

An algorithm is a function of a key's current state and the time. The state
is three integers whose meaning the algorithm defines; the zero state means
the key has never been seen. Algorithms are written once, in Go, and work
against every store.

A store keeps the state per key and updates it atomically. `Get` returns the
record with a version, 0 for an absent key, and the store's current time.
`CompareAndSet` writes only if the version is still the one the caller read,
and sets the record to expire after the algorithm's TTL, after which the zero
state gives the same answers. Two limiters racing on a key see one write
succeed and one fail; the loser reads again and retries, up to
`WithMaxAttempts` times, then returns `ErrContention`.

A store that can apply the step under its own lock implements the optional
`Updater` interface, and the limiter calls that instead. The memory store
does, so a local limiter never sees a conflict. A Redis store cannot run Go
inside Redis, so it uses compare-and-set: two round trips per decision, one
pipelined `TIME` and `HMGET`, then one Lua script that checks the version and
writes.

The store owns the clock. With the Redis store, time comes from the Redis
server, so instances with skewed clocks agree.

A refusal that leaves the state unchanged is not written, so refused calls
cost one read.

## Configuration

### Algorithms

| Constructor | Allows | State | Notes |
|---|---|---|---|
| `GCRA(rate, burst)` | `rate` calls per second on average, bursts up to `burst` | one timestamp | Decisions are exactly those of a token bucket. The usual choice for protecting a dependency or pacing a client. |
| `FixedWindow(limit, window)` | `limit` calls per window aligned to the epoch | start, count | Matches a quota stated as "N per minute". Admits up to 2×`limit` across a window boundary. |
| `SlidingWindow(limit, window)` | about `limit` calls in any window-long span | start, current, previous | Estimates the count from two aligned windows, assuming the previous window's calls were evenly spread; no boundary burst; a few percent off under uneven traffic. |

`RetryAfter` in a refusal is computed by each algorithm for its own rule:
GCRA, the time until one token has refilled; fixed window, the time to the
next boundary; sliding window, the time until the estimate, with no further
calls, falls below the limit.

### Stores

| Constructor | Scope | Options |
|---|---|---|
| `NewMemoryStore(opts...)` | one process | `WithMaxKeys(n)`, default 1024: records kept, least recently used evicted beyond it. `WithClock(fn)`, default `time.Now`. |
| `redistore.NewStore(client, opts...)` | shared through Redis | `WithKeyPrefix(p)`, default `ratelimit:`; include a hash tag such as `ratelimit:{public-api}:` to keep a limiter's keys in one cluster slot. |

An evicted or expired key returns as if never seen, which for every algorithm
means a full allowance. The Redis store requires Redis 5 or later, or Valkey.
The `ratelimit` package does not import a Redis client; only programs that
import `ratelimit/redistore` link go-redis.

### Limiter

`New(name, algorithm, store, opts...)`. The name identifies the limiter in
`Stats` and the `limiter` metric label. `WithMaxAttempts(n)`, default 8,
bounds compare-and-set retries; it applies only to stores without `Updater`.
Invalid arguments make `New` return an error wrapping `ErrInvalidOption`.

### Middleware

`Middleware(limiter, key, opts...)` returns a `func(http.Handler)
http.Handler` and an error: a nil limiter or key, or an invalid option,
comes back wrapping `ErrInvalidOption` at bootstrap rather than panicking on
the first request. `MustMiddleware` is the same for bootstrap code that
treats it as fatal, mirroring `MustRegister`. `key` is a `KeyFunc`:

| Key function | Keys on |
|---|---|
| `KeyByHeader(name)` | a request header, such as `X-API-Key`; requests without it share the key `""` |
| `KeyByRemoteAddr()` | the client IP without the port; behind a proxy, key on the forwarded header only if the proxy is trusted to set it |
| `KeyGlobal()` | one key for every request; `AdmissionGlobal` is the outbound counterpart |
| your own `func(*http.Request) string` | for example the authenticated tenant from the request context |

| Option | Effect |
|---|---|
| `WithLimitedHandler(h)` | Serves refused requests instead of the default plain-text 429. `Retry-After` and `X-RateLimit-Remaining` are set before it runs. |
| `FailClosed()` | Answers 503 when the limiter returns an error. The default lets the request through, on the grounds that a limiter that cannot decide, which only a distributed store can cause, should not take the API down with it. |
| `OnError(fn)` | Receives limiter errors, for logging. nil removes the hook. |
| `WithoutRemainingHeader()` | Omits `X-RateLimit-Remaining`. |

## Behaviour

### Decisions

`Allow(ctx, key)` returns a `Decision` and an error. When the error is nil:

| Field | Allowed | Refused |
|---|---|---|
| `Allowed` | true | false |
| `Remaining` | calls the key can still make now | 0 |
| `RetryAfter` | 0 | time until a call for this key would be allowed; always positive, and exact: a call made then is admitted |

Nothing waits: a refused call returns immediately. Callers that want to wait
do so themselves, with their own deadline, using `RetryAfter`.

### Errors

A non-nil error means the limiter could not decide, which is distinct from a
refusal: a store that is unreachable or replied unexpectedly, or
`ErrContention` after the retry budget. `Stats.Errors` and the `error` result
count them. The caller chooses the policy: `Admission` fails closed, the
middleware fails open by default, and `FailOpen(limiter, onError)` wraps any
`Allower` to allow on error.

Through `Admission`, a refusal is a `*LimitedError` carrying the key and
`RetryAfter`; `errors.Is(err, ratelimit.ErrLimited)` matches it.
[Composing](composing.md)'s "What each layer refuses" has the full
cross-package error reference, including why the "could not decide" bucket
above is deliberately left without a `Retryable` answer.

### What a refusal means to a caller

A refused call did not run and has no side effects. Inbound, respond with
`429 Too Many Requests` and `Retry-After` in whole seconds, rounded up, which
is what the middleware does. Outbound, through the breaker, translate to your
own "unavailable" error and respond with `503 Service Unavailable`; the
limiter does not know which side it is on.

### `Stats`

`Stats` returns the name, algorithm, `Keys` (records in the store, when the
store can report it), and the cumulative counters `Allowed`, `Limited`,
`Errors` and `Conflicts`. `Stats.String` renders one line:

```
ratelimit: name=public-api algorithm=gcra keys=3 allowed=812 limited=41 errors=0 conflicts=2
```

### Lifecycle

A `Limiter` has no goroutine and nothing to stop. Its metric series persist
for the life of the process, as any package-level metric does. Drop a limiter
when it is no longer needed.

## Composing

See [Composing](composing.md) for where this package sits in the full stack —
rate limit, retry, breaker, the call — and why one limiter should not serve
more than one of its three roles there.

### With a circuit breaker

`Admission(limiter, key)` returns a `func(context.Context) error` that refuses
with a `*LimitedError`, which is the shape `breaker.WithAdmission` takes.
`AdmissionGlobal(limiter)` is that with no per-key distinction, which is what
one breaker in front of one dependency wants:

```go
b, err := breaker.New("db-fallback",
    breaker.WithAdmission(ratelimit.AdmissionGlobal(limiter)),
)
```

Reach for the keyed form when several breakers share one limiter and each
needs its own budget: `ratelimit.Admission(limiter, "db-fallback")`.

The breaker runs the veto after the circuit has admitted the call. An open
circuit still answers `breaker.ErrOpen`, a call the circuit refuses consumes
no quota, and a call the limiter refuses is recorded by the breaker as
denied, with no effect on the circuit's failure count, ramp or adaptive
bulkhead. The packages do not import each other.

### As a retry budget

The same veto is what `retry.WithBudget` takes:

```go
r, err := retry.New("db-fallback", retry.Exponential(50*time.Millisecond, 2*time.Second),
    retry.WithBudget(ratelimit.AdmissionGlobal(limiter)),
)
```

Every retry asks the limiter first; a refusal ends the call. With a Redis
store this caps the retries of the whole fleet, which is the bound that holds
during an outage when every instance is retrying. Size it as a fraction of
normal traffic to the dependency.

### With a retrier

A `*LimitedError` implements `Retryable() bool`, answering true, and
`RetryDelay() time.Duration`, returning `RetryAfter`. A retrier that meets
one, for example from an admission veto inside a breaker, waits at least
`RetryAfter` before the next attempt, or gives up when that exceeds its cap.
See [`retry`](retry.md).

### With your own store or algorithm

Implement `Store` (`Get` and `CompareAndSet`, optionally `Updater` and
`KeyCounter`) for another backend, and run `ratelimitstore.Run` from its tests to
check the contract: absent keys, versioned updates, expiry and lost-update
freedom under concurrent writers. Implement `Algorithm` (`Name`, `Validate`,
`TTL`, `Step`) for another rule; it must be a pure function of the state and
the time, and should return the state unchanged when it refuses.

## Observability

### Metrics

`ratelimit.Register(reg)` registers the package's metrics once, under
`<namespace>_ratelimit_` with namespace `go` by default;
`ratelimit.WithNamespace("inventory")` changes it. Every limiter reports
under its name as the `limiter` label. Keys are not a label: per-tenant or
per-address keys would be unbounded cardinality.

| Metric | Type | Labels |
|---|---|---|
| `go_ratelimit_decisions_total` | counter | `limiter`, `algorithm`, `result` ∈ allowed, limited, error |
| `go_ratelimit_cas_conflicts_total` | counter | `limiter`, `algorithm` |
| `go_ratelimit_keys` | gauge, read from the store at scrape time, when it can report it | `limiter`, `algorithm` |

Common queries:

```promql
rate(go_ratelimit_decisions_total{result="limited"}[5m])
  / rate(go_ratelimit_decisions_total[5m])                    # share of calls refused
rate(go_ratelimit_decisions_total{result="error"}[5m])        # decisions the limiter could not make
rate(go_ratelimit_cas_conflicts_total[5m])                    # retries on a shared store; sustained means a hot key
```

### Alerting

`contrib/prometheus/alerts.yaml` contains the rules below in a `ratelimit`
group.

| Rule | Severity | Condition |
|---|---|---|
| `RateLimitRefusingMajority` | ticket | refused decisions exceed half of all decisions for 10m |
| `RateLimitErrors` | page | any `error` decisions for 2m |

The first indicates either a caller being throttled hard, on an inbound
limiter, or your own service over its quota, on an outbound one; in both
cases the quota or the caller needs attention rather than the limiter. The
second is a page because, with the middleware's fail-open default, a limiter
that cannot decide is enforcing nothing, and this is the only signal.

### Dashboard

`contrib/grafana/keel.json` includes a collapsed row for rate limiters, with
the refused share, decision rates by result, errors and keys tracked. It is
provided as a starting point.

## Testing

Algorithms are tested as pure functions with explicit times, covering each
rule, its `RetryAfter`, the fixed window's boundary burst and the sliding
window's absence of one. Stores run the contract in `conformance/ratelimitstore`:
absent keys, versioned create and update, expiry, forward-moving time, and
lost-update freedom under sixteen concurrent writers. The Redis store runs
the contract and the algorithms against a real server: `REDIS_ADDR` if set,
otherwise a disposable `valkey/valkey:8-alpine` container started through
testcontainers-go (`RATELIMIT_TEST_IMAGE` overrides the image) and terminated
afterwards; skipped when Docker is unavailable. CI takes this same path,
rather than a separate service container, so there is one mechanism to keep
working, not two. The limiter's own tests
cover the compare-and-set retry path under contention with a store that
hides its `Updater`, error propagation, the middleware's headers and
policies, and the breaker composition.

## Benchmarks

`go test -bench . -benchmem ./ratelimit/` on an Apple M5 Max (18 cores), Go
1.27.1, memory store, one key unless stated:

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `Allow`, GCRA, allowed | 43 | 0 | 0 |
| `Allow`, fixed window, allowed | 43 | 0 | 0 |
| `Allow`, sliding window, allowed | 43 | 0 | 0 |
| `Allow`, refused | 40 | 0 | 0 |
| `Allow`, 4096 rotating keys | 102 | 128 | 2 |
| `Allow`, GCRA, 18 goroutines on one key | 162 | 0 | 0 |

The three algorithms cost the same; the time is the map lookup and the mutex.
The rotating-keys case allocates for new records as keys cycle past the
eviction bound. The parallel figure is contention on the store's mutex for a
single key. A Redis store adds two network round trips per decision, which
dominates everything above.
