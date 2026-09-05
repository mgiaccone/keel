# ratelimit

The rate limiter package: `github.com/mgiaccone/keel/ratelimit`, with the Redis
store in `github.com/mgiaccone/keel/ratelimit/goredis`. See
[development](development.md) for how it is tested.

A rate limiter bounds how many calls *start* per unit of time, however fast
they complete; the bulkhead bounds how many are *outstanding*. It stands on
its own: hold API clients to their quotas in a middleware, pace a worker
against a third party, or compose it with the breaker on an outbound path.

```go
// A limiter is an algorithm applied to records in a store.
store, err := ratelimit.NewMemoryStore()                    // per instance
store    := goredis.NewStore(client)                        // or one quota across the fleet
limiter, err := ratelimit.New("public-api", ratelimit.GCRA(100, 20), store)   // 100/s per key, bursts of 20; nothing to close

// Inbound, as a plain net/http middleware: allowed requests carry
// X-RateLimit-Remaining; refused ones get Retry-After and 429.
mux.Handle("/v1/", ratelimit.Middleware(limiter, ratelimit.KeyByHeader("X-API-Key"))(api))

// Or decide by hand.
d, err := limiter.Allow(ctx, key)   // d.Allowed, d.Remaining, d.RetryAfter

// Outbound: compose with a breaker; refused calls are recorded as denied.
b, err := breaker.New("db-fallback",
    breaker.WithAdmission(ratelimit.Admission(limiter, "")),   // "" = one limit for the whole dependency
    ...
)
```

## Architecture

```
Algorithm   the rule, a pure function:  Step(state, now) → (state', decision)
Store       where a key's state lives:   Get(key) → (state, version, now)
                                          CompareAndSet(key, version, state', ttl)
Limiter     Get, Step, CompareAndSet; retry on a version conflict
```

An algorithm is written once, in Go, as a pure function over three integers
of state, and works against every store. A store knows nothing about rates:
it keeps those integers per key, swaps them atomically by version, expires
idle records, and owns the clock, so a fleet sharing a Redis store agrees on
time. Adding an algorithm is one pure function; adding a store is one `Get`
and one `CompareAndSet`, verified by the contract suite in
`ratelimit/storetest`.

| Algorithm | Guarantees | State | Trade-off |
|---|---|---|---|
| `GCRA(rate, burst)` | long-run average `rate`, bursts to `burst`; exactly a token bucket | 1 timestamp | The default for protecting a backend or pacing a client. |
| `FixedWindow(limit, window)` | `limit` per aligned window | start, count | Simplest to state as a quota; admits up to 2×`limit` across a boundary. |
| `SlidingWindow(limit, window)` | ≈`limit` in any window, from two aligned counters | start, current, previous | No boundary burst; a few percent off under uneven traffic. |

| Store | Scope | Cost per decision | Notes |
|---|---|---|---|
| `NewMemoryStore()` | one process | a mutex; the step runs under it, so no conflicts | LRU-evicted beyond `WithMaxKeys`; injectable clock for tests. |
| `goredis.NewStore(client)` | fleet-wide | 2 round trips: pipelined `TIME`+`HMGET`, then one Lua compare-and-set | Server time; records expire on the algorithm's TTL; Redis 5+ or Valkey. |

A refusal that leaves the state unchanged is not written, so refused calls
cost one read. On a distributed store two writers racing on one key see one
succeed and one retry; `WithMaxAttempts` bounds the retries, after which
`Allow` returns `ErrContention`, and `go_ratelimit_cas_conflicts_total` counts
them. A store that can apply the step under its own lock implements the
optional `Updater` and never conflicts; the memory store does.

## Decisions and errors

A refused call gets a `Decision` with `RetryAfter`, computed honestly by each
algorithm for its own rule and what the `Retry-After` header should say;
through `Admission` it is a `*LimitedError` matching `ErrLimited`. Nothing
waits: a call over the limit is refused immediately.

A store error is "could not decide", not "not allowed". `Allow` returns it,
`Stats.Errors` and `result="error"` count it, `Admission` fails closed and
`FailOpen(limiter, onError)` inverts that. The middleware fails open by
default, because an API that goes down with its Redis is usually the worse
outcome; `FailClosed()` answers 503 instead.

Inbound, a refusal is a 429 because the client exceeded a quota that is
theirs; outbound through the breaker it is your own capacity and becomes a
503 at your boundary. The limiter does not know which.

`Middleware` takes a `KeyFunc`: `KeyByHeader("X-API-Key")`,
`KeyByRemoteAddr()`, `KeyGlobal()`, or your own that reads the authenticated
tenant from the context. Options: `WithLimitedHandler` for a custom 429 body,
`OnError` for logging, `WithoutRemainingHeader` for endpoints that should not
reveal their limits.

## Testing

Algorithms are tested as pure functions with explicit times. Stores run the
contract in `ratelimit/storetest`: absent keys, create, version conflicts,
expiry, and lost-update-free concurrent compare-and-set. The Redis store runs
it against a real server: `REDIS_ADDR` if set, otherwise a disposable
`valkey/valkey:8-alpine` container started with the `docker` CLI (image
overridable with `RATELIMIT_TEST_IMAGE`) and removed afterwards; skipped when
Docker is unavailable.

## Observability

### Metrics

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

### Alerting

The rules live in `contrib/prometheus/alerts.yaml`, in the `ratelimit` group,
ready for `promtool check rules`.

| Rule | Severity | Fires when |
|---|---|---|
| `RateLimitRefusingMajority` | ticket | more than half of decisions refused for 10m |
| `RateLimitErrors` | page | a limiter could not decide: its store is unreachable, and the middleware fails open |

The first is a ticket: on an inbound limiter it means a client is being
throttled hard, on an outbound one that you are over quota; either way
someone should look at who and at whether the quota is right. The second is a
page, because with the fail-open default in `Middleware` a limiter that cannot
decide is the only signal that limits are not being enforced.

### Dashboard

`contrib/grafana/keel.json`, the dashboard shared with the breaker, has a
collapsed "Rate limiters" row filtered by the `Limiter` variable:

| Panel | Type | Shows |
|---|---|---|
| Decisions per second, by result | time series, stacked | Total height is demand; the limited band is what the quota turns away. |
| Refused share | time series | Limited decisions over all decisions, per limiter. |
| Limiter errors | time series | Decisions that could not be made. With the fail-open middleware this is the only sign limits are not enforced. |
| Keys tracked | bar gauge | Records each memory store holds, bounded by `WithMaxKeys`. Not reported for Redis stores. |
