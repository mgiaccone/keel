# keel

[![ci](https://github.com/mgiaccone/keel/actions/workflows/ci.yml/badge.svg)](https://github.com/mgiaccone/keel/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/mgiaccone/keel.svg)](https://pkg.go.dev/github.com/mgiaccone/keel)

Resilience primitives for Go. Requires Go 1.27.

| Package | Bounds |
|---|---|
| [`breaker`](docs/breaker.md) | What happens to calls: circuit breaker tripped by consecutive failures or an error rate, bulkhead (static or adaptive), per-call timeout, recovery ramp, admission veto. |
| [`ratelimit`](docs/ratelimit.md) | How fast calls start: GCRA, fixed window or sliding window over a memory or Redis store, as a `net/http` middleware or composed with the breaker. |
| [`retry`](docs/retry.md) | How many times a call is attempted: four jittered schedules, a retry budget, `Retry-After`, an `http.RoundTripper`. |

## Install

```sh
go get github.com/mgiaccone/keel@latest
```

## Quick start

### Circuit breaker

```go
b, err := breaker.New("db-fallback",
    breaker.WithIsFailure(func(err error) bool {           // "no such row" is not a failure
        return err != nil && !errors.Is(err, sql.ErrNoRows) && !errors.Is(err, context.Canceled)
    }),
    breaker.WithTimeout(2*time.Second),
    breaker.WithMaxInFlight(16),
)

row, err := b.Do(ctx, func(ctx context.Context) (Row, error) { return db.Get(ctx, key) })
if errors.Is(err, breaker.ErrOpen) || errors.Is(err, breaker.ErrProbeLimit) || errors.Is(err, breaker.ErrBulkhead) {
    // fail fast; the call never ran
}
```

### Rate limiter

```go
store, err := ratelimit.NewMemoryStore()                          // or goredis.NewStore(client) for one quota fleet-wide
limiter, err := ratelimit.New("public-api", ratelimit.GCRA(100, 20), store)

mux.Handle("/v1/", ratelimit.Middleware(limiter, ratelimit.KeyByHeader("X-API-Key"))(api))
```

### Retrier

```go
r, err := retry.New("db-fallback", retry.Exponential(50*time.Millisecond, 2*time.Second),
    retry.WithMaxAttempts(4),
    retry.WithBudget(ratelimit.Admission(limiter, "")),      // at most so many retries per second, fleet-wide with Redis
)

row, err := r.Do(ctx, func(ctx context.Context) (Row, error) {
    return b.Do(ctx, func(ctx context.Context) (Row, error) { return db.Get(ctx, key) })   // a breaker refusal is never retried
})

transport, err := retry.NewTransport(r, nil)                     // retries 408/429/502/503/504 on requests net/http would replay
client := &http.Client{Transport: transport}
```

## Documentation

- [Circuit breaker](docs/breaker.md): when a breaker is the right tool, every knob and why, guarantees, metrics, alerts, dashboard, benchmarks.
- [Rate limiter](docs/ratelimit.md): the algorithm-over-store design, algorithms, stores, Redis, the middleware, metrics, alerts, dashboard.
- [Retrier](docs/retry.md): the bounds that keep retries safe, schedules, the budget, `Retry-After`, the HTTP replay rule, metrics, alerts, dashboard.

Runnable examples with verified output live in each package's `example_test.go`;
`CONTRIBUTING.md` has how to get set up, what help is wanted and the
conventions the code follows; `make check` runs what CI runs.

## Licence

MIT, see `LICENSE`.
