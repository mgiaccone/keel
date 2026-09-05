# keel

[![ci](https://github.com/mgiaccone/keel/actions/workflows/ci.yml/badge.svg)](https://github.com/mgiaccone/keel/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/mgiaccone/keel.svg)](https://pkg.go.dev/github.com/mgiaccone/keel)

Resilience primitives for Go, built for the person on call: every state is
readable in one log line and one metric, and every knob's doc says what goes
wrong if you set it badly. State lives in a single goroutine reached through
channels; no mutexes, no atomics, no timers in the cores. Requires Go 1.27.

| Package | Bounds |
|---|---|
| [`breaker`](docs/breaker.md) | What happens to calls: circuit breaker, bulkhead (static or adaptive), per-call timeout, recovery ramp, admission veto. |
| [`ratelimit`](docs/ratelimit.md) | How fast calls start: GCRA, fixed window or sliding window over a memory or Redis store, as a `net/http` middleware or composed with the breaker. |

```sh
go get github.com/mgiaccone/keel@latest
```

## Circuit breaker

```go
b, err := breaker.New("db-fallback",
    breaker.WithIsFailure(func(err error) bool {           // "no such row" is not a failure
        return err != nil && !errors.Is(err, sql.ErrNoRows) && !errors.Is(err, context.Canceled)
    }),
    breaker.WithTimeout(2*time.Second),
    breaker.WithMaxInFlight(16),
)

row, err := b.Do(ctx, func(ctx context.Context) (Row, error) { return db.Get(ctx, key) })
if errors.Is(err, breaker.ErrOpen) || errors.Is(err, breaker.ErrBulkhead) {
    // fail fast; the call never ran
}
```

## Rate limiter

```go
store, err := ratelimit.NewMemoryStore()                          // or goredis.NewStore(client) for one quota fleet-wide
limiter, err := ratelimit.New("public-api", ratelimit.GCRA(100, 20), store)

mux.Handle("/v1/", ratelimit.Middleware(limiter, ratelimit.KeyByHeader("X-API-Key"))(api))
```

## Documentation

- [Circuit breaker](docs/breaker.md): when a breaker is the right tool, every knob and why, guarantees, benchmarks.
- [Rate limiter](docs/ratelimit.md): the algorithm-over-store design, algorithms, stores, Redis, the middleware.
- [Observability](docs/observability.md): metrics, alert rules, the Grafana dashboard.
- [Development](docs/development.md): how it is tested and how to verify a change.

Runnable examples with verified output live in each package's `example_test.go`.

## Status and licence

`v0`: the API is settling and may still change between minor versions;
`v1.0.0` will fix it. MIT licence, see `LICENSE`.
