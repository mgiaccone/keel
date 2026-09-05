# Development

```sh
gofmt -l .                                   # empty
go vet ./...
go test -race -cpu 1,2,18 -count=5 ./...     # 1 CPU is where scheduler-order bugs surface
go test -bench . -benchmem ./breaker/
go test -v ./ratelimit/goredis/              # the Redis store against a real server: REDIS_ADDR, or a Valkey container via docker
```

The suite has three layers beyond the per-property tests:

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

The rate limiter has its own tests for each algorithm's rule and `RetryAfter`,
the fixed window's boundary burst and the sliding window's absence of one, key
isolation, LRU eviction, concurrent accounting, and the breaker integration: a denied call
never runs, an open circuit wins over the limiter, and a failing limiter fails
closed.

Every blocking wait in the suite carries a deadline, so a deadlock fails at a
named line rather than hanging until the `go test` timeout. `-short` trims the
model and chaos iterations.

Third-party dependencies: `prometheus/client_golang` for the metrics, and
`redis/go-redis` linked only by programs that import `ratelimit/goredis`. The
state machines themselves use the standard library alone.
