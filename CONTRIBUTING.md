# Contributing

Before opening a pull request, run what CI runs:

```sh
gofmt -l .                                   # empty
go vet ./...
go test -race -cpu 1,2,4 -count=2 -timeout 15m ./...   # 1 CPU is where scheduler-order bugs surface
go test -bench . -benchmem ./breaker/ ./ratelimit/ ./retry/
go test -v ./ratelimit/goredis/              # the Redis store against a real server: REDIS_ADDR, or a Valkey container via docker
```

The Redis store tests need a server: set `REDIS_ADDR`, or have Docker
available and they start a disposable Valkey container themselves, removing
it afterwards. They skip when neither is present.

Third-party dependencies: `prometheus/client_golang` for the metrics, and
`redis/go-redis` linked only by programs that import `ratelimit/goredis`.
The state machines themselves use the standard library alone. Keep it that
way.

Every exported identifier has a doc comment that says what goes wrong if it
is used badly, not only what it does. Unexported package-level variables and
constants carry a leading underscore; types and functions do not. No panics in library code: constructors return errors.
