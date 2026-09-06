# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

keel: resilience primitives for Go (module `github.com/mgiaccone/keel`, Go 1.27). Three independent
packages — `breaker` (circuit breaker), `ratelimit` (rate limiter), `retry` (attempt bounds and
hedging) — each usable alone, composing through small contracts rather than importing each other.
`README.md` has the quick-start snippets; `CONTRIBUTING.md` is the human-facing onboarding doc
(how to get set up, what help is wanted, the rationale behind the rules below) and is not required
reading here — everything operative for writing code in this repo is stated directly below.

## Conventions every package follows

Follow these when adding or changing anything in `breaker`, `ratelimit` or `retry`; they are what
makes a change feel like it belongs, and a reviewer will read a violation as a mistake, not a choice.

- **Functional options, validated together.** `New(name, opts...)` takes `WithX` options. An option
  given a value that cannot be meant makes `New` return an error wrapping `ErrInvalidOption` that
  lists every such problem, not just the first. Every tunable is an option with a documented
  default, never a fixed constant, and a rule can be switched off (see e.g. `breaker.WithFailureThreshold(0)`).
- **No panics.** Constructors return errors. The only exceptions are `Must*` functions that mirror
  `prometheus.MustRegister` for bootstrap code that treats the error as fatal (`MustRegister` in
  each package, `ratelimit.MustMiddleware`) — never panic anywhere else.
- **Time and randomness are injected.** Every package has `WithClock` and, where it uses randomness,
  `WithSeed`; the per-instance RNG is seeded from the global `math/rand/v2` generator once at
  construction when `WithSeed` isn't given, and never read again after that, so nothing reads
  `time.Now` directly and every draw after construction is reproducible. The state machines
  (`breaker`'s core, `breaker`'s error-rate window) use no timers — they roll forward lazily against
  the clock when a call or inspection arrives. This is what makes the test suites deterministic.
- **Standard library only in the cores.** The only third-party dependencies are
  `prometheus/client_golang` (used by all three packages) and `redis/go-redis` (linked only by
  programs that import `ratelimit/redistore`). Don't add another one without a strong reason.
- **Packages do not import each other.** `breaker`, `ratelimit` and `retry` compose only through the
  contracts in the Architecture section below. A new package follows the same rule.
- **The same outline everywhere** — see "The shared outline" below.
- **Doc comments say what goes wrong.** Every exported identifier's comment explains the mistake it
  invites, not only what it does — see almost any `With*` option in `breaker/breaker.go` for the
  pattern (the failure mode is stated before or after the mechanics, not left implicit).
- **No narrative or section comments.** No ASCII-art dividers (`// --- Section ---`), no banners
  grouping several functions or restating what a following block of code already says in its own
  names. A comment earns its place only when the code beside it isn't self-explanatory — a
  non-obvious reason, a constraint the reader can't see locally (e.g. `hedge.go`'s note that `os.Exit`
  runs no deferred function, or `breaker.go`'s note on why the receive after a channel send can't be
  raced against `ctx`). Doc comments on exported identifiers are the one standing exception; they're
  expected regardless, per the rule above.
- **A blank line between a function's distinct phases.** Construct/setup, then each separately
  meaningful loop or check, gets its own paragraph — not because of length, a short function is
  often one phase and needs nothing, but because the reader should be able to tell where one step
  ends and the next begins without parsing every line. A constructor's own run of `if cfg.X == bad
  { errs = append(...) }` validation checks is one phase, not one per check — don't break those
  apart. This applies to production code the same as tests; it isn't a test-only habit.
- **Naming.** Unexported package-level variables and constants carry a leading underscore
  (`_defaultErrorRateBuckets`, `_bufferLimit`); types and functions do not.

## Commands

```sh
make                # list all targets
make test           # quick suite (-short trims model/chaos iterations)
make test-race      # full suite, race detector, 1/2/4 CPUs, twice — what CI actually gates on
make test-redis      # ratelimit/redistore against a real server: REDIS_ADDR, or a disposable
                     # Valkey container via testcontainers-go (skips if neither is available)
make bench           # benchmarks with allocations (source of the tables in docs/)
make bench-smoke     # benchmarks compile and run once, fast
make fmt / fmt-check # gofmt, in place / check-only
make vet
make tidy / tidy-check
make check           # everything CI runs: fmt-check tidy-check vet test-race bench-smoke contrib-check
```

Single test or package, without the Makefile:

```sh
go test -run '^TestName$' ./breaker/            # one test
go test -race -run '^TestName$' -v ./retry/     # one test, race detector, verbose
go test -race -cpu 1,4 -count=2 ./ratelimit/... # one package's suite the way CI runs it
```

`make check` is the single gate to run before considering anything done; a change that passes it
locally passes CI (`.github/workflows/ci.yml` just runs `make check` — no separate service
container; `ratelimit/redistore`'s own tests start their own Valkey via testcontainers-go, same as
running it locally).

## Versioning

Signed annotated tags (`v0.1.0`, `v0.2.0`, …) mark releases. An additive or breaking change to a
package's exported API gets a **minor** version bump; a fix or internal change with no exported API
change does not need a new tag. Tag only when asked to.

## Architecture

### Cross-package composition, not imports

`breaker`, `ratelimit` and `retry` never import each other. They compose through two small
contracts an error can implement, and one function-shaped adapter:

- `Retryable() bool` — `retry` looks for this on any error `fn` returns. The breaker's refusals
  (`ErrOpen`, `ErrProbeLimit`, `ErrBulkhead`, `ErrStopped`) answer `false`; `ratelimit.LimitedError`
  answers `true`.
- `RetryDelay() time.Duration` — implemented by `ratelimit.LimitedError` and by `retry.StatusError`
  (from the HTTP transport, for `Retry-After`); `retry` waits at least that long before the next
  attempt.
- `ratelimit.Admission(limiter, key)` / `ratelimit.AdmissionGlobal(limiter)` return a plain
  `func(context.Context) error`, the exact shape `breaker.WithAdmission` and `retry.WithBudget`
  both take. This is how a limiter becomes a breaker's veto or a retry budget without either
  package depending on `ratelimit`.

### breaker: single goroutine, reached only through channels

The circuit's entire mutable state (`machine` in `breaker.go`) lives in one goroutine (`core.run`)
and is touched nowhere else — no mutexes, no atomics, no timers in the core. Callers talk to it
over four channels (`admit`, `settle`, `inspect`, `quit`). `Do` is acquire (send a token, get back
an admission decision and a generation number) → run `fn` → release (report the outcome, block for
an ack that it was applied). The generation counter is bumped on every state transition, so a call
admitted before a trip is recognized as stale when it settles and cannot wrongly act on the new
state (e.g. close a circuit that has since reopened). `*Breaker` (the handle) and `*core` (what the
goroutine holds) are deliberately split: the goroutine never references the handle, so a dropped
`Breaker` becomes unreachable and `runtime.AddCleanup` stops the goroutine without a reference cycle.

Two independent trip rules can open the circuit: consecutive failures (`WithFailureThreshold`) and
a bucketed, lazily-rotated error-rate window (`WithErrorRate`) — either firing opens it. The
bulkhead (fixed `WithMaxInFlight` or AIMD `WithAdaptiveInFlight`) and `WithRecoveryRamp` interact:
a ramp only seeds the cap on close, after which AIMD's own additive-increase/multiplicative-decrease
rule owns further growth.

### ratelimit: algorithm over store

An `Algorithm` is a pure function, `Step(state, now) → (state', decision)`, over an opaque
three-`int64` `State`; a `Store` is where a key's `State` lives, updated atomically via
`Get`/`CompareAndSet` with an optimistic version. Algorithms know nothing about storage; stores know
nothing about rates. `Limiter.Allow` glues them: if the store implements `Updater` (only
`MemoryStore` does — the step runs under its own lock, so it never conflicts), the step runs
in-process with no retry; otherwise (e.g. `ratelimit/redistore`) `Allow` retries the read-step-CAS
round on a version conflict, up to `WithMaxAttempts`. `conformance/ratelimitstore.Run` is a
store-agnostic contract suite — any new `Store` implementation should pass it, and `redistore` does
via `redistore_test.go`. `Middleware`/`KeyFunc` compose a limiter into `net/http`; `Admission`/
`AdmissionGlobal` compose it into `breaker`/`retry` (see above).

### retry: pure schedules, two execution paths

A `Backoff` is a pure function, `Delay(retry, previous, u) → time.Duration`, where `u` is a uniform
draw the retrier's own seeded RNG supplies — schedules never read a clock or global randomness, so
they're tested with explicit inputs. There are two separate implementations of the attempt loop:
`do()` in `retry.go` is the sequential case, and `hedged()` in `hedge.go` is a full concurrent
implementation for `WithHedge` (attempt goroutines report on a buffered channel, a hedge timer is
armed/re-armed by hand) — they aren't unified because hedging's concurrency doesn't fit the
sequential loop's structure. `NewTransport` wraps a `Retrier` as an `http.RoundTripper`, replaying
only what `net/http` itself would replay (method, idempotency headers, body replayability) and
buffering or draining response bodies around retries and hedge losers so nothing leaks; under a
hedge, the context of whichever attempt's result is actually returned (the winner, or the last
failure on exhaustion) is the one left alive past the call.

### The shared outline

Every package follows the same shape: a `Stats` struct with a one-line `String()`, an `Observer` interface streaming
every event synchronously (implementations must return promptly and never call back into the type
that's calling them), package-level Prometheus collectors published via `Register`/`MustRegister`/
`WithNamespace` through the shared `internal/promutil.Register` (which is all-or-nothing — a failed
registration unregisters whatever it had already added), a guide in `docs/<package>.md` that must
move with any behavioral change in the same PR, and a matching alert group and Grafana dashboard row
under `contrib/`.

### Testing conventions

Each package's test file wraps the type under test with a fake clock, via `WithClock`, in a small
`harness`/`newHarness` helper — reuse it rather than building ad hoc setup. Beyond one test per
documented property, look for a reference-model test and a chaos test per suite (e.g.
`retry/hedge_test.go`'s `TestHedgeStatsIdentitiesUnderChaos`) — a change to a rule should extend the
model, not just add an isolated test.

### Test shapes and conventions

- **One test per SUT concern by default; name a specialist test for its behavior and outcome, not
  just its subject.** The default is 1:1: one production concern, one test surface — which is why
  neither `Store` implementation has its own `TestGet`/`TestCompareAndSet`, `ratelimitstore.Run`
  already is that test, shared. When a test genuinely can't be 1:1 — it checks a property that cuts
  across the SUT's functions, or is specific to one implementation among several sharing a contract
  — name it for the behavior it exercises and the outcome it proves, not for its subject alone.
  `TestGCRAThroughRedis` used to sit next to `TestEveryAlgorithmWorksOnRedis` with no hint that one
  goes deep on refill timing against the Redis server's own clock and the other goes wide, shallowly,
  across all three algorithms to catch a Lua-script field-encoding bug GCRA alone could never expose
  — read together they sounded redundant. Renamed to
  `TestGCRARefillsAgainstTheServerClock`, the distinguishing property is in the name, not something a
  reader has to open both bodies to find. The same fix applies package-wide: prefer
  `TestStaleOutcomeDoesNotCloseOrFreeAProbeSlot` over `TestStaleOutcomeIsIgnored` sitting near
  `TestObserverIsToldAboutAStaleOutcomeEvenThoughItDoesNotCount` — two genuinely different
  properties of the same event, not two versions of one check. And check for the version of this that
  isn't a naming problem at all: `TestSlidingWindowRetryAfterIsHonest` used to exist beside
  `TestRetryAfterIsExactForEveryAlgorithm`'s `sliding_window` case with identical parameters and an
  identical assertion — true duplication, not a naming gap, and was deleted rather than renamed. A
  catch-all named after the file it lives in, not a behavior (`TestMetrics`, once bundling
  double-registration, per-result counters, the lint pass and namespace handling into one function),
  is the same problem at file scope; split along the property lines `breaker/prometheus_test.go`
  already uses (`TestRegisterTwiceFails`, `TestMetricsLint`, `TestRegisterRejectsBadNamespace`, …).
- **Table-driven tests, when the shape fits:** three or more cases that share one assertion against
  different literal inputs. `map[string]X` for a simple value per case; `[]struct{name string; ...}`
  when order matters or fields are compound. Use `t.Run(name, ...)` per case when a case needs
  isolation (a constructor call whose panic/fatal must not kill the whole test, or a case worth
  running alone with `-run`) — e.g. `ratelimit.TestLimiterInvalidOptions`,
  `ratelimit.TestAlgorithmValidation`; use a bare loop with a self-describing `t.Errorf` when it's a
  scalar comparison — e.g. `retry/prometheus_test.go`'s namespace table. Both are in active,
  consistent use across all three packages.
- **When a table does *not* fit, even with 3+ superficially similar checks:** a sequence where each
  case's expected result depends on the cumulative state left by the previous one
  (`breaker.TestAdaptiveInFlightAIMD`,
  `breaker.TestErrorRateBucketsAgeOutIndependentlyAtTheirOwnBoundaries`), or where the assertion
  *logic* differs between cases, not just the data (`ratelimit.TestFixedWindowResetsAtBoundary`).
  Forcing these into a table hides the sequential dependency or produces a table of closures with
  nothing shared to extract. `conformance/ratelimitstore.Run`'s seven named subtests are the
  clearest example: each runs a genuinely different sequence of operations, not the same operation
  over different data. Below three cases, a table adds indirection without paying for itself — a
  bare `if`/`if` reads at least as clearly.
- **File naming:** a test file mirrors its production file 1:1 (`memorystore.go`↔
  `memorystore_test.go`), with two exceptions: `example_test.go` (Go's own idiom for testable
  examples, which never needs a pair), and a file that declares only a contract — an interface with
  no concrete behavior of its own, like `ratelimit/store.go`'s `Store`/`Record`/`KeyCounter`/
  `Updater` — which is verified through its implementations' tests and the conformance suite
  instead, not a test file of its own. If a production file's tests must split across an internal
  (`package foo`) and an external (`package foo_test`) file — typically because a shared
  test-support package imports back into `foo`, an import cycle for an internal file — name the
  external one for its role, not for a production file it doesn't pair with (see
  `ratelimit/store_contract_test.go`, whose name is accurate here because `store.go` holds nothing
  but the contract).
- **A reusable conformance-test package** (one any implementation of a documented interface,
  including a third party's, runs against itself) lives under the shared top-level `conformance/`
  directory, named `<subject><concept>` (`ratelimitstore`, not a bare `store`), not nested under the
  package whose interface it tests — nesting it there reads as if it might be a peer implementation,
  which is why `ratelimit/storetest` moved out from beside `ratelimit/redistore`, a real one.
  `conformance/` mirrors the standard library's own `testing/fstest`/`testing/iotest` — one shared
  namespace, not one per tested package — under a name that doesn't collide with the stdlib's own
  `testing` package the way a top-level `testing/` directory would. It must stay outside
  `internal/`: that visibility rule would silently block the third-party implementers it's
  documented for from importing it at all.
