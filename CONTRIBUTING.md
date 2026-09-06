# Contributing to keel

Thank you for considering it. keel is small and deliberate, and it gets
better through people using it against real dependencies and telling us
what they found. Bug reports, doc fixes, questions and code are all welcome,
and none of them needs permission first.

## Ways to help

- **Report what surprised you.** A breaker that tripped on healthy traffic, a
  limiter whose `Retry-After` was wrong, a doc paragraph that did not match
  the code. Open an issue with what you expected and what happened; a
  failing test is ideal but not required.
- **Fix the docs.** Each package's guide in `docs/` is held to its code. If
  the two disagree, the doc is wrong and a pull request that fixes it is
  always welcome, however small.
- **Pick up a roadmap issue.** The open issues describe the next primitives
  in enough detail to plan from. Comment on one before starting so nobody
  duplicates the work, and ask anything that is unclear there.
- **Review.** Reading a pull request against the design principles below and
  saying what you see is a contribution.

## Getting set up

You need Go, at the version in `go.mod`, and `make`. Then:

```sh
git clone https://github.com/mgiaccone/keel
cd keel
make          # lists the targets
make test     # the quick suite
```

The Redis store tests need a server: set `REDIS_ADDR`, or have Docker
running and `make test-redis` starts a disposable Valkey container and
removes it afterwards. They skip when neither is available, so the rest of
the suite does not depend on Redis.

## Before you open a pull request

Run `make check`. It runs exactly what CI runs: formatting, `go mod tidy`,
`go vet`, the full suite with the race detector on 1, 2 and 4 CPUs, the
benchmarks once, and the validity of the alert rules and dashboard. Green
here means green there. The single-CPU run matters: that is where
scheduler-order bugs surface.

Then, for a change of behaviour:

- Add or adjust the test for the property that changed. The suites have one
  test per documented property, a reference model and a chaos test; a new
  property gets its own test, and a change to the rules gets a change to the
  model.
- Update the package's guide in `docs/` in the same pull request. A change
  that lands without its doc is only half landed.
- If a benchmark number in a doc table moves noticeably, refresh it with
  `make bench`.

Pull requests are squashed on merge, so commit as you like while working;
the pull request title and description become the commit message, so write
them for the person reading `git log` next year. Explain why, not only what.

## What the code looks like

These are the conventions every package follows. Matching them is what makes
a change feel like it belongs; if one of them seems wrong for your case, say
so in the pull request and we will talk about it.

- **Functional options, validated together.** `New(name, opts...)` takes
  `WithX` options. An option given a value that cannot be meant makes `New`
  return an error wrapping `ErrInvalidOption` that lists every such option,
  not only the first. Every tunable is an option with a default, never a
  fixed constant, and a rule can be switched off.
- **No panics in library code.** Constructors return errors. The
  documented exceptions are `MustRegister`, mirroring Prometheus, and
  `ratelimit.MustMiddleware`; both are bootstrap conveniences that panic
  only on an argument the plain form would reject.
- **Time and randomness are injected.** `WithClock` and `WithSeed` exist so
  tests are deterministic; nothing reads `time.Now` or the global generator
  directly. The state machines use no timers.
- **Standard library only in the cores.** The third-party dependencies are
  `prometheus/client_golang` for metrics and `redis/go-redis`, linked only by
  programs that import `ratelimit/goredis`. Keep it that way.
- **Packages do not import each other.** They compose through small
  contracts: a `func(context.Context) error` seam, or an error that answers
  `Retryable() bool`. A new package follows the same rule.
- **The same outline everywhere.** `Stats` with a one-line `String`, an
  `Observer` interface, Prometheus metrics under `<namespace>_<package>_`
  with `Register` and `WithNamespace`, a guide in `docs/` on the shared
  outline, an alert group and a dashboard row in `contrib/`.
- **Doc comments say what goes wrong.** Every exported identifier explains
  the mistake it invites, not only what it does.
- **Naming.** Unexported package-level variables and constants carry a
  leading underscore; types and functions do not.

## AI assistance

keel is developed with heavy AI assistance: most of the code is written by
AI under human direction, review and testing. That is a deliberate choice,
and it extends to contributions. AI-assisted pull requests are welcome and
held to the same bar as any other: `make check` passes, the intent is clear,
the doc moves with the code, and a human stands behind the change and can
answer for it in review.

## Proposing something bigger

For a new primitive or a change to how an existing one behaves, open an
issue first and describe the problem you are solving, the API you have in
mind and how it composes with the rest. The roadmap issues are the template.
Agreeing on the shape before the code saves everyone a rewrite, and a design
that is discussed in the open is one the guide can explain later.

## Questions

Open an issue. There is no such thing as a question too small, and the
answer usually turns into a doc improvement.
