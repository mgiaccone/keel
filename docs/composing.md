# Composing

keel gives you four independent pieces and no import that stacks them for you — they compose
through the contracts in [`.claude/CLAUDE.md`](../.claude/CLAUDE.md)'s Architecture section
(a `func(context.Context) error` veto, and errors answering `Retryable() bool` /
`RetryDelay() time.Duration`), never by one package importing another. That freedom comes with
a cost: nothing enforces the order you nest them in, and the wrong order compiles, passes
review and fails only under load. This page is the rule.

## The stack

```
rate limit (inbound)  →  retry  →  breaker (timeout + bulkhead)  →  the call
```

| Layer | Package | Bounds |
|---|---|---|
| Inbound admission | `ratelimit` (`Middleware`) | How fast requests are accepted at all |
| Attempts | `retry` | How many times one operation is tried |
| Per-attempt | `breaker` | Whether an attempt runs, and how long it may take |
| Outbound budget | `ratelimit` (`WithBudget`) | How many retries the fleet may spend |

`ratelimit` appears twice, in two different roles — see point 5 below; they are not the same
limiter. A future load-shedding package sits above the inbound limiter in this stack, refusing
work before it spends a client's quota; it does not exist yet (tracked in issue #4, with the
design still under review in `docs/design/shedding.md`), so treat the top of this diagram as
provisional until that lands.

## Why each layer sits where it does

1. **Retry sits outside the breaker.** Every `breaker` refusal — `ErrOpen`, `ErrProbeLimit`,
   `ErrBulkhead`, `ErrStopped` — is the unexported type `refusal`, which answers
   `Retryable() bool { return false }` ([`breaker/breaker.go:56-66`](../breaker/breaker.go)).
   `retry.classify` consults that contract *before* `WithRetryIf`
   ([`retry/retry.go:512-522`](../retry/retry.go)) and returns as soon as it finds it, so
   `WithRetryIf` cannot override a refusal even if it tries — a refusal always ends the call as
   `Aborted`, sequentially at `retry.go:477-479` and under hedging at `hedge.go:162-166`. This
   is where "a breaker refusal is never retried" (the README) is actually enforced.

   Composing them the other way — a breaker wrapping a retrier — does not merely lose that
   guarantee, it breaks four things at once: the breaker now counts one outcome per whole
   *operation* instead of per call, so its consecutive-failure run and error-rate window trip
   on a completely different cadence than they were sized for; `WithTimeout` (point 2) now
   bounds the entire retry loop instead of one attempt; the bulkhead permit is held across
   every backoff sleep (point 3); and while half-open, the single probe slot is held for the
   whole loop instead of one call, so nothing else can probe until every retry has run out.

2. **The breaker owns the per-attempt timeout; the caller's context owns the total.**
   `breaker.WithTimeout` derives the context `fn` receives, once per admitted call
   ([`breaker/breaker.go:871-875`](../breaker/breaker.go)), and the clock starts only after any
   `WithAdmission` veto has returned (`:868`). `retry` has no timeout option at all — its only
   bounds are the attempt cap and whatever deadline the caller's own context carries
   ([`docs/retry.md`](retry.md), "When to use it"). A total-operation bound is therefore always
   a `context.WithTimeout` wrapped around the outermost `r.Do` call, not a knob inside either
   package. Skip it and the worst case is `MaxAttempts × breaker timeout`, plus the sum of the
   schedule's waits — unbounded in practice, since nothing stops it early.

   One consequence of adding that outer bound is worth knowing before you do: if it expires
   while an attempt is in flight, `fn`'s context reports the same `context.DeadlineExceeded`
   the outer one does, not `context.Canceled` — Go's own `context` propagates a parent's
   deadline error to a still-running child rather than turning it into a cancellation. The
   breaker's default `IsFailure`, `err != nil && !errors.Is(err, context.Canceled)`
   ([`breaker/breaker.go:565`](../breaker/breaker.go)), exempts `Canceled` but not
   `DeadlineExceeded`, so **this counts as a failure** against the consecutive-failure run and
   the error-rate window — even though the caller gave up, not the backend. `retry` itself
   gets this right (its own first check is the outer `ctx.Err()`, which ends the call as
   **canceled** before `classify` ever runs — see "The attempt loop" in
   [`docs/retry.md`](retry.md)), but only after the breaker has already recorded the failure.
   On a high-volume path this is the same distortion `WithErrorRate` exists to smooth out
   ([`docs/breaker.md`](breaker.md), "The error-rate window"); on a low-volume one, size
   `WithFailureThreshold` with it in mind.

3. **Bulkhead permits and retries: keel releases the permit before it waits.**
   `breaker.Do` frees the in-flight slot in a `defer` that blocks until the state goroutine
   acknowledges it ([`breaker/breaker.go:861`](../breaker/breaker.go)), so the permit is gone
   before `Do` returns to its caller — and therefore before an enclosing retrier computes the
   next backoff and sleeps. **A retry waits outside the permit, never holding it.** Two things
   follow directly: size `WithMaxInFlight` from concurrent *attempts*, not concurrent
   *operations*, since a caller mid-backoff holds nothing; and a fleet that is entirely in
   backoff occupies none of its own bulkhead, which is what leaves headroom for the probe that
   eventually closes the circuit. Nest it backwards and every sleeping caller keeps its permit,
   so callers arriving at the cap get `ErrBulkhead` because of retries that are doing nothing
   but waiting. One thing does stay inside the permit either way: the admission veto runs while
   the call holds its slot ([`breaker/breaker.go:395-397`](../breaker/breaker.go)), so a slow
   distributed limiter consumes bulkhead capacity like any other part of the call.

4. **Rate limiter placement is three separate jobs behind one type.** `ratelimit.Admission`
   and `ratelimit.AdmissionGlobal` return the same `func(context.Context) error` shape that
   both `breaker.WithAdmission` and `retry.WithBudget` accept, and it is tempting to reach for
   one limiter and wire it everywhere. Don't — the jobs are different promises, not the same
   check in three places:

   - **`ratelimit.Middleware`, outermost, inbound.** A promise made *to a caller*: their quota.
     Refuses before any dependency is chosen, at the cost of a header parse.
   - **`retry.WithBudget`, on the retry loop.** Bounds the fleet's own *retries*, per process
     with a memory store or fleet-wide with `redistore` — the bound that still holds when every
     instance is retrying at once during an outage ([`docs/retry.md`](retry.md), "When to use
     it").
   - **`breaker.WithAdmission`, per dependency.** A veto on calls to *this backend specifically*,
     running after the circuit admits the call and inside the bulkhead permit.

   Each is a promise about a different thing — a quota owed to a caller versus a bound on your
   own amplification — so each gets its own named limiter instance.
   `docs/breaker.md`'s naming rule (one live instance per name; two sharing a name merge their
   metric series) applies just as much here: three limiters sharing one name silently become
   one budget.

## The whole stack

```go
inbound, err := ratelimit.New("public-api", ratelimit.GCRA(200, 50), inboundStore)
mux.Handle("/v1/", ratelimit.MustMiddleware(inbound, ratelimit.KeyByHeader("X-API-Key"))(api))

budget, err := ratelimit.New("catalogue-retries", ratelimit.GCRA(10, 20), budgetStore)
b, err := breaker.New("catalogue",
    breaker.WithTimeout(2*time.Second),   // bounds one attempt
    breaker.WithMaxInFlight(16),          // bounds concurrent attempts, not concurrent operations
)
r, err := retry.New("catalogue", retry.Exponential(50*time.Millisecond, 2*time.Second),
    retry.WithMaxAttempts(4),
    retry.WithBudget(ratelimit.AdmissionGlobal(budget)), // bounds the fleet's retries, not this call
)

ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // bounds the whole operation
defer cancel()
row, err := r.Do(ctx, func(ctx context.Context) (Row, error) {
    return b.Do(ctx, func(ctx context.Context) (Row, error) { return db.Get(ctx, key) }) // a breaker refusal is never retried
})
```

`retry/example_test.go`'s `Example_composition` is the compiled, verified version of this
stack, with `Stats` printed from both the breaker and the retrier.

## Where a fallback reader sits

A `fallback.Reader` sits *inside* the stack above, once per source, not in place of it: each
source it reads from — the fast one and the origin — gets its own breaker, wired at the call
site with `fallback.GuardedStore`/`fallback.GuardedSource`
([`docs/fallback.md`](fallback.md), "With a circuit breaker"). Never one breaker shared across
both sources — a failing fast source would then trip the circuit in front of the origin too,
which is the one case a fallback reader exists to survive. A retrier, if the origin needs one,
wraps its guard's breaker from the outside, exactly as point 1 says a retrier wraps any other
breaker — never the other way around: the reader itself has no opinion about retries or rate
limits, only about which of its two sources answers.

## What each layer refuses

This is the complete cross-package reference: every error origin under `Do`, `Allow` or
`Get` any of the four packages can hand back, and what a caller composing them should do
about each. Each package's own error section links here for the full picture; this table is
the one place that covers all four in one view.

The governing rule behind the `Retryable()` column: an error a package originates itself as
a definitive verdict — why the operation didn't run, and whether trying again could help —
implements the contract. An error merely passed through from a pluggable dependency (`fn`,
a `Store`, an origin `Source`) does not, because the package composing it cannot know in
general whether *that* failure is transient — see the last two rows.

| Error | From | `Retryable()` | What a caller does |
|---|---|---|---|
| `ErrOpen`, `ErrProbeLimit`, `ErrBulkhead`, `ErrStopped` | `breaker` | false | Fail fast. An enclosing `retry.Do` stops on it by contract; don't loop around it by hand. |
| `*ratelimit.LimitedError` | `ratelimit.Admission`/`AdmissionGlobal` | true, with `RetryDelay()` | An enclosing `retry.Do` waits at least `RetryAfter`. Inbound, `Middleware` sets `Retry-After` on the response instead. |
| A budget veto's error | `retry.WithBudget` | n/a — ends the call as **budget** | `Do` returns the dependency's *last* error, not the veto's; the veto's own error goes only to the hook and the observers. |
| `retry.Permanent(err)` | a caller, wrapping its own `fn`'s error | false | `Do` stops immediately and returns `err` itself unwrapped when `Permanent` is the outermost wrapper — mark a non-idempotent write or a definitive "no" this way, don't rely on `WithRetryIf`. |
| `*retry.StatusError` | `retry.NewTransport`, for a retried HTTP response | true, with `RetryDelay()` from `Retry-After` | Never seen by a caller — the transport returns the final `*http.Response` once retries end, not this error. Exported only so `WithOnRetry`/`WithRetryIf` can recognise it. |
| `ratelimit.ErrContention`, or a `Store`'s own error | `ratelimit.Allow`, wrapped | neither method — falls to `retry`'s default (retries everything) | Deliberately one undifferentiated "could not decide" bucket, distinct from a refusal ([`docs/ratelimit.md`](ratelimit.md), "Errors") — `ratelimit` cannot know in general whether a given `Store`'s failure is transient. Supply your own `WithRetryIf` if the default is wrong for your `Store`. |
| `*fallback.PanicError` | `fallback.Reader.Get`, a recovered loader panic | false | A bug in the loader, not a transient condition. An enclosing `retry.Do` stops on it by contract, same as a breaker refusal. |

Two rows are deliberately absent because there is nothing package-specific to say: `fn`'s own
error from `breaker.Do` and the origin's own error from `fallback.Reader.Get` are both
returned completely unchanged — carrying whatever `Retryable`/`RetryDelay` answer *they*
came with, or none — because both packages compose an arbitrary caller-supplied dependency
they have no way to classify themselves.
