# Fallback Reader

Package `github.com/mgiaccone/keel/fallback`. A read from two sources — a fast one that may be
wrong, and an authoritative one behind per-key single-flight — with a policy you supply deciding
which one answers.

## Quick start

```go
cache, err := fallback.NewMemoryStore[string, Product]()      // per process; fallback/redistore for one copy fleet-wide
read, err := fallback.New("catalog", cache, fallback.SourceFunc[string, Product](repo.Find),
    fallback.WithPolicy(func(p Product, now time.Time) fallback.Verdict {
        if now.Sub(p.FetchedAt) < 30*time.Second {
            return fallback.Serve
        }
        return fallback.LoadOrServe                            // stale beats an error if repo is down
    }),
)

product, found, err := read.Get(ctx, id)
```

`fallback/example_test.go` contains a complete two-breaker composition, with verified output.

## When to use it

Two things go wrong on a plain read-through cache built by hand. A hot key expiring turns into a
stampede: every caller that misses at the same instant runs the same query, and nothing collapses
them into one. An outage turns into errors: the fast source holds a perfectly good answer from a
minute ago, and the code falls through to the database anyway because it only knows fresh or
absent, not "acceptable for now."

`fallback` bounds both. It is not a cache in the cache-aside/write-through/write-behind sense —
it owns no write path, and offers no `Set` to callers. It is read-through only, because that is the
only read topology a library can implement at all: cache-aside means the *application* owns the
load, which forecloses coalescing it or serving a stale answer while it runs. Refresh-ahead
(stale-while-revalidate) is in scope, read-triggered, with no sweeper and no timers. Write-through
and write-behind are permanently out of scope: they require becoming the write path — ordering,
durability, lost-update semantics — which is a consistency product, not a resilience one.
`Invalidate` is the only write-shaped hook, for a caller's own write path to use.

It also does not presume elapsed time is what makes an answer acceptable. A `Policy` judges the
value the fast source returned however the caller's domain actually decides that — a generation
counter, an ETag, a version an origin is authoritative for — with a time-based check as one
possible policy among others, never privileged over the rest.

## How it works

A `Reader` is a `Source` (an authoritative origin) and a `Store` (a fast source it may fill and
evict), joined by a `Policy`.

```
Source    answers reads:                    Get(key) → (value, found, err)
Store     a Source the reader may write:    Set(key, value), Delete(key)
Policy    judges a Store's answer:          Policy(value, now) → Verdict
Reader    Store.Get, Policy, load from Source when required, write back
```

`found=false` with a nil `err` is the ordinary shape of "not here" on both sides; folding it into
the error is what would make a breaker wrapping either source miscount an empty key space as an
outage. This is why `Get` returns `(V, bool, error)` on both interfaces, stated once instead of
twice.

`Get` consults the fast source first. Absent, or an error from it, always loads from the origin.
Present, the policy decides: serve it outright, serve it and start one background refresh, load and
serve the stale value if the load fails, or load and fail if it does. Every load — blocking or
background — runs through per-key single-flight: concurrent callers needing the same key's load
join one in-flight call rather than each starting their own. A blocking load's single-flight is per
process; a background refresh's is per process too, unless a `Leaser` is configured
(`WithLease`), in which case it is fleet-wide for as long as the lease holds.

No negative caching: a failed load never writes and never deletes. The one write outside a
successful load is the origin authoritatively reporting a key gone, which deletes the fast source's
copy — without it, a deleted entity would be served from the fast source forever.

## Configuration

### `New`

`New(name, fast, origin, opts...)`. `fast` is a `Store`, `origin` a `Source`; `name` identifies the
reader in `Stats` and metrics. Invalid arguments make `New` return an error wrapping
`ErrInvalidOption`, reporting every problem, not just the first.

| Option | Effect |
|---|---|
| `WithPolicy(p)` | Default: always `Serve` — the fast source is trusted until `Invalidate`d. |
| `WithLease(l, ttl)` | Fleet-wide coalescing of background refreshes via `Acquire`/`Release`. Without it, `ServeAndRefresh` dedupes only within one process. |
| `WithFastTimeout(d)` | Default 0 (no bound beyond `ctx`). Bounds every call to the fast source; set it whenever the fast source is a network call. |
| `WithLoadTimeout(d)` | Default 10s. Bounds every call to the origin, for a blocking load and a background refresh alike; a load runs on a context detached from any caller's cancellation, so this is the only bound on how long it can run. |
| `WithRefreshJitter(d)` | Default 0. Spreads background refreshes over `[0, d)` so a burst of values crossing a policy's threshold together does not refresh in the same instant. |
| `WithObserver(o)` | May be given more than once; observers are notified in the order attached. |
| `WithClock(fn)` | Default `time.Now`. The clock a `Policy` is judged against. |
| `WithSeed(a, b)` | Default: seeded from the runtime. Makes refresh-jitter draws reproducible. |

There is no shipped policy, not even a time-based one: a time check is a five-line `switch` a
caller can read directly (see Quick start), and shipping one would privilege elapsed time as the
model this package has no opinion on.

### Stores

| Constructor | Scope | Options |
|---|---|---|
| `NewMemoryStore[K, V](opts...)` | one process | `WithMaxKeys(n)`, default 1024: keys kept, least recently used evicted beyond it. |
| `fallback/redistore.NewStore[V](client, codec, opts...)` | shared through Redis | `WithKeyPrefix(p)`, default `fallback:`. `WithTTL(d)`, default 24h: how long an unrefreshed value survives — memory hygiene, not answer validity, which the `Policy` alone decides. Also implements `Leaser`, for `WithLease`: `WithLeaseKeyPrefix(p)`, default `fallback-lease:`. |

A memory store is a per-process tier: n instances of a fleet hold n independent copies, each
judged by the policy independently, and `Invalidate` on one instance reaches only that instance's
copy.

Unlike `ratelimit.Store`, `fallback.Store` carries no version and no server-side clock — a `Policy`
judges the value itself, whatever it encodes — so `fallback/redistore`'s `Get`, `Set` and `Delete`
are each one plain Redis command, not a Lua script. `V` is generic, so a `Codec[V]` encodes it for
storage; `redistore.JSON[V]()` fits any value that round-trips through `encoding/json`.

### `ReadOnly`

Embedding `fallback.ReadOnly[K, V]` gives a `Source` the `Store` methods as no-ops, for a fast
source someone else fills and evicts — a store already populated by a write path or a batch job.
It costs three things for as long as it is embedded: a load's write-back never reaches this source;
an origin reporting a key gone is not reflected here; and `Invalidate` itself becomes a no-op
against this source too, since it calls the same `Delete`.

### Guards

`GuardedSource(s, g)` and `GuardedStore(s, g)` wrap a `Source`/`Store` so every call runs through
`g`, typically a circuit breaker's `Do` via a small adapter (`Do` is a generic method and cannot
satisfy `Guard` directly; see `example_test.go`). Passing the result of `GuardedSource` where a
`Store` is expected is a compile error, since `Source` lacks `Set` and `Delete` — the one mistake
this split is built to catch.

## Behaviour

### The decision table

For a value `v` the fast source returned, judged by the policy against `now`:

| Fast source | Verdict | Load | `Get` returns | Write-back | Counted |
|---|---|---|---|---|---|
| value | `Serve` | not run | the value | — | Served |
| value | `ServeAndRefresh` | async | the value, immediately | — | Served |
| value | `LoadOrServe` | ok | new value | `Set` | Loaded |
| value | `LoadOrServe` | err | the stale value | — | Degraded |
| value | `Load` | ok | new value | `Set` | Loaded |
| value | `Load` | err | the error | — | Failed |
| absent | — | ok | new value | `Set` | Loaded, Misses |
| absent | — | err | the error | — | Failed, Misses |
| error | — | ok | new value | attempted | Loaded, FastErrors |
| error | — | err | the error | — | Failed, FastErrors |
| — | — | not run | `ctx.Err()` | — | Aborted |
| value | any | `found=false` | not found | `Delete` | Loaded, Misses |

A background refresh that completes successfully counts `Refreshes`, disjoint from `Loaded`: it
never resolves a `Get` by itself, so it is not a term in the identity below. One that fails counts
`LoadFailures`; a blocking load's failure is always some caller's `Degraded` or `Failed` instead.

### `Stats`

`Stats` returns cumulative counters. Identity: `Served + Loaded + Degraded + Failed + Aborted ==
Gets`. `Misses`, `Refreshes`, `LoadFailures`, `FastErrors`, `WriteBackFailures` and
`LeaseFailures` annotate a `Get` or a refresh; they are not terms in that identity. `Stats.String`
renders one line:

```
fallback: name=catalog gets=8120 served=7900 loaded=180 degraded=3 failed=1 aborted=0 misses=70 refreshes=140 load_failures=4 fast_errors=0 write_back_failures=0 lease_failures=0
```

### `Invalidate`

`Invalidate(ctx, key)` evicts `key` from the fast source. Use this rather than calling `Delete` on
the fast source directly: doing so can race a load already in flight for `key`, whose write-back
would resurrect it right after eviction. `Invalidate` poisons that flight instead, so its write-back
is dropped; its waiters still receive its result. This closes the race against a load already
running when `Invalidate` is called, not one that starts in the gap between `Invalidate` reading its
in-flight state and its own call to `Delete` — a narrow window callers for whom it matters should
close with their own higher-level exclusion.

### `Close`

`Close(ctx)` waits for every in-flight load and background refresh to finish, or for `ctx` to end.
A background refresh runs detached from any caller, so without draining it a process can exit
mid-refresh and lose a write-back. The caller must stop issuing `Get` before calling `Close`, the
same precondition `sync.WaitGroup.Wait` documents for `Add`.

## Composing

### With a circuit breaker

One breaker per resource, applied at the wiring site — never baked into an adapter, and never one
breaker shared across both sources, which would open the circuit in front of the origin the moment
the fast source alone goes down:

```go
cacheCB, err := breaker.New("catalog.cache", breaker.WithFailureThreshold(5))
dbCB, err := breaker.New("catalog.db", breaker.WithErrorRate(0.5, 30*time.Second, 10), breaker.WithMaxInFlight(32))

read, err := fallback.New("catalog",
    fallback.GuardedStore(cache, guard(cacheCB)),
    fallback.GuardedSource(repo, guard(dbCB)),
    fallback.WithPolicy(policy),
)
```

`guard` is the one adapter `example_test.go` shows: `breaker.Do` is generic and so cannot satisfy
`fallback.Guard` directly. The two breakers are tuned oppositely on purpose — the fast source trips
on a low failure threshold, fast, since refusing it is nearly free; the origin trips on an error
rate over a window, since opening it is what a caller actually feels, and it carries the bulkhead
that matters during an outage: with a wide key space, single-flight collapses duplicate keys but
the origin breaker's bulkhead is what caps distinct ones.

### With your own repository

`Source` and `Store` are satisfied by an ordinary repository — `Get(ctx, key) (V, bool, error)` is
usually already what one returns. `SourceFunc` adapts a bare function for a one-off origin with no
named type needed.

## Observability

### Metrics

`fallback.Register(reg)` registers the package's metrics once, under `<namespace>_fallback_` with
namespace `go` by default; `fallback.WithNamespace("inventory")` changes it. Every reader reports
under its name as the `name` label. There is no entries/size gauge: unlike `ratelimit.Store`'s
optional `KeyCounter`, a capability here is configured, not sniffed, and no option currently asks a
caller to state a fast source's size.

| Metric | Type | Labels |
|---|---|---|
| `go_fallback_gets_total` | counter | `name`, `outcome` ∈ served, loaded, degraded, failed, aborted |
| `go_fallback_misses_total` | counter | `name` |
| `go_fallback_refreshes_total` | counter | `name` |
| `go_fallback_load_failures_total` | counter | `name` |
| `go_fallback_fast_errors_total` | counter | `name` |
| `go_fallback_write_back_failures_total` | counter | `name` |
| `go_fallback_lease_failures_total` | counter | `name` |

Common queries:

```promql
rate(go_fallback_gets_total{outcome="degraded"}[5m])            # the origin is failing, stale answers still served
rate(go_fallback_gets_total{outcome="failed"}[5m])
  / rate(go_fallback_gets_total[5m])                            # share of reads actually failing
rate(go_fallback_fast_errors_total[5m])                         # the fast source itself is unwell
```

### Alerting

`contrib/prometheus/alerts.yaml` contains the rules below in a `fallback` group.

| Rule | Severity | Condition |
|---|---|---|
| `FallbackDegradedMajority` | ticket | degraded outcomes exceed a third of all gets for 10m |
| `FallbackFailing` | page | any failed outcomes for 2m |
| `FallbackFastSourceErrors` | ticket | any fast-source errors for 5m |

The first means the origin has been failing for a while and readers are living on stale answers —
not an outage yet, but the origin needs attention. The second means reads are failing outright:
either both sources are down, or a `Load`-verdict policy is refusing to degrade when it could. The
third means the fast source itself is unwell; `Get` still succeeds through the origin, so it is a
ticket, but every read is paying the origin's full latency until it is fixed.

### Dashboard

`contrib/grafana/keel.json` includes a collapsed "Fallback readers" row: gets per second stacked by
outcome, the degraded and failed shares, fast-source errors per second, and refreshes against load
failures. It is provided as a starting point.

## Testing

`fallback_test.go` covers every row of the decision table, single-flight coalescing under
concurrent misses, `Invalidate` poisoning an in-flight load, panic recovery, and the lease paths —
including a regression test for a background refresh that loses its lease race while a blocking
`Get` has already joined its flight, which must fall back to a real load rather than resolve with a
fabricated "not found". Stores run the contract in `conformance/fallbackstore`: absence, write-back,
overwrite, eviction, and independence between keys; `MemoryStore` runs it, and `memorystore_test.go`
separately covers what the contract does not — the LRU bound and option validation, which are
`MemoryStore`-specific, not part of what every `Store` promises. `example_test.go` verifies the
two-breaker composition's output.

## Benchmarks

`go test -bench . -benchmem ./fallback/` on an Apple M5 Max (18 cores), Go 1.27.1, memory store:

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `Get`, served | 12 | 0 | 0 |
| `Get`, 4096 rotating keys | 19 | 0 | 0 |
| `Get`, 18 goroutines on one key | 134 | 0 | 0 |

The served path is a map lookup under a mutex and one policy call. The parallel figure is
contention on the memory store's mutex for a single key.
