package breaker_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/mgiaccone/keel/breaker"
)

// Domain errors returned by the Repository. Callers see these, never the
// breaker's or the driver's.
var (
	// ErrNotFound means the record does not exist: the database answered, it
	// just had no row.
	ErrNotFound = errors.New("repo: not found")
	// ErrUnavailable means the fallback path is shed because the circuit is
	// open, a probe is already in flight, or the bulkhead is full. Callers should fail fast and not
	// retry: a retry would be rejected again or steal the probe slot that
	// recovery depends on.
	ErrUnavailable = errors.New("repo: temporarily unavailable")
)

// Record is whatever the repository stores.
type Record struct {
	Key   string
	Value string
}

// Cache is a read cache. ok is false on a miss, meaning the key is absent.
type Cache interface {
	Get(ctx context.Context, key string) (r Record, ok bool, err error)
}

// Database is the source of truth. It returns sql.ErrNoRows for an unknown
// key.
type Database interface {
	Get(ctx context.Context, key string) (Record, error)
}

// Repository reads from the cache and falls through to the database on a
// miss, with the fallback guarded by a circuit breaker.
type Repository struct {
	cache    Cache
	db       Database
	fallback *breaker.Breaker
}

// NewRepository wires the breaker. now is the clock; use time.Now outside of
// tests.
func NewRepository(cache Cache, db Database, log *slog.Logger, now func() time.Time) (*Repository, error) {
	const dependency = "db-fallback"
	fallback, err := breaker.New(dependency,
		breaker.WithFailureThreshold(3),
		breaker.WithOpenInterval(5*time.Second, time.Minute),
		// Bound every database call so a hung connection becomes a failure
		// the circuit can see, rather than a probe slot held forever. Keep it
		// well under the caller's own deadline.
		breaker.WithTimeout(2*time.Second),
		// Bound concurrency too: a slow database must not absorb every
		// goroutine in the service. Worst case is 16 × 2s of database time.
		breaker.WithMaxInFlight(16),
		// After an outage, let the misses back in gradually: two at a time
		// right after the circuit closes, one more per success, full cap
		// again after eight. A backend that has just proven it can answer one
		// probe should not receive the whole backlog at once.
		breaker.WithRecoveryRamp(2, 8),

		// sql.ErrNoRows and ErrNotFound mean the database answered
		// correctly and the answer was "no such key". The backend is
		// healthy, so they are successes for the circuit; counting them
		// as failures would trip it on a burst of lookups for unknown
		// keys. A caller cancelling its own request says nothing about
		// the database either. Everything else is a failure.
		breaker.WithIsFailure(func(err error) bool {
			switch {
			case err == nil,
				errors.Is(err, sql.ErrNoRows),
				errors.Is(err, ErrNotFound),
				errors.Is(err, context.Canceled):
				return false
			}
			return true
		}),

		// The hook runs on the breaker's goroutine: return quickly and do
		// not call back into the breaker.
		breaker.WithOnStateChange(func(from, to breaker.State) {
			log.Warn("circuit state change", "dependency", dependency, "from", from, "to", to)
		}),

		breaker.WithClock(now),
		breaker.WithSeed(7, 11), // fixed only so this example's output is stable; omit in production
	)
	if err != nil {
		return nil, fmt.Errorf("repo: %w", err)
	}
	return &Repository{cache: cache, db: db, fallback: fallback}, nil
}

// Get returns the record, consulting the database only on a cache miss.
func (r *Repository) Get(ctx context.Context, key string) (Record, error) {
	rec, ok, err := r.cache.Get(ctx, key)
	if err != nil {
		return Record{}, fmt.Errorf("repo: cache: %w", err)
	}
	// Return on a hit, not on freshness. If the repository fell through
	// whenever a cached record looked stale, every read during cache lag
	// would go to the database and the fallback path would carry full read
	// volume, which is exactly the load it is not sized for.
	if ok {
		return rec, nil
	}

	rec, err = r.fallback.Do(ctx, func(ctx context.Context) (Record, error) {
		return r.db.Get(ctx, key)
	})
	switch {
	case errors.Is(err, breaker.ErrOpen), errors.Is(err, breaker.ErrProbeLimit), errors.Is(err, breaker.ErrBulkhead):
		// Translate at the boundary so callers fail fast and do not retry.
		// The request never ran, so there is nothing to reconcile.
		return Record{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	case errors.Is(err, sql.ErrNoRows):
		return Record{}, ErrNotFound
	case err != nil:
		return Record{}, fmt.Errorf("repo: database: %w", err)
	}
	return rec, nil
}

// Health exposes the breaker snapshot for a health endpoint.
func (r *Repository) Health() breaker.Stats { return r.fallback.Stats() }

type fakeCache map[string]Record

func (c fakeCache) Get(_ context.Context, key string) (Record, bool, error) {
	r, ok := c[key]
	return r, ok, nil
}

type fakeDB struct {
	rows map[string]Record
	down atomic.Bool
}

var errConnTimeout = errors.New("dial tcp: i/o timeout")

func (d *fakeDB) Get(_ context.Context, key string) (Record, error) {
	if d.down.Load() {
		return Record{}, errConnTimeout
	}
	r, ok := d.rows[key]
	if !ok {
		return Record{}, sql.ErrNoRows
	}
	return r, nil
}

// fakeClock is atomic because the breaker's goroutine reads it while the
// example advances it.
type fakeClock struct{ ns atomic.Int64 }

func (c *fakeClock) Now() time.Time      { return time.Unix(0, c.ns.Load()) }
func (c *fakeClock) Add(d time.Duration) { c.ns.Add(int64(d)) }

func Example() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	}))
	clock := &fakeClock{}
	clock.ns.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())

	cache := fakeCache{"a": {Key: "a", Value: "cached"}}
	db := &fakeDB{rows: map[string]Record{
		"a": {Key: "a", Value: "cached"},
		"b": {Key: "b", Value: "from-db"},
	}}
	repo, err := NewRepository(cache, db, log, clock.Now)
	if err != nil {
		panic(err) // example only; a service would return this from its constructor
	}
	ctx := context.Background()

	show := func(key string) {
		r, err := repo.Get(ctx, key)
		if err != nil {
			fmt.Println(key, "->", err)
			return
		}
		fmt.Println(key, "->", r.Value)
	}

	// Cache hit: the database is never consulted.
	show("a")
	// Cache miss: fall through to the database.
	show("b")
	// Cache miss for an unknown key: no rows is a correct answer, so the
	// circuit stays closed.
	show("c")
	fmt.Println("circuit:", repo.Health().State)

	// The database goes down. Three misses time out, the circuit trips, and
	// from then on misses fail fast without touching the database.
	db.down.Store(true)
	for range 3 {
		show("d")
	}
	show("e")
	fmt.Println(repo.Health())

	// The database recovers. Once the open interval has elapsed the next miss
	// is admitted as a probe; two consecutive probe successes close the
	// circuit, and the recovery ramp caps in-flight misses at 2 until eight
	// successes have gone through.
	db.down.Store(false)
	clock.Add(repo.Health().NextProbeIn)
	show("b")
	show("b")
	fmt.Println(repo.Health())

	// Output:
	// a -> cached
	// b -> from-db
	// c -> repo: not found
	// circuit: closed
	// d -> repo: database: dial tcp: i/o timeout
	// d -> repo: database: dial tcp: i/o timeout
	// level=WARN msg="circuit state change" dependency=db-fallback from=closed to=open
	// d -> repo: database: dial tcp: i/o timeout
	// e -> repo: temporarily unavailable: breaker: circuit open
	// breaker: name=db-fallback state=open trips=1(consecutive=1) calls=6 rejected=1 shed=0 denied=0 ok=2 fail=3 canceled=0 in_flight=0/16 next_probe_in=4.693s
	// level=WARN msg="circuit state change" dependency=db-fallback from=open to=half-open
	// b -> from-db
	// level=WARN msg="circuit state change" dependency=db-fallback from=half-open to=closed
	// b -> from-db
	// breaker: name=db-fallback state=closed trips=1(consecutive=0) calls=8 rejected=1 shed=0 denied=0 ok=4 fail=3 canceled=0 in_flight=0/2(ramping)
}

func ExampleRegister() {
	// At bootstrap, once: publish the package's metrics. A service passes
	// prometheus.DefaultRegisterer; the example uses its own registry.
	reg := prometheus.NewPedanticRegistry()
	if err := breaker.Register(reg); err != nil {
		panic(err) // example only
	}

	// Every breaker reports under its name as the dependency label; series
	// live as long as the breaker does.
	b, err := breaker.New("reporting-db",
		breaker.WithFailureThreshold(2),
		breaker.WithOpenInterval(5*time.Second, time.Minute),
	)
	if err != nil {
		panic(err) // example only
	}

	ctx := context.Background()
	for range 2 {
		b.Do(ctx, func(context.Context) (int, error) { return 0, errors.New("dial tcp: i/o timeout") })
	}
	b.Do(ctx, func(context.Context) (int, error) { return 1, nil }) // rejected: circuit open

	// Print this breaker's state gauge. The vectors are shared by every
	// breaker in the process, so filter to our dependency label.
	families, err := reg.Gather()
	if err != nil {
		panic(err)
	}
	for _, f := range families {
		if f.GetName() != "go_breaker_state" {
			continue
		}
		for _, m := range f.GetMetric() {
			var dep, state string
			for _, l := range m.GetLabel() {
				switch l.GetName() {
				case "dependency":
					dep = l.GetValue()
				case "state":
					state = l.GetValue()
				}
			}
			if dep == "reporting-db" {
				fmt.Printf("go_breaker_state{dependency=%q,state=%q} %v\n", dep, state, m.GetGauge().GetValue())
			}
		}
	}
	// Output:
	// go_breaker_state{dependency="reporting-db",state="closed"} 0
	// go_breaker_state{dependency="reporting-db",state="half-open"} 0
	// go_breaker_state{dependency="reporting-db",state="open"} 1
}
