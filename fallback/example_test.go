package fallback_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/mgiaccone/keel/breaker"
	"github.com/mgiaccone/keel/fallback"
)

// Product is the value this example reads: an origin-owned record with a
// generation the policy judges the fast source's copy against.
type Product struct {
	ID   string
	Name string
	Gen  uint64
}

// guard adapts a *breaker.Breaker to [fallback.Guard]. breaker.Breaker.Do is
// a generic method and so cannot satisfy Guard directly — its own doc
// comment asks for exactly this adapter, over exactly this shape.
func guard(cb *breaker.Breaker) fallback.Guard {
	return func(ctx context.Context, fn func(context.Context) error) error {
		_, err := cb.Do(ctx, func(ctx context.Context) (struct{}, error) { return struct{}{}, fn(ctx) })
		return err
	}
}

// Example demonstrates the composition this package is built for: one
// breaker per resource, constructed at the composition root and applied at
// the wiring site — never baked into either adapter, and never a single
// breaker shared across both, which would open the circuit in front of the
// origin the moment the fast source alone goes down.
func Example() {
	ctx := context.Background()
	var gen atomic.Uint64
	gen.Store(1)

	products := map[string]Product{"p1": {ID: "p1", Name: "Widget", Gen: 1}}
	origin := fallback.SourceFunc[string, Product](func(_ context.Context, id string) (Product, bool, error) {
		p, ok := products[id]
		return p, ok, nil
	})

	fast, err := fallback.NewMemoryStore[string, Product]()
	if err != nil {
		fmt.Println("NewMemoryStore:", err)
		return
	}

	// Tuned in opposite directions: the fast source trips on a low
	// consecutive-failure count, because refusing it is nearly free; the
	// origin trips on an error rate over a window, because opening it is
	// what a caller actually feels.
	cacheCB, err := breaker.New("catalog.cache", breaker.WithFailureThreshold(5))
	if err != nil {
		fmt.Println("breaker.New:", err)
		return
	}
	dbCB, err := breaker.New("catalog.db", breaker.WithErrorRate(0.5, 30*time.Second, 10), breaker.WithMaxInFlight(32))
	if err != nil {
		fmt.Println("breaker.New:", err)
		return
	}

	reader, err := fallback.New[string, Product]("catalog",
		fallback.GuardedStore(fast, guard(cacheCB)),
		fallback.GuardedSource(origin, guard(dbCB)),
		fallback.WithPolicy(func(p Product, _ time.Time) fallback.Verdict {
			if p.Gen == gen.Load() {
				return fallback.Serve
			}
			return fallback.LoadOrServe // out of date, but better than an error if the origin is down
		}),
	)
	if err != nil {
		fmt.Println("New:", err)
		return
	}

	p, found, err := reader.Get(ctx, "p1")
	fmt.Printf("first get:  %+v found=%v err=%v\n", p, found, err)

	// The fast source now holds p1 at its current generation, so the second
	// Get is served without consulting the origin at all.
	p, found, err = reader.Get(ctx, "p1")
	fmt.Printf("second get: %+v found=%v err=%v\n", p, found, err)

	fmt.Println(reader.Stats())

	// Output:
	// first get:  {ID:p1 Name:Widget Gen:1} found=true err=<nil>
	// second get: {ID:p1 Name:Widget Gen:1} found=true err=<nil>
	// fallback: name=catalog gets=2 served=1 loaded=1 degraded=0 failed=0 aborted=0 misses=1 refreshes=0 load_failures=0 fast_errors=0 write_back_failures=0 lease_failures=0
}
