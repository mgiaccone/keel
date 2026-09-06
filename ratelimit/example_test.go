package ratelimit_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/mgiaccone/keel/breaker"
	"github.com/mgiaccone/keel/ratelimit"
)

// Example_httpMiddleware holds API clients to a per-key quota with no breaker
// involved. A refusal is 429: the client exceeded a quota that is theirs.
func Example_httpMiddleware() {
	// Per API key: one request per second on average, bursts of 3 (small so
	// the example is short). Swap the store for redistore.NewStore and the same
	// middleware enforces one quota across every instance.
	store, err := ratelimit.NewMemoryStore() // or redistore.NewStore(client) for one quota across the fleet
	if err != nil {
		panic(err) // example only
	}
	limiter, err := ratelimit.New("public-api", ratelimit.GCRA(1, 3), store)
	if err != nil {
		panic(err) // example only
	}

	api := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, "ok") })
	srv := httptest.NewServer(ratelimit.MustMiddleware(limiter, ratelimit.KeyByHeader("X-API-Key"))(api))
	defer srv.Close()

	for i := range 4 {
		req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
		req.Header.Set("X-API-Key", "acme")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		resp.Body.Close()
		fmt.Printf("request %d: %d remaining=%s retry-after=%q\n", i+1, resp.StatusCode,
			resp.Header.Get("X-RateLimit-Remaining"), resp.Header.Get("Retry-After"))
	}
	// A different key has its own bucket.
	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	req.Header.Set("X-API-Key", "globex")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	fmt.Println("globex:", resp.StatusCode)
	// Output:
	// request 1: 200 remaining=2 retry-after=""
	// request 2: 200 remaining=1 retry-after=""
	// request 3: 200 remaining=0 retry-after=""
	// request 4: 429 remaining=0 retry-after="1"
	// globex: 200
}

// Example_breaker composes the limiter with a circuit breaker on an outbound
// path: refused calls never run and are recorded by the breaker as denied.
func Example_breaker() {
	// One limiter per dependency: one call per second on average, bursts of 10.
	store, err := ratelimit.NewMemoryStore()
	if err != nil {
		panic(err) // example only
	}
	limiter, err := ratelimit.New("db-fallback", ratelimit.GCRA(1, 10), store)
	if err != nil {
		panic(err) // example only
	}

	// The breaker vetoes calls the limiter refuses. Such a call never runs,
	// costs no quota if the circuit is open, and shows up in the breaker's
	// Stats as denied.
	b, err := breaker.New("db-fallback",
		breaker.WithAdmission(ratelimit.AdmissionGlobal(limiter)),
		breaker.WithTimeout(2*time.Second),
	)
	if err != nil {
		panic(err) // example only
	}

	ctx := context.Background()
	denied := 0
	for range 12 {
		_, err := b.Do(ctx, func(context.Context) (int, error) { return 1, nil })
		var lim *ratelimit.LimitedError
		if errors.As(err, &lim) {
			denied++
		}
	}
	fmt.Println("denied:", denied)
	fmt.Println(limiter.Stats())
	s := b.Stats()
	fmt.Println("breaker denied:", s.Denied, "admitted:", s.Admitted)
	// Output:
	// denied: 2
	// ratelimit: name=db-fallback algorithm=gcra keys=1 allowed=10 limited=2 errors=0 conflicts=0
	// breaker denied: 2 admitted: 10
}
