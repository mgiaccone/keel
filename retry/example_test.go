package retry_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	"github.com/mgiaccone/keel/breaker"
	"github.com/mgiaccone/keel/ratelimit"
	"github.com/mgiaccone/keel/retry"
)

var (
	errNotFound = errors.New("catalogue: not found")
	errReset    = errors.New("read tcp: connection reset by peer")
)

// catalogue is a flaky dependency: the first two reads of any key fail with a
// reset, and unknown keys are not found.
type catalogue struct{ resets atomic.Int32 }

func (c *catalogue) Get(_ context.Context, key string) (string, error) {
	if c.resets.Add(-1) >= 0 {
		return "", errReset
	}
	if key != "sku-1" {
		return "", errNotFound
	}
	return "widget", nil
}

// Example retries reads through a breaker, with a limiter as the retry
// budget. The stack is retry → breaker → dependency: every attempt is a
// breaker call, a breaker refusal is never retried, and the budget caps how
// many retries the process may spend per second whatever the callers do.
func Example() {
	cat := &catalogue{}
	cat.resets.Store(2)

	b, err := breaker.New("catalogue",
		breaker.WithTimeout(2*time.Second),
		breaker.WithIsFailure(func(err error) bool { return err != nil && !errors.Is(err, errNotFound) }),
	)
	if err != nil {
		panic(err) // example only
	}
	store, _ := ratelimit.NewMemoryStore()
	budget, _ := ratelimit.New("catalogue-retries", ratelimit.GCRA(10, 20), store) // 10 retries/s, bursts of 20

	r, err := retry.New("catalogue",
		retry.Exponential(50*time.Millisecond, 2*time.Second),
		retry.WithMaxAttempts(4),
		retry.WithBudget(ratelimit.AdmissionGlobal(budget)),
		retry.WithOnRetry(func(attempt int, err error, delay time.Duration) {
			fmt.Printf("attempt %d failed: %v; retrying in %s\n", attempt, err, delay)
		}),
		retry.WithSeed(7, 11), // fixed only so this example's output is stable; omit in production
		retry.WithSleep(func(context.Context, time.Duration) error { return nil }), // example only: do not really wait
	)
	if err != nil {
		panic(err) // example only
	}

	get := func(key string) (string, error) {
		return r.Do(context.Background(), func(ctx context.Context) (string, error) {
			return b.Do(ctx, func(ctx context.Context) (string, error) {
				v, err := cat.Get(ctx, key)
				if errors.Is(err, errNotFound) {
					return "", retry.Permanent(err) // the answer is no; asking again will not change it
				}
				return v, err
			})
		})
	}

	v, err := get("sku-1")
	fmt.Println("sku-1:", v, err)
	v, err = get("sku-2")
	fmt.Println("sku-2:", v, err)
	fmt.Println(r.Stats())
	// Output:
	// attempt 1 failed: read tcp: connection reset by peer; retrying in 17.329928ms
	// attempt 2 failed: read tcp: connection reset by peer; retrying in 83.842509ms
	// sku-1: widget <nil>
	// sku-2:  catalogue: not found
	// retry: name=catalogue backoff=exponential calls=2 attempts=4 ok=1 exhausted=0 aborted=1 canceled=0 budget=0 waited=101.172437ms
}

// ExampleNewTransport puts a retrying transport under an http.Client. Only
// requests net/http itself would replay are retried; the server's Retry-After
// is honoured.
func ExampleNewTransport() {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) < 3 {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "warming up", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ready")
	}))
	defer srv.Close()

	r, err := retry.New("api",
		retry.Exponential(100*time.Millisecond, 5*time.Second),
		retry.WithSleep(func(context.Context, time.Duration) error { return nil }), // example only: do not really wait
	)
	if err != nil {
		panic(err) // example only
	}
	transport, err := retry.NewTransport(r, nil) // nil: in front of http.DefaultTransport
	if err != nil {
		panic(err) // example only
	}
	client := &http.Client{Transport: transport}

	resp, err := client.Get(srv.URL)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("%d %s", resp.StatusCode, body)

	// A POST is not replayed: the server sees it once and the client gets
	// the 503 to deal with.
	hits.Store(0)
	resp, _ = client.Post(srv.URL, "text/plain", nil)
	resp.Body.Close()
	fmt.Println("POST:", resp.StatusCode, "after", hits.Load(), "request")
	// Output:
	// 200 ready
	// POST: 503 after 1 request
}

// ExampleWithHedge cuts the latency tail: when an attempt has not answered
// within the hedge delay, another starts, and the first answer wins. The
// replica behind the first attempt here never answers; the hedge does.
func ExampleWithHedge() {
	r, err := retry.New("search",
		retry.Constant(0), // retries are not the point here, only hedges
		retry.WithMaxAttempts(2),
		retry.WithHedge(10*time.Millisecond),
	)
	if err != nil {
		panic(err) // example only
	}

	var attempts atomic.Int32
	v, err := r.Do(context.Background(), func(ctx context.Context) (string, error) {
		if attempts.Add(1) == 1 {
			<-ctx.Done() // a stuck replica: it answers only once the hedge has won and it is cancelled
			return "", ctx.Err()
		}
		return "results", nil
	})
	fmt.Println(v, err)
	s := r.Stats()
	fmt.Printf("attempts=%d hedged=%d hedge_won=%d\n", s.Attempts, s.Hedged, s.HedgeWon)
	// Output:
	// results <nil>
	// attempts=2 hedged=1 hedge_won=1
}
