package overload

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func serve(t testing.TB, mw func(http.Handler) http.Handler, h http.Handler, r *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	mw(h).ServeHTTP(w, r)
	return w
}

func TestMiddlewareShedsWith503AndAJitteredRetryAfter(t *testing.T) {
	h := newHarness(t, fixed{capacity: 1}, 0.6, 0.85)
	held := h.acquire(t, Critical)
	defer held(nil)

	mw, err := Middleware(h.Limiter)
	must(t, err)
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	// Every value is one second give or take 20%, so whole-second rounding
	// leaves a fleet spread over two seconds rather than arriving as one.
	seen := map[string]int{}
	for range 200 {
		w := serve(t, mw, ok, httptest.NewRequest(http.MethodGet, "/", nil))
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("code = %d, want 503", w.Code)
		}
		seen[w.Header().Get("Retry-After")]++
	}
	for value := range seen {
		if n, err := strconv.Atoi(value); err != nil || n < 1 || n > 2 {
			t.Fatalf("Retry-After = %q, want a whole number of seconds within ±20%% of one", value)
		}
	}
	if len(seen) < 2 {
		t.Fatalf("Retry-After took only the values %v; the jitter is not reaching the header", seen)
	}
}

func TestMiddlewareAdmitsAndReportsTheHandlersOutcome(t *testing.T) {
	var seen []Signal
	h := newHarness(t, recordingAlgorithm{Algorithm: fixed{capacity: 4}, seen: &seen}, 0.6, 0.85)
	mw := MustMiddleware(h.Limiter)

	body := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok")) // no explicit WriteHeader: net/http's implicit 200
	})
	if w := serve(t, mw, body, httptest.NewRequest(http.MethodGet, "/", nil)); w.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", w.Code)
	}

	// A 5xx is evidence of overload, not a fast success, so it must reach the
	// algorithm as an error rather than as a latency sample.
	boom := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	})
	serve(t, mw, boom, httptest.NewRequest(http.MethodGet, "/", nil))

	// The first signal is New's opening step; the two releases follow it.
	if len(seen) != 3 || seen[1].Err != nil || seen[2].Err == nil {
		t.Fatalf("signals = %+v, want a clean 200 then the handler's 5xx", seen)
	}
	if s := h.Stats(); s.Requests != 2 || s.Admitted != 2 || s.InFlight != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestMiddlewarePriorityDefaultsToDefaultAndIsOtherwiseTheCallersToDerive(t *testing.T) {
	h := newHarness(t, fixed{capacity: 10}, 0.6, 0.85)
	mw := MustMiddleware(h.Limiter, WithPriority(func(r *http.Request) Priority {
		if r.URL.Path == "/beacon" {
			return Sheddable
		}
		return Critical
	}))
	ok := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})

	serve(t, mw, ok, httptest.NewRequest(http.MethodGet, "/beacon", nil))
	serve(t, mw, ok, httptest.NewRequest(http.MethodGet, "/pay", nil))
	admitted, _, _, _ := h.events.snapshot()
	if admitted[Sheddable] != 1 || admitted[Critical] != 1 {
		t.Fatalf("admitted by priority = %v, want one sheddable and one critical", admitted)
	}

	plain := newHarness(t, fixed{capacity: 10}, 0.6, 0.85)
	serve(t, MustMiddleware(plain.Limiter), ok, httptest.NewRequest(http.MethodGet, "/", nil))
	admitted, _, _, _ = plain.events.snapshot()
	if admitted[Default] != 1 {
		t.Fatalf("admitted by priority = %v, want the default band without WithPriority", admitted)
	}
}

func TestMiddlewareShedHandlerReplacesTheBodyAndKeepsTheHeader(t *testing.T) {
	h := newHarness(t, fixed{capacity: 1}, 0.6, 0.85)
	held := h.acquire(t, Critical)
	defer held(nil)

	mw := MustMiddleware(h.Limiter, WithShedHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte("go away"))
	})))
	w := serve(t, mw, http.NotFoundHandler(), httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusTooManyRequests || w.Body.String() != "go away" {
		t.Fatalf("code = %d, body = %q", w.Code, w.Body.String())
	}
	if w.Header().Get("Retry-After") == "" {
		t.Error("Retry-After must be set before the shed handler runs, so a replacement can keep it")
	}
}

func TestMiddlewareInvalidOptions(t *testing.T) {
	h := newHarness(t, fixed{capacity: 1}, 0.6, 0.85)
	for name, tc := range map[string]struct {
		limiter *Limiter
		opts    []MiddlewareOption
	}{
		"nil limiter":     {nil, nil},
		"nil priority":    {h.Limiter, []MiddlewareOption{WithPriority(nil)}},
		"nil shed":        {h.Limiter, []MiddlewareOption{WithShedHandler(nil)}},
		"every one wrong": {nil, []MiddlewareOption{WithPriority(nil), WithShedHandler(nil)}},
	} {
		t.Run(name, func(t *testing.T) {
			mw, err := Middleware(tc.limiter, tc.opts...)
			if mw != nil || !errors.Is(err, ErrInvalidOption) {
				t.Fatalf("Middleware err = %v, want ErrInvalidOption", err)
			}
		})
	}
}

func TestMustMiddlewarePanicsOnAnInvalidOption(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("MustMiddleware did not panic on an invalid option")
		}
	}()
	MustMiddleware(nil)
}

func TestStatusWriterKeepsTheHandlersFlushReachable(t *testing.T) {
	h := newHarness(t, fixed{capacity: 2}, 0.6, 0.85)
	mw := MustMiddleware(h.Limiter)

	flushed := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The wrapper is not an http.Flusher itself; ResponseController has to
		// find the real writer through Unwrap.
		if err := http.NewResponseController(w).Flush(); err == nil {
			flushed = true
		}
	})
	serve(t, mw, handler, httptest.NewRequest(http.MethodGet, "/", nil))
	if !flushed {
		t.Fatal("http.ResponseController could not flush through the status wrapper")
	}
}

func TestMiddlewareWritesNothingWhenTheClientIsAlreadyGone(t *testing.T) {
	h := newHarness(t, fixed{capacity: 1}, 0.6, 0.85, WithMaxWait(time.Minute, time.Hour, time.Hour))
	held := h.acquire(t, Critical)
	defer held(nil)

	ctx, cancel := context.WithCancel(t.Context())
	r := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- serve(t, MustMiddleware(h.Limiter), http.NotFoundHandler(), r) }()

	waitFor(t, func() bool { return h.Stats().Waited == 1 })
	cancel()

	w := <-done
	if w.Code != http.StatusOK || w.Body.Len() != 0 {
		t.Fatalf("code = %d, body = %q; want nothing written to a client that has left", w.Code, w.Body.String())
	}
}

// recordingAlgorithm passes every step through and keeps the signals, so a
// test can assert what the middleware reported rather than what it did.
type recordingAlgorithm struct {
	Algorithm
	seen *[]Signal
}

func (a recordingAlgorithm) Step(s State, sig Signal, now time.Time) (State, Decision) {
	*a.seen = append(*a.seen, sig)
	return a.Algorithm.Step(s, sig, now)
}
