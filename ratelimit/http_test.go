package ratelimit

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func serve(h http.Handler, key string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-API-Key", key)
	req.RemoteAddr = "203.0.113.7:51234"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

type allowerFunc func(context.Context, string) (Decision, error)

func (f allowerFunc) Allow(ctx context.Context, key string) (Decision, error) { return f(ctx, key) }

var ok = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })

func TestMiddlewareAllowsThenLimitsWithHeaders(t *testing.T) {
	h := newHarness(t, GCRA(1, 2))
	mw := Middleware(h.Limiter, KeyByHeader("X-API-Key"))(ok)
	for i, wantRemaining := range []string{"1", "0"} {
		rec := serve(mw, "acme")
		if rec.Code != http.StatusNoContent || rec.Header().Get("X-RateLimit-Remaining") != wantRemaining {
			t.Fatalf("request %d: %d %v", i, rec.Code, rec.Header())
		}
	}
	rec := serve(mw, "acme")
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "1" || rec.Header().Get("X-RateLimit-Remaining") != "0" {
		t.Fatalf("limited: %d %v", rec.Code, rec.Header())
	}
	if rec := serve(mw, "globex"); rec.Code != http.StatusNoContent {
		t.Fatalf("other key: %d", rec.Code)
	}
}

func TestMiddlewareRetryAfterRoundsUp(t *testing.T) {
	h := newHarness(t, GCRA(0.4, 1)) // one token every 2.5s
	mw := Middleware(h.Limiter, KeyGlobal())(ok)
	serve(mw, "")
	if rec := serve(mw, ""); rec.Header().Get("Retry-After") != "3" {
		t.Fatalf("Retry-After = %q, want 3 (2.5s rounded up)", rec.Header().Get("Retry-After"))
	}
}

func TestMiddlewareKeyFuncs(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "203.0.113.7:51234"
	req.Header.Set("X-API-Key", "k")
	if got := KeyByRemoteAddr()(req); got != "203.0.113.7" {
		t.Errorf("KeyByRemoteAddr = %q", got)
	}
	req.RemoteAddr = "no-port"
	if got := KeyByRemoteAddr()(req); got != "no-port" {
		t.Errorf("KeyByRemoteAddr without port = %q", got)
	}
	if got := KeyByHeader("X-API-Key")(req); got != "k" {
		t.Errorf("KeyByHeader = %q", got)
	}
	if got := KeyGlobal()(req); got != "" {
		t.Errorf("KeyGlobal = %q", got)
	}
}

func TestMiddlewareErrorPolicy(t *testing.T) {
	boom := errors.New("redis down")
	failing := allowerFunc(func(context.Context, string) (Decision, error) { return Decision{}, boom })

	var seen error
	open := Middleware(failing, KeyGlobal(), OnError(func(_ *http.Request, err error) { seen = err }))(ok)
	if rec := serve(open, ""); rec.Code != http.StatusNoContent || !errors.Is(seen, boom) {
		t.Fatalf("fail open: %d, seen %v", rec.Code, seen)
	}
	closed := Middleware(failing, KeyGlobal(), FailClosed())(ok)
	if rec := serve(closed, ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("fail closed: %d", rec.Code)
	}
}

func TestMiddlewareCustomLimitedHandlerAndNoRemaining(t *testing.T) {
	h := newHarness(t, GCRA(1, 1))
	custom := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"error":"slow down"}`))
	})
	mw := Middleware(h.Limiter, KeyGlobal(), WithLimitedHandler(custom), WithoutRemainingHeader())(ok)
	if rec := serve(mw, ""); rec.Header().Get("X-RateLimit-Remaining") != "" {
		t.Fatalf("remaining header present: %v", rec.Header())
	}
	rec := serve(mw, "")
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Content-Type") != "application/json" || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("custom limited: %d %v", rec.Code, rec.Header())
	}
}
