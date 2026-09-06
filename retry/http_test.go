package retry

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// script is a handler that answers each request from a queue of statuses,
// the last one repeating, and records what it saw.
type script struct {
	mu       sync.Mutex
	statuses []int
	headers  http.Header
	body     string
	seen     int
	bodies   []string
}

func (s *script) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, _ := io.ReadAll(r.Body)
	s.bodies = append(s.bodies, string(b))
	status := s.statuses[min(s.seen, len(s.statuses)-1)]
	s.seen++
	for k, v := range s.headers {
		w.Header()[k] = v
	}
	w.WriteHeader(status)
	body := s.body
	if body == "" {
		body = http.StatusText(status)
	}
	io.WriteString(w, body)
}

func (s *script) requests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen
}

// transportHarness is a client on a scripted server, retrying through a
// harness retrier.
type transportHarness struct {
	*harness
	srv    *httptest.Server
	script *script
	client *http.Client
}

func newTransport(t *testing.T, statuses []int, retrierOpts []Option, opts ...TransportOption) *transportHarness {
	t.Helper()
	sc := &script{statuses: statuses}
	srv := httptest.NewServer(sc)
	t.Cleanup(srv.Close)
	h := newHarness(t, Constant(10*time.Millisecond), retrierOpts...)
	tr, err := NewTransport(h.Retrier, srv.Client().Transport, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return &transportHarness{harness: h, srv: srv, script: sc, client: &http.Client{Transport: tr}}
}

func (th *transportHarness) do(t *testing.T, method string, body io.Reader, headers ...string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, th.srv.URL, body)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := th.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestTransportRetriesStatusesThenSucceeds(t *testing.T) {
	th := newTransport(t, []int{503, 503, 200}, nil)
	resp := th.do(t, http.MethodGet, nil)
	if resp.StatusCode != 200 || th.script.requests() != 3 {
		t.Fatalf("status %d after %d requests", resp.StatusCode, th.script.requests())
	}
	if s := th.Stats(); s.Attempts != 3 || s.Succeeded != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestTransportReturnsLastResponseOpenOnExhaustion(t *testing.T) {
	th := newTransport(t, []int{503}, nil)
	th.script.body = "busy"
	resp := th.do(t, http.MethodGet, nil)
	body, err := io.ReadAll(resp.Body)
	if resp.StatusCode != 503 || string(body) != "busy" || err != nil {
		t.Fatalf("status %d body %q err %v", resp.StatusCode, body, err)
	}
	if th.script.requests() != 3 {
		t.Fatalf("%d requests, want the attempt cap of 3", th.script.requests())
	}
	if s := th.Stats(); s.Exhausted != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestTransportLeavesOtherStatusesAlone(t *testing.T) {
	for _, status := range []int{200, 404, 500} {
		th := newTransport(t, []int{status}, nil)
		if resp := th.do(t, http.MethodGet, nil); resp.StatusCode != status || th.script.requests() != 1 {
			t.Fatalf("%d: status %d after %d requests", status, resp.StatusCode, th.script.requests())
		}
	}
}

func TestTransportMethodRule(t *testing.T) {
	cases := []struct {
		name     string
		method   string
		headers  []string
		opts     []TransportOption
		requests int
	}{
		{"GET", http.MethodGet, nil, nil, 3},
		{"HEAD", http.MethodHead, nil, nil, 3},
		{"POST", http.MethodPost, nil, nil, 1},
		{"PATCH", http.MethodPatch, nil, nil, 1},
		{"PUT", http.MethodPut, nil, nil, 1},
		{"DELETE", http.MethodDelete, nil, nil, 1},
		{"POST with Idempotency-Key", http.MethodPost, []string{"Idempotency-Key", "abc"}, nil, 3},
		{"PATCH with X-Idempotency-Key", http.MethodPatch, []string{"X-Idempotency-Key", "abc"}, nil, 3},
		{"PUT opted in", http.MethodPut, nil, []TransportOption{WithIdempotentMethods("put", http.MethodDelete)}, 3},
		{"DELETE opted in", http.MethodDelete, nil, []TransportOption{WithIdempotentMethods(http.MethodPut, http.MethodDelete)}, 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			th := newTransport(t, []int{503}, nil, c.opts...)
			th.do(t, c.method, strings.NewReader("payload"), c.headers...)
			if got := th.script.requests(); got != c.requests {
				t.Fatalf("%d requests, want %d", got, c.requests)
			}
		})
	}
}

func TestTransportReplaysTheBody(t *testing.T) {
	th := newTransport(t, []int{503, 503, 200}, nil)
	th.do(t, http.MethodPost, strings.NewReader("payload"), "Idempotency-Key", "k")
	th.script.mu.Lock()
	defer th.script.mu.Unlock()
	for i, b := range th.script.bodies {
		if b != "payload" {
			t.Fatalf("attempt %d saw body %q", i+1, b)
		}
	}
	if len(th.script.bodies) != 3 {
		t.Fatalf("%d attempts", len(th.script.bodies))
	}
}

func TestTransportPassesThroughANonReplayableBody(t *testing.T) {
	th := newTransport(t, []int{503}, nil)
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, th.srv.URL, nil)
	req.Body = io.NopCloser(strings.NewReader("stream")) // GetBody stays nil: cannot be replayed
	resp, err := th.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 503 || th.script.requests() != 1 {
		t.Fatalf("status %d after %d requests", resp.StatusCode, th.script.requests())
	}
	if s := th.Stats(); s.Calls != 0 {
		t.Fatalf("a passed-through request was counted: %+v", s)
	}
}

func TestTransportHonoursRetryAfter(t *testing.T) {
	th := newTransport(t, []int{429, 200}, nil)
	th.script.headers = http.Header{"Retry-After": {"2"}}
	th.do(t, http.MethodGet, nil)
	if got := th.recorded(); len(got) != 1 || got[0] != 2*time.Second+10*time.Millisecond {
		t.Fatalf("waits = %v, want 2s plus the schedule", got)
	}

	th = newTransport(t, []int{503, 200}, nil)
	at := th.clock.Now().Add(3 * time.Second)
	th.script.headers = http.Header{"Retry-After": {at.UTC().Format(http.TimeFormat)}}
	th.do(t, http.MethodGet, nil)
	if got := th.recorded(); len(got) != 1 || got[0] != 3*time.Second+10*time.Millisecond {
		t.Fatalf("waits = %v, want 3s from the HTTP-date plus the schedule", got)
	}

	// Unparseable, or in the past: no floor.
	for _, v := range []string{"soon", th.clock.Now().Add(-time.Minute).UTC().Format(http.TimeFormat)} {
		th = newTransport(t, []int{503, 200}, nil)
		th.script.headers = http.Header{"Retry-After": {v}}
		th.do(t, http.MethodGet, nil)
		if got := th.recorded(); len(got) != 1 || got[0] != 10*time.Millisecond {
			t.Fatalf("Retry-After %q: waits = %v", v, got)
		}
	}
}

func TestTransportRetryAfterAboveCapIsTerminal(t *testing.T) {
	th := newTransport(t, []int{429}, []Option{WithMaxRetryAfter(time.Minute)})
	th.script.headers = http.Header{"Retry-After": {"3600"}}
	resp := th.do(t, http.MethodGet, nil)
	if resp.StatusCode != 429 || th.script.requests() != 1 {
		t.Fatalf("status %d after %d requests", resp.StatusCode, th.script.requests())
	}
	if s := th.Stats(); s.Aborted != 1 || len(th.recorded()) != 0 {
		t.Fatalf("stats = %+v, waits = %v", s, th.recorded())
	}
}

// closeTracker wraps a RoundTripper so the test can see which response
// bodies were closed.
type closeTracker struct {
	next   http.RoundTripper
	mu     sync.Mutex
	closed []bool
}

type trackedBody struct {
	io.ReadCloser
	closed *bool
}

func (b trackedBody) Close() error { *b.closed = true; return b.ReadCloser.Close() }

func (c *closeTracker) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := c.next.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.closed = append(c.closed, false)
	resp.Body = trackedBody{resp.Body, &c.closed[len(c.closed)-1]}
	c.mu.Unlock()
	return resp, nil
}

func TestTransportReleasesRetriedBodies(t *testing.T) {
	for _, size := range []int{16, _bufferLimit + 1} {
		th := newTransport(t, []int{503}, nil)
		th.script.body = strings.Repeat("x", size)
		tracker := &closeTracker{next: th.srv.Client().Transport}
		tr, err := NewTransport(th.Retrier, tracker)
		if err != nil {
			t.Fatal(err)
		}
		th.client.Transport = tr
		resp := th.do(t, http.MethodGet, nil)
		body, _ := io.ReadAll(resp.Body)
		if len(body) != size {
			t.Fatalf("size %d: final body has %d bytes", size, len(body))
		}
		tracker.mu.Lock()
		closed := append([]bool(nil), tracker.closed...)
		tracker.mu.Unlock()
		// A small body is buffered, so even the last network body is closed
		// and the caller reads from memory; a large one stays on the
		// connection until the caller closes it.
		wantLast := size <= _bufferLimit
		if len(closed) != 3 || !closed[0] || !closed[1] || closed[2] != wantLast {
			t.Fatalf("size %d: closed = %v, want the two retried bodies closed and the last %v", size, closed, wantLast)
		}
	}
}

type flaky struct {
	next  http.RoundTripper
	fails atomic.Int32
}

var errDial = errors.New("dial tcp: connection refused")

func (f *flaky) RoundTrip(r *http.Request) (*http.Response, error) {
	if f.fails.Add(-1) >= 0 {
		return nil, errDial
	}
	return f.next.RoundTrip(r)
}

func TestTransportRetriesTransportErrors(t *testing.T) {
	th := newTransport(t, []int{200}, nil)
	f := &flaky{next: th.srv.Client().Transport}
	f.fails.Store(2)
	tr, _ := NewTransport(th.Retrier, f)
	th.client.Transport = tr
	if resp := th.do(t, http.MethodGet, nil); resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if s := th.Stats(); s.Attempts != 3 || s.Succeeded != 1 {
		t.Fatalf("stats = %+v", s)
	}
	// Exhausted on a transport error: the error itself comes back.
	f.fails.Store(10)
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, th.srv.URL, nil)
	if _, err := th.client.Do(req); !errors.Is(err, errDial) {
		t.Fatalf("err = %v", err)
	}
}

func TestTransportInvalidOptions(t *testing.T) {
	h := newHarness(t, Constant(0))
	if _, err := NewTransport(nil, nil); !errors.Is(err, ErrInvalidOption) {
		t.Errorf("nil retrier: %v", err)
	}
	if _, err := NewTransport(h.Retrier, nil, WithIdempotentMethods()); !errors.Is(err, ErrInvalidOption) {
		t.Errorf("no methods: %v", err)
	}
	if _, err := NewTransport(h.Retrier, nil, WithIdempotentMethods("")); !errors.Is(err, ErrInvalidOption) {
		t.Errorf("empty method: %v", err)
	}
	if _, err := NewTransport(h.Retrier, nil, WithRetryResponse(nil)); !errors.Is(err, ErrInvalidOption) {
		t.Errorf("nil predicate: %v", err)
	}
	tr, err := NewTransport(h.Retrier, nil)
	if err != nil || tr.next != http.DefaultTransport {
		t.Fatalf("nil next: %v, %v", tr, err)
	}
}

func TestTransportCustomPredicateAndHook(t *testing.T) {
	var seen []string
	th := newTransport(t, []int{500, 200}, []Option{WithOnRetry(func(attempt int, err error, d time.Duration) {
		seen = append(seen, err.Error())
	})}, WithRetryResponse(func(r *http.Response) bool { return r.StatusCode >= 500 }))
	th.script.headers = http.Header{"Retry-After": {"1"}}
	if resp := th.do(t, http.MethodGet, nil); resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if len(seen) != 1 || seen[0] != "retry: HTTP 500 Internal Server Error, retry after 1s" {
		t.Fatalf("hook saw %q", seen)
	}
	var se *StatusError
	if err := error(&StatusError{Status: 503}); !errors.As(err, &se) || !se.Retryable() || se.RetryDelay() != 0 || err.Error() != "retry: HTTP 503 Service Unavailable" {
		t.Fatalf("StatusError: %v", err)
	}
}

func TestTransportContextCancelDuringWaitReturnsLastResponse(t *testing.T) {
	th := newTransport(t, []int{503}, nil)
	th.deny.Store(true) // the recorded sleep reports a cancelled context
	resp := th.do(t, http.MethodGet, nil)
	if resp.StatusCode != 503 || th.script.requests() != 1 {
		t.Fatalf("status %d after %d requests", resp.StatusCode, th.script.requests())
	}
	if s := th.Stats(); s.Canceled != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

// TestTransportHedgeClosesTheLosingResponse: the first request stalls, the
// hedge gets a 503 with a body too large to buffer, then the first answers
// 200 and wins. The 503 body must be drained and closed by the transport,
// since the caller never sees it; the winner's body stays open for the
// caller. With hedging on, a non-replayable POST still passes through once.
func TestTransportHedgeClosesTheLosingResponse(t *testing.T) {
	var hits atomic.Int32
	release := make(chan struct{})
	stalled := make(chan struct{})
	gotHedge := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch hits.Add(1) {
		case 1:
			close(stalled)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			// Far larger than the transport's read buffer: reading it after
			// RoundTrip returned needs the winner's context still alive.
			io.WriteString(w, strings.Repeat("y", 1<<20))
		case 2:
			w.WriteHeader(http.StatusServiceUnavailable)
			io.WriteString(w, strings.Repeat("x", _bufferLimit+1))
			close(gotHedge)
		default:
			io.WriteString(w, "post")
		}
	}))
	t.Cleanup(srv.Close)
	h := newHedged(t, 5*time.Millisecond, true, WithMaxAttempts(2))
	tracker := &closeTracker{next: srv.Client().Transport}
	tr, err := NewTransport(h.Retrier, tracker)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: tr}

	type result struct {
		resp *http.Response
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := client.Get(srv.URL)
		done <- result{resp, err}
	}()
	<-stalled // the first attempt is the one on the stalled request
	h.fire(t)
	<-gotHedge
	deadline := time.Now().Add(10 * time.Second)
	for h.Stats().Hedged < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// Let the 503 reach the retrier before the winner answers, so it is
	// superseded rather than drained after the fact; either way it must end
	// up closed.
	time.Sleep(20 * time.Millisecond)
	close(release)
	var r result
	select {
	case r = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the request did not return")
	}
	if r.err != nil {
		t.Fatal(r.err)
	}
	body, err := io.ReadAll(r.resp.Body)
	if err != nil || r.resp.StatusCode != 200 || len(body) != 1<<20 {
		t.Fatalf("got %d, %d bytes, %v: the winner's body must be readable after Do returned", r.resp.StatusCode, len(body), err)
	}
	for time.Now().Before(deadline) {
		tracker.mu.Lock()
		closed := append([]bool(nil), tracker.closed...)
		tracker.mu.Unlock()
		if len(closed) == 2 && closed[0] && !closed[1] {
			break
		}
		if len(closed) == 2 && closed[1] {
			t.Fatalf("closed = %v: the winner's body was closed", closed)
		}
		time.Sleep(time.Millisecond)
	}
	tracker.mu.Lock()
	closed := append([]bool(nil), tracker.closed...)
	tracker.mu.Unlock()
	if len(closed) != 2 || !closed[0] || closed[1] {
		t.Fatalf("closed = %v, want the losing 503 closed and the winner open", closed)
	}
	r.resp.Body.Close()
	if s := h.Stats(); s.Attempts != 2 || s.Hedged != 1 || s.HedgeWon != 0 || s.Succeeded != 1 {
		t.Fatalf("stats = %+v", s)
	}

	resp, err := client.Post(srv.URL, "text/plain", strings.NewReader("body")) // replayable: GetBody is set, but POST is not in the method set
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if hits.Load() != 3 || h.Stats().Calls != 1 {
		t.Fatalf("the POST was hedged: %d hits, %+v", hits.Load(), h.Stats())
	}
}

func TestTransportHedgeExhaustedResponseIsReadable(t *testing.T) {
	// A retried response larger than the buffer limit stays open on its
	// attempt's context. When that response is the one returned on
	// exhaustion, its context must outlive the call or the body is
	// unreadable; without hedging the attempt runs on the caller's context.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, strings.Repeat("x", 4*_bufferLimit))
	}))
	t.Cleanup(srv.Close)
	h := newHedged(t, 5*time.Millisecond, false, WithMaxAttempts(2))
	tr, err := NewTransport(h.Retrier, srv.Client().Transport)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := (&http.Client{Transport: tr}).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	n, err := io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusServiceUnavailable || err != nil || n != 4*_bufferLimit {
		t.Fatalf("status %d, read %d bytes, err %v", resp.StatusCode, n, err)
	}
	if s := h.Stats(); s.Exhausted != 1 || s.Attempts != 2 {
		t.Fatalf("stats = %+v", s)
	}
}
