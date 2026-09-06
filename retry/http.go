package retry

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// StatusError is what the [Transport] hands the retrier for a response it
// may retry: the status, and the Retry-After header as a delay if the server
// sent one. It implements [Retryable] and [Delayed]. Callers never see it;
// the transport returns the response itself once retries end. It is exported
// so a [WithOnRetry] hook or [WithRetryIf] predicate can recognise it.
type StatusError struct {
	Status     int
	RetryAfter time.Duration
}

func (e *StatusError) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("retry: HTTP %d %s, retry after %s", e.Status, http.StatusText(e.Status), e.RetryAfter)
	}
	return fmt.Sprintf("retry: HTTP %d %s", e.Status, http.StatusText(e.Status))
}

// Retryable reports true: the transport only creates a StatusError for a
// response its predicate chose to retry.
func (e *StatusError) Retryable() bool { return true }

// RetryDelay returns the Retry-After header as a delay, 0 when absent.
func (e *StatusError) RetryDelay() time.Duration { return e.RetryAfter }

// TransportOption configures a [Transport].
type TransportOption func(*Transport) error

// WithIdempotentMethods adds methods to the set the transport may retry
// without an idempotency header. The default set is what net/http itself
// replays on a broken connection: GET, HEAD, OPTIONS and TRACE. PUT and
// DELETE are idempotent by RFC 9110 and belong here only when the handlers
// behind them are: a PUT that appends, or a DELETE that fails on a second
// call, is replayed by this option.
func WithIdempotentMethods(methods ...string) TransportOption {
	return func(t *Transport) error {
		if len(methods) == 0 {
			return fmt.Errorf("%w: WithIdempotentMethods(): no methods", ErrInvalidOption)
		}
		for _, m := range methods {
			if m == "" {
				return fmt.Errorf("%w: WithIdempotentMethods: empty method", ErrInvalidOption)
			}
			t.methods[strings.ToUpper(m)] = true
		}
		return nil
	}
}

// WithRetryResponse sets which responses are retried. The default retries
// 408 Request Timeout, 429 Too Many Requests, 502 Bad Gateway, 503 Service
// Unavailable and 504 Gateway Timeout, the statuses that mean "not this time"
// rather than "not with this request". A 500 is not retried by default: it is
// as often a bug as an outage, and a bug does not go away on retry. The
// predicate must not consume the body.
func WithRetryResponse(fn func(*http.Response) bool) TransportOption {
	return func(t *Transport) error {
		if fn == nil {
			return fmt.Errorf("%w: WithRetryResponse(nil)", ErrInvalidOption)
		}
		t.retryResponse = fn
		return nil
	}
}

// Transport is an http.RoundTripper that retries through a [Retrier]. Set it
// as an http.Client's Transport.
//
// It retries transport errors and the responses [WithRetryResponse] selects,
// on requests net/http itself would replay: the method is GET, HEAD, OPTIONS
// or TRACE, or one added with [WithIdempotentMethods], or the request carries
// an Idempotency-Key or X-Idempotency-Key header; and the body is nil or
// http.NoBody or the request has GetBody, which http.NewRequest sets for
// bytes, strings and io readers of known types. Every other request is passed
// through untouched and uncounted, so a POST with a streaming body is never
// replayed by accident.
//
// A Retry-After header on a retried response is honoured through [Delayed],
// under the retrier's [WithMaxRetryAfter]. The body of a retried response is
// read into memory if it is small, freeing the connection before the wait,
// and drained before the next attempt otherwise. Once retries end the last
// response is returned with its body open and a nil error, whatever ended
// them, so the caller sees the 503 rather than an error the retrier made up.
//
// Under [WithHedge] the same replay rule decides whether a request may be
// hedged at all, and a losing attempt's response is drained and closed
// whenever it arrives, before or after the winner has been returned. The
// transport never leaks a body.
type Transport struct {
	retrier       *Retrier
	next          http.RoundTripper
	methods       map[string]bool
	retryResponse func(*http.Response) bool
}

// NewTransport returns a transport retrying through r in front of next, or
// http.DefaultTransport when next is nil.
func NewTransport(r *Retrier, next http.RoundTripper, opts ...TransportOption) (*Transport, error) {
	if next == nil {
		next = http.DefaultTransport
	}

	t := &Transport{
		retrier:       r,
		next:          next,
		methods:       map[string]bool{http.MethodGet: true, http.MethodHead: true, http.MethodOptions: true, http.MethodTrace: true},
		retryResponse: defaultRetryResponse,
	}
	var errs []error
	if r == nil {
		errs = append(errs, fmt.Errorf("%w: NewTransport: retrier must not be nil", ErrInvalidOption))
	}
	for _, opt := range opts {
		if err := opt(t); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return t, nil
}

func defaultRetryResponse(resp *http.Response) bool {
	switch resp.StatusCode {
	case http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

// _bufferLimit is the largest retried-response body read into memory so the
// connection is free during the wait. Larger bodies stay open and are
// drained before the next attempt.
const _bufferLimit = 64 << 10

// RoundTrip implements http.RoundTripper.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !t.replayable(req) {
		return t.next.RoundTrip(req)
	}

	// Attempts may run concurrently under WithHedge, so the only state they
	// share is the attempt counter; superseded and losing responses come back
	// through the discard hook, which is what closes their bodies.
	var attempt atomic.Int32
	resp, err := do(t.retrier, req.Context(), func(ctx context.Context) (*http.Response, error) {
		r := req
		if attempt.Add(1) > 1 {
			r = req.Clone(ctx)
			if req.GetBody != nil {
				body, err := req.GetBody()
				if err != nil {
					return nil, Permanent(err)
				}
				r.Body = body
			}
		} else if ctx != req.Context() {
			r = req.Clone(ctx) // a hedged first attempt: its own headers, since a hedge is cloning them concurrently
		}

		resp, err := t.next.RoundTrip(r)
		if err != nil {
			return nil, err
		}
		if !t.retryResponse(resp) {
			return resp, nil
		}

		buffer(resp) // a small body frees the connection before the wait; a large one is drained when superseded
		return resp, &StatusError{Status: resp.StatusCode, RetryAfter: t.retryAfter(resp)}
	}, func(resp *http.Response, _ error) {
		if resp != nil {
			drain(resp.Body)
		}
	})

	var se *StatusError
	if errors.As(err, &se) {
		return resp, nil
	}
	return resp, err
}

// replayable reports whether the request may be sent more than once: the
// rule net/http applies before it retries on a broken connection, widened by
// WithIdempotentMethods.
func (t *Transport) replayable(req *http.Request) bool {
	if req.Body != nil && req.Body != http.NoBody && req.GetBody == nil {
		return false
	}

	method := req.Method
	if method == "" {
		method = http.MethodGet
	}
	if t.methods[method] {
		return true
	}

	_, a := req.Header["Idempotency-Key"]
	_, b := req.Header["X-Idempotency-Key"]
	return a || b
}

// retryAfter parses the Retry-After header, in seconds or as an HTTP-date,
// against the retrier's clock; 0 when absent or unparseable.
func (t *Transport) retryAfter(resp *http.Response) time.Duration {
	h := resp.Header.Get("Retry-After")
	if h == "" {
		return 0
	}
	if secs, err := strconv.Atoi(h); err == nil {
		return max(time.Duration(secs)*time.Second, 0)
	}
	if at, err := http.ParseTime(h); err == nil {
		return max(at.Sub(t.retrier.cfg.now()), 0)
	}
	return 0
}

// buffer reads a small body into memory and closes the original, so the
// connection is reusable during the wait. It reports false, leaving the body
// untouched, when the body is larger than _bufferLimit.
func buffer(resp *http.Response) bool {
	data, err := io.ReadAll(io.LimitReader(resp.Body, _bufferLimit+1))

	if err != nil || len(data) > _bufferLimit {
		if len(data) > 0 {
			// Give back what was read, ahead of the rest.
			resp.Body = struct {
				io.Reader
				io.Closer
			}{io.MultiReader(bytes.NewReader(data), resp.Body), resp.Body}
		}
		return false
	}
	resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(data))
	return true
}

// drain discards what is left of a body, bounded, and closes it.
func drain(body io.ReadCloser) {
	io.Copy(io.Discard, io.LimitReader(body, _bufferLimit))
	body.Close()
}
