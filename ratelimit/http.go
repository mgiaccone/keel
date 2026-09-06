package ratelimit

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"strconv"
)

// KeyFunc derives the rate-limit key from a request: the API key, the
// authenticated tenant, the route, the client address. Requests with the same
// key share a record.
type KeyFunc func(*http.Request) string

// KeyByHeader keys on a request header, for example "X-API-Key" or
// "Authorization". Requests without the header share the key "".
func KeyByHeader(name string) KeyFunc {
	return func(r *http.Request) string { return r.Header.Get(name) }
}

// KeyByRemoteAddr keys on the client IP, without the port. Behind a proxy
// that sets X-Forwarded-For, key on that header instead, but only if the proxy
// is trusted to set it; clients can write it too.
func KeyByRemoteAddr() KeyFunc {
	return func(r *http.Request) string {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			return r.RemoteAddr
		}
		return host
	}
}

// KeyGlobal keys every request the same, for one limit on the whole endpoint.
func KeyGlobal() KeyFunc { return func(*http.Request) string { return "" } }

// MiddlewareOption configures [Middleware]. An option handed a value that
// cannot be meant makes Middleware fail with an error wrapping
// [ErrInvalidOption]; Middleware reports every invalid option, not just the
// first.
type MiddlewareOption func(*middlewareConfig) error

type middlewareConfig struct {
	limited   http.Handler
	failOpen  bool
	onError   func(*http.Request, error)
	remaining bool
}

// WithLimitedHandler sets what a refused request gets. The default writes
// 429 Too Many Requests with a plain-text body; Retry-After and
// X-RateLimit-Remaining are set before the handler runs either way.
func WithLimitedHandler(h http.Handler) MiddlewareOption {
	return func(c *middlewareConfig) error {
		if h == nil {
			return fmt.Errorf("%w: WithLimitedHandler(nil)", ErrInvalidOption)
		}
		c.limited = h
		return nil
	}
}

// FailClosed makes a limiter error refuse the request with 503 Service
// Unavailable instead of letting it through. The default is to fail open,
// because the limiter erring is only possible for a distributed limiter and
// an API that goes down with its Redis is usually the worse outcome.
func FailClosed() MiddlewareOption {
	return func(c *middlewareConfig) error {
		c.failOpen = false
		return nil
	}
}

// OnError is told about limiter errors, for logging; the limiter's own
// metrics already count them. nil removes a previously set hook.
func OnError(fn func(*http.Request, error)) MiddlewareOption {
	return func(c *middlewareConfig) error {
		c.onError = fn
		return nil
	}
}

// WithoutRemainingHeader suppresses X-RateLimit-Remaining, for endpoints that
// should not reveal their limits.
func WithoutRemainingHeader() MiddlewareOption {
	return func(c *middlewareConfig) error {
		c.remaining = false
		return nil
	}
}

func defaultLimited(w http.ResponseWriter, _ *http.Request) {
	http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
}

// Middleware holds requests to the limiter's rule, keyed by key. Allowed
// requests carry X-RateLimit-Remaining; refused ones get Retry-After in whole
// seconds, rounded up, and 429 Too Many Requests. It is plain net/http and
// composes with any router that accepts func(http.Handler) http.Handler.
//
//	limiter, err := ratelimit.New("public-api", ratelimit.GCRA(100, 20), store)
//	mw, err := ratelimit.Middleware(limiter, ratelimit.KeyByHeader("X-API-Key"))
//	mux.Handle("/v1/", mw(api))
//
// If l or key is nil, or any option is invalid, Middleware returns an error
// wrapping [ErrInvalidOption] that describes every problem, so a mistake
// surfaces at bootstrap rather than as a panic on the first request.
// [MustMiddleware] is the same for bootstrap code that treats it as fatal.
func Middleware(l Allower, key KeyFunc, opts ...MiddlewareOption) (func(http.Handler) http.Handler, error) {
	cfg := middlewareConfig{
		limited:   http.HandlerFunc(defaultLimited),
		failOpen:  true,
		remaining: true,
	}
	var errs []error
	if l == nil {
		errs = append(errs, fmt.Errorf("%w: Middleware: limiter must not be nil", ErrInvalidOption))
	}
	if key == nil {
		errs = append(errs, fmt.Errorf("%w: Middleware: key must not be nil", ErrInvalidOption))
	}
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			d, err := l.Allow(r.Context(), key(r))
			if err != nil {
				if cfg.onError != nil {
					cfg.onError(r, err)
				}

				if cfg.failOpen {
					next.ServeHTTP(w, r)
				} else {
					http.Error(w, "rate limiter unavailable", http.StatusServiceUnavailable)
				}
				return
			}

			if cfg.remaining && d.Remaining >= 0 {
				w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(d.Remaining))
			}

			if !d.Allowed {
				w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(d.RetryAfter.Seconds()))))
				cfg.limited.ServeHTTP(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}, nil
}

// MustMiddleware is [Middleware] for bootstrap code that treats an invalid
// argument as fatal, mirroring [MustRegister]. It panics on error.
func MustMiddleware(l Allower, key KeyFunc, opts ...MiddlewareOption) func(http.Handler) http.Handler {
	mw, err := Middleware(l, key, opts...)
	if err != nil {
		panic(err)
	}
	return mw
}
