package overload

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
)

// MiddlewareOption configures [Middleware]. An option handed a value that
// cannot be meant makes Middleware fail with an error wrapping
// [ErrInvalidOption]; Middleware reports every invalid option, not just the
// first.
type MiddlewareOption func(*middlewareConfig) error

type middlewareConfig struct {
	priority func(*http.Request) Priority
	shed     http.Handler
}

// WithPriority derives a request's [Priority] from the request itself: the
// route, a header a trusted gateway set, an authenticated plan tier. Default:
// every request is [Default], which switches priority shedding off in all but
// name — nothing is ever the first to go, so the limiter refuses whatever
// happens to arrive when capacity runs out.
//
// Derive it from something the client cannot set for itself. A header any
// caller can send is a header every caller will eventually send as Critical,
// and the band stops meaning anything.
func WithPriority(f func(*http.Request) Priority) MiddlewareOption {
	return func(c *middlewareConfig) error {
		if f == nil {
			return fmt.Errorf("%w: WithPriority(nil)", ErrInvalidOption)
		}
		c.priority = f
		return nil
	}
}

// WithShedHandler sets what a refused request gets. The default writes 503
// Service Unavailable with a plain-text body; Retry-After is set before the
// handler runs either way, so a replacement can keep it, overwrite it, or drop
// it. Answer 503 and not 429: the client did nothing wrong, and a 429 tells it
// to slow down permanently for what is your own transient saturation.
func WithShedHandler(h http.Handler) MiddlewareOption {
	return func(c *middlewareConfig) error {
		if h == nil {
			return fmt.Errorf("%w: WithShedHandler(nil)", ErrInvalidOption)
		}
		c.shed = h
		return nil
	}
}

func defaultPriority(*http.Request) Priority { return Default }

func defaultShed(w http.ResponseWriter, _ *http.Request) {
	http.Error(w, "server overloaded", http.StatusServiceUnavailable)
}

// Middleware holds inbound requests to the limiter's capacity and refuses the
// rest by priority. A refused request gets Retry-After, in whole seconds and
// jittered so a refused fleet does not return in lockstep, and 503 Service
// Unavailable. It is plain net/http and composes with any router that accepts
// func(http.Handler) http.Handler.
//
//	l, err := overload.New("api", overload.Gradient(4, 200), 0.6, 0.85)
//	mw, err := overload.Middleware(l, overload.WithPriority(byRoute))
//	mux.Handle("/v1/", mw(api))
//
// Scope it the way you scope a breaker: one limiter and middleware per group
// of routes with a shared latency profile, not one around the whole mux. And
// leave anything that must answer under load — a health or readiness probe —
// outside the subtree it wraps, where no priority is needed because no
// [Acquire] happens at all.
//
// It reports the handler's outcome to the algorithm: a 5xx response releases
// with an error, so a burst of server errors counts as evidence of overload
// and not merely as fast requests. Reading the status means wrapping the
// [http.ResponseWriter]; the wrapper implements Unwrap, so http.ResponseController
// reaches the real writer for flushing, hijacking and deadlines, but a handler
// that type-asserts the writer directly to some other interface will not find
// it.
//
// If l is nil, or any option is invalid, Middleware returns an error wrapping
// [ErrInvalidOption] that describes every problem, so a mistake surfaces at
// bootstrap rather than as a panic on the first request. [MustMiddleware] is
// the same for bootstrap code that treats it as fatal.
func Middleware(l *Limiter, opts ...MiddlewareOption) (func(http.Handler) http.Handler, error) {
	cfg := middlewareConfig{
		priority: defaultPriority,
		shed:     http.HandlerFunc(defaultShed),
	}
	var errs []error
	if l == nil {
		errs = append(errs, fmt.Errorf("%w: Middleware: limiter must not be nil", ErrInvalidOption))
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
			release, err := l.Acquire(r.Context(), cfg.priority(r))
			if err != nil {
				if r.Context().Err() != nil {
					return // the client is already gone; there is nobody to answer
				}
				w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(l.retryAfter().Seconds()))))
				cfg.shed.ServeHTTP(w, r)
				return
			}

			sw := &statusWriter{ResponseWriter: w}
			defer func() { release(sw.outcome()) }()
			next.ServeHTTP(sw, r)
		})
	}, nil
}

// MustMiddleware is [Middleware] for bootstrap code that treats an invalid
// argument as fatal, mirroring [MustRegister]. It panics on error.
func MustMiddleware(l *Limiter, opts ...MiddlewareOption) func(http.Handler) http.Handler {
	mw, err := Middleware(l, opts...)
	if err != nil {
		panic(err)
	}
	return mw
}

// statusWriter remembers the status a handler wrote, so the release can tell
// the algorithm whether the request actually succeeded.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// Unwrap is what [http.ResponseController] follows to reach the writer
// underneath, so wrapping does not cost a handler its Flush, Hijack or
// deadline calls.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// outcome is the error the release reports: a server error, or nil. A handler
// that wrote nothing at all counts as a success, matching net/http's own
// implicit 200.
func (w *statusWriter) outcome() error {
	if w.status >= http.StatusInternalServerError {
		return fmt.Errorf("overload: handler responded %d", w.status)
	}
	return nil
}
