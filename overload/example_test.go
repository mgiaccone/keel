package overload_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/mgiaccone/keel/overload"
)

// Example shows the shape the package is for: a limiter sized for one class of
// work, holding concurrent requests to a capacity it works out for itself, and
// giving up the least important band first when that capacity runs out.
func Example() {
	// A Gradient whose min equals its max pins capacity, which is what makes
	// an example reproducible; a real limiter gives it a range and lets the
	// number move.
	l, err := overload.New("checkout", overload.Gradient(4, 4), 0.5, 0.75)
	if err != nil {
		fmt.Println("New:", err)
		return
	}

	// Capacity 4 divides into two slots for sheddable work, three for default
	// and all four for critical.
	var held []func(error)
	for _, p := range []overload.Priority{overload.Sheddable, overload.Sheddable, overload.Default} {
		release, err := l.Acquire(context.Background(), p)
		if err != nil {
			fmt.Println("unexpected refusal:", err)
			return
		}
		held = append(held, release)
	}

	// A third sheddable request is over that band's share, even though two of
	// the four slots are still free.
	_, err = l.Acquire(context.Background(), overload.Sheddable)
	fmt.Println("sheddable:", err, errors.Is(err, overload.ErrOverloaded))

	// Critical work is admitted into the same slots the sheddable request was
	// refused for, which is the whole point of the shares.
	release, err := l.Acquire(context.Background(), overload.Critical)
	if err != nil {
		fmt.Println("unexpected refusal:", err)
		return
	}
	held = append(held, release)

	for _, release := range held {
		release(nil)
	}
	fmt.Println(l.Stats())

	// Output:
	// sheddable: overload: capacity exhausted, sheddable request shed true
	// overload: name=checkout algorithm=gradient requests=5 admitted=4 shed=1 sheddable=1 default=0 critical=0 waited=0 waited_admitted=0 waited_shed=0 capacity=4 in_flight=0
}

// ExampleMiddleware wires a limiter into net/http, with the priority derived
// from the route. Note what is not wrapped: /healthz answers whatever the
// limiter thinks, because a health check shed under load makes a busy instance
// look dead to whatever is watching it.
func ExampleMiddleware() {
	l, err := overload.New("api", overload.Gradient(1, 1), 0.5, 0.75)
	if err != nil {
		fmt.Println("New:", err)
		return
	}

	mw, err := overload.Middleware(l, overload.WithPriority(func(r *http.Request) overload.Priority {
		switch {
		case strings.HasPrefix(r.URL.Path, "/v1/beacon"):
			return overload.Sheddable
		case strings.HasPrefix(r.URL.Path, "/v1/pay"):
			return overload.Critical
		default:
			return overload.Default
		}
	}))
	if err != nil {
		fmt.Println("Middleware:", err)
		return
	}

	api := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "served", r.URL.Path)
	})
	mux := http.NewServeMux()
	mux.Handle("/v1/", mw(api))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") })

	for _, path := range []string{"/v1/pay", "/healthz"} {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		fmt.Printf("%s -> %d %s", path, w.Code, w.Body.String())
	}

	// Output:
	// /v1/pay -> 200 served /v1/pay
	// /healthz -> 200 ok
}

// ExampleAdmission composes the limiter into anything that takes keel's veto
// shape — breaker.WithAdmission, retry.WithBudget — without either package
// importing the other. Read its doc comment first: the veto holds no slot, so
// it is a coarser gate than Middleware and never teaches the limiter anything.
func ExampleAdmission() {
	l, err := overload.New("outbound", overload.Gradient(2, 2), 0.5, 0.75)
	if err != nil {
		fmt.Println("New:", err)
		return
	}

	veto := overload.Admission(l, overload.Default)
	fmt.Println("idle:", veto(context.Background()))

	release, err := l.Acquire(context.Background(), overload.Critical)
	if err != nil {
		fmt.Println("unexpected refusal:", err)
		return
	}
	fmt.Println("saturated:", veto(context.Background()))
	release(nil)

	// Output:
	// idle: <nil>
	// saturated: overload: capacity exhausted, default request shed
}
