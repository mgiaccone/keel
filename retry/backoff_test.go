package retry

import (
	"errors"
	"slices"
	"testing"
	"time"
)

// schedule renders the first n waits of a backoff at a fixed draw. u = 1 is
// the top of the jitter range, which makes the unjittered shape visible.
func schedule(b Backoff, n int, u float64) []time.Duration {
	var out []time.Duration
	var prev time.Duration
	for i := 1; i <= n; i++ {
		prev = b.Delay(i, prev, u)
		out = append(out, prev)
	}
	return out
}

func TestConstantIsConstant(t *testing.T) {
	got := schedule(Constant(50*time.Millisecond), 4, 0)
	want := []time.Duration{50 * time.Millisecond, 50 * time.Millisecond, 50 * time.Millisecond, 50 * time.Millisecond}
	if !slices.Equal(got, want) {
		t.Fatalf("schedule = %v", got)
	}
}

func TestExponentialDoublesAndCaps(t *testing.T) {
	got := schedule(Exponential(100*time.Millisecond, time.Second), 6, 1)
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, time.Second, time.Second}
	if !slices.Equal(got, want) {
		t.Fatalf("schedule = %v, want %v", got, want)
	}
	// Full jitter: u scales the whole range down to zero.
	if d := Exponential(100*time.Millisecond, time.Second).Delay(3, 0, 0.5); d != 200*time.Millisecond {
		t.Fatalf("u=0.5: %s", d)
	}
	if d := Exponential(100*time.Millisecond, time.Second).Delay(3, 0, 0); d != 0 {
		t.Fatalf("u=0: %s", d)
	}
}

func TestExponentialSaturatesWithoutOverflow(t *testing.T) {
	if d := Exponential(time.Second, 100*time.Second).Delay(1000, 0, 1); d != 100*time.Second {
		t.Fatalf("retry 1000: %s", d)
	}
	if d := Exponential(time.Second, 1<<62).Delay(1000, 0, 1); d != 1<<62 {
		t.Fatalf("huge max, retry 1000: %s", d)
	}
}

func TestDecorrelatedDrawsFromBaseToTriplePrevious(t *testing.T) {
	b := Decorrelated(100*time.Millisecond, time.Second)
	if d := b.Delay(1, 0, 0); d != 100*time.Millisecond {
		t.Fatalf("first, u=0: %s, want base", d)
	}
	if d := b.Delay(1, 0, 1); d != 300*time.Millisecond {
		t.Fatalf("first, u=1: %s, want 3×base", d)
	}
	if d := b.Delay(2, 200*time.Millisecond, 0.5); d != 350*time.Millisecond {
		t.Fatalf("previous 200ms, u=0.5: %s, want base + half of (600ms − base)", d)
	}
	if d := b.Delay(2, 500*time.Millisecond, 1); d != time.Second {
		t.Fatalf("previous 500ms, u=1: %s, want max", d)
	}
	// Under the floor, and past the cap, the draw is clamped.
	if d := b.Delay(2, 10*time.Millisecond, 0); d != 100*time.Millisecond {
		t.Fatalf("previous below base: %s", d)
	}
	if d := b.Delay(2, time.Hour, 1); d != time.Second {
		t.Fatalf("previous above max: %s", d)
	}
}

func TestFibonacciGrowsAndCaps(t *testing.T) {
	got := schedule(Fibonacci(100*time.Millisecond, time.Second), 8, 1)
	want := []time.Duration{
		100 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, 300 * time.Millisecond,
		500 * time.Millisecond, 800 * time.Millisecond, time.Second, time.Second,
	}
	if !slices.Equal(got, want) {
		t.Fatalf("schedule = %v, want %v", got, want)
	}
	if d := Fibonacci(time.Second, 10*time.Second).Delay(100, 0, 1); d != 10*time.Second {
		t.Fatalf("retry 100: %s", d)
	}
	if d := Fibonacci(time.Second, 1<<62).Delay(200, 0, 1); d != 1<<62 {
		t.Fatalf("huge max, retry 200: %s", d)
	}
}

func TestBackoffValidation(t *testing.T) {
	bad := map[string]Backoff{
		"Constant(-1)":         Constant(-time.Second),
		"Exponential(0, 1s)":   Exponential(0, time.Second),
		"Exponential(2s, 1s)":  Exponential(2*time.Second, time.Second),
		"Decorrelated(-1, 1s)": Decorrelated(-time.Second, time.Second),
		"Fibonacci(1s, 0)":     Fibonacci(time.Second, 0),
	}
	for name, b := range bad {
		if err := b.Validate(); !errors.Is(err, ErrInvalidOption) {
			t.Errorf("%s: Validate = %v", name, err)
		}
	}
	good := map[string]Backoff{
		"constant": Constant(0), "exponential": Exponential(time.Second, time.Second),
		"decorrelated": Decorrelated(time.Millisecond, time.Second), "fibonacci": Fibonacci(time.Millisecond, time.Second),
	}
	for name, b := range good {
		if err := b.Validate(); err != nil || b.Name() != name {
			t.Errorf("%s: Validate = %v, Name = %q", name, err, b.Name())
		}
	}
}
