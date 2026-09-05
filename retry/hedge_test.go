package retry

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// gate is an fn whose attempts block until the test releases them, so the
// test decides the order in which attempts answer. It records each attempt's
// context, so a test can check that a loser was cancelled.
type gate struct {
	mu      sync.Mutex
	ctxs    []context.Context
	release []chan gateResult
	done    []bool
	started chan struct{}
}

type gateResult struct {
	v     int
	err   error
	panic any
}

func newGate(t *testing.T) *gate {
	g := &gate{started: make(chan struct{}, 64)}
	t.Cleanup(func() {
		// Release whatever the test left blocked, so no goroutine outlives it.
		g.mu.Lock()
		defer g.mu.Unlock()
		for i, ch := range g.release {
			if !g.done[i] {
				ch <- gateResult{err: context.Canceled}
			}
		}
	})
	return g
}

func (g *gate) fn(ctx context.Context) (int, error) {
	g.mu.Lock()
	ch := make(chan gateResult, 1)
	g.ctxs = append(g.ctxs, ctx)
	g.release = append(g.release, ch)
	g.done = append(g.done, false)
	g.mu.Unlock()
	g.started <- struct{}{}
	r := <-ch
	if r.panic != nil {
		panic(r.panic)
	}
	return r.v, r.err
}

func (g *gate) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.release)
}

// await blocks until at least n attempts have started.
func (g *gate) await(t *testing.T, n int) {
	t.Helper()
	for g.count() < n {
		select {
		case <-g.started:
		case <-time.After(10 * time.Second):
			t.Fatalf("attempt %d never started; %d did", n, g.count())
		}
	}
}

// finish lets attempt n return v and err.
func (g *gate) finish(t *testing.T, n int, v int, err error) {
	t.Helper()
	g.send(t, n, gateResult{v: v, err: err})
}

// explode makes attempt n panic with p.
func (g *gate) explode(t *testing.T, n int, p any) {
	t.Helper()
	g.send(t, n, gateResult{panic: p})
}

func (g *gate) send(t *testing.T, n int, r gateResult) {
	t.Helper()
	g.mu.Lock()
	defer g.mu.Unlock()
	if n > len(g.release) || g.done[n-1] {
		t.Fatalf("attempt %d has not started, or was already released", n)
	}
	g.done[n-1] = true
	g.release[n-1] <- r
}

func (g *gate) ctx(n int) context.Context {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.ctxs[n-1]
}

type doResult struct {
	v   int
	err error
}

// startDo runs Do on its own goroutine and returns the channel its result
// arrives on.
func startDo(h *harness, ctx context.Context, fn func(context.Context) (int, error)) <-chan doResult {
	ch := make(chan doResult, 1)
	go func() {
		v, err := h.Do(ctx, fn)
		ch <- doResult{v, err}
	}()
	return ch
}

func await(t *testing.T, ch <-chan doResult) doResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(10 * time.Second):
		t.Fatal("Do did not return")
		panic("unreachable")
	}
}

// notDone asserts that Do has not returned yet.
func notDone(t *testing.T, ch <-chan doResult) {
	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("Do returned %+v while attempts were still in flight", r)
	case <-time.After(20 * time.Millisecond):
	}
}

func cancelled(ctx context.Context) bool { return errors.Is(ctx.Err(), context.Canceled) }

func TestHedgeOvertakesSlowAttempt(t *testing.T) {
	rec := &recorder{}
	h := newHedged(t, 5*time.Millisecond, true, WithMaxAttempts(2), WithObserver(rec))
	g := newGate(t)
	done := startDo(h, t.Context(), g.fn)
	g.await(t, 1)
	h.fire(t)
	g.await(t, 2)
	g.finish(t, 2, 7, nil)
	if r := await(t, done); r.err != nil || r.v != 7 {
		t.Fatalf("Do = %+v", r)
	}
	if !cancelled(g.ctx(1)) {
		t.Fatal("the losing attempt was not cancelled")
	}
	if g.ctx(2).Err() != nil {
		t.Fatal("the winner's context was cancelled; a value that uses it after Do returned would break")
	}
	if s := h.Stats(); s.Attempts != 2 || s.Hedged != 1 || s.HedgeWon != 1 || s.Succeeded != 1 || s.HedgeAfter != 5*time.Millisecond {
		t.Fatalf("stats = %+v", s)
	}
	g.finish(t, 1, 0, context.Canceled) // the loser answers late; nothing changes
	want := []string{"started", "attempt:1", "attempt:2", "hedge:2", "call:success/2"}
	if !slices.Equal(rec.events, want) {
		t.Fatalf("events %q, want %q", rec.events, want)
	}
	if s := h.Stats(); s.Attempts != 2 || s.Calls != 1 {
		t.Fatalf("a late loser changed the stats: %+v", s)
	}
}

func TestFastFirstAttemptNeverHedges(t *testing.T) {
	h := newHedged(t, 5*time.Millisecond, true)
	v, err := h.Do(t.Context(), func(context.Context) (int, error) { return 3, nil })
	if err != nil || v != 3 {
		t.Fatalf("Do = %d, %v", v, err)
	}
	if s := h.Stats(); s.Attempts != 1 || s.Hedged != 0 || s.HedgeWon != 0 || s.Succeeded != 1 {
		t.Fatalf("stats = %+v", s)
	}
	h.noTimer(t)
}

func TestHedgeFailureDoesNotEndTheCall(t *testing.T) {
	h := newHedged(t, 5*time.Millisecond, true, WithMaxAttempts(2))
	g := newGate(t)
	done := startDo(h, t.Context(), g.fn)
	g.await(t, 1)
	h.fire(t)
	g.await(t, 2)
	g.finish(t, 2, 0, errTransient)
	notDone(t, done)
	if cancelled(g.ctx(1)) {
		t.Fatal("the running attempt was cancelled by a hedge's failure")
	}
	g.finish(t, 1, 3, nil)
	if r := await(t, done); r.err != nil || r.v != 3 {
		t.Fatalf("Do = %+v", r)
	}
	if s := h.Stats(); s.Attempts != 2 || s.Hedged != 1 || s.HedgeWon != 0 || s.Succeeded != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestHedgePermanentFailureAbortsAtOnce(t *testing.T) {
	h := newHedged(t, 5*time.Millisecond, true, WithMaxAttempts(2))
	g := newGate(t)
	done := startDo(h, t.Context(), g.fn)
	g.await(t, 1)
	h.fire(t)
	g.await(t, 2)
	g.finish(t, 2, 0, Permanent(errBoom))
	if r := await(t, done); r.err != errBoom {
		t.Fatalf("Do = %+v, want errBoom unwrapped", r)
	}
	if !cancelled(g.ctx(1)) {
		t.Fatal("the running attempt was not cancelled by the abort")
	}
	if s := h.Stats(); s.Attempts != 2 || s.Aborted != 1 || s.Hedged != 1 || s.HedgeWon != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestEveryAttemptFailsThenTheRetryPathRuns(t *testing.T) {
	rec := &recorder{}
	asked := 0
	h := newHedged(t, 5*time.Millisecond, true, WithMaxAttempts(3), WithObserver(rec),
		WithBudget(func(context.Context) error { asked++; return nil }))
	g := newGate(t)
	done := startDo(h, t.Context(), g.fn)
	g.await(t, 1)
	h.fire(t)
	g.await(t, 2)
	g.finish(t, 1, 0, errTransient)
	notDone(t, done) // the hedge is still running
	g.finish(t, 2, 0, errTransient)
	g.await(t, 3) // the retry, after the schedule's wait
	g.finish(t, 3, 0, errBoom)
	if r := await(t, done); r.err != errBoom {
		t.Fatalf("Do = %+v, want the last error to answer", r)
	}
	want := []string{"started", "attempt:1", "attempt:2", "hedge:2", "wait:2/10ms", "attempt:3", "call:exhausted/3"}
	if !slices.Equal(rec.events, want) {
		t.Fatalf("events %q, want %q", rec.events, want)
	}
	if got := h.recorded(); !slices.Equal(got, []time.Duration{10 * time.Millisecond}) {
		t.Fatalf("waits = %v, want the schedule's wait once and no hedge delay", got)
	}
	if asked != 2 {
		t.Fatalf("budget asked %d times, want once for the hedge and once for the retry", asked)
	}
	if s := h.Stats(); s.Attempts != 3 || s.Hedged != 1 || s.Exhausted != 1 || s.Waited != 10*time.Millisecond {
		t.Fatalf("stats = %+v", s)
	}
}

func TestHedgeBudget(t *testing.T) {
	errNoBudget := errors.New("no budget")
	t.Run("a refusal stops hedging and the running attempt still wins", func(t *testing.T) {
		var hooked []string
		h := newHedged(t, 5*time.Millisecond, true, WithMaxAttempts(3),
			WithBudget(func(context.Context) error { return errNoBudget }),
			WithOnRetry(func(attempt int, err error, delay time.Duration) {
				hooked = append(hooked, fmt.Sprintf("%d:%v:%s", attempt, err, delay))
			}))
		g := newGate(t)
		done := startDo(h, t.Context(), g.fn)
		g.await(t, 1)
		h.fire(t)
		h.noTimer(t) // refused: not re-armed
		if g.count() != 1 {
			t.Fatalf("%d attempts started after a refusal", g.count())
		}
		g.finish(t, 1, 5, nil)
		if r := await(t, done); r.err != nil || r.v != 5 {
			t.Fatalf("Do = %+v", r)
		}
		if s := h.Stats(); s.Succeeded != 1 || s.Hedged != 0 || s.BudgetDenied != 0 {
			t.Fatalf("stats = %+v", s)
		}
		if want := []string{"1:no budget:0s"}; !slices.Equal(hooked, want) {
			t.Fatalf("hook saw %v, want the veto's error %v", hooked, want)
		}
	})
	t.Run("a refusal followed by failure ends the call as budget", func(t *testing.T) {
		h := newHedged(t, 5*time.Millisecond, true, WithMaxAttempts(3),
			WithBudget(func(context.Context) error { return errNoBudget }))
		g := newGate(t)
		done := startDo(h, t.Context(), g.fn)
		g.await(t, 1)
		h.fire(t)
		h.noTimer(t)
		g.finish(t, 1, 0, errTransient)
		if r := await(t, done); r.err != errTransient {
			t.Fatalf("Do = %+v, want the last error from fn", r)
		}
		if s := h.Stats(); s.BudgetDenied != 1 || s.Attempts != 1 || len(h.recorded()) != 0 {
			t.Fatalf("stats = %+v, waits = %v", s, h.recorded())
		}
	})
	t.Run("asked before every hedge", func(t *testing.T) {
		asked := 0
		h := newHedged(t, 5*time.Millisecond, true, WithMaxAttempts(3),
			WithBudget(func(context.Context) error { asked++; return nil }))
		g := newGate(t)
		done := startDo(h, t.Context(), g.fn)
		g.await(t, 1)
		h.fire(t)
		g.await(t, 2)
		h.fire(t)
		g.await(t, 3)
		h.noTimer(t) // the cap: nothing more may start
		if asked != 2 {
			t.Fatalf("budget asked %d times", asked)
		}
		g.finish(t, 3, 1, nil)
		await(t, done)
		if s := h.Stats(); s.Hedged != 2 || s.HedgeWon != 1 || s.Attempts != 3 {
			t.Fatalf("stats = %+v", s)
		}
	})
}

func TestHedgeCapBoundsHedgesAndRetriesTogether(t *testing.T) {
	h := newHedged(t, 5*time.Millisecond, true, WithMaxAttempts(2))
	g := newGate(t)
	done := startDo(h, t.Context(), g.fn)
	g.await(t, 1)
	h.fire(t)
	g.await(t, 2)
	h.noTimer(t)
	g.finish(t, 1, 0, errTransient)
	g.finish(t, 2, 0, errTransient) // the cap is reached: no retry follows
	if r := await(t, done); r.err != errTransient {
		t.Fatalf("Do = %+v", r)
	}
	if s := h.Stats(); s.Attempts != 2 || s.Exhausted != 1 || len(h.recorded()) != 0 {
		t.Fatalf("stats = %+v, waits = %v", s, h.recorded())
	}
	if !cancelled(g.ctx(1)) || !cancelled(g.ctx(2)) {
		t.Fatal("attempt contexts outlived a call with no winner; they would stay registered in the caller's context")
	}
}

// TestLateLoserPanicIsNotSwallowed runs the scenario in a child process: a
// loser that panics after the call has returned must crash the process, as
// an unrecovered panic in fn would, not vanish into a channel nobody reads.
func TestLateLoserPanicIsNotSwallowed(t *testing.T) {
	if os.Getenv("KEEL_RETRY_LATE_PANIC") == "1" {
		r, err := New("late", Constant(0), WithMaxAttempts(2), WithHedge(time.Millisecond))
		if err != nil {
			panic(err)
		}
		var n atomic.Int32
		r.Do(context.Background(), func(ctx context.Context) (int, error) {
			if n.Add(1) == 1 {
				<-ctx.Done() // the loser, cancelled once the hedge has won
				panic("late loser")
			}
			return 1, nil
		})
		time.Sleep(200 * time.Millisecond) // the loser panics on its own goroutine after Do returned
		os.Exit(0)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestLateLoserPanicIsNotSwallowed$")
	cmd.Env = append(os.Environ(), "KEEL_RETRY_LATE_PANIC=1")
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "late loser") {
		t.Fatalf("child exited with %v; output:\n%s", err, out)
	}
}

// maxAttempt records the largest attempt number any call reached. Attempt is
// called on the goroutine running the call, so calls from many goroutines
// need the atomic.
type maxAttempt struct{ max atomic.Int64 }

func (m *maxAttempt) Started() {}
func (m *maxAttempt) Attempt(n int) {
	for {
		cur := m.max.Load()
		if int64(n) <= cur || m.max.CompareAndSwap(cur, int64(n)) {
			return
		}
	}
}
func (m *maxAttempt) Hedge(int)               {}
func (m *maxAttempt) Wait(int, time.Duration) {}
func (m *maxAttempt) Call(Result, int)        {}

func TestHedgeCallerCancels(t *testing.T) {
	h := newHedged(t, 5*time.Millisecond, true, WithMaxAttempts(3))
	g := newGate(t)
	ctx, cancel := context.WithCancel(t.Context())
	done := startDo(h, ctx, g.fn)
	g.await(t, 1)
	h.fire(t)
	g.await(t, 2)
	cancel()
	if !cancelled(g.ctx(1)) || !cancelled(g.ctx(2)) {
		t.Fatal("attempts did not see the caller's cancellation")
	}
	g.finish(t, 2, 0, context.Canceled)
	if r := await(t, done); !errors.Is(r.err, context.Canceled) {
		t.Fatalf("Do = %+v", r)
	}
	if s := h.Stats(); s.Canceled != 1 || s.Attempts != 2 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestHedgePanicPropagatesToTheCaller(t *testing.T) {
	h := newHedged(t, 5*time.Millisecond, true, WithMaxAttempts(2))
	g := newGate(t)
	panicked := make(chan any, 1)
	go func() {
		defer func() { panicked <- recover() }()
		h.Do(t.Context(), g.fn)
	}()
	g.await(t, 1)
	h.fire(t)
	g.await(t, 2)
	g.explode(t, 2, "kaboom")
	select {
	case p := <-panicked:
		if p != "kaboom" {
			t.Fatalf("recovered %v", p)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the panic did not reach the caller")
	}
	if !cancelled(g.ctx(1)) {
		t.Fatal("the other attempt was not cancelled")
	}
}

// TestDiscardHookSeesOnlySupersededResults pins the contract the transport
// relies on: every result Do does not return, and nothing else, reaches the
// hook, on both paths.
func TestDiscardHookSeesOnlySupersededResults(t *testing.T) {
	type seen struct {
		v   int
		err error
	}
	var mu sync.Mutex
	var got []seen
	hook := func(v int, err error) {
		mu.Lock()
		got = append(got, seen{v, err})
		mu.Unlock()
	}
	discarded := func() []seen {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(got)
	}

	// Sequential: the two failed attempts a retry supersedes; the returned
	// value is never discarded.
	h := newHarness(t, Constant(time.Millisecond))
	v, err := do(h.Retrier, t.Context(), failing(2, errTransient, 9), hook)
	if err != nil || v != 9 {
		t.Fatalf("do = %d, %v", v, err)
	}
	if want := []seen{{0, errTransient}, {0, errTransient}}; !slices.Equal(discarded(), want) {
		t.Fatalf("discarded %v, want %v", discarded(), want)
	}

	// Hedged: a failed hedge is superseded by the winner, and a loser that
	// answers after the call returned is drained into the hook.
	got = nil
	h = newHedged(t, 5*time.Millisecond, true, WithMaxAttempts(3))
	g := newGate(t)
	res := make(chan doResult, 1)
	go func() {
		v, err := do(h.Retrier, t.Context(), g.fn, hook)
		res <- doResult{v, err}
	}()
	g.await(t, 1)
	h.fire(t)
	g.await(t, 2)
	h.fire(t)
	g.await(t, 3)
	g.finish(t, 3, 0, errTransient) // pending
	g.finish(t, 2, 4, nil)          // wins; attempt 1 is cancelled and still owes an answer
	if r := await(t, res); r.err != nil || r.v != 4 {
		t.Fatalf("do = %+v", r)
	}
	g.finish(t, 1, 8, nil) // the loser's late success is discarded, not returned
	deadline := time.Now().Add(10 * time.Second)
	for len(discarded()) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if want := []seen{{0, errTransient}, {8, nil}}; !slices.Equal(discarded(), want) {
		t.Fatalf("discarded %v, want %v", discarded(), want)
	}
}

func TestHedgeStatsIdentitiesUnderChaos(t *testing.T) {
	budgetLeft := atomic.Int64{}
	budgetLeft.Store(300)
	deepest := &maxAttempt{}
	h := newHedged(t, time.Millisecond, false, WithMaxAttempts(3), WithObserver(deepest),
		WithBudget(func(context.Context) error {
			if budgetLeft.Add(-1) < 0 {
				return errBoom
			}
			return nil
		}))
	var wg sync.WaitGroup
	const workers, calls = 16, 40
	for range workers {
		wg.Go(func() {
			for range calls {
				ctx, cancel := context.WithCancel(context.Background())
				// fn runs concurrently with itself under WithHedge, so the
				// draws come from the package generator, which is safe for that.
				h.Do(ctx, func(ctx context.Context) (int, error) {
					// Some attempts are slow enough for a hedge to overtake them.
					if rand.IntN(2) == 0 {
						select {
						case <-time.After(time.Duration(rand.IntN(500)) * time.Microsecond):
						case <-ctx.Done():
							return 0, ctx.Err()
						}
					}
					switch rand.IntN(6) {
					case 0:
						return 1, nil
					case 1:
						return 0, Permanent(errBoom)
					case 2:
						cancel()
						return 0, errBoom
					default:
						return 0, errBoom
					}
				})
				cancel()
			}
		})
	}
	wg.Wait()
	s := h.Stats()
	if s.Calls != workers*calls || s.Succeeded+s.Exhausted+s.Aborted+s.Canceled+s.BudgetDenied != s.Calls {
		t.Fatalf("identity broken: %+v", s)
	}
	if s.Attempts < s.Calls || s.Attempts > 3*s.Calls || s.Hedged > s.Attempts-s.Calls || s.HedgeWon > s.Hedged || s.HedgeWon > s.Succeeded {
		t.Fatalf("hedge identities broken: %+v", s)
	}
	if s.Succeeded == 0 || s.Exhausted == 0 || s.Aborted == 0 || s.Canceled == 0 || s.BudgetDenied == 0 || s.Hedged == 0 || s.HedgeWon == 0 {
		t.Fatalf("not every path exercised: %+v", s)
	}
	if n := deepest.max.Load(); n > 3 {
		t.Fatalf("a call reached attempt %d past a cap of 3: a stale hedge timer started it", n)
	}
}

// BenchmarkDoSuccessHedged is BenchmarkDoSuccess with WithHedge set and a
// fast fn: the cost of the goroutine, the channel and the timer the hedged
// path needs even when no hedge starts.
func BenchmarkDoSuccessHedged(b *testing.B) {
	b.ReportAllocs()
	r, err := New("bench", Exponential(time.Millisecond, time.Second), WithHedge(time.Second))
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	ok := func(context.Context) (int, error) { return 1, nil }
	for b.Loop() {
		v, _ := r.Do(ctx, ok)
		_sink += v
	}
}
