package fallback

import "fmt"

// Observer receives the reader's events, synchronously — implementations
// must return promptly and must not call back into the [Reader] that is
// calling them.
type Observer interface {
	// Get is delivered once per [Reader.Get], after the outcome is known,
	// with the same value [Stats] would count it under.
	Get(Outcome)
	// FastError is delivered whenever the fast source's Get returns an
	// error rather than found=false — never for an ordinary miss, and
	// never for a write-back or lease failure (see WriteBackFailed and
	// LeaseFailed): those arrive through a different method precisely so
	// an Observer can tell "the fast source failed a read" apart from
	// "a load succeeded but couldn't tell the fast source about it."
	FastError(err error)
	// WriteBackFailed is delivered whenever a completed load's Set or
	// Delete on the fast source fails. The load itself still succeeded —
	// its caller already has an answer — so this is purely for
	// observability, never a reason Get returns an error.
	WriteBackFailed(err error)
	// LeaseFailed is delivered whenever a [Leaser] configured via
	// [WithLease] fails to Acquire or Release. Losing a lease race
	// (Acquire returning ok=false) is not a failure and is not delivered
	// here; only Acquire/Release themselves erroring is.
	LeaseFailed(err error)
	// LoadFailed is delivered whenever a background refresh started by
	// [ServeAndRefresh] completes with an error. A blocking load's own
	// failure is never delivered here: it always resolves some caller's
	// Get as Degraded or Failed instead (see the decision table in
	// docs/fallback.md), so the caller already has it.
	LoadFailed(err error)
	// Refreshed is delivered whenever a background refresh started by
	// [ServeAndRefresh] completes successfully.
	Refreshed()
}

// Outcome is what a single [Reader.Get] resolved to; see [Stats] for the
// identity every observation satisfies.
type Outcome int

const (
	// Served means the fast source's value was returned with no load.
	Served Outcome = iota
	// Loaded means a load ran and completed without error — whether it
	// returned a value or reported the key gone (see [PanicError] for
	// the one case a load "completes" via a recovered panic instead).
	Loaded
	// Degraded means a load failed and the fast source's stale value was
	// served instead, per [LoadOrServe].
	Degraded
	// Failed means a load failed and the caller received its error.
	Failed
	// Aborted means ctx was already done; neither source was consulted.
	Aborted
)

// String returns "served", "loaded", "degraded", "failed" or "aborted".
func (o Outcome) String() string {
	switch o {
	case Served:
		return "served"
	case Loaded:
		return "loaded"
	case Degraded:
		return "degraded"
	case Failed:
		return "failed"
	case Aborted:
		return "aborted"
	default:
		return fmt.Sprintf("Outcome(%d)", int(o))
	}
}

// PanicError wraps a value recovered from a panic in the loader. It is
// never re-raised: the goroutine running the load belongs to the [Reader],
// not to whichever caller's Get is waiting on it, so a caller cannot be made
// to crash for a defect in code it did not call directly.
type PanicError struct {
	Value any
	Stack []byte
}

// Error implements error.
func (e *PanicError) Error() string { return fmt.Sprintf("fallback: loader panicked: %v", e.Value) }

// Retryable reports false: a panic is a bug in the loader, not a transient
// condition, and running it again will not fix it. This is the
// Retryable() bool contract package retry looks for, so a retrier composed
// around a guarded origin stops on it instead of retrying a panic up to its
// attempt cap.
func (e *PanicError) Retryable() bool { return false }
