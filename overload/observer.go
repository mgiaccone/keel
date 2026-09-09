package overload

// Observer receives the limiter's events, synchronously and on the goroutine
// that caused them — implementations must return promptly and must not call
// back into the [Limiter] that is calling them. The limiter holds its lock
// across the call, so a callback that re-enters [Acquire] or [Stats]
// deadlocks rather than merely misbehaving.
type Observer interface {
	// Admitted is delivered once per request that got a slot, whether it got
	// it immediately or after queueing.
	Admitted(p Priority)
	// Shed is delivered once per request that did not, including one whose
	// context ended while it queued — the same requests [Stats] counts under
	// Shed.
	Shed(p Priority)
	// Waited is delivered when a request starts queueing under [WithMaxWait],
	// not when it stops: at delivery it is not yet known whether the wait ends
	// in an admission or a refusal, and Admitted or Shed follows to say which.
	// Never delivered without WithMaxWait.
	Waited(p Priority)
	// CapacityChanged is delivered whenever [Algorithm.Step] returns a
	// capacity different from the last one, and never for a step that left it
	// where it was — so a quiet stream here means a stable limiter, not a
	// stalled one.
	CapacityChanged(capacity int)
}
