package fallback

import "time"

// Verdict is what [Policy] decides about a value the fast source returned.
type Verdict int

const (
	// Serve uses the value; no load runs.
	Serve Verdict = iota
	// ServeAndRefresh uses the value immediately and starts at most one
	// background load per key to replace it; see [WithLease] for what
	// "one" means across a fleet rather than one process.
	ServeAndRefresh
	// LoadOrServe loads and waits; if the load fails, the value already
	// held is served instead of the error. This is what makes an outage
	// degrade rather than fail: row 4 of the package's decision table.
	LoadOrServe
	// Load loads and waits; if the load fails, the caller gets the error.
	// Use this only where a wrong-but-recent answer is worse than none.
	Load
)

// Policy judges a value the fast source returned. now comes from the
// [Reader]'s injected clock, not time.Now, so a policy stays a pure function
// of its inputs and is testable without a real clock.
//
// The package has no default idea of what makes a value acceptable — not a
// TTL, not a version, nothing. Whatever decides it lives on V, because that
// is the one thing every caller's fast source actually agrees on: the value
// itself. A nil Policy defaults to always [Serve], meaning the fast source is
// trusted until [Reader.Invalidate] removes an entry — appropriate for a
// fast source nothing but this reader ever writes to.
type Policy[V any] func(v V, now time.Time) Verdict
