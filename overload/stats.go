package overload

import "fmt"

// Stats is a snapshot of a [Limiter]: cumulative counters, and two live
// gauges.
//
// Identity: Admitted + Shed == Requests. Waited, WaitedThenAdmitted and
// WaitedThenShed annotate a subset of those requests — the ones that queued
// under [WithMaxWait] — rather than adding terms to it, the same relationship
// fallback.Stats's Misses has to its Gets.
type Stats struct {
	// Name is the limiter's name and Algorithm its [Algorithm]'s Name(), both
	// as given to [New].
	Name, Algorithm string

	// Requests is every call to [Acquire] or through [Admission] that named a
	// valid priority.
	Requests uint64
	// Admitted is Requests that got a slot, immediately or after queueing.
	Admitted uint64
	// Shed is Requests that did not. It counts a request whose context ended
	// while it queued, as well as a refusal: the request was not served either
	// way, and splitting the two out would break the identity above for a
	// distinction the metric label does not draw either.
	Shed uint64
	// ShedByPriority is Shed split by the priority that was refused, indexed
	// by [Priority]. A rising sheddable count with the other two at zero is
	// the package working as intended; a rising critical count is the signal
	// that capacity is gone.
	ShedByPriority [3]uint64

	// Waited is Requests that found no capacity and queued under
	// [WithMaxWait]; always 0 without it.
	Waited uint64
	// WaitedThenAdmitted is Waited that a freed slot reached in time — the
	// queue's whole return on the goroutines it held.
	WaitedThenAdmitted uint64
	// WaitedThenShed is Waited that was refused anyway, by the ceiling d, by
	// the give-up rule, or by its own context ending. Sustained non-zero here
	// means the queue is costing memory to delay refusals, not to avoid them:
	// shorten d, or take the queue off.
	WaitedThenShed uint64

	// Capacity is what the algorithm currently says may run at once, and
	// InFlight how much of it is in use. Both are live values, not cumulative.
	Capacity, InFlight int
}

// String renders the snapshot as one log line:
//
//	overload: name=api algorithm=gradient requests=8120 admitted=7940 shed=180 sheddable=171 default=9 critical=0 waited=204 waited_admitted=190 waited_shed=14 capacity=48 in_flight=31
func (s Stats) String() string {
	return fmt.Sprintf(
		"overload: name=%s algorithm=%s requests=%d admitted=%d shed=%d sheddable=%d default=%d critical=%d waited=%d waited_admitted=%d waited_shed=%d capacity=%d in_flight=%d",
		s.Name, s.Algorithm, s.Requests, s.Admitted, s.Shed,
		s.ShedByPriority[Sheddable], s.ShedByPriority[Default], s.ShedByPriority[Critical],
		s.Waited, s.WaitedThenAdmitted, s.WaitedThenShed, s.Capacity, s.InFlight,
	)
}
