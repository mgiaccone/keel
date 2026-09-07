package fallback

import "fmt"

// Stats is a snapshot of a [Reader]'s counters, all cumulative.
//
// Identity: Served + Loaded + Degraded + Failed + Aborted == Gets. Misses,
// Refreshes, LoadFailures and FastErrors annotate a Get or a background
// refresh; they are not terms in that identity.
type Stats struct {
	// Name is the reader's name, as given to [New].
	Name string

	// Gets is every call to [Reader.Get].
	Gets uint64
	// Served is Gets answered from the fast source with no load.
	Served uint64
	// Loaded is Gets whose blocking load completed without error —
	// including one that found the key gone at the origin, which is
	// success from the load's own perspective. A background refresh
	// started by [ServeAndRefresh] never counts here; see Refreshes.
	Loaded uint64
	// Degraded is Gets where a load failed and a stale value was served
	// instead, per [LoadOrServe].
	Degraded uint64
	// Failed is Gets where a load failed and the caller received the error.
	Failed uint64
	// Aborted is Gets whose ctx was already done; neither source was
	// consulted.
	Aborted uint64

	// Misses is Gets where the fast source reported the key absent, or a
	// blocking load found the origin had none for it either.
	Misses uint64
	// Refreshes is background refreshes, started by [ServeAndRefresh],
	// that completed successfully. Disjoint from Loaded: a refresh never
	// resolves a Get by itself, so it is not a term in the Gets identity.
	Refreshes uint64
	// LoadFailures is background refreshes that returned an error. A
	// blocking load's failure is counted in Degraded or Failed instead,
	// since it always resolves some caller's Get.
	LoadFailures uint64
	// FastErrors is calls to the fast source that returned an error
	// rather than found=false.
	FastErrors uint64
	// WriteBackFailures is completed loads whose Set or Delete on the fast
	// source failed. The load itself still succeeded; this never affects a
	// Get's outcome, only how much of what a load produced actually made it
	// into the fast source.
	WriteBackFailures uint64
	// LeaseFailures is background refreshes whose [Leaser] failed to
	// Acquire or Release, as opposed to losing an ordinary lease race
	// (Acquire ok=false), which counts nowhere.
	LeaseFailures uint64
}

// String renders the snapshot as one log line, for example:
//
//	fallback: name=catalog gets=8120 served=7900 loaded=180 degraded=3 failed=1 aborted=0 misses=70 refreshes=140 load_failures=4 fast_errors=0 write_back_failures=0 lease_failures=0
func (s Stats) String() string {
	return fmt.Sprintf(
		"fallback: name=%s gets=%d served=%d loaded=%d degraded=%d failed=%d aborted=%d misses=%d refreshes=%d load_failures=%d fast_errors=%d write_back_failures=%d lease_failures=%d",
		s.Name, s.Gets, s.Served, s.Loaded, s.Degraded, s.Failed, s.Aborted, s.Misses, s.Refreshes, s.LoadFailures, s.FastErrors,
		s.WriteBackFailures, s.LeaseFailures,
	)
}
