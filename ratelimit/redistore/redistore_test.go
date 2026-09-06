package redistore_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/mgiaccone/keel/conformance/ratelimitstore"
	"github.com/mgiaccone/keel/ratelimit"
	"github.com/mgiaccone/keel/ratelimit/redistore"
)

// must fails the test now if err is not nil. It exists so the constructors
// this suite calls constantly don't each need a three-line
// if err != nil { t.Fatal(err) } beside them.
func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// _addr and _skipReason are set once by TestMain, before any test runs, and
// read by newClient. REDIS_ADDR if set — a manual override for environments
// with no Docker access — otherwise one disposable Valkey container through
// testcontainers-go (image overridable with RATELIMIT_TEST_IMAGE), terminated
// when the package's tests finish. If neither is available, _skipReason is
// set and every test that needs a server skips with it; testcontainers-go's
// own reaper is the backstop if the test binary is killed before TestMain's
// own cleanup runs.
var (
	_addr       string
	_skipReason string
)

func TestMain(m *testing.M) {
	var container *tcredis.RedisContainer
	redisAddr := os.Getenv("REDIS_ADDR")
	image := os.Getenv("RATELIMIT_TEST_IMAGE")
	if image == "" {
		image = "valkey/valkey:8-alpine"
	}
	ctx := context.Background()

	if redisAddr != "" {
		_addr = redisAddr
	} else if c, err := tcredis.Run(ctx, image); err != nil {
		_skipReason = fmt.Sprintf("REDIS_ADDR not set and no Valkey container could be started (is Docker running?): %v", err)
	} else if connStr, err := c.ConnectionString(ctx); err != nil {
		container = c // still started; terminate it below even though it's unusable
		_skipReason = fmt.Sprintf("container connection string: %v", err)
	} else if opts, err := redis.ParseURL(connStr); err != nil {
		container = c
		_skipReason = fmt.Sprintf("parse connection string: %v", err)
	} else {
		container = c
		_addr = opts.Addr
		fmt.Printf("redistore_test: started %s at %s\n", image, _addr)
	}

	code := m.Run()
	// Not a defer: os.Exit below runs no deferred function in the process,
	// so this has to be an ordinary statement between m.Run() and os.Exit.
	if container != nil {
		if err := container.Terminate(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "redistore_test: terminate container: %v\n", err)
		}
	}
	os.Exit(code)
}

// newClient returns a client on the package's shared server, skipping the
// test if TestMain could not reach one.
func newClient(t *testing.T) *redis.Client {
	t.Helper()
	if _skipReason != "" {
		t.Skip(_skipReason)
	}

	client := redis.NewClient(&redis.Options{Addr: _addr})
	t.Cleanup(func() { client.Close() })

	deadline := time.Now().Add(20 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := client.Ping(ctx).Err()
		cancel()
		if err == nil {
			return client
		}
		if time.Now().After(deadline) {
			t.Fatalf("redis at %s not ready: %v", _addr, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// prefix gives a test its own key namespace on the shared server.
func prefix() string { return fmt.Sprintf("ratelimit-test:%d:", time.Now().UnixNano()) }

// TestStoreContract runs the store contract every Store must satisfy.
func TestStoreContract(t *testing.T) {
	client := newClient(t)
	ratelimitstore.Run(t, func(t *testing.T) ratelimit.Store {
		store, err := redistore.NewStore(client, redistore.WithKeyPrefix(prefix()))
		must(t, err)
		return store
	})
}

// TestGCRARefillsAgainstTheServerClock goes deep on one algorithm: burst
// consumption, an exact RetryAfter bound, and — the property genuinely
// specific to a distributed store — a sleep past RetryAfter that proves
// refill works against Redis's own clock, not the local process's.
func TestGCRARefillsAgainstTheServerClock(t *testing.T) {
	client := newClient(t)
	store, err := redistore.NewStore(client, redistore.WithKeyPrefix(prefix()))
	must(t, err)
	l, err := ratelimit.New("it", ratelimit.GCRA(5, 3), store)
	must(t, err)
	ctx := context.Background()

	for i := range 3 {
		d, err := l.Allow(ctx, "k")
		if err != nil || !d.Allowed || d.Remaining != 2-i {
			t.Fatalf("call %d: %+v %v", i, d, err)
		}
	}

	d, err := l.Allow(ctx, "k")
	if err != nil || d.Allowed || d.RetryAfter <= 0 || d.RetryAfter > 200*time.Millisecond {
		t.Fatalf("limited: %+v %v; want RetryAfter ≤ 200ms (1 token at 5/s)", d, err)
	}

	time.Sleep(d.RetryAfter + 20*time.Millisecond)
	if d, err := l.Allow(ctx, "k"); err != nil || !d.Allowed {
		t.Fatalf("after RetryAfter: %+v %v", d, err)
	}

	if d, err := l.Allow(ctx, "other"); err != nil || !d.Allowed || d.Remaining != 2 {
		t.Fatalf("other key: %+v %v", d, err)
	}

	if s := l.Stats(); s.Allowed != 5 || s.Limited != 1 || s.Errors != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

// TestEveryAlgorithmWorksOnRedis goes wide, shallowly, across all three
// algorithms: State{A, B, C} means something different for each — GCRA only
// ever writes A — so this is what would catch a script bug that corrupts B
// or C, which the GCRA-only test above could never expose.
func TestEveryAlgorithmWorksOnRedis(t *testing.T) {
	client := newClient(t)
	for name, algo := range map[string]ratelimit.Algorithm{
		"gcra":           ratelimit.GCRA(1, 2),
		"fixed_window":   ratelimit.FixedWindow(2, time.Hour),
		"sliding_window": ratelimit.SlidingWindow(2, time.Hour),
	} {
		t.Run(name, func(t *testing.T) {
			store, err := redistore.NewStore(client, redistore.WithKeyPrefix(prefix()))
			must(t, err)
			l, err := ratelimit.New(name, algo, store)
			must(t, err)
			ctx := context.Background()

			for i := range 2 {
				if d, err := l.Allow(ctx, "k"); err != nil || !d.Allowed {
					t.Fatalf("call %d: %+v %v", i, d, err)
				}
			}

			if d, err := l.Allow(ctx, "k"); err != nil || d.Allowed || d.RetryAfter <= 0 {
				t.Fatalf("third: %+v %v", d, err)
			}
		})
	}
}

// TestLimitIsSharedAcrossClients checks the one property a distributed store
// has and an in-process one cannot: two separate connections see the same
// server-side state.
func TestLimitIsSharedAcrossClients(t *testing.T) {
	client := newClient(t)
	other := redis.NewClient(&redis.Options{Addr: client.Options().Addr})
	defer other.Close()
	p := prefix()
	storeA, err := redistore.NewStore(client, redistore.WithKeyPrefix(p))
	must(t, err)
	storeB, err := redistore.NewStore(other, redistore.WithKeyPrefix(p))
	must(t, err)
	a, _ := ratelimit.New("shared", ratelimit.GCRA(1, 2), storeA)
	b, _ := ratelimit.New("shared", ratelimit.GCRA(1, 2), storeB)
	ctx := context.Background()

	a.Allow(ctx, "k")
	b.Allow(ctx, "k")

	if d, err := a.Allow(ctx, "k"); err != nil || d.Allowed {
		t.Fatalf("burst of 2 was not shared between clients: %+v %v", d, err)
	}
}

// TestUnreachableServerIsAnError checks real network-failure handling, which
// MemoryStore has nothing analogous to.
func TestUnreachableServerIsAnError(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond, MaxRetries: -1})
	defer client.Close()
	store, err := redistore.NewStore(client)
	must(t, err)
	l, err := ratelimit.New("down", ratelimit.GCRA(1, 1), store)
	must(t, err)

	if _, err := l.Allow(context.Background(), "k"); err == nil {
		t.Fatal("expected an error from an unreachable server")
	}

	if s := l.Stats(); s.Errors != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

// TestNewStoreInvalidOptions plays the same role TestMemoryStoreInvalidOptions
// does for MemoryStore: a nil client (or, once one exists, a bad option) is
// caught at construction, not the first call.
func TestNewStoreInvalidOptions(t *testing.T) {
	if _, err := redistore.NewStore(nil); !errors.Is(err, redistore.ErrInvalidOption) {
		t.Fatalf("nil client: err = %v", err)
	}
}
