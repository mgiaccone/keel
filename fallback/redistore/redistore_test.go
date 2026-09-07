package redistore_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/mgiaccone/keel/conformance/fallbackstore"
	"github.com/mgiaccone/keel/fallback"
	"github.com/mgiaccone/keel/fallback/redistore"
)

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// _addr and _skipReason are set once by TestMain, before any test runs, and
// read by newClient. REDIS_ADDR if set — a manual override for environments
// with no Docker access — otherwise one disposable Valkey container through
// testcontainers-go (image overridable with FALLBACK_TEST_IMAGE), terminated
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
	image := os.Getenv("FALLBACK_TEST_IMAGE")
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
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
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

// prefix gives a test its own key namespace on the shared server. The
// counter, not just the clock, is what makes it unique: -count/-cpu run
// this package's tests several times in quick succession, and two calls to
// time.Now().UnixNano() alone could in principle land in the same tick.
var _prefixSeq atomic.Uint64

func prefix() string {
	return fmt.Sprintf("fallback-test:%d-%d:", time.Now().UnixNano(), _prefixSeq.Add(1))
}

// TestStoreContract runs the store contract every fallback.Store must
// satisfy.
func TestStoreContract(t *testing.T) {
	client := newClient(t)
	fallbackstore.Run(t, func(t *testing.T) fallback.Store[string, int] {
		store, err := redistore.NewStore[int](client, redistore.JSON[int](), redistore.WithKeyPrefix[int](prefix()))
		must(t, err)
		return store
	}, "k1", "k2", 1, 2)
}

// TestValueSharedAcrossClients checks the one property a distributed store
// has and MemoryStore cannot: two separate connections see the same
// server-side value.
func TestValueSharedAcrossClients(t *testing.T) {
	client := newClient(t)
	other := redis.NewClient(&redis.Options{Addr: client.Options().Addr})
	defer other.Close()
	p := prefix()

	storeA, err := redistore.NewStore[string](client, redistore.JSON[string](), redistore.WithKeyPrefix[string](p))
	must(t, err)
	storeB, err := redistore.NewStore[string](other, redistore.JSON[string](), redistore.WithKeyPrefix[string](p))
	must(t, err)

	ctx := t.Context()
	must(t, storeA.Set(ctx, "k", "hello"))
	if v, found, err := storeB.Get(ctx, "k"); err != nil || !found || v != "hello" {
		t.Fatalf("Get from the other client = %v, %v, %v, want hello, true, nil", v, found, err)
	}
}

// TestTTLExpiresAnUnrefreshedValue checks WithTTL, which is not part of the
// shared Store contract — MemoryStore has no equivalent, since its bound is
// key count, not time.
func TestTTLExpiresAnUnrefreshedValue(t *testing.T) {
	client := newClient(t)
	store, err := redistore.NewStore[int](client, redistore.JSON[int](),
		redistore.WithKeyPrefix[int](prefix()), redistore.WithTTL[int](60*time.Millisecond))
	must(t, err)

	ctx := t.Context()
	must(t, store.Set(ctx, "k", 1))

	deadline := time.Now().Add(5 * time.Second)
	for {
		_, found, err := store.Get(ctx, "k")
		must(t, err)
		if !found {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("value did not expire")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestLeaseAcquireReleaseAndExpiry exercises fallback.Leaser: a second
// Acquire on a held key loses the race; Release frees it for the next one;
// an unreleased lease still expires on its own ttl.
func TestLeaseAcquireReleaseAndExpiry(t *testing.T) {
	client := newClient(t)
	// Both prefixes: this test deliberately leaves its final lease
	// unreleased, so a shared lease prefix across repeats (-count, -cpu)
	// would collide with that leftover exactly like a shared value prefix
	// would — WithKeyPrefix alone does not isolate lease keys.
	store, err := redistore.NewStore[int](client, redistore.JSON[int](),
		redistore.WithKeyPrefix[int](prefix()), redistore.WithLeaseKeyPrefix[int](prefix()))
	must(t, err)
	ctx := t.Context()

	token, ok, err := store.Acquire(ctx, "k", time.Minute)
	if err != nil || !ok || token == "" {
		t.Fatalf("first Acquire = %q, %v, %v, want a token, true, nil", token, ok, err)
	}

	if _, ok, err := store.Acquire(ctx, "k", time.Minute); err != nil || ok {
		t.Fatalf("second Acquire = %v, %v, want false, nil (held elsewhere)", ok, err)
	}

	must(t, store.Release(ctx, "k", token))

	token2, ok, err := store.Acquire(ctx, "k", 60*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("Acquire after Release = %v, %v, want true, nil", ok, err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		_, ok, err := store.Acquire(ctx, "k", time.Minute)
		must(t, err)
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lease did not expire")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The expired lease's own token can no longer release anything; the
	// caller who lost the race above already replaced it.
	must(t, store.Release(ctx, "k", token2))
}

// TestUnreachableServerIsAnError checks real network-failure handling, which
// MemoryStore has nothing analogous to.
func TestUnreachableServerIsAnError(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond, MaxRetries: -1})
	defer client.Close()
	store, err := redistore.NewStore[int](client, redistore.JSON[int]())
	must(t, err)

	if _, _, err := store.Get(t.Context(), "k"); err == nil {
		t.Fatal("expected an error from an unreachable server")
	}
}

// TestNewStoreInvalidOptions plays the same role TestMemoryStoreInvalidOptions
// does for MemoryStore: a nil client or codec is caught at construction, not
// the first call.
func TestNewStoreInvalidOptions(t *testing.T) {
	if _, err := redistore.NewStore[int](nil, redistore.JSON[int]()); !errors.Is(err, redistore.ErrInvalidOption) {
		t.Fatalf("nil client: err = %v", err)
	}
	if _, err := redistore.NewStore[int](redis.NewClient(&redis.Options{}), nil); !errors.Is(err, redistore.ErrInvalidOption) {
		t.Fatalf("nil codec: err = %v", err)
	}
}
