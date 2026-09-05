package goredis_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mgiaccone/keel/ratelimit"
	"github.com/mgiaccone/keel/ratelimit/goredis"
	"github.com/mgiaccone/keel/ratelimit/storetest"
)

// These tests need a server. They use REDIS_ADDR if set; otherwise they start
// a disposable Valkey container with the docker CLI (image overridable with
// RATELIMIT_TEST_IMAGE), honouring DOCKER_HOST and DOCKER_CONTEXT, and remove
// it afterwards. They skip when neither is available.

func newClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = startContainer(t)
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
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
			t.Fatalf("redis at %s not ready: %v", addr, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func startContainer(t *testing.T) string {
	t.Helper()
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Skip("REDIS_ADDR not set and docker not on PATH")
	}
	if out, err := exec.Command(docker, "info", "--format", "{{.ServerVersion}}").CombinedOutput(); err != nil || strings.TrimSpace(string(out)) == "" {
		t.Skipf("REDIS_ADDR not set and docker daemon not reachable: %v %s", err, out)
	}
	image := os.Getenv("RATELIMIT_TEST_IMAGE")
	if image == "" {
		image = "valkey/valkey:8-alpine"
	}
	out, err := exec.Command(docker, "run", "-d", "--rm", "-p", "127.0.0.1::6379", image).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run %s: %v\n%s", image, err, out)
	}
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() {
		if out, err := exec.Command(docker, "rm", "-f", id).CombinedOutput(); err != nil {
			t.Logf("docker rm -f %s: %v %s", id[:12], err, out)
		}
	})
	for attempt := 0; ; attempt++ {
		out, err = exec.Command(docker, "port", id, "6379/tcp").CombinedOutput()
		if err == nil && strings.TrimSpace(string(out)) != "" {
			break
		}
		if attempt == 50 {
			t.Fatalf("docker port: %v\n%s", err, out)
		}
		time.Sleep(100 * time.Millisecond)
	}
	addr := strings.TrimSpace(strings.SplitN(string(out), "\n", 2)[0])
	t.Logf("started %s as %s at %s", image, id[:12], addr)
	return addr
}

func prefix() string { return fmt.Sprintf("ratelimit-test:%d:", time.Now().UnixNano()) }

// TestStoreContract runs the store contract every Store must satisfy.
func TestStoreContract(t *testing.T) {
	client := newClient(t)
	storetest.Run(t, func(t *testing.T) ratelimit.Store {
		return goredis.NewStore(client, goredis.WithKeyPrefix(prefix()))
	})
}

func TestGCRAThroughRedis(t *testing.T) {
	client := newClient(t)
	l, err := ratelimit.New("it", ratelimit.GCRA(5, 3), goredis.NewStore(client, goredis.WithKeyPrefix(prefix())))
	if err != nil {
		t.Fatal(err)
	}
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

func TestLimitIsSharedAcrossClients(t *testing.T) {
	client := newClient(t)
	other := redis.NewClient(&redis.Options{Addr: client.Options().Addr})
	defer other.Close()
	p := prefix()
	a, _ := ratelimit.New("shared", ratelimit.GCRA(1, 2), goredis.NewStore(client, goredis.WithKeyPrefix(p)))
	b, _ := ratelimit.New("shared", ratelimit.GCRA(1, 2), goredis.NewStore(other, goredis.WithKeyPrefix(p)))
	ctx := context.Background()
	a.Allow(ctx, "k")
	b.Allow(ctx, "k")
	if d, err := a.Allow(ctx, "k"); err != nil || d.Allowed {
		t.Fatalf("burst of 2 was not shared between clients: %+v %v", d, err)
	}
}

func TestEveryAlgorithmWorksOnRedis(t *testing.T) {
	client := newClient(t)
	for name, algo := range map[string]ratelimit.Algorithm{
		"gcra":           ratelimit.GCRA(1, 2),
		"fixed_window":   ratelimit.FixedWindow(2, time.Hour),
		"sliding_window": ratelimit.SlidingWindow(2, time.Hour),
	} {
		t.Run(name, func(t *testing.T) {
			l, err := ratelimit.New(name, algo, goredis.NewStore(client, goredis.WithKeyPrefix(prefix())))
			if err != nil {
				t.Fatal(err)
			}
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

func TestUnreachableServerIsAnError(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond, MaxRetries: -1})
	defer client.Close()
	l, err := ratelimit.New("down", ratelimit.GCRA(1, 1), goredis.NewStore(client))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Allow(context.Background(), "k"); err == nil {
		t.Fatal("expected an error from an unreachable server")
	}
	if s := l.Stats(); s.Errors != 1 {
		t.Fatalf("stats = %+v", s)
	}
}
