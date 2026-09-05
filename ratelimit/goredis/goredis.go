// Package goredis is a [ratelimit.Store] backed by Redis through go-redis, so
// one limit is shared across every instance of a service.
//
//	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	store := goredis.NewStore(client)
//	limiter, err := ratelimit.New("public-api", ratelimit.GCRA(100, 20), store)
//
// A key's record is a hash of its version and three state integers. Get is one
// pipelined round trip (TIME and HMGET), so time comes from the Redis server
// and skewed instance clocks agree; CompareAndSet is one Lua script run
// atomically, sent as EVALSHA with an EVAL fallback. Records expire on the
// algorithm's TTL. Requires Redis 5 or later, or Valkey.
package goredis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mgiaccone/keel/ratelimit"
)

// Store implements [ratelimit.Store] on a go-redis client.
type Store struct {
	client redis.UniversalClient
	prefix string
}

var _ ratelimit.Store = (*Store)(nil)

// _cas writes the record if its version is still ARGV[1] ("0" = absent).
// Redis hands the script every argument as a string; go-redis formats the
// integers exactly, so the version and state round-trip without loss.
var _cas = redis.NewScript(`
local cur = redis.call('HGET', KEYS[1], 'v')
if cur == false then cur = '0' end
if cur ~= ARGV[1] then return 0 end
local v = tonumber(cur) + 1
redis.call('HSET', KEYS[1], 'v', v, 'a', ARGV[2], 'b', ARGV[3], 'c', ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return 1
`)

// Option configures a [Store].
type Option func(*Store)

// WithKeyPrefix sets the prefix of every Redis key; default "ratelimit:". To
// pin all of a limiter's keys to one Redis Cluster slot, include a hash tag:
// "ratelimit:{public-api}:".
func WithKeyPrefix(prefix string) Option {
	return func(s *Store) { s.prefix = prefix }
}

// NewStore returns a store on client, which may be a single-node, Cluster or
// Sentinel client.
func NewStore(client redis.UniversalClient, opts ...Option) *Store {
	s := &Store{client: client, prefix: "ratelimit:"}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Get implements [ratelimit.Store]. TIME and HMGET travel in one pipelined
// round trip.
func (s *Store) Get(ctx context.Context, key string) (ratelimit.Record, time.Time, error) {
	pipe := s.client.Pipeline()
	serverTime := pipe.Time(ctx)
	fields := pipe.HMGet(ctx, s.prefix+key, "v", "a", "b", "c")
	if _, err := pipe.Exec(ctx); err != nil {
		return ratelimit.Record{}, time.Time{}, err
	}
	now := serverTime.Val()

	values := fields.Val()
	if values[0] == nil {
		return ratelimit.Record{}, now, nil
	}
	var version, a, b, c int64
	for i, dst := range []*int64{&version, &a, &b, &c} {
		if err := parseField(values[i], dst); err != nil {
			return ratelimit.Record{}, now, fmt.Errorf("goredis: %q field %d: %w", key, i, err)
		}
	}
	return ratelimit.Record{Version: uint64(version), State: ratelimit.State{A: a, B: b, C: c}}, now, nil
}

// parseField decodes one HMGET value, a decimal string.
func parseField(v any, dst *int64) error {
	str, ok := v.(string)
	if !ok {
		return fmt.Errorf("got %T, want string", v)
	}
	n, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return err
	}
	*dst = n
	return nil
}

// CompareAndSet implements [ratelimit.Store] with one atomic script run.
func (s *Store) CompareAndSet(ctx context.Context, key string, expect uint64, state ratelimit.State, ttl time.Duration) (bool, error) {
	keys := []string{s.prefix + key}
	args := []any{expect, state.A, state.B, state.C, max(ttl.Milliseconds(), 1)}
	written, err := _cas.Run(ctx, s.client, keys, args...).Int()
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, err
	}
	return written == 1, nil
}
