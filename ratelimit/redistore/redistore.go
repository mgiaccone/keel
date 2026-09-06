// Package redistore is a [ratelimit.Store] backed by Redis through go-redis,
// so one limit is shared across every instance of a service.
//
//	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	store, err := redistore.NewStore(client)
//	limiter, err := ratelimit.New("public-api", ratelimit.GCRA(100, 20), store)
//
// A key's record is a hash of its version and three state integers. Get and
// CompareAndSet are each one Lua script run atomically on the node that owns
// the key, sent as EVALSHA with an EVAL fallback. Get reads the server's TIME
// in the same script as the record, so time comes from the one Redis node
// that holds the key: skewed instance clocks agree, and under Redis Cluster a
// key is never timed by a node other than its owner. Records expire on the
// algorithm's TTL. Requires Redis 5 or later, or Valkey.
package redistore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mgiaccone/keel/ratelimit"
)

// ErrInvalidOption is wrapped by every error [NewStore] returns for an
// argument or option value that cannot be meant.
var ErrInvalidOption = errors.New("redistore: invalid option")

// Store implements [ratelimit.Store] on a go-redis client.
type Store struct {
	client redis.UniversalClient
	prefix string
}

var _ ratelimit.Store = (*Store)(nil)

// _get returns the node's TIME (seconds, microseconds) followed by the
// record's four fields, absent ones as nil. Reading the clock in the script
// pins it to the node that owns the key.
var _get = redis.NewScript(`
local t = redis.call('TIME')
local f = redis.call('HMGET', KEYS[1], 'v', 'a', 'b', 'c')
return {t[1], t[2], f[1], f[2], f[3], f[4]}
`)

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

// Option configures a [Store]. An option given a value that cannot be meant
// makes [NewStore] return an error wrapping [ErrInvalidOption]; NewStore
// reports every invalid option, not just the first.
type Option func(*Store) error

// WithKeyPrefix sets the prefix of every Redis key; default "ratelimit:". To
// pin all of a limiter's keys to one Redis Cluster slot, include a hash tag:
// "ratelimit:{public-api}:".
func WithKeyPrefix(prefix string) Option {
	return func(s *Store) error {
		s.prefix = prefix
		return nil
	}
}

// NewStore returns a store on client, which may be a single-node, Cluster or
// Sentinel client. It is deliberately built on a client the caller
// constructs and owns, rather than connection parameters this package would
// turn into one itself: a client is usually shared with the rest of the
// service, carries whichever of go-redis's TLS, auth, pool and topology
// settings the deployment needs, and its teardown belongs to whoever created
// it.
//
// If client is nil, or any option is invalid, NewStore returns an error
// wrapping [ErrInvalidOption] that describes every problem.
func NewStore(client redis.UniversalClient, opts ...Option) (*Store, error) {
	s := &Store{client: client, prefix: "ratelimit:"}
	var errs []error
	if client == nil {
		errs = append(errs, fmt.Errorf("%w: NewStore: client must not be nil", ErrInvalidOption))
	}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return s, nil
}

// Get implements [ratelimit.Store]: one script run on the key's node returns
// its clock and the record together.
func (s *Store) Get(ctx context.Context, key string) (ratelimit.Record, time.Time, error) {
	values, err := _get.Run(ctx, s.client, []string{s.prefix + key}).Slice()
	if err != nil {
		return ratelimit.Record{}, time.Time{}, err
	}
	if len(values) != 6 {
		return ratelimit.Record{}, time.Time{}, fmt.Errorf("redistore: %q: script returned %d values, want 6", key, len(values))
	}

	var secs, micros int64
	if err := parseField(values[0], &secs); err != nil {
		return ratelimit.Record{}, time.Time{}, fmt.Errorf("redistore: %q: TIME seconds: %w", key, err)
	}
	if err := parseField(values[1], &micros); err != nil {
		return ratelimit.Record{}, time.Time{}, fmt.Errorf("redistore: %q: TIME microseconds: %w", key, err)
	}
	now := time.Unix(secs, micros*int64(time.Microsecond))

	fields := values[2:]
	if fields[0] == nil {
		return ratelimit.Record{}, now, nil
	}
	var version, a, b, c int64
	for i, dst := range []*int64{&version, &a, &b, &c} {
		if err := parseField(fields[i], dst); err != nil {
			return ratelimit.Record{}, now, fmt.Errorf("redistore: %q field %d: %w", key, i, err)
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
