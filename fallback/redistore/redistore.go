// Package redistore is a [fallback.Store] and [fallback.Leaser] backed by
// Redis through go-redis, so a fast source — and fleet-wide refresh
// coalescing — is shared across every instance of a service.
//
//	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	store, err := redistore.NewStore(client, redistore.JSON[Product]())
//	read, err := fallback.New("catalog", store, origin, fallback.WithLease(store, 5*time.Second))
//
// Unlike [ratelimit.Store], there is no compare-and-set and no server-side
// clock read here: [fallback.Store] carries no version and no time of its
// own — a [fallback.Policy] judges the value itself, whatever that value
// encodes — so Get, Set and Delete are each one plain Redis command, not a
// script. [WithTTL] bounds how long an unrefreshed value survives; that is
// memory hygiene, not answer validity, which the Policy alone decides.
// Requires Redis 5 or later, or Valkey.
package redistore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mgiaccone/keel/fallback"
)

// ErrInvalidOption is wrapped by every error [NewStore] returns for an
// argument or option value that cannot be meant.
var ErrInvalidOption = errors.New("redistore: invalid option")

// Codec encodes a value for storage as Redis bytes and decodes it back.
type Codec[V any] interface {
	Encode(v V) ([]byte, error)
	Decode(b []byte) (V, error)
}

// JSON is a [Codec] using encoding/json. It fits any V that round-trips
// through json.Marshal/Unmarshal; a V that does not — one needing a custom
// binary framing, say — needs its own Codec instead.
func JSON[V any]() Codec[V] { return jsonCodec[V]{} }

type jsonCodec[V any] struct{}

func (jsonCodec[V]) Encode(v V) ([]byte, error) { return json.Marshal(v) }

func (jsonCodec[V]) Decode(b []byte) (V, error) {
	var v V
	err := json.Unmarshal(b, &v)
	return v, err
}

// _release deletes a lease only if token still holds it — never a lease
// that expired and was re-acquired by another instance in the meantime.
var _release = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
	return redis.call('DEL', KEYS[1])
end
return 0
`)

// Store implements [fallback.Store] and [fallback.Leaser] on a go-redis
// client, keyed by string.
type Store[V any] struct {
	client      redis.UniversalClient
	codec       Codec[V]
	prefix      string
	leasePrefix string
	ttl         time.Duration
}

var (
	_ fallback.Store[string, int] = (*Store[int])(nil)
	_ fallback.Leaser[string]     = (*Store[int])(nil)
)

// Option configures a [Store]. An option given a value that cannot be meant
// makes [NewStore] return an error wrapping [ErrInvalidOption]; NewStore
// reports every invalid option, not just the first.
type Option[V any] func(*Store[V]) error

// WithKeyPrefix sets the prefix of every value key; default "fallback:". To
// pin all of a reader's keys to one Redis Cluster slot, include a hash tag:
// "fallback:{catalog}:".
func WithKeyPrefix[V any](prefix string) Option[V] {
	return func(s *Store[V]) error {
		s.prefix = prefix
		return nil
	}
}

// WithLeaseKeyPrefix sets the prefix of every lease key acquired through
// [fallback.WithLease]; default "fallback-lease:". Keep it distinct from
// WithKeyPrefix, or a lease key and a value key can collide.
func WithLeaseKeyPrefix[V any](prefix string) Option[V] {
	return func(s *Store[V]) error {
		s.leasePrefix = prefix
		return nil
	}
}

// WithTTL bounds how long a value survives with no write to refresh it;
// default 24h. Size it for how long an abandoned key — one whose reader
// stopped calling Get, or whose policy stopped requesting refreshes — may
// sit before Redis reclaims it, not for how long an answer stays acceptable:
// that is entirely the [fallback.Policy]'s decision, made independently of
// this TTL every time the value is actually read.
func WithTTL[V any](d time.Duration) Option[V] {
	return func(s *Store[V]) error {
		if d <= 0 {
			return fmt.Errorf("%w: WithTTL(%s): must be positive", ErrInvalidOption, d)
		}
		s.ttl = d
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
// If client or codec is nil, or any option is invalid, NewStore returns an
// error wrapping [ErrInvalidOption] that describes every problem.
func NewStore[V any](client redis.UniversalClient, codec Codec[V], opts ...Option[V]) (*Store[V], error) {
	s := &Store[V]{client: client, codec: codec, prefix: "fallback:", leasePrefix: "fallback-lease:", ttl: 24 * time.Hour}
	var errs []error
	if client == nil {
		errs = append(errs, fmt.Errorf("%w: NewStore: client must not be nil", ErrInvalidOption))
	}
	if codec == nil {
		errs = append(errs, fmt.Errorf("%w: NewStore: codec must not be nil", ErrInvalidOption))
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

// Get implements [fallback.Store].
func (s *Store[V]) Get(ctx context.Context, key string) (V, bool, error) {
	var zero V
	b, err := s.client.Get(ctx, s.prefix+key).Bytes()
	if errors.Is(err, redis.Nil) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, err
	}

	v, err := s.codec.Decode(b)
	if err != nil {
		return zero, false, fmt.Errorf("redistore: %q: decode: %w", key, err)
	}
	return v, true, nil
}

// Set implements [fallback.Store]. It refreshes [WithTTL] unconditionally on
// every call, the same as any other write to the key.
func (s *Store[V]) Set(ctx context.Context, key string, v V) error {
	b, err := s.codec.Encode(v)
	if err != nil {
		return fmt.Errorf("redistore: %q: encode: %w", key, err)
	}
	return s.client.Set(ctx, s.prefix+key, b, s.ttl).Err()
}

// Delete implements [fallback.Store]. Deleting an absent key is a no-op.
func (s *Store[V]) Delete(ctx context.Context, key string) error {
	return s.client.Del(ctx, s.prefix+key).Err()
}

// Acquire implements [fallback.Leaser] with SET NX PX: the first instance to
// reach Redis for key holds it for ttl; a leaseholder that dies before
// calling Release leaves the lease to expire on its own, which is what
// bounds [fallback.WithLease]'s guarantee to "per lease interval" rather
// than absolutely once.
func (s *Store[V]) Acquire(ctx context.Context, key string, ttl time.Duration) (string, bool, error) {
	token, err := randomToken()
	if err != nil {
		return "", false, err
	}
	ok, err := s.client.SetNX(ctx, s.leasePrefix+key, token, ttl).Result()
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}
	return token, true, nil
}

// Release implements [fallback.Leaser]: it deletes the lease only if token
// still holds it, so a caller running late — past its own lease's ttl —
// cannot release a lease a different instance has since acquired.
func (s *Store[V]) Release(ctx context.Context, key, token string) error {
	return _release.Run(ctx, s.client, []string{s.leasePrefix + key}, token).Err()
}

// randomToken returns a value opaque enough that no two concurrent Acquire
// calls, anywhere in the fleet, produce the same one.
func randomToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
