// Package lease implements go-distributed-coordination fenced leases on top
// of any object store that honors conditional writes.
//
// Each coordination key is one small JSON object holding the owner, fencing
// token, and expiry. Acquisition creates the object with IfAbsent or replaces
// an expired or released lease with IfMatchVersion; renewal and release are
// IfMatchVersion compare-and-swaps against the version written by the holder.
// Release keeps the object in a released state instead of deleting it, so the
// next acquisition continues the per-key token sequence and fencing tokens
// increase strictly across release and expiry.
//
// Expiry is evaluated against the clocks of the processes involved. Clocks
// must agree to well within one TTL; protected resources must still check
// fencing tokens, which stay correct under skew and pauses.
package lease

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
	objectstore "github.com/justusschock/go-object-storage"
)

const (
	recordFormat        = 1
	defaultPollInterval = time.Second
	maximumRecordSize   = 64 << 10
)

// Options configures a Backend.
type Options struct {
	// Clock supplies time for expiry and polling; nil uses the system clock.
	Clock coordination.Clock
	// PollInterval bounds how long a waiting Acquire sleeps before
	// re-reading a held lease; zero means one second. Waiters also wake at
	// the holder's expiry when that comes sooner.
	PollInterval time.Duration
}

// Backend grants leases stored as objects in one bucket under one prefix.
type Backend struct {
	store        objectstore.Store
	bucket       objectstore.Bucket
	prefix       string
	clock        coordination.Clock
	pollInterval time.Duration
}

// New constructs a lease backend. Lease objects are stored at prefix + "/" +
// AcquireRequest.Key, or at the request key itself when prefix is empty.
func New(store objectstore.Store, bucket objectstore.Bucket, prefix string, options Options) (*Backend, error) {
	if store == nil {
		return nil, errors.New("lease: nil object store")
	}
	if err := objectstore.ValidateBucket(bucket); err != nil {
		return nil, err
	}
	if prefix != "" {
		if err := objectstore.ValidateKey(objectstore.Key(prefix + "/lease")); err != nil || strings.HasSuffix(prefix, "/") {
			return nil, fmt.Errorf("lease: invalid prefix %q", prefix)
		}
	}
	if options.PollInterval < 0 {
		return nil, errors.New("lease: negative poll interval")
	}
	if options.PollInterval == 0 {
		options.PollInterval = defaultPollInterval
	}
	if options.Clock == nil {
		options.Clock = systemClock{}
	}
	return &Backend{store: store, bucket: bucket, prefix: prefix, clock: options.Clock, pollInterval: options.PollInterval}, nil
}

// record is the stored lease document.
type record struct {
	Format     int       `json:"format"`
	Key        string    `json:"key"`
	Owner      string    `json:"owner"`
	Token      uint64    `json:"token"`
	AcquiredAt time.Time `json:"acquired_at"`
	ExpiresAt  time.Time `json:"expires_at"`
	Released   bool      `json:"released,omitempty"`
}

func (current record) info() coordination.LeaseInfo {
	return coordination.LeaseInfo{
		Key: current.Key, Owner: current.Owner, Token: coordination.FencingToken(current.Token),
		AcquiredAt: current.AcquiredAt, ExpiresAt: current.ExpiresAt,
	}
}

// Acquire grants the lease when it is absent, released, or expired. With
// Wait it polls until then or until ctx ends.
func (backend *Backend) Acquire(ctx context.Context, request coordination.AcquireRequest) (coordination.Lease, error) {
	if request.Key == "" || request.Owner == "" || request.TTL <= 0 {
		return nil, coordination.ErrInvalidRequest
	}
	key, err := backend.objectKey(request.Key)
	if err != nil {
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		current, version, err := backend.read(ctx, key)
		exists := err == nil
		if err != nil && !errors.Is(err, objectstore.ErrNotFound) {
			return nil, err
		}
		now := backend.clock.Now()
		if exists && !current.Released && current.ExpiresAt.After(now) {
			if !request.Wait {
				return nil, coordination.ErrNotAcquired
			}
			delay := min(backend.pollInterval, current.ExpiresAt.Sub(now))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-backend.clock.After(delay):
			}
			continue
		}
		next := record{
			Format: recordFormat, Key: request.Key, Owner: request.Owner, Token: 1,
			AcquiredAt: now, ExpiresAt: now.Add(request.TTL),
		}
		options := objectstore.PutOptions{IfAbsent: true}
		if exists {
			if current.Token == ^uint64(0) {
				return nil, fmt.Errorf("lease: fencing token space exhausted for %q", request.Key)
			}
			next.Token = current.Token + 1
			options = objectstore.PutOptions{IfMatchVersion: version}
		}
		written, err := backend.write(ctx, key, next, options)
		if errors.Is(err, objectstore.ErrPrecondition) {
			continue // another contender changed the lease; re-read it
		}
		if err != nil {
			return nil, err
		}
		return backend.grant(key, next, written.Version), nil
	}
}

func (backend *Backend) grant(key objectstore.Key, current record, version string) *lease {
	granted := &lease{
		backend: backend,
		key:     key,
		record:  current,
		version: version,
		done:    make(chan struct{}),
		wake:    make(chan struct{}, 1),
	}
	// Arm the expiry timer before returning so that a clock advanced right
	// after Acquire observes it.
	granted.timer = backend.clock.After(current.ExpiresAt.Sub(backend.clock.Now()))
	go granted.watch()
	return granted
}

func (backend *Backend) objectKey(key string) (objectstore.Key, error) {
	objectKey := objectstore.Key(key)
	if backend.prefix != "" {
		objectKey = objectstore.Key(backend.prefix + "/" + key)
	}
	if err := objectstore.ValidateKey(objectKey); err != nil {
		return "", fmt.Errorf("%w: %v", coordination.ErrInvalidRequest, err)
	}
	return objectKey, nil
}

func (backend *Backend) read(ctx context.Context, key objectstore.Key) (record, string, error) {
	body, object, err := backend.store.Get(ctx, backend.bucket, key, objectstore.GetOptions{})
	if err != nil {
		return record{}, "", err
	}
	data, err := io.ReadAll(io.LimitReader(body, maximumRecordSize+1))
	if closeErr := body.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return record{}, "", err
	}
	var current record
	if len(data) > maximumRecordSize || json.Unmarshal(data, &current) != nil || current.Format != recordFormat {
		return record{}, "", fmt.Errorf("lease: malformed lease object %s/%s", backend.bucket, key)
	}
	return current, object.Version, nil
}

func (backend *Backend) write(ctx context.Context, key objectstore.Key, next record, options objectstore.PutOptions) (objectstore.Object, error) {
	data, err := json.Marshal(next)
	if err != nil {
		return objectstore.Object{}, fmt.Errorf("lease: encode lease: %w", err)
	}
	options.ContentType = "application/json"
	options.Size = int64(len(data))
	return backend.store.Put(ctx, backend.bucket, key, bytes.NewReader(data), options)
}

type lease struct {
	backend *Backend
	key     objectstore.Key

	// operation serializes Renew and Release so each compare-and-swap uses
	// the version written by the previous one.
	operation sync.Mutex

	mu      sync.Mutex
	record  record
	version string
	lost    bool
	done    chan struct{}
	timer   <-chan time.Time
	wake    chan struct{}
}

// Info returns the latest granted lease information.
func (granted *lease) Info() coordination.LeaseInfo {
	granted.mu.Lock()
	defer granted.mu.Unlock()
	return granted.record.info()
}

// Done closes once on release, expiry, or detected replacement.
func (granted *lease) Done() <-chan struct{} {
	return granted.done
}

// Renew extends the lease to now+ttl with a compare-and-swap.
func (granted *lease) Renew(ctx context.Context, ttl time.Duration) (coordination.LeaseInfo, error) {
	if ttl <= 0 {
		return coordination.LeaseInfo{}, coordination.ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return coordination.LeaseInfo{}, err
	}
	granted.operation.Lock()
	defer granted.operation.Unlock()
	current, version, err := granted.live()
	if err != nil {
		return coordination.LeaseInfo{}, err
	}
	now := granted.backend.clock.Now()
	current.ExpiresAt = now.Add(ttl)
	written, err := granted.backend.write(ctx, granted.key, current, objectstore.PutOptions{IfMatchVersion: version})
	if errors.Is(err, objectstore.ErrPrecondition) {
		granted.lose()
		return coordination.LeaseInfo{}, coordination.ErrLeaseLost
	}
	if err != nil {
		return coordination.LeaseInfo{}, err
	}
	granted.mu.Lock()
	granted.record, granted.version = current, written.Version
	granted.timer = granted.backend.clock.After(ttl)
	granted.mu.Unlock()
	select {
	case granted.wake <- struct{}{}:
	default:
	}
	return current.info(), nil
}

// Release marks the lease released with a compare-and-swap and closes Done.
// It returns ErrLeaseLost when the lease already expired or was replaced.
func (granted *lease) Release(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	granted.operation.Lock()
	defer granted.operation.Unlock()
	current, version, err := granted.live()
	if err != nil {
		return err
	}
	current.Released = true
	current.ExpiresAt = granted.backend.clock.Now()
	_, err = granted.backend.write(ctx, granted.key, current, objectstore.PutOptions{IfMatchVersion: version})
	if errors.Is(err, objectstore.ErrPrecondition) {
		granted.lose()
		return coordination.ErrLeaseLost
	}
	if err != nil {
		return err
	}
	granted.lose()
	return nil
}

// live returns the current record and version, or ErrLeaseLost after loss or
// local expiry.
func (granted *lease) live() (record, string, error) {
	granted.mu.Lock()
	current, version, lost := granted.record, granted.version, granted.lost
	granted.mu.Unlock()
	if lost {
		return record{}, "", coordination.ErrLeaseLost
	}
	if !current.ExpiresAt.After(granted.backend.clock.Now()) {
		granted.lose()
		return record{}, "", coordination.ErrLeaseLost
	}
	return current, version, nil
}

func (granted *lease) lose() {
	granted.mu.Lock()
	defer granted.mu.Unlock()
	if !granted.lost {
		granted.lost = true
		close(granted.done)
	}
}

// watch closes Done when the latest armed expiry timer fires and the lease
// has not been renewed past it.
func (granted *lease) watch() {
	for {
		granted.mu.Lock()
		timer := granted.timer
		granted.mu.Unlock()
		select {
		case <-granted.done:
			return
		case <-granted.wake:
		case <-timer:
			granted.mu.Lock()
			if granted.timer == timer && !granted.lost {
				remaining := granted.record.ExpiresAt.Sub(granted.backend.clock.Now())
				if remaining <= 0 {
					granted.lost = true
					close(granted.done)
				} else {
					granted.timer = granted.backend.clock.After(remaining)
				}
			}
			granted.mu.Unlock()
		}
	}
}

type systemClock struct{}

func (systemClock) Now() time.Time                             { return time.Now() }
func (systemClock) After(delay time.Duration) <-chan time.Time { return time.After(delay) }

var _ coordination.Backend = (*Backend)(nil)
