package lease_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
	"github.com/justusschock/go-distributed-coordination/conformance"
	objectstore "github.com/justusschock/go-object-storage"
	"github.com/justusschock/go-object-storage/backend/fs"
	"github.com/justusschock/go-object-storage/backend/memory"
	"github.com/justusschock/go-object-storage/lease"
)

// FR-OBJ-23
func TestConformanceMemoryStore(t *testing.T) {
	conformance.Run(t, func(t *testing.T) (coordination.Backend, func(time.Duration)) {
		clock := newFakeClock(time.Unix(100, 0))
		return newBackend(t, memory.New(), lease.Options{Clock: clock}), clock.Advance
	})
}

func TestConformanceFilesystemStore(t *testing.T) {
	conformance.Run(t, func(t *testing.T) (coordination.Backend, func(time.Duration)) {
		store, err := fs.New(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		clock := newFakeClock(time.Unix(100, 0))
		return newBackend(t, store, lease.Options{Clock: clock}), clock.Advance
	})
}

// FR-OBJ-23
func TestExpiryClosesDoneAtTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := newBackend(t, memory.New(), lease.Options{})
		start := time.Now()
		granted := acquire(t, backend, "jobs/a", "one", 10*time.Second, false)
		<-granted.Done()
		if elapsed := time.Since(start); elapsed != 10*time.Second {
			t.Fatalf("lease expired after %v, want 10s", elapsed)
		}
		if _, err := granted.Renew(context.Background(), time.Second); !errors.Is(err, coordination.ErrLeaseLost) {
			t.Fatalf("Renew after expiry error = %v, want ErrLeaseLost", err)
		}
		if err := granted.Release(context.Background()); !errors.Is(err, coordination.ErrLeaseLost) {
			t.Fatalf("Release after expiry error = %v, want ErrLeaseLost", err)
		}
	})
}

func TestRenewExtendsFromCurrentTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := newBackend(t, memory.New(), lease.Options{})
		start := time.Now()
		granted := acquire(t, backend, "jobs/renew", "one", 10*time.Second, false)
		time.Sleep(8 * time.Second)
		info, err := granted.Renew(context.Background(), 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if want := start.Add(18 * time.Second); !info.ExpiresAt.Equal(want) {
			t.Fatalf("renewed expiry = %v, want %v", info.ExpiresAt, want)
		}
		time.Sleep(9 * time.Second)
		select {
		case <-granted.Done():
			t.Fatal("renewed lease expired at its original deadline")
		default:
		}
		if _, err := backend.Acquire(context.Background(), coordination.AcquireRequest{Key: "jobs/renew", Owner: "two", TTL: time.Second}); !errors.Is(err, coordination.ErrNotAcquired) {
			t.Fatalf("contender error = %v, want ErrNotAcquired", err)
		}
		<-granted.Done()
		if elapsed := time.Since(start); elapsed != 18*time.Second {
			t.Fatalf("renewed lease expired after %v, want 18s", elapsed)
		}
	})
}

// FR-OBJ-23
func TestWaitPollsUntilRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := newBackend(t, memory.New(), lease.Options{PollInterval: 2 * time.Second})
		start := time.Now()
		holder := acquire(t, backend, "jobs/wait", "holder", time.Minute, false)
		go func() {
			time.Sleep(5 * time.Second)
			if err := holder.Release(context.Background()); err != nil {
				t.Errorf("Release: %v", err)
			}
		}()
		waiter := acquire(t, backend, "jobs/wait", "waiter", time.Minute, true)
		defer func() { _ = waiter.Release(context.Background()) }()
		// Polls at 2s and 4s see the holder; the poll at 6s sees the release.
		if elapsed := time.Since(start); elapsed != 6*time.Second {
			t.Fatalf("waiter acquired after %v, want 6s", elapsed)
		}
		if waiter.Info().Token != holder.Info().Token+1 || waiter.Info().Owner != "waiter" {
			t.Fatalf("waiter info = %+v, holder %+v", waiter.Info(), holder.Info())
		}
	})
}

func TestWaitWakesAtHolderExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := newBackend(t, memory.New(), lease.Options{PollInterval: time.Hour})
		start := time.Now()
		holder := acquire(t, backend, "jobs/expiry", "holder", 3*time.Second, false)
		waiter := acquire(t, backend, "jobs/expiry", "waiter", time.Minute, true)
		defer func() { _ = waiter.Release(context.Background()) }()
		if elapsed := time.Since(start); elapsed != 3*time.Second {
			t.Fatalf("waiter acquired after %v, want holder TTL 3s", elapsed)
		}
		<-holder.Done()
		if _, err := holder.Renew(context.Background(), time.Minute); !errors.Is(err, coordination.ErrLeaseLost) {
			t.Fatalf("stale holder Renew error = %v, want ErrLeaseLost", err)
		}
	})
}

// FR-OBJ-10, FR-OBJ-23
func TestWaitHonorsCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := newBackend(t, memory.New(), lease.Options{})
		holder := acquire(t, backend, "jobs/cancel", "holder", time.Hour, false)
		defer func() { _ = holder.Release(context.Background()) }()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		start := time.Now()
		_, err := backend.Acquire(ctx, coordination.AcquireRequest{Key: "jobs/cancel", Owner: "waiter", TTL: time.Minute, Wait: true})
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != 30*time.Second {
			t.Fatalf("Acquire = %v after %v, want DeadlineExceeded after 30s", err, time.Since(start))
		}
	})
}

// FR-OBJ-23
func TestConcurrentAcquireGrantsOneHolder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := newBackend(t, memory.New(), lease.Options{})
		const contenders = 16
		var wait sync.WaitGroup
		results := make(chan coordination.Lease, contenders)
		for range contenders {
			wait.Add(1)
			go func() {
				defer wait.Done()
				granted, err := backend.Acquire(context.Background(), coordination.AcquireRequest{Key: "jobs/race", Owner: "contender", TTL: time.Minute})
				if err == nil {
					results <- granted
				} else if !errors.Is(err, coordination.ErrNotAcquired) {
					t.Errorf("Acquire error = %v", err)
				}
			}()
		}
		wait.Wait()
		close(results)
		var holders []coordination.Lease
		for granted := range results {
			holders = append(holders, granted)
		}
		if len(holders) != 1 || holders[0].Info().Token != 1 {
			t.Fatalf("holders = %d, want exactly one with token 1", len(holders))
		}
		if err := holders[0].Release(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestKeepAliveHoldsLeasePastTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := newBackend(t, memory.New(), lease.Options{})
		granted := acquire(t, backend, "jobs/keepalive", "one", 3*time.Second, false)
		ctx, cancel := context.WithCancel(context.Background())
		future, err := coordination.KeepAlive(ctx, granted, 3*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Minute)
		select {
		case <-granted.Done():
			t.Fatal("kept-alive lease expired")
		default:
		}
		cancel()
		if _, err := future.Await(context.Background()); !errors.Is(err, context.Canceled) {
			t.Fatalf("KeepAlive result = %v, want context.Canceled", err)
		}
		if err := granted.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRejectsInvalidRequestsAndMalformedObjects(t *testing.T) {
	store := memory.New()
	backend := newBackend(t, store, lease.Options{})
	for _, request := range []coordination.AcquireRequest{
		{Key: "", Owner: "one", TTL: time.Second},
		{Key: "a", Owner: "", TTL: time.Second},
		{Key: "a", Owner: "one"},
		{Key: "../escape", Owner: "one", TTL: time.Second},
	} {
		if _, err := backend.Acquire(context.Background(), request); !errors.Is(err, coordination.ErrInvalidRequest) {
			t.Fatalf("Acquire(%+v) error = %v, want ErrInvalidRequest", request, err)
		}
	}
	if _, err := store.Put(context.Background(), "coordination", "leases/corrupt", bytes.NewReader([]byte("{")), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Acquire(context.Background(), coordination.AcquireRequest{Key: "corrupt", Owner: "one", TTL: time.Second}); err == nil {
		t.Fatal("Acquire accepted a malformed lease object")
	}
	if _, err := lease.New(store, "coordination", "bad/", lease.Options{}); err == nil {
		t.Fatal("New accepted a trailing-slash prefix")
	}
	if _, err := lease.New(nil, "coordination", "", lease.Options{}); err == nil {
		t.Fatal("New accepted a nil store")
	}
}

func newBackend(t *testing.T, store objectstore.Store, options lease.Options) *lease.Backend {
	t.Helper()
	backend, err := lease.New(store, "coordination", "leases", options)
	if err != nil {
		t.Fatal(err)
	}
	return backend
}

func acquire(t *testing.T, backend coordination.Backend, key, owner string, ttl time.Duration, wait bool) coordination.Lease {
	t.Helper()
	granted, err := backend.Acquire(context.Background(), coordination.AcquireRequest{Key: key, Owner: owner, TTL: ttl, Wait: wait})
	if err != nil {
		t.Fatal(err)
	}
	return granted
}

// fakeClock is a manually advanced coordination.Clock for the conformance
// suite, which drives time through its advance callback.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []fakeTimer
}

type fakeTimer struct {
	at      time.Time
	channel chan time.Time
}

func newFakeClock(now time.Time) *fakeClock { return &fakeClock{now: now} }

func (clock *fakeClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *fakeClock) After(delay time.Duration) <-chan time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	timer := fakeTimer{at: clock.now.Add(delay), channel: make(chan time.Time, 1)}
	if delay <= 0 {
		timer.channel <- clock.now
		return timer.channel
	}
	clock.timers = append(clock.timers, timer)
	return timer.channel
}

func (clock *fakeClock) Advance(delay time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(delay)
	remaining := clock.timers[:0]
	for _, timer := range clock.timers {
		if timer.at.After(clock.now) {
			remaining = append(remaining, timer)
			continue
		}
		timer.channel <- clock.now
	}
	clock.timers = remaining
}
