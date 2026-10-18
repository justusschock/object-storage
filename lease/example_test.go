package lease_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	coordination "github.com/justusschock/distributed-coordination"
	"github.com/justusschock/object-storage/backend/memory"
	"github.com/justusschock/object-storage/lease"
)

func ExampleNew() {
	backend, err := lease.New(memory.New(), "coordination", "leases", lease.Options{})
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	held, err := backend.Acquire(ctx, coordination.AcquireRequest{Key: "jobs/nightly", Owner: "worker-1", TTL: time.Minute})
	if err != nil {
		panic(err)
	}
	_, err = backend.Acquire(ctx, coordination.AcquireRequest{Key: "jobs/nightly", Owner: "worker-2", TTL: time.Minute})
	fmt.Println(held.Info().Token, errors.Is(err, coordination.ErrNotAcquired))
	if err := held.Release(ctx); err != nil {
		panic(err)
	}
	// Output: 1 true
}
