package handoff_test

import (
	"context"
	"fmt"

	lifecyclehandoff "github.com/justusschock/go-lifecycle/handoff"
	"github.com/justusschock/go-object-storage/backend/memory"
	"github.com/justusschock/go-object-storage/handoff"
)

func ExampleNewTransport() {
	transport, err := handoff.NewTransport(memory.New(), "handoffs", "artifacts")
	if err != nil {
		panic(err)
	}
	artifact, err := lifecyclehandoff.NewArtifact("application/json", 1, []byte(`{"step":3}`), nil)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	receipt, err := transport.Send(ctx, artifact)
	if err != nil {
		panic(err)
	}
	received, err := transport.Receive(ctx, receipt)
	if err != nil {
		panic(err)
	}
	fmt.Println(received.MediaType, string(received.Data), received.ID == artifact.ID)
	// Output: application/json {"step":3} true
}
