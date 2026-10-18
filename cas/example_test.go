package cas_test

import (
	"context"
	"fmt"
	"io"
	"strings"

	coordinationmemory "github.com/justusschock/distributed-coordination/memory"
	"github.com/justusschock/object-storage/backend/memory"
	"github.com/justusschock/object-storage/cas"
)

func ExampleNew() {
	// A nil metadata cache disables caching; the coordination backend fences
	// concurrent writers of the same content.
	store, err := cas.New(memory.New(), "blobs", "sha256", nil, coordinationmemory.NewBackend(nil), "writer-1")
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	digest, _, err := store.Put(ctx, strings.NewReader("hello"))
	if err != nil {
		panic(err)
	}
	body, _, err := store.Open(ctx, digest, nil)
	if err != nil {
		panic(err)
	}
	defer func() { _ = body.Close() }()
	data, err := io.ReadAll(body) // verified against digest while streaming
	if err != nil {
		panic(err)
	}
	fmt.Println(digest, string(data))
	// Output: sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824 hello
}
