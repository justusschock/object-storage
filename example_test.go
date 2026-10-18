package objectstore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	objectstore "github.com/justusschock/object-storage"
	"github.com/justusschock/object-storage/backend/memory"
)

// Example writes an immutable object with a create-only condition and a
// declared digest, then reads an exact byte range. It is the README's quick
// start; keep the two in sync.
func Example() {
	ctx := context.Background()
	store := memory.New()
	data := []byte("hello object storage")
	digest := sha256.Sum256(data)

	object, err := store.Put(ctx, "assets", "greeting.txt", bytes.NewReader(data), objectstore.PutOptions{
		IfAbsent: true,
		SHA256:   &digest,
		Metadata: map[string]string{"content": "example"},
	})
	if err != nil {
		panic(err)
	}

	body, metadata, err := store.Get(ctx, object.Bucket, object.Key, objectstore.GetOptions{
		Range: &objectstore.ByteRange{Offset: 6, Length: 6},
	})
	if err != nil {
		panic(err)
	}
	defer func() { _ = body.Close() }()

	rangeBytes, err := io.ReadAll(body)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s (%d-byte object)\n", rangeBytes, metadata.Size)
	// Output: object (20-byte object)
}

func ExampleStore_exactRange() {
	store := memory.New()
	_, err := store.Put(context.Background(), "assets", "digits.txt", bytes.NewReader([]byte("0123456789")), objectstore.PutOptions{})
	if err != nil {
		panic(err)
	}
	body, object, err := store.Get(context.Background(), "assets", "digits.txt", objectstore.GetOptions{Range: &objectstore.ByteRange{Offset: 3, Length: 4}})
	if err != nil {
		panic(err)
	}
	defer func() { _ = body.Close() }()
	data, err := io.ReadAll(body)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s %d\n", data, object.Size)
	// Output: 3456 10
}

// ExampleStore_compareAndSwap updates an object only if nobody replaced it
// since it was read: IfMatchVersion is the portable compare-and-swap.
func ExampleStore_compareAndSwap() {
	ctx := context.Background()
	store := memory.New()
	first, err := store.Put(ctx, "state", "counter", bytes.NewReader([]byte("1")), objectstore.PutOptions{IfAbsent: true})
	if err != nil {
		panic(err)
	}
	if _, err := store.Put(ctx, "state", "counter", bytes.NewReader([]byte("2")), objectstore.PutOptions{IfMatchVersion: first.Version}); err != nil {
		panic(err)
	}
	// A second writer still holding the first version loses.
	_, err = store.Put(ctx, "state", "counter", bytes.NewReader([]byte("2")), objectstore.PutOptions{IfMatchVersion: first.Version})
	fmt.Println(errors.Is(err, objectstore.ErrPrecondition))
	// Output: true
}
