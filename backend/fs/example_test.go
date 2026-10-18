package fs_test

import (
	"bytes"
	"context"
	"fmt"
	"os"

	objectstore "github.com/justusschock/go-object-storage"
	"github.com/justusschock/go-object-storage/backend/fs"
)

func ExampleNew() {
	directory, err := os.MkdirTemp("", "objects-*")
	if err != nil {
		panic(err)
	}
	defer func() { _ = os.RemoveAll(directory) }()

	// Every process that opens the same directory shares the store.
	store, err := fs.New(directory)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	if _, err := store.Put(ctx, "assets", "a/b.txt", bytes.NewReader([]byte("value")), objectstore.PutOptions{IfAbsent: true}); err != nil {
		panic(err)
	}
	_, err = store.Put(ctx, "assets", "a/b.txt", bytes.NewReader([]byte("other")), objectstore.PutOptions{IfAbsent: true})
	fmt.Println(err != nil)
	// Output: true
}
