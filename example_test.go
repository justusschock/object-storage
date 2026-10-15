package objectstore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"

	objectstore "github.com/justusschock/go-object-storage"
	"github.com/justusschock/go-object-storage/backend/memory"
)

func Example() {
	store := memory.New()
	data := []byte("hello")
	digest := sha256.Sum256(data)
	object, err := store.Put(context.Background(), "assets", "greeting.txt", bytes.NewReader(data), objectstore.PutOptions{IfAbsent: true, SHA256: &digest})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s %d %x\n", object.Key, object.Size, object.SHA256[:4])
	// Output: greeting.txt 5 2cf24dba
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
