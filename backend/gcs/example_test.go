package gcs_test

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	gcsadapter "github.com/justusschock/go-object-storage/backend/gcs"
	"google.golang.org/api/option"
)

func ExampleNew() {
	// Any *storage.Client works; this one targets a local emulator endpoint
	// and needs no credentials. The store does not close the client.
	client, err := storage.NewClient(context.Background(),
		option.WithEndpoint("http://127.0.0.1:4443/storage/v1/"),
		option.WithoutAuthentication(),
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = client.Close() }()
	store, err := gcsadapter.New(client, gcsadapter.Options{ChunkSize: 8 << 20, Concurrency: 32})
	fmt.Println(store != nil, err == nil)
	// Output: true true
}
