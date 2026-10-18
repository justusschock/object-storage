package pack_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/justusschock/go-object-storage/backend/memory"
	"github.com/justusschock/go-object-storage/cas"
	"github.com/justusschock/go-object-storage/pack"
)

func Example() {
	ctx := context.Background()
	store := memory.New()
	writer, err := pack.NewWriter(os.TempDir())
	if err != nil {
		panic(err)
	}
	blobs := []string{"first blob", "second blob"}
	for _, blob := range blobs {
		if err := writer.Add(cas.Digest(sha256.Sum256([]byte(blob))), strings.NewReader(blob), int64(len(blob))); err != nil {
			panic(err)
		}
	}
	metadata, err := writer.Commit(ctx, store, "packs", "v1")
	if err != nil {
		panic(err)
	}

	// Metadata is all a reader needs; each blob is one exact ranged read.
	reader, err := pack.OpenReader(ctx, store, metadata)
	if err != nil {
		panic(err)
	}
	body, entry, err := reader.Open(ctx, cas.Digest(sha256.Sum256([]byte("second blob"))))
	if err != nil {
		panic(err)
	}
	defer func() { _ = body.Close() }()
	data, err := io.ReadAll(body)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data), entry.Length, metadata.DataSize)
	// Output: second blob 11 21
}
