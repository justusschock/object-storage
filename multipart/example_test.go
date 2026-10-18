package multipart_test

import (
	"bytes"
	"context"
	"fmt"

	objectstore "github.com/justusschock/object-storage"
	"github.com/justusschock/object-storage/backend/memory"
	"github.com/justusschock/object-storage/multipart"
)

func ExampleNewUploader() {
	store := memory.New()
	uploader, err := multipart.NewUploader(store, multipart.Options{PartSize: 5 << 20, MaxParallel: 4})
	if err != nil {
		panic(err)
	}
	data := bytes.Repeat([]byte("x"), 12<<20) // three parts
	object, err := uploader.UploadReaderAt(context.Background(), "uploads", "large.bin", bytes.NewReader(data), int64(len(data)), objectstore.PutOptions{})
	if err != nil {
		panic(err)
	}
	fmt.Println(object.Size, object.SHA256Known)
	// Output: 12582912 true
}
