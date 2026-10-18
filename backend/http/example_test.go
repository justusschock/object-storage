package http_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	nethttp "net/http"
	"net/http/httptest"
	"time"

	objectstore "github.com/justusschock/object-storage"
	httpstore "github.com/justusschock/object-storage/backend/http"
)

func ExampleNew() {
	// A static server publishing <root>/<bucket>/<key>; any web server or CDN
	// with Range support works the same way.
	server := httptest.NewServer(nethttp.HandlerFunc(func(writer nethttp.ResponseWriter, request *nethttp.Request) {
		if request.URL.Path != "/remote/packs/pack-1" {
			nethttp.NotFound(writer, request)
			return
		}
		writer.Header().Set("ETag", `"v1"`)
		nethttp.ServeContent(writer, request, "", time.Time{}, bytes.NewReader([]byte("hello object storage")))
	}))
	defer server.Close()

	store, err := httpstore.New(server.URL+"/remote", httpstore.Options{BearerToken: "optional-token"})
	if err != nil {
		panic(err)
	}
	body, object, err := store.Get(context.Background(), "packs", "pack-1", objectstore.GetOptions{
		Range: &objectstore.ByteRange{Offset: 6, Length: 6},
	})
	if err != nil {
		panic(err)
	}
	defer func() { _ = body.Close() }()
	data, err := io.ReadAll(body)
	if err != nil {
		panic(err)
	}
	_, err = store.Put(context.Background(), "packs", "pack-2", bytes.NewReader(nil), objectstore.PutOptions{})
	fmt.Printf("%s %d %s %v\n", data, object.Size, object.Version, err)
	// Output: object 20 "v1" http store: operation not supported: read-only store
}
