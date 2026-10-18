package sftp_test

import (
	"bytes"
	"context"
	"fmt"
	"io"

	objectstore "github.com/justusschock/object-storage"
	sftpadapter "github.com/justusschock/object-storage/backend/sftp"
	"github.com/justusschock/object-storage/backend/sftp/sftptest"
)

func ExampleNew() {
	// An in-process SSH server on 127.0.0.1 stands in for a real SFTP host.
	server, err := sftptest.Start(sftptest.Options{})
	if err != nil {
		panic(err)
	}
	defer func() { _ = server.Close() }()
	client, err := server.Dial(server.ClientConfig())
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Close() }()

	store, err := sftpadapter.New(client, "objects-root", sftpadapter.Options{})
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	first, err := store.Put(ctx, "bucket", "greeting", bytes.NewReader([]byte("hello")), objectstore.PutOptions{IfAbsent: true})
	if err != nil {
		panic(err)
	}
	_, err = store.Put(ctx, "bucket", "greeting", bytes.NewReader([]byte("hello again")), objectstore.PutOptions{IfAbsent: true})
	fmt.Println("second IfAbsent put:", err)
	if _, err := store.Put(ctx, "bucket", "greeting", bytes.NewReader([]byte("hello, world")), objectstore.PutOptions{IfMatchVersion: first.Version}); err != nil {
		panic(err)
	}
	body, object, err := store.Get(ctx, "bucket", "greeting", objectstore.GetOptions{Range: &objectstore.ByteRange{Offset: 7, Length: 5}})
	if err != nil {
		panic(err)
	}
	defer func() { _ = body.Close() }()
	data, err := io.ReadAll(body)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data), object.Size, store.AtomicReplace())
	// Output:
	// second IfAbsent put: object storage: precondition failed
	// world 12 true
}
