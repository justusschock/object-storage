package azure_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	azureadapter "github.com/justusschock/object-storage/backend/azure"
)

func ExampleNew() {
	// Production code would use azblob.NewClient with an Entra ID credential
	// or azblob.NewClientWithSharedKeyCredential; constructing a store sends
	// no requests.
	client, err := azblob.NewClientWithNoCredential("https://account.blob.core.windows.net/", nil)
	if err != nil {
		panic(err)
	}
	store, err := azureadapter.New(client, azureadapter.Options{BlockSize: 32 << 20, Concurrency: 8})
	fmt.Println(store != nil, err == nil)

	// Presigning needs a Shared Key credential, from the client or Options.
	_, err = store.PresignGet(context.Background(), "container", "key", time.Minute, nil)
	fmt.Println(errors.Is(err, azureadapter.ErrPresignUnavailable))
	// Output:
	// true true
	// true
}
