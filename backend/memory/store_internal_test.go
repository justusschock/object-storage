package memory

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	objectstore "github.com/justusschock/go-object-storage"
)

func TestPutCanceledWhileWaitingForLockDoesNotMutate(t *testing.T) {
	store := New()
	store.mu.Lock()
	locked := true
	defer func() {
		if locked {
			store.mu.Unlock()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	reader := &eofSignalingReader{reader: bytes.NewReader([]byte("value")), done: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		_, err := store.Put(ctx, "objects", "canceled", reader, objectstore.PutOptions{})
		result <- err
	}()

	<-reader.done
	cancel()
	store.mu.Unlock()
	locked = false

	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Put error = %v, want context.Canceled", err)
	}
	if _, err := store.Stat(context.Background(), "objects", "canceled"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("canceled Put mutated store: %v", err)
	}
}

type eofSignalingReader struct {
	reader *bytes.Reader
	done   chan struct{}
	once   sync.Once
}

func (reader *eofSignalingReader) Read(buffer []byte) (int, error) {
	count, err := reader.reader.Read(buffer)
	if errors.Is(err, io.EOF) {
		reader.once.Do(func() { close(reader.done) })
	}
	return count, err
}
