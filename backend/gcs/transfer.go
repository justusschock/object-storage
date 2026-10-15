package gcs

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

func seekStart(body io.Reader) (int64, bool) {
	seeker, ok := body.(io.Seeker)
	if !ok {
		return 0, false
	}
	offset, err := seeker.Seek(0, io.SeekCurrent)
	return offset, err == nil
}

func hashRemainder(ctx context.Context, head []byte, body io.Reader) ([32]byte, error) {
	hash := sha256.New()
	_, _ = hash.Write(head)
	if _, err := io.Copy(hash, contextReader{ctx: ctx, reader: body}); err != nil {
		return [32]byte{}, err
	}
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}

type spooledFile struct{ file *os.File }

func (spooled spooledFile) close() {
	name := spooled.file.Name()
	_ = spooled.file.Close()
	_ = os.Remove(name)
}

// spool copies head and the rest of body to a temporary file while hashing,
// and returns the file rewound to its start.
func spool(ctx context.Context, directory string, head []byte, body io.Reader) (spooledFile, [32]byte, error) {
	file, err := os.CreateTemp(directory, "go-object-storage-gcs-*")
	if err != nil {
		return spooledFile{}, [32]byte{}, fmt.Errorf("gcs store: create spool file: %w", err)
	}
	spooled := spooledFile{file: file}
	hash := sha256.New()
	writer := io.MultiWriter(file, hash)
	if _, err := writer.Write(head); err != nil {
		spooled.close()
		return spooledFile{}, [32]byte{}, err
	}
	if _, err := io.Copy(writer, contextReader{ctx: ctx, reader: body}); err != nil {
		spooled.close()
		return spooledFile{}, [32]byte{}, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		spooled.close()
		return spooledFile{}, [32]byte{}, err
	}
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return spooled, digest, nil
}

// exactReader returns exactly remaining bytes and reports a short body as
// io.ErrUnexpectedEOF.
type exactReader struct {
	body      io.ReadCloser
	remaining int64
}

func (reader *exactReader) Read(buffer []byte) (int, error) {
	if reader.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(buffer)) > reader.remaining {
		buffer = buffer[:reader.remaining]
	}
	count, err := reader.body.Read(buffer)
	reader.remaining -= int64(count)
	if errors.Is(err, io.EOF) {
		if reader.remaining > 0 {
			return count, io.ErrUnexpectedEOF
		}
		return count, io.EOF
	}
	return count, err
}

func (reader *exactReader) Close() error { return reader.body.Close() }

// contextReader stops reading once its context ends.
type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader contextReader) Read(buffer []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(buffer)
}

// recordingReader remembers the first non-EOF read error, which tells body
// failures apart from upload failures.
type recordingReader struct {
	reader io.Reader
	err    error
}

func (reader *recordingReader) Read(buffer []byte) (int, error) {
	count, err := reader.reader.Read(buffer)
	if err != nil && !errors.Is(err, io.EOF) && reader.err == nil {
		reader.err = err
	}
	return count, err
}

// runParallel runs work for indexes [0, count) with at most limit concurrent
// calls and returns the first error, canceling the remaining work.
func runParallel(ctx context.Context, count, limit int, work func(context.Context, int) error) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	slots := make(chan struct{}, max(limit, 1))
	var wait sync.WaitGroup
	for index := range count {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		wait.Add(1)
		go func() {
			defer wait.Done()
			defer func() { <-slots }()
			if err := work(ctx, index); err != nil {
				cancel(err)
			}
		}()
	}
	wait.Wait()
	return context.Cause(ctx)
}
