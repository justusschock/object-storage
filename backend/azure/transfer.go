package azure

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	objectstore "github.com/justusschock/go-object-storage"
)

// blockID returns the base64 block ID of block number within the staging
// session or multipart upload identified by session (32 hexadecimal
// characters). Every ID the adapter stages has the same 40-byte length,
// because Azure requires equal-length IDs among a blob's uncommitted blocks.
func blockID(session string, number int) string {
	return base64.StdEncoding.EncodeToString(fmt.Appendf(nil, "%s%08d", session, number))
}

// newSessionID returns 32 random hexadecimal characters.
func newSessionID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("azure store: create identifier: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

// putBlocks stages source in blockSize chunks and commits them with one Put
// Block List carrying the conditions and the digest metadata. The running
// digest and length are checked before the commit, so a mismatching body
// never publishes the key.
func (store *Store) putBlocks(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, source io.Reader, blockSize int64, metadata map[string]string, options objectstore.PutOptions) (objectstore.Object, error) {
	session, err := newSessionID()
	if err != nil {
		return objectstore.Object{}, err
	}
	client := store.blockBlob(bucket, key)
	ids, total, digest, err := store.stageBlocks(ctx, client, session, contextReader{ctx: ctx, reader: source}, blockSize)
	if err != nil {
		return objectstore.Object{}, err
	}
	if options.SHA256 != nil && *options.SHA256 != digest {
		return objectstore.Object{}, objectstore.ErrPrecondition
	}
	if options.Size > 0 && options.Size != total {
		return objectstore.Object{}, fmt.Errorf("%w: size mismatch", objectstore.ErrPrecondition)
	}
	response, err := client.CommitBlockList(ctx, ids, &blockblob.CommitBlockListOptions{
		HTTPHeaders:      contentHeaders(options.ContentType),
		Metadata:         sdkMetadata(metadata, map[string]string{sha256MetadataKey: hex.EncodeToString(digest[:])}),
		AccessConditions: writeConditions(options.IfAbsent, options.IfMatchVersion),
	})
	if err != nil {
		if bloberror.HasCode(err, bloberror.InvalidBlockList) {
			return objectstore.Object{}, fmt.Errorf("%w: staged blocks were discarded by a concurrent write: %v", objectstore.ErrPrecondition, err) //nolint:errorlint // one classification per error: the cause is detail, not a second sentinel (ADR-0005)
		}
		return objectstore.Object{}, conditionalWriteError(err, options.IfMatchVersion)
	}
	return written(bucket, key, response.ETag, response.LastModified, total, metadata, &digest)
}

// stageBlocks reads source sequentially in blockSize chunks, hashing each
// chunk into the whole-object digest, and stages up to Concurrency chunks in
// parallel, each with a transactional Content-MD5.
func (store *Store) stageBlocks(ctx context.Context, client *blockblob.Client, session string, source io.Reader, blockSize int64) ([]string, int64, [32]byte, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	whole := sha256.New()
	var (
		ids   []string
		wait  sync.WaitGroup
		total int64
	)
	slots := make(chan struct{}, max(store.options.Concurrency, 1))
	fail := func(err error) ([]string, int64, [32]byte, error) {
		cancel(err)
		wait.Wait()
		return nil, 0, [32]byte{}, err
	}
	for number := 1; ; number++ {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return fail(context.Cause(ctx))
		}
		buffer := make([]byte, blockSize)
		count, readErr := io.ReadFull(source, buffer)
		if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			<-slots
			return fail(readErr)
		}
		if count == 0 {
			<-slots
			break
		}
		if number > maximumBlockCount {
			<-slots
			return fail(fmt.Errorf("azure store: object exceeds %d blocks of %d bytes", maximumBlockCount, blockSize))
		}
		chunk := buffer[:count]
		_, _ = whole.Write(chunk)
		total += int64(count)
		id := blockID(session, number)
		ids = append(ids, id)
		wait.Add(1)
		go func() {
			defer wait.Done()
			defer func() { <-slots }()
			checksum := md5.Sum(chunk)
			_, err := client.StageBlock(ctx, id, streaming.NopCloser(bytes.NewReader(chunk)), &blockblob.StageBlockOptions{
				TransactionalValidation: blob.TransferValidationTypeMD5(checksum[:]),
			})
			if err != nil {
				cancel(mapError(err))
			}
		}()
		if readErr != nil {
			break
		}
	}
	wait.Wait()
	if err := context.Cause(ctx); err != nil {
		return nil, 0, [32]byte{}, err
	}
	var digest [32]byte
	copy(digest[:], whole.Sum(nil))
	return ids, total, digest, nil
}

// errNotRewindable reports that a streamed part body cannot be resent.
var errNotRewindable = errors.New("azure store: streamed part body cannot be rewound")

// partBody streams exactly size bytes of a part to Put Block while hashing
// them. It satisfies the SDK's io.ReadSeekCloser requirement without
// buffering: seeks are accepted only before the first byte is read. The
// final bytes are withheld until the length and SHA-256 digest are verified
// and the source is known to hold no extra bytes; on a mismatch the request
// body fails before completion, so Azure never stages a block with wrong
// content.
type partBody struct {
	ctx    context.Context
	source io.Reader
	size   int64
	digest [32]byte
	hash   interface {
		io.Writer
		Sum([]byte) []byte
	}
	read     int64
	verified bool
	failure  error
}

func newPartBody(ctx context.Context, source io.Reader, size int64, digest [32]byte) *partBody {
	return &partBody{ctx: ctx, source: source, size: size, digest: digest, hash: sha256.New()}
}

func (body *partBody) Read(buffer []byte) (int, error) {
	if body.failure != nil {
		return 0, body.failure
	}
	if body.read == body.size {
		return 0, io.EOF
	}
	if err := body.ctx.Err(); err != nil {
		return 0, err
	}
	if int64(len(buffer)) > body.size-body.read {
		buffer = buffer[:body.size-body.read]
	}
	count, err := body.source.Read(buffer)
	_, _ = body.hash.Write(buffer[:count])
	body.read += int64(count)
	if body.read == body.size {
		if failure := body.verify(); failure != nil {
			body.failure = failure
			return 0, failure
		}
		return count, nil
	}
	if errors.Is(err, io.EOF) {
		body.failure = fmt.Errorf("%w: part body has %d bytes, want %d", objectstore.ErrInvalidPart, body.read, body.size)
		return 0, body.failure
	}
	if err != nil {
		body.failure = err
		return 0, err
	}
	return count, nil
}

// verify checks the digest and that the source is exhausted.
func (body *partBody) verify() error {
	var extra [1]byte
	for {
		count, err := body.source.Read(extra[:])
		if count > 0 {
			return fmt.Errorf("%w: part body is longer than %d bytes", objectstore.ErrInvalidPart, body.size)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}
	var computed [32]byte
	copy(computed[:], body.hash.Sum(nil))
	if computed != body.digest {
		return fmt.Errorf("%w: part SHA-256 mismatch", objectstore.ErrInvalidPart)
	}
	body.verified = true
	return nil
}

func (body *partBody) Seek(offset int64, whence int) (int64, error) {
	if offset != 0 || body.read != 0 {
		return 0, errNotRewindable
	}
	switch whence {
	case io.SeekStart, io.SeekCurrent:
		return 0, nil
	case io.SeekEnd:
		return body.size, nil
	}
	return 0, errNotRewindable
}

func (body *partBody) Close() error { return nil }

// exactReader yields exactly remaining bytes and reports a short body as
// io.ErrUnexpectedEOF instead of a silent truncation.
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

// runParallel calls work for every index with at most limit concurrent calls.
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
