package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	objectstore "github.com/justusschock/go-object-storage"
)

// putStream uploads source as a multipart object whose metadata declares
// digest. The running digest and length are checked before completion, so a
// mismatching body aborts the upload and never publishes the key.
func (store *Store) putStream(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, source io.Reader, digest [32]byte, options objectstore.PutOptions) (objectstore.Object, error) {
	metadata, err := metadataWithDigest(options.Metadata, digest)
	if err != nil {
		return objectstore.Object{}, err
	}
	nonce, err := newNonce()
	if err != nil {
		return objectstore.Object{}, err
	}
	metadata[uploadMetadataKey] = nonce
	created, err := store.client.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: stringPointer(string(bucket)), Key: stringPointer(string(key)), ContentType: optionalString(options.ContentType),
		Metadata: metadata, ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	if err != nil {
		return objectstore.Object{}, mapError(err)
	}
	uploadID := optionalDereference(created.UploadId)
	if uploadID == "" {
		return objectstore.Object{}, errors.New("s3 store: missing multipart upload ID")
	}
	abort := func(primary error) (objectstore.Object, error) {
		if abortErr := store.abortUpload(ctx, string(bucket), string(key), uploadID); abortErr != nil && !errors.Is(abortErr, objectstore.ErrNotFound) {
			return objectstore.Object{}, errors.Join(primary, fmt.Errorf("s3 store: abort multipart upload: %w", abortErr))
		}
		return objectstore.Object{}, primary
	}

	partSize := store.partSize(options.Size)
	parts, total, computed, err := store.uploadParts(ctx, string(bucket), string(key), uploadID, contextReader{ctx: ctx, reader: source}, partSize)
	if err != nil {
		return abort(err)
	}
	if computed != digest {
		return abort(objectstore.ErrPrecondition)
	}
	if options.Size > 0 && options.Size != total {
		return abort(fmt.Errorf("%w: size mismatch", objectstore.ErrPrecondition))
	}
	output, err := store.complete(ctx, string(bucket), string(key), uploadID, parts, total, options)
	if err != nil {
		return abort(err)
	}
	return store.published(ctx, bucket, key, publication{etag: optionalDereference(output.ETag), size: total, digest: &digest, nonce: nonce, metadata: options.Metadata})
}

// uploadParts reads source sequentially in partSize chunks, hashing each
// chunk into the whole-object digest, and uploads up to Concurrency chunks in
// parallel.
func (store *Store) uploadParts(ctx context.Context, bucket, key, uploadID string, source io.Reader, partSize int64) ([]types.CompletedPart, int64, [32]byte, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	whole := sha256.New()
	var (
		mu    sync.Mutex
		parts []types.CompletedPart
		wait  sync.WaitGroup
		total int64
	)
	slots := make(chan struct{}, max(store.options.Concurrency, 1))
	fail := func(err error) ([]types.CompletedPart, int64, [32]byte, error) {
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
		if ctx.Err() != nil {
			<-slots
			return fail(context.Cause(ctx))
		}
		buffer := make([]byte, partSize)
		count, readErr := io.ReadFull(source, buffer)
		if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			<-slots
			return fail(readErr)
		}
		if count == 0 && number > 1 {
			<-slots
			break
		}
		if number > maximumPartCount {
			<-slots
			return fail(fmt.Errorf("s3 store: object exceeds %d parts of %d bytes", maximumPartCount, partSize))
		}
		chunk := buffer[:count]
		_, _ = whole.Write(chunk)
		total += int64(count)
		wait.Add(1)
		go func(number int, chunk []byte) {
			defer wait.Done()
			defer func() { <-slots }()
			digest := sha256.Sum256(chunk)
			etag, err := store.uploadPart(ctx, bucket, key, uploadID, number, bytes.NewReader(chunk), int64(len(chunk)), digest)
			if err != nil {
				cancel(err)
				return
			}
			mu.Lock()
			parts = append(parts, completedPart(number, etag, digest))
			mu.Unlock()
		}(number, chunk)
		if readErr != nil {
			break
		}
	}
	wait.Wait()
	if err := context.Cause(ctx); err != nil {
		return nil, 0, [32]byte{}, err
	}
	sort.Slice(parts, func(left, right int) bool { return *parts[left].PartNumber < *parts[right].PartNumber })
	var digest [32]byte
	copy(digest[:], whole.Sum(nil))
	return parts, total, digest, nil
}

// uploadPart sends one part whose SigV4 payload hash and checksum header are
// derived from digest, so S3 rejects bytes that do not match it. body need
// not be seekable.
func (store *Store) uploadPart(ctx context.Context, bucket, key, uploadID string, number int, body io.Reader, size int64, digest [32]byte) (string, error) {
	output, err := store.client.UploadPart(ctx, &awss3.UploadPartInput{
		Bucket: stringPointer(bucket), Key: stringPointer(key), UploadId: stringPointer(uploadID),
		PartNumber: int32Pointer(int32(number)), Body: body, ContentLength: int64Pointer(size),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256, ChecksumSHA256: stringPointer(base64.StdEncoding.EncodeToString(digest[:])),
	}, withPayloadHash(digest))
	if err != nil {
		return "", mapError(err)
	}
	if output.ETag == nil || *output.ETag == "" {
		return "", errors.New("s3 store: missing part ETag")
	}
	return *output.ETag, nil
}

func completedPart(number int, etag string, digest [32]byte) types.CompletedPart {
	return types.CompletedPart{
		PartNumber: int32Pointer(int32(number)), ETag: stringPointer(etag),
		ChecksumSHA256: stringPointer(base64.StdEncoding.EncodeToString(digest[:])),
	}
}

// complete finishes a multipart upload on its final key with the put
// conditions evaluated atomically by S3.
func (store *Store) complete(ctx context.Context, bucket, key, uploadID string, parts []types.CompletedPart, total int64, options objectstore.PutOptions) (*awss3.CompleteMultipartUploadOutput, error) {
	input := &awss3.CompleteMultipartUploadInput{
		Bucket: stringPointer(bucket), Key: stringPointer(key), UploadId: stringPointer(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}, MpuObjectSize: int64Pointer(total),
		IfMatch: optionalString(options.IfMatchVersion),
	}
	if options.IfAbsent {
		input.IfNoneMatch = stringPointer("*")
	}
	output, err := store.client.CompleteMultipartUpload(ctx, input)
	if err != nil {
		return nil, conditionalWriteError(err, options.IfMatchVersion)
	}
	return output, nil
}

func (store *Store) abortUpload(ctx context.Context, bucket, key, uploadID string) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	_, err := store.client.AbortMultipartUpload(cleanupCtx, &awss3.AbortMultipartUploadInput{
		Bucket: stringPointer(bucket), Key: stringPointer(key), UploadId: stringPointer(uploadID),
	})
	return mapError(err)
}

// partSize returns the configured part size, grown so that an object of the
// expected size fits within the S3 part-count limit.
func (store *Store) partSize(expected int64) int64 {
	size := store.options.PartSize
	if expected > 0 {
		const mebibyte = int64(1 << 20)
		needed := (expected + maximumPartCount - 1) / maximumPartCount
		needed = (needed + mebibyte - 1) / mebibyte * mebibyte
		size = max(size, needed)
	}
	return size
}

// withPayloadHash makes SigV4 sign the request with digest as the payload
// hash instead of reading the body to compute it. S3 rejects a body whose
// SHA-256 differs, and unseekable bodies can be streamed over plain HTTP.
func withPayloadHash(digest [32]byte) func(*awss3.Options) {
	encoded := hex.EncodeToString(digest[:])
	return func(options *awss3.Options) {
		options.APIOptions = append(options.APIOptions, func(stack *middleware.Stack) error {
			return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("GoObjectStoragePayloadHash", func(ctx context.Context, input middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
				return next.HandleFinalize(v4.SetPayloadHash(ctx, encoded), input)
			}), middleware.Before)
		})
	}
}

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
	_ = spooled.file.Close()
	_ = os.Remove(spooled.file.Name())
}

// spool writes head and the rest of body to a temporary file while hashing,
// then rewinds the file.
func spool(ctx context.Context, directory string, head []byte, body io.Reader) (spooledFile, [32]byte, error) {
	file, err := os.CreateTemp(directory, "go-object-storage-s3-*")
	if err != nil {
		return spooledFile{}, [32]byte{}, fmt.Errorf("s3 store: create spool file: %w", err)
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

// verifyingReader hashes and counts the bytes it forwards.
type verifyingReader struct {
	reader io.Reader
	hash   interface {
		io.Writer
		Sum([]byte) []byte
	}
	count int64
	eof   bool
}

func newVerifyingReader(reader io.Reader) *verifyingReader {
	return &verifyingReader{reader: reader, hash: sha256.New()}
}

func (reader *verifyingReader) Read(buffer []byte) (int, error) {
	count, err := reader.reader.Read(buffer)
	if count > 0 {
		_, _ = reader.hash.Write(buffer[:count])
		reader.count += int64(count)
	}
	if errors.Is(err, io.EOF) {
		reader.eof = true
	}
	return count, err
}

func (reader *verifyingReader) digest() [32]byte {
	var digest [32]byte
	copy(digest[:], reader.hash.Sum(nil))
	return digest
}

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

func sortObjects(objects []objectstore.Object) {
	sort.Slice(objects, func(left, right int) bool { return objects[left].Key < objects[right].Key })
}
