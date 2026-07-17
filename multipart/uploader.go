// Package multipart provides bounded multipart upload orchestration.
package multipart

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sort"

	concurrency "github.com/justusschock/go-concurrency"
	objectstore "github.com/justusschock/go-object-storage"
)

const minimumPartSize = int64(5 << 20)

// Options controls multipart sizing and fan-out.
type Options struct {
	PartSize    int64
	MaxParallel int
}

// Uploader uploads fixed sections with bounded parallelism.
type Uploader struct {
	store       objectstore.MultipartStore
	partSize    int64
	maxParallel int
}

// NewUploader validates options using the portable five-MiB minimum part size.
func NewUploader(store objectstore.MultipartStore, options Options) (*Uploader, error) {
	return newUploader(store, options, minimumPartSize)
}

func newUploader(store objectstore.MultipartStore, options Options, minimum int64) (*Uploader, error) {
	if store == nil {
		return nil, errors.New("multipart: nil store")
	}
	if options.PartSize < minimum {
		return nil, fmt.Errorf("multipart: part size must be at least %d", minimum)
	}
	if options.MaxParallel <= 0 {
		return nil, errors.New("multipart: maximum parallelism must be positive")
	}
	return &Uploader{store: store, partSize: options.PartSize, maxParallel: options.MaxParallel}, nil
}

// UploadReaderAt uploads size bytes from reader and completes one object.
func (uploader *Uploader) UploadReaderAt(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, reader io.ReaderAt, size int64, options objectstore.PutOptions) (objectstore.Object, error) {
	if err := ctx.Err(); err != nil {
		return objectstore.Object{}, err
	}
	if err := objectstore.ValidateBucket(bucket); err != nil {
		return objectstore.Object{}, err
	}
	if err := objectstore.ValidateKey(key); err != nil {
		return objectstore.Object{}, err
	}
	if reader == nil {
		return objectstore.Object{}, errors.New("multipart: nil reader")
	}
	if size < 0 {
		return objectstore.Object{}, errors.New("multipart: negative size")
	}

	upload, err := uploader.store.BeginMultipart(ctx, bucket, key, options.Clone())
	if err != nil {
		return objectstore.Object{}, err
	}
	abort := func(primary error) error {
		abortErr := uploader.store.AbortMultipart(context.WithoutCancel(ctx), upload)
		if abortErr != nil {
			return errors.Join(primary, abortErr)
		}
		return primary
	}

	partCount := int((size + uploader.partSize - 1) / uploader.partSize)
	if partCount == 0 {
		partCount = 1
	}
	parts := make([]section, partCount)
	for index := range parts {
		offset := int64(index) * uploader.partSize
		length := min(uploader.partSize, size-offset)
		parts[index] = section{number: index + 1, offset: offset, length: length}
	}

	completed, err := concurrency.ParallelMap(ctx, parts, uploader.maxParallel, func(ctx context.Context, part section) (objectstore.CompletedPart, error) {
		digest, err := hashSection(ctx, reader, part.offset, part.length)
		if err != nil {
			return objectstore.CompletedPart{}, err
		}
		body := io.NewSectionReader(reader, part.offset, part.length)
		return uploader.store.PutPart(ctx, upload, part.number, body, part.length, digest)
	})
	if err != nil {
		return objectstore.Object{}, abort(err)
	}
	sort.Slice(completed, func(left, right int) bool { return completed[left].Number < completed[right].Number })
	object, err := uploader.store.CompleteMultipart(ctx, upload, completed)
	if err != nil {
		return objectstore.Object{}, abort(err)
	}
	return object, nil
}

type section struct {
	number int
	offset int64
	length int64
}

func hashSection(ctx context.Context, reader io.ReaderAt, offset, length int64) ([32]byte, error) {
	hash := sha256.New()
	sectionReader := io.NewSectionReader(reader, offset, length)
	buffer := make([]byte, 32<<10)
	for {
		if err := ctx.Err(); err != nil {
			return [32]byte{}, err
		}
		count, err := sectionReader.Read(buffer)
		if count > 0 {
			_, _ = hash.Write(buffer[:count])
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return [32]byte{}, err
		}
	}
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}
