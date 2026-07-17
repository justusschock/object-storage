package objectstore

import (
	"context"
	"io"
)

// Store provides immutable-version object operations.
type Store interface {
	Put(context.Context, Bucket, Key, io.Reader, PutOptions) (Object, error)
	Get(context.Context, Bucket, Key, GetOptions) (io.ReadCloser, Object, error)
	Stat(context.Context, Bucket, Key) (Object, error)
	Delete(context.Context, Bucket, Key, string) error
	List(context.Context, Bucket, ListOptions) (Page, error)
}

// Upload identifies one in-progress multipart object.
type Upload struct {
	ID      string
	Bucket  Bucket
	Key     Key
	Options PutOptions
}

// Clone returns an upload with independently owned options.
func (upload Upload) Clone() Upload {
	upload.Options = upload.Options.Clone()
	return upload
}

// CompletedPart identifies one immutable uploaded part.
type CompletedPart struct {
	Number int
	ETag   string
	Size   int64
	SHA256 [32]byte
}

// MultipartStore extends Store with explicit multipart custody.
type MultipartStore interface {
	Store
	BeginMultipart(context.Context, Bucket, Key, PutOptions) (Upload, error)
	PutPart(context.Context, Upload, int, io.Reader, int64, [32]byte) (CompletedPart, error)
	CompleteMultipart(context.Context, Upload, []CompletedPart) (Object, error)
	AbortMultipart(context.Context, Upload) error
}
