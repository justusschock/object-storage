package objectstore

import "errors"

var (
	// ErrNotFound identifies an absent object or multipart upload.
	ErrNotFound = errors.New("object storage: not found")
	// ErrPrecondition identifies a failed conditional operation.
	ErrPrecondition = errors.New("object storage: precondition failed")
	// ErrInvalidBucket identifies a malformed bucket.
	ErrInvalidBucket = errors.New("object storage: invalid bucket")
	// ErrInvalidKey identifies a malformed object key.
	ErrInvalidKey = errors.New("object storage: invalid key")
	// ErrInvalidRange identifies an invalid byte range.
	ErrInvalidRange = errors.New("object storage: invalid byte range")
	// ErrInvalidPart identifies invalid multipart input or ordering.
	ErrInvalidPart = errors.New("object storage: invalid multipart part")
)
