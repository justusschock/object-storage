// Package objectstore defines backend-neutral object storage contracts.
//
// A Store keeps immutable object versions addressed by Bucket and Key:
// Put publishes a new version atomically, Get and Stat read the current one,
// Delete removes it and List pages through a bucket in key order.
// MultipartStore adds explicit multipart uploads. Optional capabilities
// (BatchStater, BatchDeleter, Copier, Presigner) are discovered with type
// assertions.
//
// PutOptions.IfAbsent and PutOptions.IfMatchVersion are evaluated atomically
// with publication and form the portable compare-and-swap primitive. A
// declared SHA-256 digest and size are verified before an object becomes
// visible. Failures are classified by the sentinel errors ErrNotFound,
// ErrPrecondition, ErrInvalidBucket, ErrInvalidKey, ErrInvalidRange and
// ErrInvalidPart; test them with errors.Is. Context cancellation is returned
// as the context's error.
//
// Implementations live in backend/memory and backend/fs (this module) and in
// the nested modules backend/s3, backend/gcs, backend/azure, backend/sftp and
// backend/http. Every implementation is safe for concurrent use and passes
// the conformance suite in package storetest. The packages cas, pack,
// multipart, lease and handoff build on any Store.
package objectstore
