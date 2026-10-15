package objectstore

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"
)

// Bucket identifies a backend-neutral object namespace.
type Bucket string

// Key identifies an object within a bucket.
type Key string

// ValidateBucket checks the portable bucket invariants.
func ValidateBucket(bucket Bucket) error {
	if bucket == "" {
		return fmt.Errorf("%w: empty value", ErrInvalidBucket)
	}
	if strings.ContainsRune(string(bucket), '\x00') {
		return fmt.Errorf("%w: contains NUL", ErrInvalidBucket)
	}
	return nil
}

// ValidateKey checks the portable key invariants.
func ValidateKey(key Key) error {
	value := string(key)
	if value == "" {
		return fmt.Errorf("%w: empty value", ErrInvalidKey)
	}
	if strings.HasPrefix(value, "/") {
		return fmt.Errorf("%w: leading slash", ErrInvalidKey)
	}
	if strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%w: contains NUL", ErrInvalidKey)
	}
	if strings.ContainsRune(value, '\\') {
		return fmt.Errorf("%w: contains backslash", ErrInvalidKey)
	}
	for _, segment := range strings.Split(value, "/") {
		if segment == ".." {
			return fmt.Errorf("%w: parent segment", ErrInvalidKey)
		}
	}
	return nil
}

// ByteRange identifies the half-open interval [Offset, Offset+Length).
type ByteRange struct {
	Offset int64
	Length int64
}

// Validate rejects negative offsets, non-positive lengths, and overflow.
func (byteRange ByteRange) Validate() error {
	if byteRange.Offset < 0 || byteRange.Length <= 0 || byteRange.Offset > math.MaxInt64-byteRange.Length {
		return ErrInvalidRange
	}
	return nil
}

// Object describes one immutable object version.
//
// SHA256 is meaningful only when SHA256Known is true. Backends report an
// unknown digest instead of reading object bytes when the digest is not
// recorded in cheap metadata: for example foreign objects written by other
// tools, listings without IncludeMetadata, or multipart uploads completed
// without a declared digest. An unknown digest leaves SHA256 zero, so code
// that compares SHA256 against an expected digest fails closed.
type Object struct {
	Bucket      Bucket
	Key         Key
	Version     string
	ETag        string
	Size        int64
	ModifiedAt  time.Time
	SHA256      [32]byte
	SHA256Known bool
	Metadata    map[string]string
}

// Digest returns the object SHA-256 digest and whether it is known.
func (object Object) Digest() ([32]byte, bool) {
	if !object.SHA256Known {
		return [32]byte{}, false
	}
	return object.SHA256, true
}

// Clone returns metadata storage owned by the caller.
func (object Object) Clone() Object {
	object.Metadata = maps.Clone(object.Metadata)
	return object
}

// PutOptions configures one conditional object write.
//
// IfAbsent and IfMatchVersion are evaluated atomically with publication and
// together form the portable compare-and-swap primitive: read an object's
// Version, then write with IfMatchVersion set to it; ErrPrecondition reports
// that another writer won. Versions identify stored contents, and an S3
// entity tag repeats when identical bytes are written again, so CAS users
// should encode a changing value (a counter, token, or timestamp) in the
// object.
//
// A non-nil SHA256 is verified before the object becomes visible. Size, when
// positive, is the exact expected body length: it lets backends stream bodies
// without buffering, and a mismatch fails with ErrPrecondition. Zero means
// the length is unknown; negative values are invalid.
type PutOptions struct {
	ContentType    string
	Metadata       map[string]string
	IfAbsent       bool
	IfMatchVersion string
	SHA256         *[32]byte
	Size           int64
}

// ValidateSize rejects negative expected sizes with ErrInvalidRange.
func (options PutOptions) ValidateSize() error {
	if options.Size < 0 {
		return fmt.Errorf("%w: negative size", ErrInvalidRange)
	}
	return nil
}

// Clone returns options with independently owned reference fields.
func (options PutOptions) Clone() PutOptions {
	options.Metadata = maps.Clone(options.Metadata)
	if options.SHA256 != nil {
		digest := *options.SHA256
		options.SHA256 = &digest
	}
	return options
}

// GetOptions configures one conditional object read.
type GetOptions struct {
	Range          *ByteRange
	IfMatchVersion string
}

// Clone returns options with an independently owned range.
func (options GetOptions) Clone() GetOptions {
	if options.Range != nil {
		byteRange := *options.Range
		options.Range = &byteRange
	}
	return options
}

// ListOptions configures a stable, key-ordered page.
//
// Listed objects always carry Bucket, Key, Version, ETag, Size, and
// ModifiedAt. User Metadata and SHA256 may be omitted (nil Metadata and
// SHA256Known false) unless IncludeMetadata is set, in which case every listed
// object carries the fields Stat returns. Backends that hold full metadata
// cheaply may always include it.
type ListOptions struct {
	Prefix          string
	After           Key
	Limit           int
	IncludeMetadata bool
}

// Page is one page of key-ordered object metadata.
type Page struct {
	Objects   []Object
	NextAfter Key
}

// Clone returns a page with independently owned object and metadata storage.
func (page Page) Clone() Page {
	page.Objects = slices.Clone(page.Objects)
	for index := range page.Objects {
		page.Objects[index] = page.Objects[index].Clone()
	}
	return page
}
