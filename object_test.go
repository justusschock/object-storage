package objectstore_test

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	objectstore "github.com/justusschock/go-object-storage"
)

// FR-OBJ-01
func TestValidateBucket(t *testing.T) {
	t.Parallel()

	for _, bucket := range []objectstore.Bucket{"", "bad\x00bucket"} {
		if err := objectstore.ValidateBucket(bucket); !errors.Is(err, objectstore.ErrInvalidBucket) {
			t.Fatalf("ValidateBucket(%q) error = %v, want %v", bucket, err, objectstore.ErrInvalidBucket)
		}
	}
	for _, bucket := range []objectstore.Bucket{"objects", "tenant/neutral"} {
		if err := objectstore.ValidateBucket(bucket); err != nil {
			t.Fatalf("ValidateBucket(%q) error = %v", bucket, err)
		}
	}
}

// FR-OBJ-01
func TestValidateKey(t *testing.T) {
	t.Parallel()

	invalid := []objectstore.Key{
		"",
		"/a",
		"..",
		"../a",
		"a/..",
		"a/../b",
		"a\\b",
		"a\x00b",
	}
	for _, key := range invalid {
		if err := objectstore.ValidateKey(key); !errors.Is(err, objectstore.ErrInvalidKey) {
			t.Fatalf("ValidateKey(%q) error = %v, want %v", key, err, objectstore.ErrInvalidKey)
		}
	}

	valid := []objectstore.Key{"a", "a/b", "a//b", "a/.../b", "a/.hidden", "a/b/", "ü/对象"}
	for _, key := range valid {
		if err := objectstore.ValidateKey(key); err != nil {
			t.Fatalf("ValidateKey(%q) error = %v", key, err)
		}
	}
}

// FR-OBJ-02
func TestByteRangeValidate(t *testing.T) {
	t.Parallel()

	invalid := []objectstore.ByteRange{
		{Offset: -1, Length: 1},
		{Offset: 0, Length: 0},
		{Offset: 5, Length: -1},
		{Offset: math.MaxInt64, Length: 1},
	}
	for _, byteRange := range invalid {
		if err := byteRange.Validate(); !errors.Is(err, objectstore.ErrInvalidRange) {
			t.Fatalf("Validate(%+v) error = %v, want %v", byteRange, err, objectstore.ErrInvalidRange)
		}
	}

	for _, byteRange := range []objectstore.ByteRange{{Offset: 0, Length: 1}, {Offset: 5, Length: 10}} {
		if err := byteRange.Validate(); err != nil {
			t.Fatalf("Validate(%+v) error = %v", byteRange, err)
		}
	}
}

// FR-OBJ-08
func TestContractClonesOwnMutableFields(t *testing.T) {
	t.Parallel()

	digest := [32]byte{1}
	modifiedAt := time.Date(2026, time.July, 17, 9, 30, 0, 0, time.UTC)
	object := objectstore.Object{
		Bucket:     "objects",
		Key:        "path/value",
		Version:    "version-1",
		ETag:       "etag-1",
		Size:       17,
		ModifiedAt: modifiedAt,
		SHA256:     digest,
		Metadata:   map[string]string{"object": "original"},
	}
	put := objectstore.PutOptions{
		ContentType:    "application/octet-stream",
		Metadata:       map[string]string{"put": "original"},
		IfAbsent:       true,
		IfMatchVersion: "version-0",
		SHA256:         &digest,
	}
	get := objectstore.GetOptions{
		Range:          &objectstore.ByteRange{Offset: 1, Length: 2},
		IfMatchVersion: "version-1",
	}
	upload := objectstore.Upload{ID: "upload-1", Bucket: object.Bucket, Key: object.Key, Options: put}
	page := objectstore.Page{Objects: []objectstore.Object{object}, NextAfter: "path/value"}

	objectClone := object.Clone()
	putClone := put.Clone()
	getClone := get.Clone()
	uploadClone := upload.Clone()
	pageClone := page.Clone()
	for name, values := range map[string][2]any{
		"object": {objectClone, object},
		"put":    {putClone, put},
		"get":    {getClone, get},
		"upload": {uploadClone, upload},
		"page":   {pageClone, page},
	} {
		if !reflect.DeepEqual(values[0], values[1]) {
			t.Fatalf("%s clone = %#v, want value-preserving %#v", name, values[0], values[1])
		}
	}

	objectClone.Metadata["object"] = "clone"
	putClone.Metadata["put"] = "clone"
	putClone.SHA256[0] = 2
	getClone.Range.Offset = 9
	uploadClone.Options.Metadata["put"] = "upload clone"
	pageClone.Objects[0].Metadata["object"] = "page clone"
	pageClone.Objects = append(pageClone.Objects, objectstore.Object{})

	if object.Metadata["object"] != "original" {
		t.Fatalf("Object.Clone mutated source: %+v", object)
	}
	if put.Metadata["put"] != "original" || put.SHA256[0] != 1 {
		t.Fatalf("PutOptions.Clone mutated source: %+v", put)
	}
	if get.Range.Offset != 1 {
		t.Fatalf("GetOptions.Clone mutated source: %+v", get)
	}
	if upload.Options.Metadata["put"] != "original" {
		t.Fatalf("Upload.Clone mutated source: %+v", upload)
	}
	if len(page.Objects) != 1 || page.Objects[0].Metadata["object"] != "original" {
		t.Fatalf("Page.Clone mutated source: %+v", page)
	}
}

// FR-OBJ-08
func TestContractClonesPreserveNilFields(t *testing.T) {
	t.Parallel()

	if clone := (objectstore.Object{}).Clone(); clone.Metadata != nil {
		t.Fatalf("Object.Clone() metadata = %#v, want nil", clone.Metadata)
	}
	if clone := (objectstore.PutOptions{}).Clone(); clone.Metadata != nil || clone.SHA256 != nil {
		t.Fatalf("PutOptions.Clone() = %#v, want nil fields", clone)
	}
	if clone := (objectstore.GetOptions{}).Clone(); clone.Range != nil {
		t.Fatalf("GetOptions.Clone() range = %#v, want nil", clone.Range)
	}
	if clone := (objectstore.Page{}).Clone(); clone.Objects != nil {
		t.Fatalf("Page.Clone() objects = %#v, want nil", clone.Objects)
	}
}
