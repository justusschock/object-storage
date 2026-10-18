// Package handoff transports lifecycle artifacts through object storage.
package handoff

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"unicode/utf8"

	lifecyclehandoff "github.com/justusschock/lifecycle/handoff"
	objectstore "github.com/justusschock/object-storage"
)

const envelopeFormat = "go-object-storage/handoff/v1"

var (
	// ErrDigestMismatch identifies a corrupted or misbound artifact envelope.
	ErrDigestMismatch = errors.New("object handoff: digest mismatch")
	// ErrInvalidReceipt identifies a receipt outside this transport namespace.
	ErrInvalidReceipt = errors.New("object handoff: invalid receipt")
)

// Transport stores canonical artifact envelopes as immutable objects. It is
// safe for concurrent use.
type Transport struct {
	store  objectstore.Store
	bucket objectstore.Bucket
	prefix string
}

type envelopeMetadata struct {
	Format     string            `json:"format"`
	ArtifactID string            `json:"artifact_id"`
	MediaType  string            `json:"media_type"`
	Version    uint64            `json:"version"`
	Digest     string            `json:"digest"`
	DataLength int64             `json:"data_length"`
	DataSHA256 string            `json:"data_sha256"`
	Metadata   map[string]string `json:"metadata"`
}

// NewTransport validates a backend-neutral object namespace.
func NewTransport(store objectstore.Store, bucket objectstore.Bucket, prefix string) (*Transport, error) {
	if isNil(store) {
		return nil, errors.New("object handoff: nil store")
	}
	if err := objectstore.ValidateBucket(bucket); err != nil {
		return nil, err
	}
	if err := objectstore.ValidateKey(objectstore.Key(prefix + "/artifact")); err != nil || strings.HasSuffix(prefix, "/") {
		return nil, fmt.Errorf("object handoff: invalid prefix %q", prefix)
	}
	return &Transport{store: store, bucket: bucket, prefix: prefix}, nil
}

// Send validates and immutably publishes an artifact envelope.
func (transport *Transport) Send(ctx context.Context, artifact lifecyclehandoff.Artifact) (lifecyclehandoff.Receipt, error) {
	if err := ctx.Err(); err != nil {
		return lifecyclehandoff.Receipt{}, err
	}
	artifact = artifact.Clone()
	if err := artifact.Validate(); err != nil {
		return lifecyclehandoff.Receipt{}, err
	}
	if !utf8.ValidString(artifact.MediaType) {
		return lifecyclehandoff.Receipt{}, errors.New("object handoff: artifact contains invalid UTF-8")
	}
	for key, value := range artifact.Metadata {
		if !utf8.ValidString(key) || !utf8.ValidString(value) {
			return lifecyclehandoff.Receipt{}, errors.New("object handoff: artifact contains invalid UTF-8")
		}
	}
	frame, frameDigest, err := marshalEnvelope(artifact)
	if err != nil {
		return lifecyclehandoff.Receipt{}, err
	}
	key := transport.key(artifact.ID)
	_, err = transport.store.Put(ctx, transport.bucket, key, bytes.NewReader(frame), objectstore.PutOptions{IfAbsent: true, SHA256: &frameDigest})
	if errors.Is(err, objectstore.ErrPrecondition) {
		object, statErr := transport.store.Stat(ctx, transport.bucket, key)
		if statErr != nil {
			return lifecyclehandoff.Receipt{}, errors.Join(err, statErr)
		}
		if object.SHA256 != frameDigest {
			return lifecyclehandoff.Receipt{}, ErrDigestMismatch
		}
		err = nil
	}
	if err != nil {
		return lifecyclehandoff.Receipt{}, err
	}
	return lifecyclehandoff.Receipt{ArtifactID: artifact.ID, Location: string(key)}, nil
}

// Receive validates a receipt and reconstructs its canonical artifact.
func (transport *Transport) Receive(ctx context.Context, receipt lifecyclehandoff.Receipt) (lifecyclehandoff.Artifact, error) {
	if err := ctx.Err(); err != nil {
		return lifecyclehandoff.Artifact{}, err
	}
	if !validDigestHex(receipt.ArtifactID) || receipt.Location != string(transport.key(receipt.ArtifactID)) {
		return lifecyclehandoff.Artifact{}, ErrInvalidReceipt
	}
	body, object, err := transport.store.Get(ctx, transport.bucket, objectstore.Key(receipt.Location), objectstore.GetOptions{})
	if err != nil {
		return lifecyclehandoff.Artifact{}, err
	}
	frame, readErr := io.ReadAll(body)
	closeErr := body.Close()
	if readErr != nil || closeErr != nil {
		return lifecyclehandoff.Artifact{}, errors.Join(readErr, closeErr)
	}
	if err := ctx.Err(); err != nil {
		return lifecyclehandoff.Artifact{}, err
	}
	frameDigest := sha256.Sum256(frame)
	if object.SHA256 != frameDigest {
		return lifecyclehandoff.Artifact{}, ErrDigestMismatch
	}
	metadata, data, err := unmarshalEnvelope(frame)
	if err != nil {
		return lifecyclehandoff.Artifact{}, fmt.Errorf("%w: %v", ErrDigestMismatch, err) //nolint:errorlint // one classification per error: the cause is detail, not a second sentinel (ADR-0005)
	}
	dataDigest := sha256.Sum256(data)
	if metadata.Format != envelopeFormat || metadata.ArtifactID != receipt.ArtifactID || metadata.Digest != receipt.ArtifactID ||
		metadata.DataLength != int64(len(data)) || metadata.DataSHA256 != hex.EncodeToString(dataDigest[:]) {
		return lifecyclehandoff.Artifact{}, ErrDigestMismatch
	}
	artifact, err := lifecyclehandoff.NewArtifact(metadata.MediaType, metadata.Version, data, metadata.Metadata)
	if err != nil {
		return lifecyclehandoff.Artifact{}, fmt.Errorf("%w: %v", ErrDigestMismatch, err) //nolint:errorlint // one classification per error: the cause is detail, not a second sentinel (ADR-0005)
	}
	if artifact.ID != metadata.ArtifactID || hex.EncodeToString(artifact.Digest[:]) != metadata.Digest {
		return lifecyclehandoff.Artifact{}, ErrDigestMismatch
	}
	return artifact.Clone(), nil
}

func (transport *Transport) key(artifactID string) objectstore.Key {
	return objectstore.Key(transport.prefix + "/" + artifactID + "/" + artifactID)
}

func marshalEnvelope(artifact lifecyclehandoff.Artifact) ([]byte, [32]byte, error) {
	dataDigest := sha256.Sum256(artifact.Data)
	metadata := envelopeMetadata{
		Format:     envelopeFormat,
		ArtifactID: artifact.ID,
		MediaType:  artifact.MediaType,
		Version:    artifact.Version,
		Digest:     hex.EncodeToString(artifact.Digest[:]),
		DataLength: int64(len(artifact.Data)),
		DataSHA256: hex.EncodeToString(dataDigest[:]),
		Metadata:   artifact.Clone().Metadata,
	}
	header, err := json.Marshal(metadata)
	if err != nil {
		return nil, [32]byte{}, err
	}
	frame := make([]byte, 8+len(header)+len(artifact.Data))
	binary.BigEndian.PutUint64(frame[:8], uint64(len(header)))
	copy(frame[8:], header)
	copy(frame[8+len(header):], artifact.Data)
	return frame, sha256.Sum256(frame), nil
}

func unmarshalEnvelope(frame []byte) (envelopeMetadata, []byte, error) {
	if len(frame) < 8 {
		return envelopeMetadata{}, nil, errors.New("truncated frame")
	}
	headerLength := binary.BigEndian.Uint64(frame[:8])
	if headerLength > uint64(len(frame)-8) {
		return envelopeMetadata{}, nil, errors.New("truncated header")
	}
	headerEnd := 8 + int(headerLength)
	header := frame[8:headerEnd]
	decoder := json.NewDecoder(bytes.NewReader(header))
	decoder.DisallowUnknownFields()
	var metadata envelopeMetadata
	if err := decoder.Decode(&metadata); err != nil {
		return envelopeMetadata{}, nil, err
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return envelopeMetadata{}, nil, err
	}
	canonical, err := json.Marshal(metadata)
	if err != nil {
		return envelopeMetadata{}, nil, err
	}
	if !bytes.Equal(header, canonical) {
		return envelopeMetadata{}, nil, errors.New("non-canonical metadata")
	}
	return metadata, bytes.Clone(frame[headerEnd:]), nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("multiple JSON values")
	}
	return err
}

func validDigestHex(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func isNil(value any) bool {
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() {
		return true
	}
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

var _ lifecyclehandoff.Transport = (*Transport)(nil)
