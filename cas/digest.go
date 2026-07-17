package cas

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrInvalidDigest identifies a malformed SHA-256 digest.
	ErrInvalidDigest = errors.New("cas: invalid digest")
	// ErrDigestMismatch identifies content that does not match its address.
	ErrDigestMismatch = errors.New("cas: digest mismatch")
)

// Digest is a SHA-256 content address.
type Digest [32]byte

// String returns the canonical algorithm-prefixed representation.
func (digest Digest) String() string {
	return "sha256:" + digest.hex()
}

// ParseDigest parses the canonical SHA-256 representation.
func ParseDigest(value string) (Digest, error) {
	if !strings.HasPrefix(value, "sha256:") {
		return Digest{}, ErrInvalidDigest
	}
	encoded := strings.TrimPrefix(value, "sha256:")
	if len(encoded) != 64 {
		return Digest{}, ErrInvalidDigest
	}
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		return Digest{}, fmt.Errorf("%w: %v", ErrInvalidDigest, err)
	}
	var digest Digest
	copy(digest[:], decoded)
	return digest, nil
}

func (digest Digest) hex() string {
	return hex.EncodeToString(digest[:])
}
