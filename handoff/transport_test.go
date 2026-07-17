package handoff

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"strings"
	"testing"

	lifecyclehandoff "github.com/justusschock/go-lifecycle/handoff"
	objectstore "github.com/justusschock/go-object-storage"
	"github.com/justusschock/go-object-storage/backend/memory"
)

func TestTransportRoundTripsDefensiveArtifactCopies(t *testing.T) {
	objects := memory.New()
	transport, err := NewTransport(objects, "handoffs", "artifacts")
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := lifecyclehandoff.NewArtifact("application/test", 1, []byte("payload"), map[string]string{"owner": "original"})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := transport.Send(context.Background(), artifact)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ArtifactID != artifact.ID || receipt.Location == "" {
		t.Fatalf("receipt = %#v", receipt)
	}
	received, err := transport.Receive(context.Background(), receipt)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(received, artifact) {
		t.Fatalf("received = %#v, want %#v", received, artifact)
	}
	received.Data[0] = 'X'
	received.Metadata["owner"] = "mutated"
	again, err := transport.Receive(context.Background(), receipt)
	if err != nil || !bytes.Equal(again.Data, artifact.Data) || again.Metadata["owner"] != "original" {
		t.Fatalf("second Receive = (%#v, %v)", again, err)
	}
}

func TestTransportRejectsTamperedArtifact(t *testing.T) {
	objects := memory.New()
	transport, err := NewTransport(objects, "handoffs", "artifacts")
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := lifecyclehandoff.NewArtifact("application/test", 1, []byte("ok"), nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := transport.Send(context.Background(), artifact)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := objects.Stat(context.Background(), "handoffs", objectstore.Key(receipt.Location))
	if err != nil {
		t.Fatal(err)
	}
	if err := objects.Delete(context.Background(), "handoffs", objectstore.Key(receipt.Location), stored.Version); err != nil {
		t.Fatal(err)
	}
	if _, err := objects.Put(context.Background(), "handoffs", objectstore.Key(receipt.Location), bytes.NewReader([]byte("bad")), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := transport.Receive(context.Background(), receipt); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("Receive error = %v, want ErrDigestMismatch", err)
	}
}

func TestTransportValidatesReceiptsAndCancellation(t *testing.T) {
	objects := memory.New()
	transport, err := NewTransport(objects, "handoffs", "artifacts")
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := lifecyclehandoff.NewArtifact("application/test", 1, []byte("ok"), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := transport.Send(ctx, artifact); !errors.Is(err, context.Canceled) {
		t.Fatalf("Send error = %v, want context.Canceled", err)
	}
	for _, receipt := range []lifecyclehandoff.Receipt{
		{},
		{ArtifactID: artifact.ID, Location: "other/key"},
		{ArtifactID: "wrong", Location: "artifacts/wrong/wrong"},
	} {
		if _, err := transport.Receive(context.Background(), receipt); err == nil {
			t.Fatalf("Receive(%#v) returned nil error", receipt)
		}
	}
}

func TestTransportSendIsIdempotent(t *testing.T) {
	objects := memory.New()
	transport, err := NewTransport(objects, "handoffs", "artifacts")
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := lifecyclehandoff.NewArtifact("application/test", 1, []byte("ok"), nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := transport.Send(context.Background(), artifact)
	if err != nil {
		t.Fatal(err)
	}
	second, err := transport.Send(context.Background(), artifact)
	if err != nil || second != first {
		t.Fatalf("second Send = (%#v, %v), want %#v", second, err, first)
	}
}

func TestEnvelopeRejectsNonCanonicalAndDuplicateJSON(t *testing.T) {
	artifact, err := lifecyclehandoff.NewArtifact("application/test", 1, []byte("ok"), map[string]string{"key": "value"})
	if err != nil {
		t.Fatal(err)
	}
	frame, _, err := marshalEnvelope(artifact)
	if err != nil {
		t.Fatal(err)
	}
	header, data := splitFrame(t, frame)
	for _, modified := range [][]byte{
		append([]byte("{ "), header[1:]...),
		[]byte(strings.Replace(string(header), `"format":`, `"format":"go-object-storage/handoff/v1","format":`, 1)),
		[]byte(strings.Replace(string(header), `"key":"value"`, `"key":"first","key":"value"`, 1)),
	} {
		if _, _, err := unmarshalEnvelope(joinFrame(modified, data)); err == nil {
			t.Fatalf("unmarshalEnvelope accepted non-canonical header %s", modified)
		}
	}
}

func splitFrame(t *testing.T, frame []byte) ([]byte, []byte) {
	t.Helper()
	length := binary.BigEndian.Uint64(frame[:8])
	end := 8 + int(length)
	return bytes.Clone(frame[8:end]), bytes.Clone(frame[end:])
}

func joinFrame(header, data []byte) []byte {
	frame := make([]byte, 8+len(header)+len(data))
	binary.BigEndian.PutUint64(frame[:8], uint64(len(header)))
	copy(frame[8:], header)
	copy(frame[8+len(header):], data)
	return frame
}

var _ lifecyclehandoff.Transport = (*Transport)(nil)
