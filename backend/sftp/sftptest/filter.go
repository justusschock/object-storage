package sftptest

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// SFTP v3 packet types and status codes used by the filter.
const (
	packetVersion  = 2
	packetRename   = 18
	packetStatus   = 101
	packetExtended = 200

	statusFailure       = 4
	statusOpUnsupported = 8

	maxPacketLength = 1 << 20
)

// filter sits between an SSH channel and the pkg/sftp server and rewrites
// the SFTP packet stream to emulate limited servers. Client packets are read
// whole and either forwarded or answered directly; server packets are
// reassembled and written whole under one mutex, so injected responses never
// interleave with server packets.
type filter struct {
	channel io.ReadWriteCloser
	dir     string
	options Options

	readBuffer []byte // forwarded client bytes not yet read by the server

	writeMu     sync.Mutex
	pending     []byte // server bytes not yet forming a whole packet
	versionSeen bool
}

func newFilter(channel io.ReadWriteCloser, dir string, options Options) *filter {
	return &filter{channel: channel, dir: dir, options: options}
}

// Read is called only by the server's packet reader goroutine.
func (filter *filter) Read(buffer []byte) (int, error) {
	for len(filter.readBuffer) == 0 {
		packet, err := readPacket(filter.channel)
		if err != nil {
			return 0, err
		}
		if response := filter.intercept(packet[4:]); response != nil {
			if err := filter.writeWhole(response); err != nil {
				return 0, err
			}
			continue
		}
		filter.readBuffer = packet
	}
	count := copy(buffer, filter.readBuffer)
	filter.readBuffer = filter.readBuffer[count:]
	return count, nil
}

func (filter *filter) Write(data []byte) (int, error) {
	filter.writeMu.Lock()
	defer filter.writeMu.Unlock()
	filter.pending = append(filter.pending, data...)
	for len(filter.pending) >= 4 {
		total := 4 + int(binary.BigEndian.Uint32(filter.pending))
		if len(filter.pending) < total {
			break
		}
		packet := filter.pending[:total]
		if !filter.versionSeen {
			filter.versionSeen = true
			if filter.options.DisableExtensions && total > 4 && packet[4] == packetVersion && total >= 9 {
				// Keep the type and version number, drop every extension pair.
				packet = binary.BigEndian.AppendUint32(nil, 5)
				packet = append(packet, filter.pending[4:9]...)
			}
		}
		if _, err := filter.channel.Write(packet); err != nil {
			return 0, err
		}
		filter.pending = append(filter.pending[:0], filter.pending[total:]...)
	}
	return len(data), nil
}

func (filter *filter) Close() error { return filter.channel.Close() }

func (filter *filter) writeWhole(packet []byte) error {
	filter.writeMu.Lock()
	defer filter.writeMu.Unlock()
	_, err := filter.channel.Write(packet)
	return err
}

// intercept returns a response packet for client requests the filter
// answers itself, or nil to forward the request.
func (filter *filter) intercept(body []byte) []byte {
	if len(body) < 5 {
		return nil
	}
	kind, id, rest := body[0], binary.BigEndian.Uint32(body[1:5]), body[5:]
	switch {
	case kind == packetExtended && filter.options.DisableExtensions:
		name, _, ok := readString(rest)
		if ok && strings.HasSuffix(name, "@openssh.com") {
			return statusPacket(id, statusOpUnsupported, "extension disabled by sftptest")
		}
	case kind == packetRename && filter.options.StrictRename:
		_, rest, ok := readString(rest)
		if !ok {
			return nil
		}
		target, _, ok := readString(rest)
		if !ok {
			return nil
		}
		if _, err := os.Lstat(filter.resolve(target)); err == nil {
			return statusPacket(id, statusFailure, "rename target exists")
		}
	}
	return nil
}

// resolve mirrors pkg/sftp's server path mapping for a working directory.
func (filter *filter) resolve(name string) string {
	if strings.HasPrefix(name, "/") {
		return filepath.FromSlash(name)
	}
	return filepath.Join(filter.dir, filepath.FromSlash(name))
}

func readPacket(reader io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header[:])
	if length == 0 || length > maxPacketLength {
		return nil, errors.New("sftptest: invalid packet length")
	}
	packet := make([]byte, 4+int(length))
	copy(packet, header[:])
	if _, err := io.ReadFull(reader, packet[4:]); err != nil {
		return nil, err
	}
	return packet, nil
}

func readString(data []byte) (string, []byte, bool) {
	if len(data) < 4 {
		return "", nil, false
	}
	length := binary.BigEndian.Uint32(data)
	if uint64(length) > uint64(len(data)-4) {
		return "", nil, false
	}
	return string(data[4 : 4+length]), data[4+length:], true
}

func statusPacket(id, code uint32, message string) []byte {
	body := []byte{packetStatus}
	body = binary.BigEndian.AppendUint32(body, id)
	body = binary.BigEndian.AppendUint32(body, code)
	body = binary.BigEndian.AppendUint32(body, uint32(len(message)))
	body = append(body, message...)
	body = binary.BigEndian.AppendUint32(body, 0) // empty language tag
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(body))), body...)
}
