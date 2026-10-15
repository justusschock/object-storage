// Package sftptest runs an in-process SSH server with the SFTP subsystem on a
// 127.0.0.1 loopback listener, for tests of code that uses SFTP clients.
//
// The SFTP subsystem is github.com/pkg/sftp's filesystem server
// (sftp.NewServer with sftp.WithServerWorkingDirectory). It serves the real
// filesystem with POSIX semantics: exclusive create (SSH_FXF_EXCL maps to
// O_EXCL), atomic replacing renames (both SSH_FXP_RENAME and the
// posix-rename@openssh.com extension use rename(2)), and hard links
// (hardlink@openssh.com). Relative paths resolve against Server.Dir. The
// server is not a chroot: absolute paths reach the host filesystem with the
// permissions of the test process, so use it only with trusted test code.
//
// Options.DisableExtensions and Options.StrictRename emulate more limited
// servers so that callers can test their fallbacks.
//
// Both password and public-key authentication are accepted for Server.User.
package sftptest

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	pkgsftp "github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// DefaultUser is the login name used when Options.User is empty.
const DefaultUser = "sftptest"

// Options configures a Server. The zero value serves a new temporary
// directory with a random password and every server extension enabled.
type Options struct {
	// Dir is the served working directory. Empty creates a temporary
	// directory (t.TempDir for NewServer; os.MkdirTemp for Start, removed by
	// Close).
	Dir string
	// User is the accepted login name; empty selects DefaultUser.
	User string
	// Password is the accepted password; empty selects a random password.
	Password string
	// DisableExtensions removes every extension (posix-rename@openssh.com,
	// hardlink@openssh.com, statvfs@openssh.com) from the SFTP version
	// handshake and answers extended requests for them with
	// SSH_FX_OP_UNSUPPORTED, emulating a plain SFTP v3 server.
	DisableExtensions bool
	// StrictRename makes SSH_FXP_RENAME fail with SSH_FX_FAILURE when the
	// target exists, as SFTP v3 specifies and OpenSSH implements, instead of
	// replacing the target.
	StrictRename bool
}

// Server is a running SSH server with the SFTP subsystem.
type Server struct {
	// Addr is the listener address, "127.0.0.1:<port>".
	Addr string
	// Dir is the served working directory.
	Dir string
	// User and Password are the accepted credentials.
	User     string
	Password string
	// HostKey is the server's public host key.
	HostKey ssh.PublicKey
	// ClientSigner is the private key accepted for public-key
	// authentication of User.
	ClientSigner ssh.Signer

	options  Options
	listener net.Listener
	config   *ssh.ServerConfig
	ownsDir  bool

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup
	once   sync.Once
	err    error
}

// NewServer starts a server with default options and stops it when the test
// ends.
func NewServer(tb testing.TB) *Server {
	tb.Helper()
	return NewServerWithOptions(tb, Options{})
}

// NewServerWithOptions starts a server serving options.Dir (default
// tb.TempDir()) and stops it when the test ends.
func NewServerWithOptions(tb testing.TB, options Options) *Server {
	tb.Helper()
	if options.Dir == "" {
		options.Dir = tb.TempDir()
	}
	server, err := Start(options)
	if err != nil {
		tb.Fatalf("sftptest: %v", err)
	}
	tb.Cleanup(func() {
		if err := server.Close(); err != nil {
			tb.Errorf("sftptest: close: %v", err)
		}
	})
	return server
}

// Start starts a server without a testing.TB. The caller must call Close.
func Start(options Options) (*Server, error) {
	server := &Server{options: options, conns: make(map[net.Conn]struct{})}
	if options.Dir == "" {
		dir, err := os.MkdirTemp("", "sftptest-")
		if err != nil {
			return nil, err
		}
		options.Dir, server.ownsDir = dir, true
	}
	fail := func(err error) (*Server, error) {
		if server.ownsDir {
			_ = os.RemoveAll(options.Dir)
		}
		return nil, err
	}
	server.Dir = options.Dir
	server.User = options.User
	if server.User == "" {
		server.User = DefaultUser
	}
	server.Password = options.Password
	if server.Password == "" {
		var secret [16]byte
		if _, err := rand.Read(secret[:]); err != nil {
			return fail(err)
		}
		server.Password = hex.EncodeToString(secret[:])
	}
	hostSigner, err := newSigner()
	if err != nil {
		return fail(err)
	}
	server.HostKey = hostSigner.PublicKey()
	server.ClientSigner, err = newSigner()
	if err != nil {
		return fail(err)
	}
	authorized := server.ClientSigner.PublicKey().Marshal()
	server.config = &ssh.ServerConfig{
		PasswordCallback: func(meta ssh.ConnMetadata, password []byte) (*ssh.Permissions, error) {
			if meta.User() == server.User && subtle.ConstantTimeCompare(password, []byte(server.Password)) == 1 {
				return nil, nil
			}
			return nil, errors.New("sftptest: password rejected")
		},
		PublicKeyCallback: func(meta ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			if meta.User() == server.User && bytes.Equal(key.Marshal(), authorized) {
				return nil, nil
			}
			return nil, errors.New("sftptest: public key rejected")
		},
	}
	server.config.AddHostKey(hostSigner)
	server.listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fail(err)
	}
	server.Addr = server.listener.Addr().String()
	server.options = options
	server.wg.Add(1)
	go server.accept()
	return server, nil
}

// ClientConfig returns a client configuration that authenticates User with
// Password and pins HostKey.
func (server *Server) ClientConfig() *ssh.ClientConfig {
	return server.clientConfig(ssh.Password(server.Password))
}

// PublicKeyClientConfig returns a client configuration that authenticates
// User with ClientSigner and pins HostKey.
func (server *Server) PublicKeyClientConfig() *ssh.ClientConfig {
	return server.clientConfig(ssh.PublicKeys(server.ClientSigner))
}

func (server *Server) clientConfig(auth ssh.AuthMethod) *ssh.ClientConfig {
	return &ssh.ClientConfig{
		User:            server.User,
		Auth:            []ssh.AuthMethod{auth},
		HostKeyCallback: ssh.FixedHostKey(server.HostKey),
		Timeout:         10 * time.Second,
	}
}

// Dial opens an SSH connection with config and starts an SFTP client on it.
// Closing the returned client also closes the SSH connection.
func (server *Server) Dial(config *ssh.ClientConfig, options ...pkgsftp.ClientOption) (*pkgsftp.Client, error) {
	connection, err := ssh.Dial("tcp", server.Addr, config)
	if err != nil {
		return nil, err
	}
	client, err := pkgsftp.NewClient(connection, options...)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	go func() {
		_ = client.Wait()
		_ = connection.Close()
	}()
	return client, nil
}

// Client dials the server with password authentication and closes the
// client when the test ends.
func (server *Server) Client(tb testing.TB, options ...pkgsftp.ClientOption) *pkgsftp.Client {
	tb.Helper()
	client, err := server.Dial(server.ClientConfig(), options...)
	if err != nil {
		tb.Fatalf("sftptest: dial: %v", err)
	}
	tb.Cleanup(func() { _ = client.Close() })
	return client
}

// Close stops the listener, closes every open connection, waits for the
// connection handlers, and removes a directory created by Start. It is safe
// to call more than once.
func (server *Server) Close() error {
	server.once.Do(func() {
		server.mu.Lock()
		server.closed = true
		err := server.listener.Close()
		for connection := range server.conns {
			_ = connection.Close()
		}
		server.mu.Unlock()
		server.wg.Wait()
		if server.ownsDir {
			err = errors.Join(err, os.RemoveAll(server.Dir))
		}
		server.err = err
	})
	return server.err
}

func (server *Server) accept() {
	defer server.wg.Done()
	for {
		connection, err := server.listener.Accept()
		if err != nil {
			return
		}
		server.mu.Lock()
		if server.closed {
			server.mu.Unlock()
			_ = connection.Close()
			return
		}
		server.conns[connection] = struct{}{}
		server.wg.Add(1)
		server.mu.Unlock()
		go server.handle(connection)
	}
}

func (server *Server) handle(connection net.Conn) {
	defer server.wg.Done()
	defer func() {
		server.mu.Lock()
		delete(server.conns, connection)
		server.mu.Unlock()
		_ = connection.Close()
	}()
	serverConn, channels, requests, err := ssh.NewServerConn(connection, server.config)
	if err != nil {
		return
	}
	defer func() { _ = serverConn.Close() }()
	var sessions sync.WaitGroup
	sessions.Add(1)
	go func() {
		defer sessions.Done()
		ssh.DiscardRequests(requests)
	}()
	for newChannel := range channels {
		if newChannel.ChannelType() != "session" {
			_ = newChannel.Reject(ssh.UnknownChannelType, "only session channels are supported")
			continue
		}
		channel, channelRequests, err := newChannel.Accept()
		if err != nil {
			continue
		}
		sessions.Add(1)
		go func() {
			defer sessions.Done()
			server.session(channel, channelRequests)
		}()
	}
	sessions.Wait()
}

// session serves one session channel: it accepts exactly one "sftp"
// subsystem request and rejects everything else.
func (server *Server) session(channel ssh.Channel, requests <-chan *ssh.Request) {
	var handled sync.WaitGroup
	defer handled.Wait() // the request loop ends once the channel is closed
	defer func() { _ = channel.Close() }()
	start := make(chan bool, 1)
	handled.Add(1)
	go func() {
		defer handled.Done()
		started := false
		for request := range requests {
			accept := !started && request.Type == "subsystem" && subsystemName(request.Payload) == "sftp"
			if request.WantReply {
				_ = request.Reply(accept, nil)
			}
			if accept {
				started = true
				start <- true
			}
		}
		if !started {
			start <- false
		}
	}()
	if !<-start {
		return
	}
	var stream io.ReadWriteCloser = channel
	if server.options.DisableExtensions || server.options.StrictRename {
		stream = newFilter(channel, server.Dir, server.options)
	}
	sftpServer, err := pkgsftp.NewServer(stream, pkgsftp.WithServerWorkingDirectory(server.Dir))
	if err != nil {
		return
	}
	_ = sftpServer.Serve()
	_ = sftpServer.Close()
}

func subsystemName(payload []byte) string {
	if len(payload) < 4 {
		return ""
	}
	length := binary.BigEndian.Uint32(payload)
	if uint64(length) != uint64(len(payload)-4) {
		return ""
	}
	return string(payload[4:])
}

func newSigner() (ssh.Signer, error) {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("sftptest: generate key: %w", err)
	}
	return ssh.NewSignerFromKey(key)
}
