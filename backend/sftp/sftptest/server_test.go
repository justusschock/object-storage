package sftptest

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	pkgsftp "github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

func TestPasswordAuthentication(t *testing.T) {
	server := NewServer(t)
	client := server.Client(t)
	file, err := client.Create("hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(server.Dir, "hello.txt"))
	if err != nil || string(data) != "hello" {
		t.Fatalf("served file = (%q, %v)", data, err)
	}
	working, err := client.Getwd()
	if err != nil || working == "" {
		t.Fatalf("Getwd = (%q, %v)", working, err)
	}
}

func TestPublicKeyAuthentication(t *testing.T) {
	server := NewServer(t)
	client, err := server.Dial(server.PublicKeyClientConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if err := os.WriteFile(filepath.Join(server.Dir, "key.txt"), []byte("by key"), 0o644); err != nil {
		t.Fatal(err)
	}
	file, err := client.Open("key.txt")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(file)
	_ = file.Close()
	if err != nil || string(data) != "by key" {
		t.Fatalf("read = (%q, %v)", data, err)
	}
}

func TestRejectedCredentials(t *testing.T) {
	server := NewServerWithOptions(t, Options{User: "alice", Password: "correct"})
	if server.User != "alice" || server.Password != "correct" {
		t.Fatalf("credentials = %q/%q", server.User, server.Password)
	}
	wrongPassword := server.ClientConfig()
	wrongPassword.Auth = []ssh.AuthMethod{ssh.Password("wrong")}
	wrongUser := server.ClientConfig()
	wrongUser.User = "mallory"
	otherKey, err := newSigner()
	if err != nil {
		t.Fatal(err)
	}
	wrongKey := server.PublicKeyClientConfig()
	wrongKey.Auth = []ssh.AuthMethod{ssh.PublicKeys(otherKey)}
	wrongHost := server.ClientConfig()
	wrongHost.HostKeyCallback = ssh.FixedHostKey(otherKey.PublicKey())
	for name, config := range map[string]*ssh.ClientConfig{"password": wrongPassword, "user": wrongUser, "key": wrongKey, "host key": wrongHost} {
		if client, err := server.Dial(config); err == nil {
			_ = client.Close()
			t.Fatalf("wrong %s was accepted", name)
		}
	}
}

func TestExtensions(t *testing.T) {
	server := NewServer(t)
	client := server.Client(t)
	for _, extension := range []string{"posix-rename@openssh.com", "hardlink@openssh.com"} {
		if _, ok := client.HasExtension(extension); !ok {
			t.Fatalf("default server does not advertise %s", extension)
		}
	}
	writeFiles(t, server.Dir, "a", "b")
	if err := client.PosixRename("a", "b"); err != nil {
		t.Fatal(err)
	}
	if err := client.Link("b", "c"); err != nil {
		t.Fatal(err)
	}
	writeFiles(t, server.Dir, "d")
	if err := client.Rename("d", "c"); err != nil {
		t.Fatalf("default rename over an existing file: %v", err)
	}

	limited := NewServerWithOptions(t, Options{DisableExtensions: true, StrictRename: true})
	limitedClient := limited.Client(t)
	for _, extension := range []string{"posix-rename@openssh.com", "hardlink@openssh.com", "statvfs@openssh.com"} {
		if _, ok := limitedClient.HasExtension(extension); ok {
			t.Fatalf("limited server advertises %s", extension)
		}
	}
	writeFiles(t, limited.Dir, "a", "b")
	var status *pkgsftp.StatusError
	if err := limitedClient.PosixRename("a", "b"); !errors.As(err, &status) || status.Code != statusOpUnsupported {
		t.Fatalf("PosixRename on limited server error = %v, want SSH_FX_OP_UNSUPPORTED", err)
	}
	if err := limitedClient.Link("a", "c"); !errors.As(err, &status) || status.Code != statusOpUnsupported {
		t.Fatalf("Link on limited server error = %v, want SSH_FX_OP_UNSUPPORTED", err)
	}
	if err := limitedClient.Rename("a", "b"); !errors.As(err, &status) || status.Code != statusFailure {
		t.Fatalf("Rename over an existing file error = %v, want SSH_FX_FAILURE", err)
	}
	if err := limitedClient.Rename("a", "c"); err != nil {
		t.Fatalf("Rename to a new name: %v", err)
	}
	// Large transfers pass through the packet filter intact.
	payload := make([]byte, 3<<20+17)
	for index := range payload {
		payload[index] = byte(index * 31)
	}
	file, err := limitedClient.Create("large")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	file, err = limitedClient.Open("large")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(file)
	_ = file.Close()
	if err != nil || string(data) != string(payload) {
		t.Fatalf("large round trip: %d bytes, %v", len(data), err)
	}
}

func TestStartAndClose(t *testing.T) {
	server, err := Start(Options{})
	if err != nil {
		t.Fatal(err)
	}
	client, err := server.Dial(server.ClientConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Mkdir("directory"); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := os.Stat(server.Dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary directory after Close: %v", err)
	}
	if _, err := client.Getwd(); err == nil {
		t.Fatal("client still works after server Close")
	}
	_ = client.Close()
}

func writeFiles(t *testing.T, dir string, names ...string) {
	t.Helper()
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
