package sftp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	pkgsftp "github.com/pkg/sftp"
)

// errLockHeld reports that an exclusive lock-file create found an existing
// lock file.
var errLockHeld = errors.New("sftp store: lock held")

// lockFS is the remote filesystem surface the bucket lock needs. It is an
// interface so that the lock protocol can be tested with an in-memory
// filesystem under testing/synctest.
type lockFS interface {
	// create exclusively creates name (O_CREATE|O_EXCL) and returns an open
	// handle, or errLockHeld when the file already exists.
	create(name string) (lockHandle, error)
	// read returns the file contents or an error wrapping os.ErrNotExist.
	read(name string) ([]byte, error)
	// remove deletes name.
	remove(name string) error
}

// lockHandle is an open, writable lock file.
type lockHandle interface {
	WriteAt(data []byte, offset int64) (int, error)
	Close() error
}

// bucketLocker serializes conditional writes per bucket: an in-process
// semaphore orders goroutines of one Store without polling, and an exclusive
// lock file orders processes sharing the remote root.
//
// The lock file contains the owner token and a heartbeat counter that the
// owner rewrites every staleAge/4 while it holds the lock. A waiter treats a
// lock as stale when its contents have not changed for staleAge on the
// waiter's own monotonic clock, so stale detection does not depend on
// synchronized clocks between clients or between client and server.
type bucketLocker struct {
	fs       lockFS
	staleAge time.Duration // <= 0 disables stale-lock breaking
	maxPoll  time.Duration

	mu   sync.Mutex
	sems map[string]chan struct{}
}

func newBucketLocker(fs lockFS, staleAge, maxPoll time.Duration) *bucketLocker {
	return &bucketLocker{fs: fs, staleAge: staleAge, maxPoll: maxPoll, sems: make(map[string]chan struct{})}
}

func (locker *bucketLocker) semaphore(name string) chan struct{} {
	locker.mu.Lock()
	defer locker.mu.Unlock()
	sem, ok := locker.sems[name]
	if !ok {
		sem = make(chan struct{}, 1)
		locker.sems[name] = sem
	}
	return sem
}

// lock acquires the lock file name. The returned function releases it.
func (locker *bucketLocker) lock(ctx context.Context, name string) (func(), error) {
	sem := locker.semaphore(name)
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	release, err := locker.lockFile(ctx, name)
	if err != nil {
		<-sem
		return nil, err
	}
	return func() {
		release()
		<-sem
	}, nil
}

func (locker *bucketLocker) lockFile(ctx context.Context, name string) (func(), error) {
	token, err := randomHex()
	if err != nil {
		return nil, err
	}
	delay := time.Millisecond
	var (
		observed   []byte
		observedAt time.Time
		seen       bool
	)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		handle, err := locker.fs.create(name)
		if err == nil {
			return locker.hold(name, token, handle)
		}
		if !errors.Is(err, errLockHeld) {
			return nil, fmt.Errorf("sftp store: create lock %s: %w", name, err)
		}
		if locker.staleAge > 0 {
			contents, err := locker.fs.read(name)
			switch {
			case errors.Is(err, os.ErrNotExist):
				seen = false
				continue // released between create and read
			case err != nil:
				return nil, fmt.Errorf("sftp store: read lock %s: %w", name, err)
			case !seen || !bytes.Equal(contents, observed):
				observed, observedAt, seen = contents, time.Now(), true
			case time.Since(observedAt) >= locker.staleAge:
				locker.breakStale(name, observed)
				seen = false
				continue
			}
		}
		if err := sleep(ctx, jitter(delay)); err != nil {
			return nil, err
		}
		delay = min(2*delay, locker.maxPoll)
	}
}

// breakStale removes a lock whose contents are still exactly the stale
// contents. The read-compare-remove sequence is not atomic: an owner that
// beats between the read and the remove loses its lock, which is why stale
// breaking is best-effort.
func (locker *bucketLocker) breakStale(name string, stale []byte) {
	contents, err := locker.fs.read(name)
	if err == nil && bytes.Equal(contents, stale) {
		_ = locker.fs.remove(name)
	}
}

// hold writes the owner record, starts the heartbeat, and returns the
// release function.
func (locker *bucketLocker) hold(name, token string, handle lockHandle) (func(), error) {
	if _, err := handle.WriteAt(lockRecord(token, 0), 0); err != nil {
		_ = handle.Close()
		_ = locker.fs.remove(name)
		return nil, fmt.Errorf("sftp store: write lock %s: %w", name, err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		if locker.staleAge <= 0 {
			<-stop
			return
		}
		ticker := time.NewTicker(max(locker.staleAge/4, time.Millisecond))
		defer ticker.Stop()
		for beat := uint64(1); ; beat++ {
			select {
			case <-stop:
				return
			case <-ticker.C:
				_, _ = handle.WriteAt(lockRecord(token, beat), 0)
			}
		}
	}()
	return func() {
		close(stop)
		<-done
		_ = handle.Close()
		// Remove the lock only while it is still ours: if it was broken as
		// stale, another owner may hold a new lock file under the same name.
		contents, err := locker.fs.read(name)
		if err == nil && lockOwner(contents) == token {
			_ = locker.fs.remove(name)
		}
	}, nil
}

// lockRecord is a fixed-length record, so heartbeats overwrite it in place.
func lockRecord(token string, beat uint64) []byte {
	return fmt.Appendf(nil, "owner=%s beat=%020d\n", token, beat)
}

func lockOwner(contents []byte) string {
	owner, _, _ := strings.Cut(strings.TrimPrefix(string(contents), "owner="), " ")
	return owner
}

func sleep(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func jitter(delay time.Duration) time.Duration {
	if delay <= 1 {
		return delay
	}
	return delay/2 + rand.N(delay/2+1)
}

// sftpLockFS implements lockFS on an SFTP client. Exclusive creation uses
// SSH_FXF_CREAT|SSH_FXF_EXCL, which servers implement with open(O_EXCL).
type sftpLockFS struct {
	client *pkgsftp.Client
}

func (fs sftpLockFS) create(name string) (lockHandle, error) {
	var err error
	for range 8 {
		var file *pkgsftp.File
		file, err = fs.client.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL)
		if err == nil {
			return file, nil
		}
		// SFTP v3 has no "already exists" status, so classify the failure.
		_, statErr := fs.client.Lstat(name)
		switch {
		case statErr == nil:
			return nil, errLockHeld
		case !errors.Is(statErr, os.ErrNotExist):
			return nil, errors.Join(err, statErr)
		case errors.Is(err, os.ErrNotExist):
			// The bucket directory does not exist yet.
			if err := fs.client.MkdirAll(path.Dir(name)); err != nil {
				return nil, err
			}
		}
		// Otherwise the holder released the lock between the two requests.
	}
	return nil, err
}

func (fs sftpLockFS) read(name string) ([]byte, error) {
	file, err := fs.client.Open(name)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(file, 4096))
	return data, errors.Join(err, file.Close())
}

func (fs sftpLockFS) remove(name string) error { return fs.client.Remove(name) }
