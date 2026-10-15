package sftp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

const (
	testStaleAge = time.Minute
	testMaxPoll  = 100 * time.Millisecond
	testLockName = "root/b-00/sftp.lock"
)

// memoryLockFS is an in-memory lockFS with POSIX file semantics: a handle
// keeps writing to its own file after the name was removed or recreated.
type memoryLockFS struct {
	mu      sync.Mutex
	files   map[string]*memoryFile
	creates int
}

type memoryFile struct {
	fs      *memoryLockFS
	data    []byte
	stalled bool // writes are silently dropped, as from a stalled holder
}

func newMemoryLockFS() *memoryLockFS { return &memoryLockFS{files: make(map[string]*memoryFile)} }

func (fs *memoryLockFS) create(name string) (lockHandle, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.creates++
	if _, ok := fs.files[name]; ok {
		return nil, errLockHeld
	}
	file := &memoryFile{fs: fs}
	fs.files[name] = file
	return file, nil
}

func (fs *memoryLockFS) read(name string) ([]byte, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	file, ok := fs.files[name]
	if !ok {
		return nil, os.ErrNotExist
	}
	return append([]byte{}, file.data...), nil
}

func (fs *memoryLockFS) remove(name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if _, ok := fs.files[name]; !ok {
		return os.ErrNotExist
	}
	delete(fs.files, name)
	return nil
}

func (fs *memoryLockFS) put(name string, data []byte) *memoryFile {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	file := &memoryFile{fs: fs, data: data}
	fs.files[name] = file
	return file
}

func (fs *memoryLockFS) owner(name string) string {
	data, err := fs.read(name)
	if err != nil {
		return ""
	}
	return lockOwner(data)
}

func (file *memoryFile) WriteAt(data []byte, offset int64) (int, error) {
	file.fs.mu.Lock()
	defer file.fs.mu.Unlock()
	if file.stalled {
		return len(data), nil
	}
	if end := int(offset) + len(data); end > len(file.data) {
		file.data = append(file.data, make([]byte, end-len(file.data))...)
	}
	copy(file.data[offset:], data)
	return len(data), nil
}

func (file *memoryFile) Close() error { return nil }

func (file *memoryFile) stall() {
	file.fs.mu.Lock()
	defer file.fs.mu.Unlock()
	file.stalled = true
}

// lockAsync acquires the lock in a goroutine and reports the release
// function (or error) on the returned channel.
func lockAsync(ctx context.Context, locker *bucketLocker) <-chan lockResult {
	result := make(chan lockResult, 1)
	go func() {
		release, err := locker.lock(ctx, testLockName)
		result <- lockResult{release: release, err: err}
	}()
	return result
}

type lockResult struct {
	release func()
	err     error
}

func TestLockContentionAcrossProcesses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs := newMemoryLockFS()
		first := newBucketLocker(fs, testStaleAge, testMaxPoll)
		second := newBucketLocker(fs, testStaleAge, testMaxPoll)
		release, err := first.lock(t.Context(), testLockName)
		if err != nil {
			t.Fatal(err)
		}
		waiting := lockAsync(t.Context(), second)
		// A live holder heartbeats, so it is never broken as stale, however
		// long it holds the lock.
		time.Sleep(10 * testStaleAge)
		synctest.Wait()
		select {
		case result := <-waiting:
			t.Fatalf("second locker acquired a held lock: %v", result.err)
		default:
		}
		release()
		start := time.Now()
		result := <-waiting
		if result.err != nil {
			t.Fatal(result.err)
		}
		if waited := time.Since(start); waited > testMaxPoll {
			t.Fatalf("waiter noticed the release after %v, want at most %v", waited, testMaxPoll)
		}
		result.release()
		if _, err := fs.read(testLockName); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("lock file after release: %v", err)
		}
	})
}

func TestStaleLockIsBroken(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs := newMemoryLockFS()
		fs.put(testLockName, lockRecord("deadbeefdeadbeefdeadbeefdeadbeef", 7))
		locker := newBucketLocker(fs, testStaleAge, testMaxPoll)
		start := time.Now()
		release, err := locker.lock(t.Context(), testLockName)
		if err != nil {
			t.Fatal(err)
		}
		waited := time.Since(start)
		if waited < testStaleAge || waited > testStaleAge+2*testMaxPoll {
			t.Fatalf("stale lock broken after %v, want about %v", waited, testStaleAge)
		}
		if owner := fs.owner(testLockName); owner == "deadbeefdeadbeefdeadbeefdeadbeef" || owner == "" {
			t.Fatalf("lock owner after breaking = %q", owner)
		}
		release()
	})
}

func TestEmptyStaleLockIsBroken(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs := newMemoryLockFS()
		fs.put(testLockName, nil) // creator crashed before writing its record
		locker := newBucketLocker(fs, testStaleAge, testMaxPoll)
		release, err := locker.lock(t.Context(), testLockName)
		if err != nil {
			t.Fatal(err)
		}
		release()
	})
}

func TestStalledHolderLosesLockAndKeepsNewOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs := newMemoryLockFS()
		first := newBucketLocker(fs, testStaleAge, testMaxPoll)
		second := newBucketLocker(fs, testStaleAge, testMaxPoll)
		release, err := first.lock(t.Context(), testLockName)
		if err != nil {
			t.Fatal(err)
		}
		fs.mu.Lock()
		held := fs.files[testLockName]
		fs.mu.Unlock()
		held.stall() // heartbeats no longer reach the server
		result := <-lockAsync(t.Context(), second)
		if result.err != nil {
			t.Fatal(result.err)
		}
		newOwner := fs.owner(testLockName)
		release() // must not remove the new owner's lock
		if owner := fs.owner(testLockName); owner != newOwner || owner == "" {
			t.Fatalf("lock owner after stale holder released = %q, want %q", owner, newOwner)
		}
		result.release()
	})
}

func TestDisabledStaleBreakingWaitsForContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs := newMemoryLockFS()
		fs.put(testLockName, lockRecord("deadbeefdeadbeefdeadbeefdeadbeef", 0))
		locker := newBucketLocker(fs, -1, testMaxPoll)
		ctx, cancel := context.WithTimeout(t.Context(), 100*testStaleAge)
		defer cancel()
		if _, err := locker.lock(ctx, testLockName); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("lock error = %v, want DeadlineExceeded", err)
		}
	})
}

func TestLockCancellationReleasesSemaphore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs := newMemoryLockFS()
		locker := newBucketLocker(fs, testStaleAge, testMaxPoll)
		release, err := locker.lock(t.Context(), testLockName)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		waiting := lockAsync(ctx, locker)
		synctest.Wait()
		cancel()
		if result := <-waiting; !errors.Is(result.err, context.Canceled) {
			t.Fatalf("canceled lock error = %v", result.err)
		}
		release()
		release, err = locker.lock(t.Context(), testLockName)
		if err != nil {
			t.Fatal(err)
		}
		release()
	})
}

func TestInProcessLockDoesNotPoll(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const goroutines = 8
		fs := newMemoryLockFS()
		locker := newBucketLocker(fs, testStaleAge, testMaxPoll)
		var (
			wait   sync.WaitGroup
			mu     sync.Mutex
			inside int
			errs   []error
		)
		for index := range goroutines {
			wait.Add(1)
			go func() {
				defer wait.Done()
				release, err := locker.lock(t.Context(), testLockName)
				if err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					return
				}
				mu.Lock()
				inside++
				if inside != 1 {
					errs = append(errs, fmt.Errorf("goroutine %d entered with %d holders", index, inside))
				}
				mu.Unlock()
				time.Sleep(time.Second)
				mu.Lock()
				inside--
				mu.Unlock()
				release()
			}()
		}
		wait.Wait()
		if len(errs) != 0 {
			t.Fatal(errors.Join(errs...))
		}
		if fs.creates != goroutines {
			t.Fatalf("lock file create attempts = %d, want %d (goroutines must queue in process)", fs.creates, goroutines)
		}
	})
}
