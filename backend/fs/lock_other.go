//go:build !(darwin || dragonfly || freebsd || linux || netbsd || openbsd || windows)

package fs

import "sync"

var processLocks sync.Map

// lockFile serializes writers within this process only; this platform has no
// supported advisory file lock.
func lockFile(path string) (func(), error) {
	value, _ := processLocks.LoadOrStore(path, &sync.Mutex{})
	mutex := value.(*sync.Mutex)
	mutex.Lock()
	return mutex.Unlock, nil
}
