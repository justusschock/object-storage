//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package fs

import (
	"fmt"
	"os"
	"syscall"
)

// lockFile takes an exclusive flock on path. flock locks belong to open file
// descriptions, so every call opens its own descriptor and excludes other
// goroutines of this process as well as other processes.
func lockFile(path string) (func(), error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("fs store: open lock: %w", err)
	}
	for {
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX)
		if err != syscall.EINTR {
			break
		}
	}
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("fs store: lock: %w", err)
	}
	return func() {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
	}, nil
}
