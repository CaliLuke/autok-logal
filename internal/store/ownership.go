package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

// Reset removes the disposable database while holding the same lock as Start.
// Callers must obtain explicit confirmation for this destructive operation.
func Reset(path string) error {
	if path == "" {
		return errors.New("database path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	s := &Store{cfg: Config{Path: path}}
	if err := s.acquireOwnership(); err != nil {
		return err
	}
	defer s.releaseOwnership()
	return removeDatabase(path)
}

func (s *Store) acquireOwnership() error {
	for _, candidate := range []string{s.cfg.Path, s.cfg.Path + "-wal", s.cfg.Path + "-shm", s.cfg.Path + ".lock"} {
		info, err := os.Lstat(candidate)
		if err == nil && info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("refuse symlinked telemetry file %s", candidate)
		}
		if err == nil && !info.Mode().IsRegular() {
			return fmt.Errorf("refuse non-regular telemetry file %s", candidate)
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	lockFile, err := os.OpenFile(s.cfg.Path+".lock", os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lockFile.Close()
		return fmt.Errorf("another Logal process owns %s: %w", s.cfg.Path, err)
	}
	s.lockFile = lockFile
	if err := rejectOpenDescriptors(s.cfg.Path); err != nil {
		s.releaseOwnership()
		return err
	}
	return nil
}

func rejectOpenDescriptors(path string) error {
	for _, candidate := range []string{path, path + "-wal", path + "-shm"} {
		if _, err := os.Stat(candidate); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}
		inspectionCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		output, err := exec.CommandContext(inspectionCtx, "lsof", "-t", "--", candidate).Output()
		cancel()
		if err == nil && len(output) > 0 {
			return fmt.Errorf("refuse telemetry database already open by another process: %s", candidate)
		}
		var exitErr *exec.ExitError
		if err != nil && (!errors.As(err, &exitErr) || exitErr.ExitCode() != 1) {
			return fmt.Errorf("inspect open descriptors for %s: %w", candidate, err)
		}
	}
	return nil
}

func (s *Store) releaseOwnership() {
	if s.lockFile == nil {
		return
	}
	_ = syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
	_ = s.lockFile.Close()
	s.lockFile = nil
	// Keep the inode: unlinking after unlock lets competing owners lock different files.
}

func removeDatabase(path string) error {
	if err := rejectOpenDescriptors(path); err != nil {
		return err
	}
	for _, candidate := range []string{path, path + "-wal", path + "-shm"} {
		if err := os.Remove(candidate); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}
