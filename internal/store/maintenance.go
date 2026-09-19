package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/mattn/go-sqlite3"
)

const (
	mainHighWater   = int64(2 << 30)
	activeHardLimit = int64(3 << 30)
	requestReserve  = int64(64 << 20)
	freeDiskFloor   = uint64(5 << 30)
	walNotReady     = int64(256 << 20)
)

func (s *Store) maintenanceLoop(ctx context.Context) {
	defer close(s.done)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			maintenanceCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_ = s.Maintain(maintenanceCtx, time.Now())
			cancel()
			if ctx.Err() != nil {
				return
			}
		case <-s.stop:
			return
		}
	}
}

func (s *Store) Maintain(ctx context.Context, now time.Time) (err error) {
	if err := s.mu.LockContext(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	// Publish recovery before releasing the writer. Otherwise a subsequent write
	// failure can be overwritten by the previous maintenance result.
	defer func() {
		if s.closing.Load() {
			return
		}
		if err != nil {
			// Cleanup is time-bounded work. Exhausting its budget does not mean
			// the writer failed; capacity checks still govern every admission.
			// Preserve any earlier storage failure until maintenance succeeds.
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				s.setOperationalError(err)
			}
		} else {
			s.lastError.Store(nil)
			s.ready.Store(true)
		}
	}()
	if s.db == nil || s.closing.Load() {
		return errors.New("store is closed")
	}
	cutoff := now.Add(-time.Duration(s.cfg.RetentionHours) * time.Hour).UnixNano()
	// Drain backlogs in fair rounds; the maintenance context bounds total work.
	for {
		more := false
		for _, signal := range []struct {
			table   string
			deleted *atomic.Uint64
		}{{"otel_logs", &s.deletedLogs}, {"otel_spans", &s.deletedSpans}, {"otel_metric_points", &s.deletedMetrics}} {
			result, err := s.db.ExecContext(ctx, `DELETE FROM `+signal.table+` WHERE id IN (SELECT id FROM `+signal.table+` WHERE received_at_unix_nano < ? ORDER BY received_at_unix_nano LIMIT 5000)`, cutoff)
			if err != nil {
				return err
			}
			n, err := result.RowsAffected()
			if err != nil {
				return err
			}
			signal.deleted.Add(uint64(n))
			more = more || n == 5000
		}
		if !more {
			break
		}
	}
	// Reclaim existing free pages before deciding whether to evict fresh data.
	if err := incrementalVacuum(ctx, s.db); err != nil {
		return err
	}
	if err := checkpoint(ctx, s.db, "PASSIVE"); err != nil {
		return err
	}
	for pressureIterations := 0; pressureIterations < 200; pressureIterations++ {
		databaseBytes, activeBytes, freeBytes, walBytes := diskState(s.cfg.Path)
		pressure := databaseBytes >= mainHighWater || activeBytes+requestReserve >= activeHardLimit || freeBytes < freeDiskFloor+uint64(requestReserve)
		if !pressure && walBytes < walNotReady {
			return nil
		}
		// A pinned reader must not cause repeated deletion while truncation fails.
		if err := checkpoint(ctx, s.db, "TRUNCATE"); err != nil {
			return err
		}
		databaseBytes, activeBytes, freeBytes, _ = diskState(s.cfg.Path)
		if databaseBytes < mainHighWater && activeBytes+requestReserve < activeHardLimit && freeBytes >= freeDiskFloor+uint64(requestReserve) {
			return nil
		}
		var freePages int
		if err := s.db.QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&freePages); err != nil {
			return err
		}
		if freePages == 0 {
			before := s.deletedLogs.Load() + s.deletedSpans.Load() + s.deletedMetrics.Load()
			if err := s.deletePressureBatch(ctx); err != nil {
				return err
			}
			if s.deletedLogs.Load()+s.deletedSpans.Load()+s.deletedMetrics.Load() == before {
				return errors.New("disk pressure persists with no telemetry left to evict")
			}
		}
		if err := incrementalVacuum(ctx, s.db); err != nil {
			return err
		}
	}
	return errors.New("disk pressure persists after bounded cleanup")
}

func incrementalVacuum(ctx context.Context, db *sql.DB) error {
	// This pragma yields rows while it works. Exec stops at the first row and
	// can reclaim only one page; exhaust the result to complete the batch.
	rows, err := db.QueryContext(ctx, `PRAGMA incremental_vacuum(4096)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
	}
	return rows.Err()
}

func checkpoint(ctx context.Context, db *sql.DB, mode string) error {
	var busy, pages, checkpointed int
	if err := db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(`+mode+`)`).Scan(&busy, &pages, &checkpointed); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	if busy != 0 {
		return errors.New("checkpoint blocked by an active reader")
	}
	return nil
}

func (s *Store) deletePressureBatch(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `
		SELECT signal, id FROM (
			SELECT 0 AS signal, id, received_at_unix_nano FROM otel_logs
			UNION ALL
			SELECT 1 AS signal, id, received_at_unix_nano FROM otel_spans
			UNION ALL
			SELECT 2 AS signal, id, received_at_unix_nano FROM otel_metric_points
		) ORDER BY received_at_unix_nano, signal, id LIMIT 5000`)
	if err != nil {
		return err
	}
	var logIDs, spanIDs, metricIDs []int64
	for rows.Next() {
		var signal int
		var id int64
		if err := rows.Scan(&signal, &id); err != nil {
			_ = rows.Close()
			return err
		}
		switch signal {
		case 0:
			logIDs = append(logIDs, id)
		case 1:
			spanIDs = append(spanIDs, id)
		case 2:
			metricIDs = append(metricIDs, id)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(logIDs)+len(spanIDs)+len(metricIDs) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, id := range logIDs {
		if _, err := tx.ExecContext(ctx, `DELETE FROM otel_logs WHERE id=?`, id); err != nil {
			return err
		}
	}
	for _, id := range spanIDs {
		if _, err := tx.ExecContext(ctx, `DELETE FROM otel_spans WHERE id=?`, id); err != nil {
			return err
		}
	}
	for _, id := range metricIDs {
		if _, err := tx.ExecContext(ctx, `DELETE FROM otel_metric_points WHERE id=?`, id); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.deletedLogs.Add(uint64(len(logIDs)))
	s.deletedSpans.Add(uint64(len(spanIDs)))
	s.deletedMetrics.Add(uint64(len(metricIDs)))
	return nil
}

func (s *Store) Snapshot(ctx context.Context) Snapshot {
	snapshot := s.OperationalSnapshot()
	if err := s.mu.LockContext(ctx); err != nil {
		snapshot.SnapshotError = err.Error()
		return snapshot
	}
	defer s.mu.Unlock()
	snapshot = s.OperationalSnapshot()
	if s.db != nil {
		for _, query := range []struct {
			table  string
			oldest *int64
		}{
			{"otel_metric_points", &snapshot.OldestMetric},
			{"otel_logs", &snapshot.OldestLog},
			{"otel_spans", &snapshot.OldestSpan},
		} {
			if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MIN(received_at_unix_nano),0) FROM `+query.table).Scan(query.oldest); err != nil {
				snapshot.SnapshotError = err.Error()
				break
			}
		}
	}
	return snapshot
}

// OperationalSnapshot returns the counters and capacity state without waiting
// for the SQLite writer. It is intended for periodic health reporting.
func (s *Store) OperationalSnapshot() Snapshot {
	snapshot := Snapshot{CommittedMetrics: s.committedMetrics.Load(), DeletedMetrics: s.deletedMetrics.Load(), Ready: s.ready.Load(), CommittedLogs: s.committedLogs.Load(), CommittedSpans: s.committedSpans.Load(), DeletedLogs: s.deletedLogs.Load(), DeletedSpans: s.deletedSpans.Load()}
	snapshot.DatabaseBytes, snapshot.ActiveBytes, snapshot.FreeBytes, snapshot.WALBytes = diskState(s.cfg.Path)
	if lastError := s.lastError.Load(); lastError != nil {
		snapshot.LastError = *lastError
	}
	snapshot.Ready = snapshot.Ready && !s.closing.Load() && snapshot.ActiveBytes+requestReserve < activeHardLimit && snapshot.WALBytes < walNotReady && snapshot.FreeBytes >= freeDiskFloor+uint64(requestReserve)

	return snapshot
}

func diskState(path string) (databaseBytes, activeBytes int64, freeBytes uint64, walBytes int64) {
	if info, err := os.Stat(path); err == nil {
		databaseBytes = info.Size()
	}
	if info, err := os.Stat(path + "-wal"); err == nil {
		walBytes = info.Size()
	}
	for _, candidate := range []string{path, path + "-wal", path + "-shm"} {
		if info, err := os.Stat(candidate); err == nil {
			activeBytes += info.Size()
		}
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(filepath.Dir(path), &fs); err == nil {
		freeBytes = uint64(fs.Bavail) * uint64(fs.Bsize)
	}
	return databaseBytes, activeBytes, freeBytes, walBytes
}

func (s *Store) setOperationalError(err error) {
	message := err.Error()
	s.lastError.Store(&message)
	s.ready.Store(false)
}

func (s *Store) admissionErrorLocked() error {
	if s.db == nil || !s.ready.Load() || s.closing.Load() {
		return errors.New("store is not ready")
	}
	_, activeBytes, freeBytes, walBytes := diskState(s.cfg.Path)
	if activeBytes+requestReserve >= activeHardLimit {
		return errors.New("active footprint has no request reserve")
	}
	if freeBytes < freeDiskFloor+uint64(requestReserve) {
		return errors.New("free disk is below ingestion floor")
	}
	if walBytes >= walNotReady {
		return errors.New("WAL is above readiness limit")
	}
	return nil
}

// Invalid records and client cancellations do not mean that SQLite is unhealthy.
// Actual storage failures stop admission until maintenance verifies recovery.
func (s *Store) recordWriteError(err error) {
	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) {
		switch sqliteErr.Code {
		case sqlite3.ErrIoErr, sqlite3.ErrFull, sqlite3.ErrCorrupt, sqlite3.ErrNotADB, sqlite3.ErrReadonly, sqlite3.ErrCantOpen:
			s.setOperationalError(err)
		}
	}
}
