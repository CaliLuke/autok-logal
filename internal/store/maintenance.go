package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/mattn/go-sqlite3"
)

const (
	activeHardLimit = int64(3 << 30)
	requestReserve  = int64(64 << 20)
	freeDiskFloor   = uint64(5 << 30)
	walNotReady     = int64(256 << 20)
)

// Run immediately after startup and promptly resume bounded catch-up work.
// Each pass releases the writer before waiting, so ingestion gets a turn.
func (s *Store) maintenanceLoop(ctx context.Context) {
	defer close(s.done)
	for {
		maintenanceCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := s.Maintain(maintenanceCtx, time.Now())
		cancel()
		delay := 30 * time.Second
		if errors.Is(err, context.DeadlineExceeded) {
			delay = 250 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return
		case <-s.stop:
			timer.Stop()
			return
		}
	}
}

func (s *Store) Maintain(ctx context.Context, now time.Time) (err error) {
	started := time.Now()
	defer func() {
		outcome := "completed"
		if errors.Is(err, context.DeadlineExceeded) {
			outcome = "budget_exhausted"
		} else if errors.Is(err, context.Canceled) {
			outcome = "canceled"
		} else if err != nil {
			outcome = "failed"
		}
		s.maintenance.Store(&maintenanceResult{outcome: outcome, durationMS: time.Since(started).Milliseconds(), finishedAt: time.Now().UTC()})
	}()

	for {
		more, err := s.maintainRound(ctx, now)
		if err != nil {
			return err
		}
		if !more {
			return nil
		}
	}
}

// Yield the writer between rounds so catch-up cannot monopolize ingestion or
// prevent status snapshots for the entire maintenance budget.
func (s *Store) maintainRound(ctx context.Context, now time.Time) (more bool, err error) {

	if err := s.mu.LockContext(ctx); err != nil {
		return false, err
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
		return false, errors.New("store is closed")
	}
	cutoff := now.Add(-time.Duration(s.cfg.RetentionHours) * time.Hour).UnixNano()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	// Always reclaim prior committed deletions first. Expiration used to drain
	// the entire budget before vacuum/checkpoint, starving physical reclamation.
	if err := incrementalVacuum(ctx, s.db); err != nil {
		return false, err
	}
	if err := checkpoint(ctx, s.db, "PASSIVE"); err != nil {
		return false, err
	}
	databaseBytes, activeBytes, freeBytes, walBytes := diskState(s.cfg.Path)
	pressure := databaseBytes > s.cfg.databaseLimit() || activeBytes+requestReserve >= activeHardLimit || freeBytes < freeDiskFloor+uint64(requestReserve)
	if pressure || walBytes >= walNotReady {
		// A pinned reader must not cause deletion of fresh data to grow the WAL.
		if err := checkpoint(ctx, s.db, "TRUNCATE"); err != nil {
			return false, err
		}
		databaseBytes, activeBytes, freeBytes, _ = diskState(s.cfg.Path)
		pressure = databaseBytes > s.cfg.databaseLimit() || activeBytes+requestReserve >= activeHardLimit || freeBytes < freeDiskFloor+uint64(requestReserve)
		var freePages int
		if err := s.db.QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&freePages); err != nil {
			return false, err
		}
		if pressure && freePages == 0 {
			before := s.deletedLogs.Load() + s.deletedSpans.Load() + s.deletedMetrics.Load()
			if err := s.deletePressureBatch(ctx); err != nil {
				return false, err
			}
			if s.deletedLogs.Load()+s.deletedSpans.Load()+s.deletedMetrics.Load() == before {
				return false, errors.New("disk pressure persists with no telemetry left to evict")
			}
		}
	}
	// One small batch per signal; rotate the first signal across interrupted
	// passes so a busy logs table cannot starve spans or metrics.
	more = pressure
	signals := []struct {
		table   string
		deleted *atomic.Uint64
	}{
		{"otel_logs", &s.deletedLogs}, {"otel_spans", &s.deletedSpans}, {"otel_metric_points", &s.deletedMetrics},
	}
	for range signals {
		signal := signals[s.maintenanceNextSignal]
		s.maintenanceNextSignal = (s.maintenanceNextSignal + 1) % len(signals)
		result, err := s.db.ExecContext(ctx, `DELETE FROM `+signal.table+` WHERE id IN (SELECT id FROM `+signal.table+` WHERE received_at_unix_nano < ? ORDER BY received_at_unix_nano LIMIT 256)`, cutoff)
		if err != nil {
			return false, err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return false, err
		}
		signal.deleted.Add(uint64(n))
		more = more || n > 0
	}
	return more, nil
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
	for i, ids := range [][]int64{logIDs, spanIDs, metricIDs} {
		if len(ids) == 0 {
			continue
		}
		table := []string{"otel_logs", "otel_spans", "otel_metric_points"}[i]
		args := make([]any, len(ids))
		for j, id := range ids {
			args[j] = id
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE id IN (`+strings.TrimRight(strings.Repeat("?,", len(ids)), ",")+`)`, args...); err != nil {
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
	snapshot := Snapshot{MaxDatabaseBytes: s.cfg.databaseLimit(), RetentionHours: s.cfg.RetentionHours, CommittedMetrics: s.committedMetrics.Load(), DeletedMetrics: s.deletedMetrics.Load(), Ready: s.ready.Load(), CommittedLogs: s.committedLogs.Load(), CommittedSpans: s.committedSpans.Load(), DeletedLogs: s.deletedLogs.Load(), DeletedSpans: s.deletedSpans.Load()}
	snapshot.DatabaseBytes, snapshot.ActiveBytes, snapshot.FreeBytes, snapshot.WALBytes = diskState(s.cfg.Path)
	if result := s.maintenance.Load(); result != nil {
		snapshot.MaintenanceOutcome = result.outcome
		snapshot.MaintenanceDurationMS = result.durationMS
		snapshot.MaintenanceFinishedAt = result.finishedAt
	}
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
