package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"
)

var ErrDatabaseMismatch = errors.New("database path does not match this collector")

type ClearResult struct {
	Database       string    `json:"database"`
	ClearedAt      time.Time `json:"cleared_at"`
	DeletedLogs    int64     `json:"deleted_logs"`
	DeletedSpans   int64     `json:"deleted_spans"`
	DeletedMetrics int64     `json:"deleted_metric_points"`
}

// Clear removes committed telemetry in one transaction using the collector's
// existing writer. Exports serialized after this operation can insert new data.
// Keep schema, ownership, and lifetime counters intact.
func (s *Store) Clear(ctx context.Context, expectedPath string) (result ClearResult, err error) {
	if err := s.mu.LockContext(ctx); err != nil {
		return result, err
	}
	defer s.mu.Unlock()
	if s.db == nil || s.closing.Load() {
		return result, errors.New("store is closed")
	}
	if !filepath.IsAbs(expectedPath) {
		return result, ErrDatabaseMismatch
	}
	actual, err := filepath.Abs(s.cfg.Path)
	if err != nil {
		return result, err
	}
	actual, err = filepath.EvalSymlinks(actual)
	if err != nil {
		return result, err
	}
	expected, err := filepath.EvalSymlinks(expectedPath)
	if err != nil || actual != expected {
		return result, ErrDatabaseMismatch
	}
	// Deletion must remain available under ingestion pressure. Do not apply the
	// admission reserve or readiness checks used to accept new telemetry.
	defer func() { s.recordWriteError(err) }()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	counts := []*int64{&result.DeletedLogs, &result.DeletedSpans, &result.DeletedMetrics}
	for i, table := range []string{"otel_logs", "otel_spans", "otel_metric_points"} {
		deleted, err := tx.ExecContext(ctx, "DELETE FROM "+table)
		if err != nil {
			return ClearResult{}, fmt.Errorf("clear %s: %w", table, err)
		}
		*counts[i], err = deleted.RowsAffected()
		if err != nil {
			return ClearResult{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return ClearResult{}, err
	}
	s.deletedLogs.Add(uint64(result.DeletedLogs))
	s.deletedSpans.Add(uint64(result.DeletedSpans))
	s.deletedMetrics.Add(uint64(result.DeletedMetrics))
	result.Database = actual
	result.ClearedAt = time.Now().UTC()
	return result, nil
}
