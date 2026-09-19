package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
)

func seedClearStore(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	if err := s.InsertLogs(ctx, []LogRecord{{Fingerprint: [32]byte{1}, ReceivedAt: time.Now().UnixNano(), ServiceName: "test", BodyJSON: `{}`, PayloadJSON: `{}`}}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertSpans(ctx, []SpanRecord{{Fingerprint: [32]byte{2}, ReceivedAt: time.Now().UnixNano(), TraceID: make([]byte, 16), SpanID: make([]byte, 8), ServiceName: "test", Name: "span", PayloadJSON: `{}`}}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertMetricPoints(ctx, []MetricPointRecord{metricRecord(3, time.Now().UnixNano(), "test")}); err != nil {
		t.Fatal(err)
	}
}

func assertClearCounts(t *testing.T, s *Store, want int) {
	t.Helper()
	for _, table := range []string{"otel_logs", "otel_spans", "otel_metric_points"} {
		var count int
		if err := s.db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != want {
			t.Fatalf("%s count=%d want=%d err=%v", table, count, want, err)
		}
	}
}

func TestClearPreservesWriterAndAllowsNextRun(t *testing.T) {
	s := startTestStore(t)
	seedClearStore(t, s)
	original, err := os.Stat(s.cfg.Path)
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.Clear(context.Background(), s.cfg.Path)
	if err != nil || result.DeletedLogs != 1 || result.DeletedSpans != 1 || result.DeletedMetrics != 1 || result.ClearedAt.IsZero() {
		t.Fatalf("%+v err=%v", result, err)
	}
	assertClearCounts(t, s, 0)
	if !s.ready.Load() || s.lockFile == nil {
		t.Fatal("clear lost readiness or writer ownership")
	}
	snapshot := s.OperationalSnapshot()
	if snapshot.DeletedLogs != 1 || snapshot.DeletedSpans != 1 || snapshot.DeletedMetrics != 1 || snapshot.CommittedLogs != 1 || snapshot.CommittedSpans != 1 || snapshot.CommittedMetrics != 1 {
		t.Fatalf("lifetime counters: %+v", snapshot)
	}
	// Identical telemetry can be imported again because deduplication keys were
	// deleted. The collector keeps the same file and schema.
	seedClearStore(t, s)
	assertClearCounts(t, s, 1)
	current, err := os.Stat(s.cfg.Path)
	if err != nil || !os.SameFile(original, current) {
		t.Fatalf("clear replaced the database: %v", err)
	}
	var version int
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != schemaVersion {
		t.Fatalf("clear changed schema: version=%d err=%v", version, err)
	}
	if _, err := s.Clear(context.Background(), s.cfg.Path); err != nil {
		t.Fatal(err)
	}
	empty, err := s.Clear(context.Background(), s.cfg.Path)
	if err != nil || empty.DeletedLogs+empty.DeletedSpans+empty.DeletedMetrics != 0 {
		t.Fatalf("empty clear: %+v %v", empty, err)
	}
}

func TestClearRollsBackAllSignalsOnFailure(t *testing.T) {
	s := startTestStore(t)
	seedClearStore(t, s)
	if _, err := s.db.Exec(`CREATE TRIGGER fail_clear BEFORE DELETE ON otel_metric_points BEGIN SELECT RAISE(ABORT, 'test deletion failure'); END`); err != nil {
		t.Fatal(err)
	}
	result, err := s.Clear(context.Background(), s.cfg.Path)
	if err == nil || result.DeletedLogs+result.DeletedSpans+result.DeletedMetrics != 0 {
		t.Fatalf("partial clear reported: %+v %v", result, err)
	}
	assertClearCounts(t, s, 1)
	if snapshot := s.OperationalSnapshot(); snapshot.DeletedLogs+snapshot.DeletedSpans+snapshot.DeletedMetrics != 0 {
		t.Fatal("rollback changed deletion counters", snapshot)
	}
}

func TestClearCancellationRollsBackEarlierSignals(t *testing.T) {
	s := startTestStore(t)
	seedClearStore(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection, err := s.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	err = connection.Raw(func(raw any) error {
		return raw.(*sqlite3.SQLiteConn).RegisterFunc("cancel_clear_test", func() int {
			cancel()
			return 0
		}, false)
	})
	_ = connection.Close()
	if err != nil {
		t.Fatal(err)
	}
	// Cancel after logs and spans were deleted, while the last signal is still
	// inside the transaction. No timing sleeps or large fixtures are needed.
	if _, err := s.db.Exec(`CREATE TEMP TRIGGER cancel_clear BEFORE DELETE ON otel_metric_points BEGIN SELECT cancel_clear_test(); END`); err != nil {
		t.Fatal(err)
	}
	result, err := s.Clear(ctx, s.cfg.Path)
	if err == nil || ctx.Err() == nil || result.DeletedLogs+result.DeletedSpans+result.DeletedMetrics != 0 {
		t.Fatalf("canceled clear result=%+v err=%v context=%v", result, err, ctx.Err())
	}
	assertClearCounts(t, s, 1)
	if snapshot := s.OperationalSnapshot(); snapshot.DeletedLogs+snapshot.DeletedSpans+snapshot.DeletedMetrics != 0 {
		t.Fatal("cancellation changed deletion counters", snapshot)
	}
}

func TestClearRejectsWrongDatabaseAndCanceledLock(t *testing.T) {
	s := startTestStore(t)
	seedClearStore(t, s)
	for _, path := range []string{"", "relative.sqlite", filepath.Join(t.TempDir(), "other.sqlite")} {
		if _, err := s.Clear(context.Background(), path); !errors.Is(err, ErrDatabaseMismatch) {
			t.Fatalf("path %q: %v", path, err)
		}
	}
	s.mu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, err := s.Clear(ctx, s.cfg.Path)
	cancel()
	s.mu.Unlock()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("clear ignored deadline while waiting for writer: %v", err)
	}
	assertClearCounts(t, s, 1)
	// A pressure-induced not-ready state must not prevent clearing old data.
	s.ready.Store(false)
	if _, err := s.Clear(context.Background(), s.cfg.Path); err != nil {
		t.Fatal(err)
	}
	assertClearCounts(t, s, 0)
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Clear(context.Background(), s.cfg.Path); err == nil {
		t.Fatal("clear accepted after shutdown")
	}
}
