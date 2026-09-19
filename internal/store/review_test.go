package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCanceledWorkDoesNotWaitForWriter(t *testing.T) {
	s := startTestStore(t)
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, work := range map[string]func(context.Context) error{
		"logs":        func(ctx context.Context) error { return s.InsertLogs(ctx, nil) },
		"spans":       func(ctx context.Context) error { return s.InsertSpans(ctx, nil) },
		"metrics":     func(ctx context.Context) error { return s.InsertMetricPoints(ctx, nil) },
		"maintenance": func(ctx context.Context) error { return s.Maintain(ctx, time.Now()) },
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- work(ctx) }()
			select {
			case err := <-result:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("error=%v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("cancellation trapped behind writer")
			}
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result := make(chan Snapshot, 1)
	go func() { result <- s.Snapshot(ctx) }()
	select {
	case snapshot := <-result:
		if snapshot.SnapshotError == "" || !snapshot.Ready {
			t.Fatalf("snapshot=%+v", snapshot)
		}
	case <-time.After(time.Second):
		t.Fatal("status trapped behind writer")
	}
}

func TestStorageFailureStopsAdmission(t *testing.T) {
	s := startTestStore(t)
	if _, err := s.db.Exec(`PRAGMA query_only=ON`); err != nil {
		t.Fatal(err)
	}
	err := s.InsertLogs(context.Background(), []LogRecord{{Fingerprint: [32]byte{1}, ServiceName: "test", BodyJSON: `{}`, PayloadJSON: `{}`}})
	if err == nil {
		t.Fatal("read-only write succeeded")
	}
	snapshot := s.OperationalSnapshot()
	if snapshot.Ready || snapshot.LastError == "" {
		t.Fatalf("failed writer reported healthy: %+v", snapshot)
	}
	if err := s.InsertSpans(context.Background(), nil); err == nil {
		t.Fatal("unhealthy writer admitted another signal")
	}
	if _, err := s.db.Exec(`PRAGMA query_only=OFF`); err != nil {
		t.Fatal(err)
	}
	if err := s.Maintain(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if snapshot := s.OperationalSnapshot(); !snapshot.Ready || snapshot.LastError != "" {
		t.Fatalf("recovery did not restore readiness: %+v", snapshot)
	}
}

func TestOwnershipRefusesHardLinks(t *testing.T) {
	for _, suffix := range []string{"", "-wal", "-shm", ".lock"} {
		t.Run(suffix, func(t *testing.T) {
			dir := t.TempDir()
			target, path := filepath.Join(dir, "valuable"), filepath.Join(dir, "telemetry.sqlite")
			if err := os.WriteFile(target, []byte("preserve"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Link(target, path+suffix); err != nil {
				t.Fatal(err)
			}
			s := &Store{cfg: Config{Path: path}}
			if err := s.acquireOwnership(); err == nil {
				s.releaseOwnership()
				t.Fatal("hard link accepted")
			}
			data, err := os.ReadFile(target)
			if err != nil || string(data) != "preserve" {
				t.Fatalf("target changed: %q %v", data, err)
			}
		})
	}
}

func TestStartPreservesMixedUntaggedDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mixed.sqlite")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE otel_logs(body TEXT); CREATE TABLE sqlitex_private(value TEXT); INSERT INTO sqlitex_private VALUES('keep')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := &Store{cfg: Config{Path: path, RetentionHours: 48}, stop: make(chan struct{}), done: make(chan struct{})}
	err = s.Start(context.Background(), nil)
	if err == nil {
		s.Shutdown(context.Background())
		t.Fatal("mixed database accepted")
	}
	if !strings.Contains(err.Error(), "not owned") {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil || string(before) != string(after) {
		t.Fatal("foreign data changed")
	}
}

func TestRetentionDrainsMoreThanOneBatch(t *testing.T) {
	s := startTestStore(t)
	if _, err := s.db.Exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<5001)
 INSERT INTO otel_logs(fingerprint,received_at_unix_nano,service_name,body_json,payload_json)
 SELECT randomblob(32),1,'test','{}','{}' FROM n`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Maintain(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if s.deletedLogs.Load() != 5001 {
		t.Fatalf("expired rows left behind: deleted=%d", s.deletedLogs.Load())
	}
}
