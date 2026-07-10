package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func startTestStore(t *testing.T) *Store {
	t.Helper()
	s := &Store{cfg: Config{Path: filepath.Join(t.TempDir(), "otel.debug.sqlite"), RetentionHours: 48}, stop: make(chan struct{}), done: make(chan struct{})}
	if err := s.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return s
}

func TestStartResetsLegacyDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "otel.debug.sqlite")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE otel_logs(body TEXT); INSERT INTO otel_logs VALUES('disposable')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s := &Store{cfg: Config{Path: path, RetentionHours: 48}, stop: make(chan struct{}), done: make(chan struct{})}
	if err := s.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	var legacyColumns, version int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('otel_logs') WHERE name='fingerprint'`).Scan(&legacyColumns); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if legacyColumns != 1 || version != schemaVersion {
		t.Fatalf("fingerprint_columns=%d version=%d", legacyColumns, version)
	}
}

func TestStartResetsCorruptDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "otel.debug.sqlite")
	if err := os.WriteFile(path, []byte("not sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := &Store{cfg: Config{Path: path, RetentionHours: 48}, stop: make(chan struct{}), done: make(chan struct{})}
	if err := s.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	var result string
	if err := s.db.QueryRow(`PRAGMA quick_check`).Scan(&result); err != nil {
		t.Fatal(err)
	}
	if result != "ok" {
		t.Fatalf("quick_check=%q", result)
	}
}

func TestStartRefusesToResetDatabaseHeldByAnotherWriter(t *testing.T) {
	if _, err := exec.LookPath("lsof"); err != nil {
		t.Skip("lsof is required for descriptor safety")
	}
	path := filepath.Join(t.TempDir(), "otel.debug.sqlite")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE legacy(value TEXT)`); err != nil {
		t.Fatal(err)
	}

	s := &Store{cfg: Config{Path: path, RetentionHours: 48}, stop: make(chan struct{}), done: make(chan struct{})}
	if err := s.Start(context.Background(), nil); err == nil {
		t.Fatal("expected reset refusal while legacy database is open")
	}
}

func TestFailedStartCanShutdownWithoutHanging(t *testing.T) {
	s := &Store{cfg: Config{Path: filepath.Join(t.TempDir(), "otel.debug.sqlite"), RetentionHours: 0}, stop: make(chan struct{}), done: make(chan struct{})}
	if err := s.Start(context.Background(), nil); err == nil {
		t.Fatal("expected invalid retention failure")
	}
	done := make(chan error, 1)
	go func() { done <- s.Shutdown(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown hung after failed start")
	}
}

func TestStartRejectsSymlinkedDatabase(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.sqlite")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "otel.debug.sqlite")
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	s := &Store{cfg: Config{Path: path, RetentionHours: 48}, stop: make(chan struct{}), done: make(chan struct{})}
	if err := s.Start(context.Background(), nil); err == nil {
		t.Fatal("expected symlink rejection")
	}
}

func TestStartRefusesForeignSQLiteDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "foreign.sqlite")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA application_id=12345; CREATE TABLE important(value TEXT); INSERT INTO important VALUES('keep')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s := &Store{cfg: Config{Path: path, RetentionHours: 48}, stop: make(chan struct{}), done: make(chan struct{})}
	if err := s.Start(context.Background(), nil); err == nil {
		t.Fatal("expected foreign database rejection")
	}
	check, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	var journalMode string
	if err := check.QueryRow(`PRAGMA journal_mode`).Scan(&journalMode); err != nil || journalMode != "delete" {
		t.Fatalf("foreign journal mode changed: mode=%q err=%v", journalMode, err)
	}
	var value string
	if err := check.QueryRow(`SELECT value FROM important`).Scan(&value); err != nil || value != "keep" {
		t.Fatalf("foreign database was changed: value=%q err=%v", value, err)
	}
	for _, sidecar := range []string{path + "-wal", path + "-shm", path + ".lock"} {
		if _, err := os.Stat(sidecar); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("foreign sidecar retained: %s", sidecar)
		}
	}
}

func TestStartDoesNotTouchForeignWALDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "foreign-wal.sqlite")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`PRAGMA journal_mode=WAL; CREATE TABLE important(value TEXT); INSERT INTO important VALUES('keep')`); err != nil {
		t.Fatal(err)
	}
	before := make(map[string][]byte)
	for _, sidecar := range []string{path + "-wal", path + "-shm"} {
		contents, err := os.ReadFile(sidecar)
		if err != nil {
			t.Fatalf("read foreign sidecar before inspection: %v", err)
		}
		before[sidecar] = contents
	}
	s := &Store{cfg: Config{Path: path, RetentionHours: 48}, stop: make(chan struct{}), done: make(chan struct{})}
	if err := s.Start(context.Background(), nil); err == nil {
		t.Fatal("expected foreign WAL database rejection")
	}
	for sidecar, expected := range before {
		actual, err := os.ReadFile(sidecar)
		if err != nil {
			t.Fatalf("foreign sidecar removed: %s: %v", sidecar, err)
		}
		if !bytes.Equal(actual, expected) {
			t.Fatalf("foreign sidecar changed: %s", sidecar)
		}
	}
	var value string
	if err := db.QueryRow(`SELECT value FROM important`).Scan(&value); err != nil || value != "keep" {
		t.Fatalf("foreign database was changed: value=%q err=%v", value, err)
	}
}

func TestClassifyDatabaseReadsCommittedOwnershipFromWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owned-wal.sqlite")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(fmt.Sprintf(`PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA application_id=%d; CREATE TABLE owned(value TEXT)`, applicationID)); err != nil {
		t.Fatal(err)
	}
	database, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := int(binary.BigEndian.Uint32(database[68:72])); got != 0 {
		t.Fatalf("test requires ownership only in WAL, main application_id=%d", got)
	}
	wal, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}

	owned, immutable, err := classifyDatabaseForInspection(path)
	if err != nil {
		t.Fatal(err)
	}
	if !owned || immutable {
		t.Fatalf("WAL ownership not recognized: owned=%v immutable=%v", owned, immutable)
	}
	actual, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, wal) {
		t.Fatal("ownership inspection changed WAL")
	}
}

func TestStartResetsCurrentMetadataWithDriftedSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "drifted.sqlite")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(fmt.Sprintf(`PRAGMA application_id=%d; PRAGMA user_version=%d; CREATE TABLE otel_logs(body TEXT)`, applicationID, schemaVersion)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s := &Store{cfg: Config{Path: path, RetentionHours: 48}, stop: make(chan struct{}), done: make(chan struct{})}
	if err := s.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	var fingerprintColumns int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('otel_logs') WHERE name='fingerprint'`).Scan(&fingerprintColumns); err != nil {
		t.Fatal(err)
	}
	if fingerprintColumns != 1 {
		t.Fatalf("fingerprint columns=%d", fingerprintColumns)
	}
}

func TestStartResetsSignedSchemaAfterTampering(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tampered.sqlite")
	first := &Store{cfg: Config{Path: path, RetentionHours: 48}, stop: make(chan struct{}), done: make(chan struct{})}
	if err := first.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := first.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE marker(value TEXT)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	second := &Store{cfg: Config{Path: path, RetentionHours: 48}, stop: make(chan struct{}), done: make(chan struct{})}
	if err := second.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	defer second.Shutdown(context.Background())
	var markerTables int
	if err := second.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name='marker'`).Scan(&markerTables); err != nil {
		t.Fatal(err)
	}
	if markerTables != 0 {
		t.Fatalf("tampered schema was retained")
	}
}

func TestStartResetsMetadataTableAfterTampering(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tampered-metadata.sqlite")
	first := &Store{cfg: Config{Path: path, RetentionHours: 48}, stop: make(chan struct{}), done: make(chan struct{})}
	if err := first.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := first.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE logal_metadata ADD COLUMN unexpected TEXT`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	second := &Store{cfg: Config{Path: path, RetentionHours: 48}, stop: make(chan struct{}), done: make(chan struct{})}
	if err := second.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	defer second.Shutdown(context.Background())
	var unexpectedColumns int
	if err := second.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('logal_metadata') WHERE name='unexpected'`).Scan(&unexpectedColumns); err != nil {
		t.Fatal(err)
	}
	if unexpectedColumns != 0 {
		t.Fatal("tampered metadata schema was retained")
	}
}

func TestMaintainDeletesExpiredRows(t *testing.T) {
	s := startTestStore(t)
	now := time.Now()
	record := LogRecord{Fingerprint: [32]byte{1}, ReceivedAt: now.Add(-49 * time.Hour).UnixNano(), ServiceName: "test", BodyJSON: `{"string":"old"}`, PayloadJSON: `{}`}
	if err := s.InsertLogs(context.Background(), []LogRecord{record}); err != nil {
		t.Fatal(err)
	}
	if err := s.Maintain(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM otel_logs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expired rows=%d", count)
	}
}

func TestInsertLogsIsIdempotent(t *testing.T) {
	s := startTestStore(t)
	record := LogRecord{Fingerprint: [32]byte{2}, ReceivedAt: time.Now().UnixNano(), ServiceName: "test", BodyJSON: `{"string":"once"}`, PayloadJSON: `{}`}
	if err := s.InsertLogs(context.Background(), []LogRecord{record, record}); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM otel_logs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("rows=%d", count)
	}
}
