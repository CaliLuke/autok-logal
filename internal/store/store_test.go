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

func metricRecord(fingerprint byte, receivedAt int64, metricName string) MetricPointRecord {
	value := int64(fingerprint)
	return MetricPointRecord{
		Fingerprint: [32]byte{fingerprint},
		ReceivedAt:  receivedAt,
		ServiceName: "test-service",
		MetricName:  metricName,
		MetricType:  "gauge",
		NumberKind:  "int",
		NumberInt:   &value,
		PayloadJSON: `{}`,
	}
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

func TestInsertMetricPointsIsIdempotentAcrossRetriesAndOverlaps(t *testing.T) {
	s := startTestStore(t)
	now := time.Now().UnixNano()
	first := metricRecord(10, now, "first")
	second := metricRecord(11, now, "second")

	if err := s.InsertMetricPoints(context.Background(), []MetricPointRecord{first, second}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertMetricPoints(context.Background(), []MetricPointRecord{second}); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM otel_metric_points`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("metric rows=%d", count)
	}
	if got := s.Snapshot(context.Background()).CommittedMetrics; got != 2 {
		t.Fatalf("committed metric points=%d", got)
	}
}

func TestInsertMetricPointsKeepsDistinctSameTimePoints(t *testing.T) {
	s := startTestStore(t)
	now := time.Now().UnixNano()
	first := metricRecord(20, now, "same")
	second := metricRecord(21, now, "same")
	first.Time = 123
	second.Time = 123

	if err := s.InsertMetricPoints(context.Background(), []MetricPointRecord{first, second}); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM otel_metric_points WHERE metric_name='same' AND time_unix_nano=123`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("same-time metric rows=%d", count)
	}
}

func TestInsertMetricPointsRollsBackInvalidLaterRecord(t *testing.T) {
	s := startTestStore(t)
	valid := metricRecord(30, time.Now().UnixNano(), "valid")
	invalid := metricRecord(31, time.Now().UnixNano(), "invalid")
	invalid.PayloadJSON = `{`

	if err := s.InsertMetricPoints(context.Background(), []MetricPointRecord{valid, invalid}); err == nil {
		t.Fatal("expected invalid payload error")
	}
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM otel_metric_points`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("atomic request retained %d metric rows", count)
	}
	if got := s.Snapshot(context.Background()).CommittedMetrics; got != 0 {
		t.Fatalf("committed metric points=%d", got)
	}
}

func TestMaintainExpiresMetricsByReceiptTime(t *testing.T) {
	s := startTestStore(t)
	now := time.Now()
	expired := metricRecord(40, now.Add(-49*time.Hour).UnixNano(), "expired")
	expired.StartTime = 0
	expired.Time = 0
	current := metricRecord(41, now.UnixNano(), "current")
	current.StartTime = now.Add(-365 * 24 * time.Hour).UnixNano()
	current.Time = now.Add(-365 * 24 * time.Hour).UnixNano()

	if err := s.InsertMetricPoints(context.Background(), []MetricPointRecord{expired, current}); err != nil {
		t.Fatal(err)
	}
	if err := s.Maintain(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	var names string
	if err := s.db.QueryRow(`SELECT group_concat(metric_name, ',') FROM otel_metric_points`).Scan(&names); err != nil {
		t.Fatal(err)
	}
	if names != "current" {
		t.Fatalf("remaining metrics=%q", names)
	}
	snapshot := s.Snapshot(context.Background())
	if snapshot.DeletedMetrics != 1 || snapshot.OldestMetric != current.ReceivedAt {
		t.Fatalf("metric snapshot after expiration=%+v", snapshot)
	}
}

func TestDeletePressureBatchUsesGlobalReceiptOrder(t *testing.T) {
	s := startTestStore(t)
	if _, err := s.db.Exec(`
		INSERT INTO otel_metric_points
			(fingerprint,received_at_unix_nano,service_name,metric_name,metric_type,payload_json)
		VALUES(CAST(printf('%032d', 0) AS BLOB),1,'test','oldest','gauge','{}');
		WITH RECURSIVE ids(value) AS (SELECT 1 UNION ALL SELECT value+1 FROM ids WHERE value < 4999)
		INSERT INTO otel_logs
			(fingerprint,received_at_unix_nano,service_name,body_json,payload_json)
		SELECT CAST(printf('%032d', value) AS BLOB),value+1,'test','{}','{}' FROM ids;
		INSERT INTO otel_spans
			(fingerprint,received_at_unix_nano,trace_id,span_id,service_name,name,start_time_unix_nano,end_time_unix_nano,payload_json)
		VALUES(CAST(printf('%032d', 5000) AS BLOB),5001,zeroblob(16),zeroblob(8),'test','newest',0,0,'{}');
	`); err != nil {
		t.Fatal(err)
	}

	if err := s.deletePressureBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	var logs, spans, metrics int
	if err := s.db.QueryRow(`SELECT (SELECT COUNT(*) FROM otel_logs),(SELECT COUNT(*) FROM otel_spans),(SELECT COUNT(*) FROM otel_metric_points)`).Scan(&logs, &spans, &metrics); err != nil {
		t.Fatal(err)
	}
	if logs != 0 || spans != 1 || metrics != 0 {
		t.Fatalf("remaining logs=%d spans=%d metrics=%d", logs, spans, metrics)
	}
	snapshot := s.Snapshot(context.Background())
	if snapshot.DeletedLogs != 4999 || snapshot.DeletedSpans != 0 || snapshot.DeletedMetrics != 1 {
		t.Fatalf("pressure deletion counters=%+v", snapshot)
	}
}

func TestStartResetsOwnedV4DatabaseToV5(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v4.sqlite")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(fmt.Sprintf(`PRAGMA application_id=%d; PRAGMA user_version=4; CREATE TABLE otel_logs(marker TEXT); INSERT INTO otel_logs VALUES('v4')`, applicationID)); err != nil {
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
	var version, metricTables int
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='otel_metric_points'`).Scan(&metricTables); err != nil {
		t.Fatal(err)
	}
	if version != 5 || metricTables != 1 {
		t.Fatalf("version=%d metric_tables=%d", version, metricTables)
	}
}

func TestMetricSchemaSignatureCoversTableAndIndexes(t *testing.T) {
	s := startTestStore(t)
	var objects int
	if err := s.db.QueryRow(`
		SELECT COUNT(*) FROM sqlite_master
		WHERE name IN ('otel_metric_points','idx_metric_points_received','idx_metric_points_service_time','idx_metric_points_name_time')
	`).Scan(&objects); err != nil {
		t.Fatal(err)
	}
	if objects != 4 {
		t.Fatalf("metric schema objects=%d", objects)
	}
	stored := ""
	if err := s.db.QueryRow(`SELECT value FROM logal_metadata WHERE key='schema_signature'`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	actual, err := calculateSchemaSignature(s.db)
	if err != nil {
		t.Fatal(err)
	}
	if stored != actual {
		t.Fatalf("stored schema signature does not cover current schema")
	}
}
