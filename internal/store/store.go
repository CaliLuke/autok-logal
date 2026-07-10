package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

const (
	applicationID   = 0x4c4f474c
	schemaVersion   = 4
	mainHighWater   = int64(2 << 30)
	activeHardLimit = int64(3 << 30)
	requestReserve  = int64(64 << 20)
	freeDiskFloor   = uint64(5 << 30)
	walNotReady     = int64(256 << 20)
)

var Type = component.MustNewType("logal_store")

type Config struct {
	Path           string `mapstructure:"path"`
	RetentionHours int    `mapstructure:"retention_hours"`
}

type LogRecord struct {
	Fingerprint    [32]byte
	ReceivedAt     int64
	Time           int64
	ServiceName    string
	SeverityNumber int32
	SeverityText   string
	TraceID        []byte
	SpanID         []byte
	RequestID      string
	ProductID      string
	Component      string
	Op             string
	BodyJSON       string
	PayloadJSON    string
}

type SpanRecord struct {
	Fingerprint  [32]byte
	ReceivedAt   int64
	TraceID      []byte
	SpanID       []byte
	ParentSpanID []byte
	ServiceName  string
	Name         string
	StartTime    int64
	EndTime      int64
	RequestID    string
	ProductID    string
	PayloadJSON  string
}

type Snapshot struct {
	Ready          bool   `json:"ready"`
	DatabaseBytes  int64  `json:"database_bytes"`
	WALBytes       int64  `json:"wal_bytes"`
	OldestLog      int64  `json:"oldest_log_received_unix_nano"`
	OldestSpan     int64  `json:"oldest_span_received_unix_nano"`
	CommittedLogs  uint64 `json:"committed_logs"`
	CommittedSpans uint64 `json:"committed_spans"`
	DeletedLogs    uint64 `json:"deleted_logs"`
	DeletedSpans   uint64 `json:"deleted_spans"`
	ActiveBytes    int64  `json:"active_bytes"`
	FreeBytes      uint64 `json:"free_bytes"`
	LastError      string `json:"last_error,omitempty"`
}

type Store struct {
	cfg            Config
	db             *sql.DB
	mu             sync.Mutex
	stop           chan struct{}
	done           chan struct{}
	lockFile       *os.File
	shutdownOnce   sync.Once
	maintenanceRun atomic.Bool
	ready          atomic.Bool
	lastError      atomic.Pointer[string]
	committedLogs  atomic.Uint64
	committedSpans atomic.Uint64
	deletedLogs    atomic.Uint64
	deletedSpans   atomic.Uint64
}

func NewFactory() extension.Factory {
	return extension.NewFactory(Type, func() component.Config {
		return &Config{RetentionHours: 48}
	}, func(_ context.Context, _ extension.Settings, cfg component.Config) (extension.Extension, error) {
		return &Store{cfg: *cfg.(*Config), stop: make(chan struct{}), done: make(chan struct{})}, nil
	}, component.StabilityLevelAlpha)
}

func (s *Store) Start(context.Context, component.Host) error {
	if s.cfg.Path == "" {
		return errors.New("logal store path is required")
	}
	if s.cfg.RetentionHours <= 0 {
		return errors.New("retention_hours must be positive")
	}
	if err := os.MkdirAll(filepath.Dir(s.cfg.Path), 0o700); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}
	if err := s.acquireOwnership(); err != nil {
		return err
	}
	if err := s.openCurrentSchema(); err != nil {
		s.releaseOwnership()
		return err
	}
	if err := s.probeWritePath(); err != nil {
		_ = s.db.Close()
		s.releaseOwnership()
		return fmt.Errorf("probe write path: %w", err)
	}
	s.ready.Store(true)
	s.maintenanceRun.Store(true)
	go s.maintenanceLoop()
	return nil
}

func (s *Store) Shutdown(ctx context.Context) error {
	s.ready.Store(false)
	s.shutdownOnce.Do(func() { close(s.stop) })
	if s.maintenanceRun.Load() {
		select {
		case <-s.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		s.releaseOwnership()
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	err := s.db.Close()
	s.releaseOwnership()
	return err
}

func (s *Store) acquireOwnership() error {
	for _, candidate := range []string{s.cfg.Path, s.cfg.Path + "-wal", s.cfg.Path + "-shm"} {
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
	if err := rejectOpenDescriptors(s.cfg.Path); err != nil {
		return err
	}
	lockFile, err := os.OpenFile(s.cfg.Path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lockFile.Close()
		return fmt.Errorf("another Logal process owns %s: %w", s.cfg.Path, err)
	}
	s.lockFile = lockFile
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
		output, err := exec.Command("lsof", "-t", "--", candidate).Output()
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
	_ = os.Remove(s.cfg.Path + ".lock")
}

func (s *Store) probeWritePath() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	fingerprint := make([]byte, 32)
	if _, err := tx.Exec(`INSERT INTO otel_logs (fingerprint,received_at_unix_nano,service_name,body_json,payload_json) VALUES(?,?,?,?,?)`, fingerprint, time.Now().UnixNano(), "logal-probe", `{"empty":true}`, `{}`); err != nil {
		return err
	}
	var count int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM otel_logs WHERE fingerprint=?`, fingerprint).Scan(&count); err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("probe row count=%d", count)
	}
	return tx.Rollback()
}

func (s *Store) openCurrentSchema() error {
	if _, statErr := os.Stat(s.cfg.Path); statErr == nil {
		owned, immutable, err := classifyDatabaseForInspection(s.cfg.Path)
		if err != nil {
			return err
		}
		if !owned {
			return fmt.Errorf("refuse to replace SQLite database not owned by Logal: %s", s.cfg.Path)
		}
		inspectionDB, err := openInspectionDB(s.cfg.Path, immutable)
		if err != nil {
			if resetErr := removeDatabase(s.cfg.Path); resetErr != nil {
				return errors.Join(err, resetErr)
			}
		} else {
			current, disposable, inspectErr := inspectSchema(inspectionDB)
			_ = inspectionDB.Close()
			if inspectErr != nil {
				current, disposable = false, true
			}
			if !current {
				if !disposable {
					return fmt.Errorf("refuse to replace SQLite database not owned by Logal: %s", s.cfg.Path)
				}
				if err := removeDatabase(s.cfg.Path); err != nil {
					return fmt.Errorf("reset disposable database: %w", err)
				}
			}
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	db, err := openDB(s.cfg.Path)
	if err != nil {
		return fmt.Errorf("open telemetry database: %w", err)
	}
	if err := createSchema(db); err != nil {
		_ = db.Close()
		return err
	}
	s.db = db
	if err := os.Chmod(s.cfg.Path, 0o600); err != nil {
		_ = db.Close()
		return fmt.Errorf("set database permissions: %w", err)
	}
	return nil
}

func classifyDatabaseForInspection(path string) (owned bool, immutable bool, err error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return false, false, err
	}
	if len(contents) == 0 {
		return true, true, nil
	}
	if len(contents) < 100 || string(contents[:16]) != "SQLite format 3\x00" {
		// The configured Logal path is disposable; unreadable bytes are treated as
		// a corrupt prior database and reset after the SQLite open confirms failure.
		return true, true, nil
	}
	appID := int(binary.BigEndian.Uint32(contents[68:72]))
	if appID == 0 {
		if _, walErr := os.Stat(path + "-wal"); walErr == nil {
			appID, err = inspectWALApplicationID(path)
			if err != nil {
				return false, false, err
			}
		} else if !errors.Is(walErr, os.ErrNotExist) {
			return false, false, walErr
		}
	}
	if appID != 0 && appID != applicationID {
		return false, false, nil
	}
	if appID == 0 {
		for _, sidecar := range []string{path + "-wal", path + "-shm"} {
			if _, statErr := os.Stat(sidecar); statErr == nil {
				return false, false, nil
			} else if !errors.Is(statErr, os.ErrNotExist) {
				return false, false, statErr
			}
		}
		return true, true, nil
	}
	return true, false, nil
}

func inspectWALApplicationID(path string) (int, error) {
	tempDir, err := os.MkdirTemp(filepath.Dir(path), ".logal-inspect-")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(tempDir)
	tempPath := filepath.Join(tempDir, filepath.Base(path))
	for _, suffix := range []string{"", "-wal"} {
		if err := copyInspectionFile(path+suffix, tempPath+suffix); err != nil {
			return 0, err
		}
	}
	db, err := sql.Open("sqlite3", tempPath+"?_busy_timeout=5000")
	if err != nil {
		return 0, err
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	var appID int
	if err := db.QueryRow(`PRAGMA application_id`).Scan(&appID); err != nil {
		return 0, err
	}
	return appID, nil
}

func copyInspectionFile(sourcePath, targetPath string) error {
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	target, err := os.OpenFile(targetPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(target, source); err != nil {
		_ = target.Close()
		return err
	}
	return target.Close()
}

func openInspectionDB(path string, immutable bool) (*sql.DB, error) {
	immutableOption := ""
	if immutable {
		immutableOption = "&immutable=1"
	}
	db, err := sql.Open("sqlite3", path+"?mode=ro&_query_only=1&_busy_timeout=5000"+immutableOption)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func openDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", path+"?_busy_timeout=5000&_journal_mode=WAL&_synchronous=FULL")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func inspectSchema(db *sql.DB) (current bool, disposable bool, err error) {
	var appID, version int
	if err := db.QueryRow(`PRAGMA application_id`).Scan(&appID); err != nil {
		return false, false, err
	}
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return false, false, err
	}
	if appID == 0 && version == 0 {
		var tables, telemetryTables int
		if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&tables); err != nil {
			return false, false, err
		}
		if tables == 0 {
			return true, false, nil
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('otel_logs','otel_spans')`).Scan(&telemetryTables); err != nil {
			return false, false, err
		}
		return false, telemetryTables > 0, nil
	}
	if appID != applicationID {
		return false, false, nil
	}
	if version != schemaVersion {
		return false, true, nil
	}
	structurallyCurrent, err := schemaHasRequiredColumns(db)
	if err != nil {
		return false, true, err
	}
	if !structurallyCurrent {
		return false, true, nil
	}
	var storedSignature string
	if err := db.QueryRow(`SELECT value FROM logal_metadata WHERE key='schema_signature'`).Scan(&storedSignature); err != nil {
		return false, true, nil
	}
	actualSignature, err := calculateSchemaSignature(db)
	if err != nil || storedSignature != actualSignature {
		return false, true, err
	}
	var result string
	if err := db.QueryRow(`PRAGMA quick_check`).Scan(&result); err != nil {
		return false, true, err
	}
	return result == "ok", result != "ok", nil
}

func schemaHasRequiredColumns(db *sql.DB) (bool, error) {
	required := map[string][]string{
		"otel_logs":      {"fingerprint", "received_at_unix_nano", "service_name", "severity_text", "body_json", "payload_json"},
		"otel_spans":     {"fingerprint", "received_at_unix_nano", "trace_id", "span_id", "service_name", "name", "payload_json"},
		"logal_metadata": {"key", "value"},
	}
	for table, columns := range required {
		var count int
		placeholders := strings.TrimRight(strings.Repeat("?,", len(columns)), ",")
		args := make([]any, 0, len(columns)+1)
		args = append(args, table)
		for _, column := range columns {
			args = append(args, column)
		}
		query := fmt.Sprintf(`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name IN (%s)`, placeholders)
		if err := db.QueryRow(query, args...).Scan(&count); err != nil {
			return false, err
		}
		if count != len(columns) {
			return false, nil
		}
	}
	return true, nil
}

func calculateSchemaSignature(db *sql.DB) (string, error) {
	rows, err := db.Query(`SELECT type, name, COALESCE(sql,'') FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name`)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	hash := sha256.New()
	for rows.Next() {
		var objectType, name, sqlText string
		if err := rows.Scan(&objectType, &name, &sqlText); err != nil {
			return "", err
		}
		_, _ = fmt.Fprintf(hash, "%s\x00%s\x00%s\x00", objectType, name, sqlText)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func removeDatabase(path string) error {
	for _, candidate := range []string{path, path + "-wal", path + "-shm"} {
		if _, err := os.Stat(candidate); err == nil {
			command := exec.Command("lsof", "-t", "--", candidate)
			output, lsofErr := command.Output()
			if lsofErr == nil && len(output) > 0 {
				return fmt.Errorf("refuse to reset %s while another process has it open", candidate)
			}
			var exitErr *exec.ExitError
			if lsofErr != nil && (!errors.As(lsofErr, &exitErr) || exitErr.ExitCode() != 1) {
				return fmt.Errorf("inspect open descriptors for %s: %w", candidate, lsofErr)
			}
		}
	}
	for _, candidate := range []string{path, path + "-wal", path + "-shm"} {
		if err := os.Remove(candidate); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func createSchema(db *sql.DB) error {
	statements := []string{
		`PRAGMA auto_vacuum=INCREMENTAL`,
		`PRAGMA foreign_keys=ON`,
		`PRAGMA wal_autocheckpoint=1000`,
		`PRAGMA journal_size_limit=67108864`,
		fmt.Sprintf(`PRAGMA application_id=%d`, applicationID),
		fmt.Sprintf(`PRAGMA user_version=%d`, schemaVersion),
		`CREATE TABLE IF NOT EXISTS logal_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT`,
		`CREATE TABLE IF NOT EXISTS otel_logs (
			id INTEGER PRIMARY KEY,
			fingerprint BLOB NOT NULL UNIQUE CHECK(length(fingerprint)=32),
			received_at_unix_nano INTEGER NOT NULL,
			time_unix_nano INTEGER NOT NULL DEFAULT 0,
			service_name TEXT NOT NULL,
			severity_number INTEGER NOT NULL DEFAULT 0,
			severity_text TEXT NOT NULL DEFAULT '',
			trace_id BLOB, span_id BLOB, request_id TEXT, product_id TEXT,
			component TEXT, op TEXT,
			body_json TEXT NOT NULL CHECK(json_valid(body_json)),
			payload_json TEXT NOT NULL CHECK(json_valid(payload_json))
		) STRICT`,
		`CREATE TABLE IF NOT EXISTS otel_spans (
			id INTEGER PRIMARY KEY,
			fingerprint BLOB NOT NULL CHECK(length(fingerprint)=32),
			received_at_unix_nano INTEGER NOT NULL,
			trace_id BLOB NOT NULL CHECK(length(trace_id)=16),
			span_id BLOB NOT NULL CHECK(length(span_id)=8),
			parent_span_id BLOB,
			service_name TEXT NOT NULL, name TEXT NOT NULL,
			start_time_unix_nano INTEGER NOT NULL,
			end_time_unix_nano INTEGER NOT NULL,
			request_id TEXT, product_id TEXT,
			payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
			UNIQUE(trace_id, span_id)
		) STRICT`,
		`CREATE INDEX IF NOT EXISTS idx_logs_received ON otel_logs(received_at_unix_nano)`,
		`CREATE INDEX IF NOT EXISTS idx_logs_trace ON otel_logs(trace_id, span_id)`,
		`CREATE INDEX IF NOT EXISTS idx_logs_service_time ON otel_logs(service_name, time_unix_nano)`,
		`CREATE INDEX IF NOT EXISTS idx_logs_request ON otel_logs(request_id, time_unix_nano)`,
		`CREATE INDEX IF NOT EXISTS idx_logs_product ON otel_logs(product_id, time_unix_nano)`,
		`CREATE INDEX IF NOT EXISTS idx_logs_component ON otel_logs(component, time_unix_nano)`,
		`CREATE INDEX IF NOT EXISTS idx_spans_received ON otel_spans(received_at_unix_nano)`,
		`CREATE INDEX IF NOT EXISTS idx_spans_trace ON otel_spans(trace_id, span_id)`,
		`CREATE INDEX IF NOT EXISTS idx_spans_service_start ON otel_spans(service_name, start_time_unix_nano)`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			return err
		}
	}
	signature, err := calculateSchemaSignature(db)
	if err != nil {
		return err
	}
	if _, err := db.Exec(`INSERT OR REPLACE INTO logal_metadata(key,value) VALUES('schema_signature',?)`, signature); err != nil {
		return err
	}
	return nil
}

func (s *Store) InsertLogs(ctx context.Context, records []LogRecord) error {
	if !s.ready.Load() {
		return errors.New("store is not ready")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.admissionErrorLocked(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO otel_logs
		(fingerprint,received_at_unix_nano,time_unix_nano,service_name,severity_number,severity_text,trace_id,span_id,request_id,product_id,component,op,body_json,payload_json)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	var inserted uint64
	for _, r := range records {
		result, err := stmt.ExecContext(ctx, r.Fingerprint[:], r.ReceivedAt, r.Time, r.ServiceName, r.SeverityNumber, r.SeverityText, nullableBytes(r.TraceID), nullableBytes(r.SpanID), nullable(r.RequestID), nullable(r.ProductID), nullable(r.Component), nullable(r.Op), r.BodyJSON, r.PayloadJSON)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n > 0 {
			inserted += uint64(n)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.committedLogs.Add(inserted)
	return nil
}

func (s *Store) InsertSpans(ctx context.Context, records []SpanRecord) error {
	if !s.ready.Load() {
		return errors.New("store is not ready")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.admissionErrorLocked(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO otel_spans
		(fingerprint,received_at_unix_nano,trace_id,span_id,parent_span_id,service_name,name,start_time_unix_nano,end_time_unix_nano,request_id,product_id,payload_json)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(trace_id,span_id) DO NOTHING`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	var inserted uint64
	for _, r := range records {
		var existing []byte
		err := tx.QueryRowContext(ctx, `SELECT fingerprint FROM otel_spans WHERE trace_id=? AND span_id=?`, r.TraceID, r.SpanID).Scan(&existing)
		if err == nil {
			if string(existing) != string(r.Fingerprint[:]) {
				return errors.New("span identity conflicts with committed content")
			}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if _, err := stmt.ExecContext(ctx, r.Fingerprint[:], r.ReceivedAt, r.TraceID, r.SpanID, nullableBytes(r.ParentSpanID), r.ServiceName, r.Name, r.StartTime, r.EndTime, nullable(r.RequestID), nullable(r.ProductID), r.PayloadJSON); err != nil {
			return err
		}
		inserted++
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.committedSpans.Add(inserted)
	return nil
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}
func nullableBytes(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func (s *Store) maintenanceLoop() {
	defer close(s.done)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := s.Maintain(context.Background(), time.Now()); err != nil {
				s.setOperationalError(err)
			} else {
				s.lastError.Store(nil)
				s.ready.Store(true)
			}
		case <-s.stop:
			return
		}
	}
}

func (s *Store) Maintain(ctx context.Context, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := now.Add(-time.Duration(s.cfg.RetentionHours) * time.Hour).UnixNano()
	logs, err := s.db.ExecContext(ctx, `DELETE FROM otel_logs WHERE id IN (SELECT id FROM otel_logs WHERE received_at_unix_nano < ? ORDER BY received_at_unix_nano LIMIT 5000)`, cutoff)
	if err != nil {
		return err
	}
	spans, err := s.db.ExecContext(ctx, `DELETE FROM otel_spans WHERE id IN (SELECT id FROM otel_spans WHERE received_at_unix_nano < ? ORDER BY received_at_unix_nano LIMIT 5000)`, cutoff)
	if err != nil {
		return err
	}
	ln, _ := logs.RowsAffected()
	sn, _ := spans.RowsAffected()
	if ln+sn > 0 {
		if _, err := s.db.ExecContext(ctx, `PRAGMA incremental_vacuum(4096)`); err != nil {
			return err
		}
	}
	if _, err := s.db.ExecContext(ctx, `PRAGMA wal_checkpoint(PASSIVE)`); err != nil {
		return err
	}
	for pressureIterations := 0; pressureIterations < 200; pressureIterations++ {
		databaseBytes, activeBytes, freeBytes, _ := diskState(s.cfg.Path)
		pressure := databaseBytes >= mainHighWater || activeBytes+requestReserve >= activeHardLimit || freeBytes < freeDiskFloor+uint64(requestReserve)
		if !pressure {
			break
		}
		if err := s.deletePressureBatch(ctx); err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx, `PRAGMA incremental_vacuum(4096)`); err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
			return err
		}
	}
	s.deletedLogs.Add(uint64(ln))
	s.deletedSpans.Add(uint64(sn))
	return nil
}

func (s *Store) deletePressureBatch(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `
		SELECT signal, id FROM (
			SELECT 0 AS signal, id, received_at_unix_nano FROM otel_logs
			UNION ALL
			SELECT 1 AS signal, id, received_at_unix_nano FROM otel_spans
		) ORDER BY received_at_unix_nano, signal, id LIMIT 5000`)
	if err != nil {
		return err
	}
	var logIDs, spanIDs []int64
	for rows.Next() {
		var signal int
		var id int64
		if err := rows.Scan(&signal, &id); err != nil {
			_ = rows.Close()
			return err
		}
		if signal == 0 {
			logIDs = append(logIDs, id)
		} else {
			spanIDs = append(spanIDs, id)
		}
	}
	if err := rows.Close(); err != nil {
		return err
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
	if err := tx.Commit(); err != nil {
		return err
	}
	s.deletedLogs.Add(uint64(len(logIDs)))
	s.deletedSpans.Add(uint64(len(spanIDs)))
	return nil
}

func (s *Store) Snapshot(ctx context.Context) Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := Snapshot{Ready: s.ready.Load(), CommittedLogs: s.committedLogs.Load(), CommittedSpans: s.committedSpans.Load(), DeletedLogs: s.deletedLogs.Load(), DeletedSpans: s.deletedSpans.Load()}
	snapshot.DatabaseBytes, snapshot.ActiveBytes, snapshot.FreeBytes, snapshot.WALBytes = diskState(s.cfg.Path)
	if lastError := s.lastError.Load(); lastError != nil {
		snapshot.LastError = *lastError
	}
	snapshot.Ready = snapshot.Ready && snapshot.ActiveBytes+requestReserve < activeHardLimit && snapshot.WALBytes < walNotReady && snapshot.FreeBytes >= freeDiskFloor+uint64(requestReserve)
	if s.db != nil {
		_ = s.db.QueryRowContext(ctx, `SELECT COALESCE(MIN(received_at_unix_nano),0) FROM otel_logs`).Scan(&snapshot.OldestLog)
		_ = s.db.QueryRowContext(ctx, `SELECT COALESCE(MIN(received_at_unix_nano),0) FROM otel_spans`).Scan(&snapshot.OldestSpan)
	}
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

func Find(host component.Host, configured string) (*Store, error) {
	for id, extension := range host.GetExtensions() {
		if id.String() == configured || id.Type() == Type {
			if found, ok := extension.(*Store); ok {
				return found, nil
			}
		}
	}
	return nil, fmt.Errorf("store extension %q not found", configured)
}
