package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	_ "github.com/mattn/go-sqlite3"
)

const (
	applicationID = 0x4c4f474c
	schemaVersion = 7
)

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
	// Setting auto_vacuum after WAL initializes the file does not activate it.
	// Rebuild once under our ownership lock so pressure cleanup can reclaim free
	// pages instead of repeatedly deleting fresh telemetry from a bloated file.
	var vacuumMode int
	if err := db.QueryRow(`PRAGMA auto_vacuum`).Scan(&vacuumMode); err != nil {
		_ = db.Close()
		return fmt.Errorf("inspect telemetry compaction mode: %w", err)
	}
	if vacuumMode != 2 {
		if _, err := db.Exec(`PRAGMA auto_vacuum=INCREMENTAL; VACUUM`); err != nil {
			_ = db.Close()
			return fmt.Errorf("enable telemetry compaction: %w", err)
		}
	}
	if err := os.Chmod(s.cfg.Path, 0o600); err != nil {
		_ = db.Close()
		return fmt.Errorf("set database permissions: %w", err)
	}
	s.db = db
	return nil
}

func classifyDatabaseForInspection(path string) (owned bool, immutable bool, err error) {
	file, err := os.Open(path)
	if err != nil {
		return false, false, err
	}
	defer file.Close()
	contents := make([]byte, 100)
	n, err := io.ReadFull(file, contents)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return false, false, err
	}
	contents = contents[:n]
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
	db, err := sql.Open("sqlite3", sqliteURI(tempPath, "_busy_timeout=5000"))
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
	db, err := sql.Open("sqlite3", sqliteURI(path, "mode=ro&_query_only=1&_busy_timeout=5000"+immutableOption))
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

// sqliteURI escapes filenames so '?' and '#' cannot become driver options.
func sqliteURI(path, options string) string {
	absolute, err := filepath.Abs(path)
	if err != nil {
		absolute = path
	}
	return (&url.URL{Scheme: "file", Path: absolute, RawQuery: options}).String()
}

func openDB(path string) (*sql.DB, error) {
	// SQLite's initial mode follows umask; create privately before opening it.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite3", sqliteURI(path, "_busy_timeout=5000&_journal_mode=WAL&_synchronous=FULL"))
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
		"otel_metric_points": {"fingerprint", "received_at_unix_nano", "service_name", "metric_name", "metric_type", "payload_json"},
		"otel_logs":          {"fingerprint", "received_at_unix_nano", "service_name", "severity_text", "body_json", "payload_json"},
		"otel_spans":         {"fingerprint", "received_at_unix_nano", "trace_id", "span_id", "service_name", "name", "payload_json"},
		"logal_metadata":     {"key", "value"},
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
		`CREATE TABLE IF NOT EXISTS otel_metric_points (
			id INTEGER PRIMARY KEY,
			fingerprint BLOB NOT NULL UNIQUE CHECK(length(fingerprint) = 32),
			received_at_unix_nano INTEGER NOT NULL,
			service_name TEXT NOT NULL,
			metric_name TEXT NOT NULL,
			metric_type TEXT NOT NULL CHECK(metric_type IN ('gauge','sum','histogram','exponential_histogram','summary')),
			start_time_unix_nano INTEGER NOT NULL DEFAULT 0,
			time_unix_nano INTEGER NOT NULL DEFAULT 0,
			number_kind TEXT CHECK(number_kind IS NULL OR number_kind IN ('int','double')),
			number_int INTEGER,
			number_double REAL,
			aggregate_count TEXT,
			aggregate_sum REAL,
			aggregate_min REAL,
			aggregate_max REAL,
			payload_json TEXT NOT NULL CHECK(json_valid(payload_json))
		) STRICT`,
		`CREATE INDEX IF NOT EXISTS idx_logs_received ON otel_logs(received_at_unix_nano)`,
		`CREATE INDEX IF NOT EXISTS idx_logs_trace ON otel_logs(trace_id, span_id)`,
		`CREATE INDEX IF NOT EXISTS idx_logs_service_time ON otel_logs(service_name, time_unix_nano)`,
		`CREATE INDEX IF NOT EXISTS idx_logs_request ON otel_logs(request_id, time_unix_nano)`,
		`CREATE INDEX IF NOT EXISTS idx_logs_product ON otel_logs(product_id, time_unix_nano)`,
		`CREATE INDEX IF NOT EXISTS idx_logs_component ON otel_logs(component, time_unix_nano)`,
		`CREATE INDEX IF NOT EXISTS idx_spans_received ON otel_spans(received_at_unix_nano)`,
		`CREATE INDEX IF NOT EXISTS idx_spans_service_start ON otel_spans(service_name, start_time_unix_nano)`,
		`CREATE INDEX IF NOT EXISTS idx_metric_points_received ON otel_metric_points(received_at_unix_nano)`,
		`CREATE INDEX IF NOT EXISTS idx_metric_points_service_time ON otel_metric_points(service_name, time_unix_nano)`,
		`CREATE INDEX IF NOT EXISTS idx_metric_points_name_time ON otel_metric_points(metric_name, time_unix_nano)`,
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
