package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

var Type = component.MustNewType("logal_store")

var ErrSpanConflict = errors.New("span identity conflicts with committed content")

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
	SnapshotError    string `json:"snapshot_error,omitempty"`
	OldestMetric     int64  `json:"oldest_metric_received_unix_nano"`
	CommittedMetrics uint64 `json:"committed_metric_points"`
	DeletedMetrics   uint64 `json:"deleted_metric_points"`
	Ready            bool   `json:"ready"`
	DatabaseBytes    int64  `json:"database_bytes"`
	WALBytes         int64  `json:"wal_bytes"`
	OldestLog        int64  `json:"oldest_log_received_unix_nano"`
	OldestSpan       int64  `json:"oldest_span_received_unix_nano"`
	CommittedLogs    uint64 `json:"committed_logs"`
	CommittedSpans   uint64 `json:"committed_spans"`
	DeletedLogs      uint64 `json:"deleted_logs"`
	DeletedSpans     uint64 `json:"deleted_spans"`
	ActiveBytes      int64  `json:"active_bytes"`
	FreeBytes        uint64 `json:"free_bytes"`
	LastError        string `json:"last_error,omitempty"`
}

type Store struct {
	committedMetrics  atomic.Uint64
	deletedMetrics    atomic.Uint64
	cfg               Config
	db                *sql.DB
	mu                contextMutex
	stop              chan struct{}
	done              chan struct{}
	lockFile          *os.File
	shutdownOnce      sync.Once
	maintenanceRun    atomic.Bool
	maintenanceCancel context.CancelFunc
	closing           atomic.Bool
	ready             atomic.Bool
	lastError         atomic.Pointer[string]
	committedLogs     atomic.Uint64
	committedSpans    atomic.Uint64
	deletedLogs       atomic.Uint64
	deletedSpans      atomic.Uint64
}

func NewFactory() extension.Factory {
	return extension.NewFactory(Type, func() component.Config {
		return &Config{RetentionHours: 48}
	}, func(_ context.Context, _ extension.Settings, cfg component.Config) (extension.Extension, error) {
		return &Store{cfg: *cfg.(*Config), stop: make(chan struct{}), done: make(chan struct{})}, nil
	}, component.StabilityLevelAlpha)
}

func (s *Store) Start(context.Context, component.Host) error {
	if err := s.cfg.Validate(); err != nil {
		return err
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
		s.db = nil
		s.releaseOwnership()
		return fmt.Errorf("probe write path: %w", err)
	}
	maintenanceCtx, cancel := context.WithCancel(context.Background())
	s.maintenanceCancel = cancel
	s.ready.Store(true)
	s.maintenanceRun.Store(true)
	go s.maintenanceLoop(maintenanceCtx)
	return nil
}

func (s *Store) Shutdown(ctx context.Context) error {
	s.closing.Store(true)
	s.ready.Store(false)
	s.shutdownOnce.Do(func() {
		close(s.stop)
		if s.maintenanceCancel != nil {
			s.maintenanceCancel()
		}
	})
	// Cancellation interrupts maintenance SQL. Always finish releasing the writer,
	// even when the caller's shutdown deadline has already expired.
	if s.maintenanceRun.Load() {
		<-s.done
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.releaseOwnership()
	if s.db == nil {
		return nil
	}
	checkpointErr := checkpoint(ctx, s.db, "TRUNCATE")
	closeErr := s.db.Close()
	s.db = nil
	return errors.Join(checkpointErr, closeErr)
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

func (s *Store) InsertLogs(ctx context.Context, records []LogRecord) (err error) {
	if !s.ready.Load() {
		return errors.New("store is not ready")
	}
	if err := s.mu.LockContext(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	defer func() { s.recordWriteError(err) }()
	if err := s.admissionErrorLocked(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO otel_logs
		(fingerprint,received_at_unix_nano,time_unix_nano,service_name,severity_number,severity_text,trace_id,span_id,request_id,product_id,component,op,body_json,payload_json)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(fingerprint) DO NOTHING`)
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

func (s *Store) InsertSpans(ctx context.Context, records []SpanRecord) (err error) {
	if !s.ready.Load() {
		return errors.New("store is not ready")
	}
	if err := s.mu.LockContext(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	defer func() { s.recordWriteError(err) }()
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
				return ErrSpanConflict
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

func Find(host component.Host, configured string) (*Store, error) {
	for id, extension := range host.GetExtensions() {
		if id.String() == configured {
			if found, ok := extension.(*Store); ok {
				return found, nil
			}
		}
	}
	return nil, fmt.Errorf("store extension %q not found", configured)
}

func (cfg *Config) Validate() error {
	if cfg.Path == "" {
		return errors.New("logal store path is required")
	}
	if cfg.RetentionHours <= 0 || cfg.RetentionHours > int((1<<63-1)/int64(time.Hour)) {
		return errors.New("retention_hours must be positive and fit in a time.Duration")
	}
	return nil
}
