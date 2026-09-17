package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
)

func TestShutdownReleasesOwnershipWithCanceledContext(t *testing.T) {
	s := startTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown error=%v", err)
	}
	if s.db != nil || s.lockFile != nil || s.OperationalSnapshot().Ready {
		t.Fatal("shutdown leaked writer or readiness")
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("repeat shutdown: %v", err)
	}
	if err := s.InsertLogs(context.Background(), nil); err == nil {
		t.Fatal("closed store accepted writes")
	}
	successor := &Store{cfg: s.cfg, stop: make(chan struct{}), done: make(chan struct{})}
	if err := successor.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	defer successor.Shutdown(context.Background())
}

func TestOwnershipKeepsOneLockInodeAcrossRestarts(t *testing.T) {
	s := startTestStore(t)
	before, err := os.Stat(s.cfg.Path + ".lock")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(s.cfg.Path + ".lock")
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("lock inode changed")
	}
	if err := s.acquireOwnership(); err != nil {
		t.Fatal(err)
	}
	defer s.releaseOwnership()
	contender := &Store{cfg: s.cfg}
	if err := contender.acquireOwnership(); err == nil {
		contender.releaseOwnership()
		t.Fatal("second owner acquired lock")
	}
}

func TestStartRefusesSymlinkedLock(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	s := &Store{cfg: Config{Path: filepath.Join(dir, "db.sqlite"), RetentionHours: 48}}
	if err := os.Symlink(target, s.cfg.Path+".lock"); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(context.Background(), nil); err == nil {
		t.Fatal("symlinked lock accepted")
	}
}

func TestSQLitePathEscapesURICharacters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "debug?mode=ro# telemetry.sqlite")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE marker(value TEXT); INSERT INTO marker VALUES('keep')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("mode=%v", info.Mode())
	}
	readOnly, err := openInspectionDB(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	if _, err := readOnly.Exec(`INSERT INTO marker VALUES('bad')`); err == nil {
		t.Fatal("inspection database was writable")
	}
}

func TestLogConstraintFailureRollsBackWholeBatch(t *testing.T) {
	s := startTestStore(t)
	records := []LogRecord{
		{Fingerprint: [32]byte{1}, ReceivedAt: time.Now().UnixNano(), ServiceName: "test", BodyJSON: `{}`, PayloadJSON: `{}`},
		{Fingerprint: [32]byte{2}, ReceivedAt: time.Now().UnixNano(), ServiceName: "test", BodyJSON: `invalid`, PayloadJSON: `{}`},
	}
	if err := s.InsertLogs(context.Background(), records); err == nil {
		t.Fatal("invalid body silently dropped")
	}
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM otel_logs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 || s.committedLogs.Load() != 0 {
		t.Fatalf("partial commit: rows=%d", count)
	}
}

func TestIncrementalVacuumReclaimsMoreThanOnePage(t *testing.T) {
	s := startTestStore(t)
	if _, err := s.db.Exec(`CREATE TABLE scratch(value BLOB); INSERT INTO scratch VALUES(zeroblob(1048576)); DELETE FROM scratch`); err != nil {
		t.Fatal(err)
	}
	var before, after int
	if err := s.db.QueryRow(`PRAGMA freelist_count`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := incrementalVacuum(context.Background(), s.db); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`PRAGMA freelist_count`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before < 100 || after != 0 {
		t.Fatalf("free pages before=%d after=%d", before, after)
	}
}

func TestCheckpointReportsPinnedReader(t *testing.T) {
	s := startTestStore(t)
	if _, err := s.db.Exec(`PRAGMA busy_timeout=1`); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertLogs(context.Background(), []LogRecord{{Fingerprint: [32]byte{1}, ReceivedAt: 1, ServiceName: "test", BodyJSON: `{}`, PayloadJSON: `{}`}}); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite3", sqliteURI(s.cfg.Path, "mode=ro"))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	tx, err := reader.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var count int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM otel_logs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertLogs(context.Background(), []LogRecord{{Fingerprint: [32]byte{2}, ReceivedAt: 2, ServiceName: "test", BodyJSON: `{}`, PayloadJSON: `{}`}}); err != nil {
		t.Fatal(err)
	}
	if err := checkpoint(context.Background(), s.db, "TRUNCATE"); err == nil || !strings.Contains(err.Error(), "active reader") {
		t.Fatalf("checkpoint error=%v", err)
	}
}

type testHost struct {
	extensions map[component.ID]component.Component
}

func (h testHost) GetExtensions() map[component.ID]component.Component { return h.extensions }

func TestFindHonorsConfiguredStore(t *testing.T) {
	first, second := &Store{}, &Store{}
	host := testHost{map[component.ID]component.Component{component.NewIDWithName(Type, "first"): first, component.NewIDWithName(Type, "second"): second}}
	found, err := Find(host, "logal_store/second")
	if err != nil || found != second {
		t.Fatalf("found=%p err=%v", found, err)
	}
	if _, err := Find(host, "logal_store/missing"); err == nil {
		t.Fatal("missing store fell back to unrelated writer")
	}
}

func TestLockIsReleasedAfterFailedStart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "foreign.sqlite")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA application_id=123; CREATE TABLE important(value TEXT)`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	s := &Store{cfg: Config{Path: path, RetentionHours: 48}, stop: make(chan struct{}), done: make(chan struct{})}
	if err := s.Start(context.Background(), nil); err == nil {
		t.Fatal("foreign db accepted")
	}
	lock, err := os.OpenFile(path+".lock", os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
}

func TestResetSharesWriterOwnership(t *testing.T) {
	s := startTestStore(t)
	if err := Reset(s.cfg.Path); err == nil {
		t.Fatal("reset removed an active database")
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := Reset(s.cfg.Path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(s.cfg.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("database still exists: %v", err)
	}
	if _, err := os.Stat(s.cfg.Path + ".lock"); err != nil {
		t.Fatalf("stable lock removed: %v", err)
	}
}

func TestOwnershipRejectsOpenSidecarWithoutOpenDatabase(t *testing.T) {
	for _, suffix := range []string{"-wal", "-shm"} {
		t.Run(suffix, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "telemetry.sqlite")
			if err := os.WriteFile(path, nil, 0600); err != nil {
				t.Fatal(err)
			}
			sidecar, err := os.OpenFile(path+suffix, os.O_CREATE|os.O_RDWR, 0600)
			if err != nil {
				t.Fatal(err)
			}
			defer sidecar.Close()
			if err := rejectOpenDescriptors(path); err == nil {
				t.Fatalf("open %s sidecar was not detected", suffix)
			}
		})
	}
}
