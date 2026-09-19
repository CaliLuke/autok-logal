package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
)

func TestDatabaseFullRollsBackAndRecovers(t *testing.T) {
	s := startTestStore(t)
	var pageCount, originalLimit int
	if err := s.db.QueryRow(`PRAGMA page_count`).Scan(&pageCount); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`PRAGMA max_page_count`).Scan(&originalLimit); err != nil {
		t.Fatal(err)
	}
	// SQLite enforces a real allocation failure without exhausting the host disk.
	if _, err := s.db.Exec(fmt.Sprintf(`PRAGMA max_page_count=%d`, pageCount)); err != nil {
		t.Fatal(err)
	}
	records := []LogRecord{
		{Fingerprint: [32]byte{1}, ReceivedAt: time.Now().UnixNano(), ServiceName: "capacity", BodyJSON: `{}`, PayloadJSON: `{}`},
		{Fingerprint: [32]byte{2}, ReceivedAt: time.Now().UnixNano(), ServiceName: "capacity", BodyJSON: `{}`, PayloadJSON: `{"body":"` + strings.Repeat("x", 1<<20) + `"}`},
	}
	err := s.InsertLogs(context.Background(), records)
	var sqliteErr sqlite3.Error
	if !errors.As(err, &sqliteErr) || sqliteErr.Code != sqlite3.ErrFull {
		t.Fatalf("expected SQLITE_FULL: %v", err)
	}
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM otel_logs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 || s.committedLogs.Load() != 0 {
		t.Fatalf("full transaction partially committed: rows=%d", count)
	}
	if snapshot := s.OperationalSnapshot(); snapshot.Ready || snapshot.LastError == "" {
		t.Fatalf("full writer reported healthy: %+v", snapshot)
	}
	if _, err := s.db.Exec(fmt.Sprintf(`PRAGMA max_page_count=%d`, originalLimit)); err != nil {
		t.Fatal(err)
	}
	if err := s.Maintain(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertLogs(context.Background(), records); err != nil {
		t.Fatalf("retry after capacity recovery: %v", err)
	}
	if got := s.committedLogs.Load(); got != 2 {
		t.Fatalf("committed after recovery=%d", got)
	}
}
