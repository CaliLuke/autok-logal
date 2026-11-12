package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestSizeRetentionEvictsOldestFreshDataAndPreservesNewestPayload(t *testing.T) {
	s := startTestStore(t)
	s.cfg.MaxDatabaseBytes = 2 << 20
	now := time.Now()
	payload := `{"value":"` + strings.Repeat("x", 1024) + `"}`
	if _, err := s.db.Exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<6000)
 INSERT INTO otel_logs(fingerprint,received_at_unix_nano,service_name,body_json,payload_json)
 SELECT randomblob(32),?+i,'size-test','{}',? FROM n`, now.UnixNano(), payload); err != nil {
		t.Fatal(err)
	}
	if err := checkpoint(context.Background(), s.db, "TRUNCATE"); err != nil {
		t.Fatal(err)
	}
	before, _, _, _ := diskState(s.cfg.Path)
	if before <= s.cfg.databaseLimit() {
		t.Fatalf("fixture did not exceed limit: %d", before)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.Maintain(ctx, now); err != nil {
		t.Fatal(err)
	}
	after, _, _, _ := diskState(s.cfg.Path)
	var count int
	var oldest int64
	var stored string
	if err := s.db.QueryRow(`SELECT COUNT(*),MIN(received_at_unix_nano) FROM otel_logs`).Scan(&count, &oldest); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT payload_json FROM otel_logs ORDER BY received_at_unix_nano DESC LIMIT 1`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if after > s.cfg.databaseLimit() || count != 1000 || oldest != now.UnixNano()+5001 || stored != payload {
		t.Fatalf("before=%d after=%d rows=%d oldest=%d preserved=%v", before, after, count, oldest, stored == payload)
	}
}

func TestReclaimRunsBeforeInterruptedExpiration(t *testing.T) {
	s := startTestStore(t)
	if _, err := s.db.Exec(`CREATE TABLE scratch(value BLOB);
 INSERT INTO scratch VALUES(zeroblob(1048576)); DELETE FROM scratch;
 INSERT INTO otel_logs(fingerprint,received_at_unix_nano,service_name,body_json,payload_json) VALUES(randomblob(32),1,'expired','{}','{}');
 CREATE TRIGGER interrupt_expiration BEFORE DELETE ON otel_logs BEGIN SELECT RAISE(FAIL,'interrupted expiration'); END;`); err != nil {
		t.Fatal(err)
	}
	if err := checkpoint(context.Background(), s.db, "TRUNCATE"); err != nil {
		t.Fatal(err)
	}
	before, _, _, _ := diskState(s.cfg.Path)
	if err := s.Maintain(context.Background(), time.Now()); err == nil || !strings.Contains(err.Error(), "interrupted expiration") {
		t.Fatalf("maintenance error=%v", err)
	}
	after, _, _, _ := diskState(s.cfg.Path)
	if before-after < 512<<10 {
		t.Fatalf("committed deletions were not reclaimed before expiration: before=%d after=%d", before, after)
	}
}

func TestSizeRetentionReclaimsFreeSpaceBeforeEvictingFreshData(t *testing.T) {
	s := startTestStore(t)
	s.cfg.MaxDatabaseBytes = 1 << 20
	if _, err := s.db.Exec(`CREATE TABLE scratch(value BLOB); INSERT INTO scratch VALUES(zeroblob(2097152)); DELETE FROM scratch;
 INSERT INTO otel_logs(fingerprint,received_at_unix_nano,service_name,body_json,payload_json) VALUES(randomblob(32),?,'fresh','{}','{}')`, time.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
	if err := checkpoint(context.Background(), s.db, "TRUNCATE"); err != nil {
		t.Fatal(err)
	}
	if err := s.Maintain(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM otel_logs`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	size, _, _, _ := diskState(s.cfg.Path)
	if n != 1 || size > s.cfg.databaseLimit() || s.deletedLogs.Load() != 0 {
		t.Fatalf("fresh data evicted instead of reclaiming space: count=%d bytes=%d", n, size)
	}
}

func TestRetentionSizeConfiguration(t *testing.T) {
	for _, limit := range []int64{-1, 1, activeHardLimit} {
		cfg := Config{Path: "test.sqlite", RetentionHours: 48, MaxDatabaseBytes: limit}
		if cfg.Validate() == nil {
			t.Fatalf("accepted invalid size %d", limit)
		}
	}
	cfg := Config{Path: "test.sqlite", RetentionHours: 48}
	if cfg.databaseLimit() != 512<<20 || cfg.Validate() != nil {
		t.Fatal("invalid default retention size")
	}
}
