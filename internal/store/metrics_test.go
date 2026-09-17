package store

import (
	"context"
	"testing"
	"time"
)

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
