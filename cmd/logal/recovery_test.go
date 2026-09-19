package main

import (
	"context"
	"os"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

func assertSignalCounts(t *testing.T, c *collector, logs, spans, metrics int) {
	t.Helper()
	for table, want := range map[string]int{"otel_logs": logs, "otel_spans": spans, "otel_metric_points": metrics} {
		if got := c.count(t, "SELECT COUNT(*) FROM "+table); got != want {
			t.Fatalf("%s: got %d rows, want %d", table, got, want)
		}
	}
}

func TestCollectorCrashRecovery(t *testing.T) {
	c := startCollector(t, "")
	exportSignals(t, c.grpcConnection(t), contractLogs(), contractTraces(), contractMetrics())
	assertSignalCounts(t, c, 1, 1, 2)
	// Confirm this exercises uncheckpointed WAL recovery rather than only reopening
	// an already clean database after a normal shutdown.
	if info, err := os.Stat(c.dbPath + "-wal"); err != nil || info.Size() <= 32 {
		t.Fatalf("expected pending WAL before crash: %v", err)
	}
	c.crash(t)
	successor := startCollector(t, c.dbPath)
	assertSignalCounts(t, successor, 1, 1, 2) // Must survive before any replay.
	connection := successor.grpcConnection(t)
	exportSignals(t, connection, contractLogs(), contractTraces(), contractMetrics())
	assertSignalCounts(t, successor, 1, 1, 2) // Retry identities survive restart too.
	logs, traces, metrics := contractLogs(), contractTraces(), contractMetrics()
	logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().SetStr("after crash")
	traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).SetSpanID(pcommon.SpanID{9})
	for i := range metrics.ResourceMetrics().Len() {
		metrics.ResourceMetrics().At(i).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0).SetIntValue(2)
	}
	exportSignals(t, connection, logs, traces, metrics)
	assertSignalCounts(t, successor, 2, 2, 4)
	successor.stop(t)
	reopened := startCollector(t, c.dbPath)
	assertSignalCounts(t, reopened, 2, 2, 4)
	db := reopened.readDB(t)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var integrity string
	if err := db.QueryRowContext(ctx, `PRAGMA integrity_check`).Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatalf("recovered database integrity=%q error=%v", integrity, err)
	}
}
