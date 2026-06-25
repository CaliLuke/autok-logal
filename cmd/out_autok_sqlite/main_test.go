package main

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

func TestExpandRecordDropsNonLogOTLPEnvelope(t *testing.T) {
	records := expandRecord(map[string]any{
		"resourceSpans": []any{
			map[string]any{"scopeSpans": []any{map[string]any{"spans": []any{map[string]any{"name": "GET /projects"}}}}},
		},
		"resource": map[string]any{
			"attributes": map[string]any{"service.name": "auto-k-auth"},
		},
	})

	if len(records) != 0 {
		t.Fatalf("expected trace envelope to be dropped, got %d records", len(records))
	}
}

func TestExpandRecordDropsEmptyRecord(t *testing.T) {
	records := expandRecord(map[string]any{})
	if len(records) != 0 {
		t.Fatalf("expected empty record to be dropped, got %d records", len(records))
	}
}

func TestExpandRecordPreservesOTLPLogContext(t *testing.T) {
	records := expandRecord(map[string]any{
		"resourceLogs": []any{
			map[string]any{
				"resource": map[string]any{
					"attributes": []any{
						map[string]any{"key": "service.name", "value": map[string]any{"stringValue": "auto-k-auth"}},
					},
				},
				"scopeLogs": []any{
					map[string]any{
						"scope": map[string]any{"name": "auto-k-auth"},
						"logRecords": []any{
							map[string]any{
								"timeUnixNano":   "1782087149341000000",
								"severityText":   "INFO",
								"body":           map[string]any{"stringValue": "OpenTelemetry initialized."},
								"attributes":     []any{map[string]any{"key": "event.action", "value": map[string]any{"stringValue": "telemetry.initialized"}}},
								"traceId":        "trace-1",
								"spanId":         "span-1",
								"severityNumber": 9,
							},
						},
					},
				},
			},
		},
	})

	if len(records) != 1 {
		t.Fatalf("expected one log record, got %d", len(records))
	}

	record := toLogRecord(nil, "otel", records[0])
	if record.Body != "OpenTelemetry initialized." {
		t.Fatalf("body mismatch: %q", record.Body)
	}
	if record.Component.String != "auto-k-auth" {
		t.Fatalf("component mismatch: %q", record.Component.String)
	}
	if record.ResourceJSON == "" || record.ResourceJSON == "{}" {
		t.Fatalf("resource context was not preserved: %q", record.ResourceJSON)
	}
	if record.ScopeName.String != "auto-k-auth" {
		t.Fatalf("scope name mismatch: %q", record.ScopeName.String)
	}
	if record.Timestamp != "2026-06-22T00:12:29.341Z" {
		t.Fatalf("timestamp mismatch: %q", record.Timestamp)
	}
}

func TestTimestampValueConvertsUnixNanoFields(t *testing.T) {
	record := map[string]any{"observedTimeUnixNano": "1782087149341000000"}

	got := timestampValue(record, "observedTimeUnixNano")
	if got != "2026-06-22T00:12:29.341Z" {
		t.Fatalf("timestamp mismatch: %q", got)
	}
}

func TestToLogRecordPromotesProductID(t *testing.T) {
	record := toLogRecord(nil, "frontend", map[string]any{
		"body":       "bulk edit failed",
		"product_id": "product-1",
	})

	if record.ProductID.String != "product-1" {
		t.Fatalf("product id mismatch: %q", record.ProductID.String)
	}
}

func TestToLogRecordPromotesLegacyProjectIDToProductID(t *testing.T) {
	record := toLogRecord(nil, "frontend", map[string]any{
		"body":       "bulk edit failed",
		"project_id": "product-legacy",
	})

	if record.ProductID.String != "product-legacy" {
		t.Fatalf("legacy project id mismatch: %q", record.ProductID.String)
	}
}

func TestToLogRecordPromotesLegacyCamelProjectIDToProductID(t *testing.T) {
	record := toLogRecord(nil, "frontend", map[string]any{
		"body":      "bulk edit failed",
		"projectId": "product-camel-legacy",
	})

	if record.ProductID.String != "product-camel-legacy" {
		t.Fatalf("legacy camel project id mismatch: %q", record.ProductID.String)
	}
}

func TestOpenContextMigratesLegacyProjectIDColumn(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "otel.debug.sqlite")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		CREATE TABLE otel_logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp TEXT NOT NULL,
			observed_timestamp TEXT NOT NULL,
			trace_id TEXT,
			span_id TEXT,
			request_id TEXT,
			project_id TEXT,
			severity_text TEXT NOT NULL,
			severity_number INTEGER NOT NULL,
			body TEXT NOT NULL,
			component TEXT,
			op TEXT,
			attributes_json TEXT,
			resource_json TEXT,
			scope_name TEXT,
			scope_version TEXT
		);
		INSERT INTO otel_logs (
			timestamp, observed_timestamp, project_id, severity_text, severity_number, body
		) VALUES (
			'2026-06-25T01:44:20Z', '2026-06-25T01:44:20Z', 'legacy-product', 'INFO', 9, 'probe'
		);
	`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, err := openContext(dbPath, defaultRetentionHours)
	if err != nil {
		t.Fatal(err)
	}
	defer ctx.db.Close()

	var productID string
	if err := ctx.db.QueryRow(`SELECT product_id FROM otel_logs WHERE body = 'probe'`).Scan(&productID); err != nil {
		t.Fatal(err)
	}
	if productID != "legacy-product" {
		t.Fatalf("product_id was not backfilled: %q", productID)
	}
}

func TestRecordSinkErrorPersistsDiagnosticRow(t *testing.T) {
	ctx, err := openContext(filepath.Join(t.TempDir(), "otel.debug.sqlite"), defaultRetentionHours)
	if err != nil {
		t.Fatal(err)
	}
	defer ctx.db.Close()

	ctx.recordSinkError("insert", errFake("missing product_id"), []logRecord{{Body: "probe", ProductID: nullString("product-1")}})

	var phase string
	var message string
	var recordCount int
	var sampleJSON string
	if err := ctx.db.QueryRow(`SELECT phase, message, record_count, sample_json FROM autok_logal_errors ORDER BY id DESC LIMIT 1`).Scan(&phase, &message, &recordCount, &sampleJSON); err != nil {
		t.Fatal(err)
	}
	if phase != "insert" || !strings.Contains(message, "missing product_id") || recordCount != 1 {
		t.Fatalf("unexpected sink error row: phase=%q message=%q record_count=%d", phase, message, recordCount)
	}
	if !strings.Contains(sampleJSON, "product-1") {
		t.Fatalf("sample json did not include record context: %q", sampleJSON)
	}
}

type errFake string

func (e errFake) Error() string {
	return string(e)
}
