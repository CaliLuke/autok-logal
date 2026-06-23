package main

import "testing"

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
