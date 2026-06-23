package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	_ "github.com/mattn/go-sqlite3"
)

const (
	defaultRetentionHours = 48
)

type pluginContext struct {
	db             *sql.DB
	retentionHours int
	mu             sync.Mutex
}

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, "autok_sqlite", "Auto-K local SQLite log sink")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	dbPath := firstNonEmpty(
		output.FLBPluginConfigKey(plugin, "db_path"),
		os.Getenv("AUTOK_LOGAL_DB_PATH"),
		"otel.debug.sqlite",
	)
	retentionHours := parsePositiveInt(
		firstNonEmpty(output.FLBPluginConfigKey(plugin, "retention_hours"), os.Getenv("AUTOK_LOGAL_RETENTION_HOURS")),
		defaultRetentionHours,
	)

	ctx, err := openContext(dbPath, retentionHours)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[autok_sqlite] init failed: %v\n", err)
		return output.FLB_ERROR
	}

	output.FLBPluginSetContext(plugin, ctx)
	fmt.Fprintf(os.Stderr, "[autok_sqlite] writing logs to %s\n", dbPath)
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctxPtr unsafe.Pointer, data unsafe.Pointer, length C.int, tag *C.char) int {
	ctx, ok := output.FLBPluginGetContext(ctxPtr).(*pluginContext)
	if !ok || ctx == nil || ctx.db == nil {
		return output.FLB_ERROR
	}

	records, err := decodeRecords(data, int(length), C.GoString(tag))
	if err != nil {
		fmt.Fprintf(os.Stderr, "[autok_sqlite] decode failed: %v\n", err)
		return output.FLB_ERROR
	}
	if len(records) == 0 {
		return output.FLB_OK
	}

	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	if err := ctx.insert(records); err != nil {
		fmt.Fprintf(os.Stderr, "[autok_sqlite] insert failed, asking Fluent Bit to retry: %v\n", err)
		return output.FLB_RETRY
	}
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func main() {}

type logRecord struct {
	Timestamp         string
	ObservedTimestamp string
	TraceID           sql.NullString
	SpanID            sql.NullString
	RequestID         sql.NullString
	ProjectID         sql.NullString
	SeverityText      string
	SeverityNumber    int
	Body              string
	Component         sql.NullString
	Op                sql.NullString
	AttributesJSON    string
	ResourceJSON      string
	ScopeName         sql.NullString
	ScopeVersion      sql.NullString
}

func openContext(dbPath string, retentionHours int) (*pluginContext, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000&_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx := &pluginContext{db: db, retentionHours: retentionHours}
	if err := ctx.initSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return ctx, nil
}

func (ctx *pluginContext) initSchema() error {
	statements := []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA synchronous=NORMAL`,
		`PRAGMA busy_timeout=5000`,
		`CREATE TABLE IF NOT EXISTS otel_logs (
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
		)`,
		`CREATE INDEX IF NOT EXISTS idx_otel_logs_timestamp ON otel_logs(timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_otel_logs_severity_text ON otel_logs(severity_text)`,
		`CREATE INDEX IF NOT EXISTS idx_otel_logs_trace_id ON otel_logs(trace_id)`,
		`CREATE INDEX IF NOT EXISTS idx_otel_logs_request_id ON otel_logs(request_id)`,
		`CREATE INDEX IF NOT EXISTS idx_otel_logs_project_id ON otel_logs(project_id)`,
		`CREATE INDEX IF NOT EXISTS idx_otel_logs_component ON otel_logs(component)`,
		`CREATE INDEX IF NOT EXISTS idx_otel_logs_op ON otel_logs(op)`,
		`CREATE INDEX IF NOT EXISTS idx_otel_logs_body ON otel_logs(body)`,
		`DROP VIEW IF EXISTS logs`,
		`CREATE VIEW IF NOT EXISTS logs AS SELECT * FROM otel_logs`,
	}
	for _, statement := range statements {
		if _, err := ctx.db.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}

func (ctx *pluginContext) insert(records []logRecord) error {
	tx, err := ctx.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	stmt, err := tx.Prepare(`
		INSERT INTO otel_logs (
			timestamp, observed_timestamp, trace_id, span_id, request_id, project_id,
			severity_text, severity_number, body, component, op,
			attributes_json, resource_json, scope_name, scope_version
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, record := range records {
		if _, err := stmt.Exec(
			record.Timestamp,
			record.ObservedTimestamp,
			record.TraceID,
			record.SpanID,
			record.RequestID,
			record.ProjectID,
			record.SeverityText,
			record.SeverityNumber,
			record.Body,
			record.Component,
			record.Op,
			record.AttributesJSON,
			record.ResourceJSON,
			record.ScopeName,
			record.ScopeVersion,
		); err != nil {
			return err
		}
	}

	if ctx.retentionHours > 0 {
		if _, err := tx.Exec(`DELETE FROM otel_logs WHERE timestamp < datetime('now', ?)`, fmt.Sprintf("-%d hours", ctx.retentionHours)); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func decodeRecords(data unsafe.Pointer, length int, tag string) ([]logRecord, error) {
	decoder := output.NewDecoder(data, length)
	var records []logRecord
	for {
		ret, ts, rawRecord := output.GetRecord(decoder)
		if ret == -1 {
			break
		}
		if ret != 0 {
			return nil, fmt.Errorf("fluent bit decoder returned %d", ret)
		}

		record := normalizeMap(rawRecord)
		for _, flattened := range expandRecord(record) {
			records = append(records, toLogRecord(ts, tag, flattened))
		}
	}
	return records, nil
}

func expandRecord(record map[string]any) []map[string]any {
	if len(record) == 0 {
		return nil
	}
	if expanded := expandOTLPResourceLogs(record); len(expanded) > 0 {
		return expanded
	}
	for _, key := range []string{"logRecords", "logs", "records"} {
		if entries, ok := record[key].([]any); ok {
			expanded := make([]map[string]any, 0, len(entries))
			for _, entry := range entries {
				if mapped, ok := entry.(map[string]any); ok {
					expanded = append(expanded, mergeRecordContext(record, mapped))
				}
			}
			if len(expanded) > 0 {
				return expanded
			}
		}
	}
	if isOTLPNonLogEnvelope(record) {
		return nil
	}
	if !hasLogBody(record) && hasAnyKey(record, "resource", "scope") {
		return nil
	}
	return []map[string]any{record}
}

func toLogRecord(ts any, tag string, record map[string]any) logRecord {
	attributes := mergeMaps(map[string]any{}, mapValue(record, "attributes"))
	resource := resourceValue(record)
	body := firstNonEmpty(stringValue(record, "body"), stringValue(record, "message"), stringValue(record, "log"), stringValue(record, "event"), stringValue(record, "op"), "log")
	op := firstNonEmpty(stringValue(record, "op"), stringValue(attributes, "event.action"), stringValue(record, "event"), body)
	serviceName := stringValue(resource, "service.name")
	component := firstNonEmpty(stringValue(record, "component"), stringValue(attributes, "app.component"), serviceName, tag)
	severityText := strings.ToUpper(firstNonEmpty(stringValue(record, "severity_text"), stringValue(record, "severityText"), stringValue(record, "level"), "INFO"))
	timestamp := firstNonEmpty(
		timestampValue(record, "timestamp"),
		timestampValue(record, "time"),
		timestampValue(record, "timeUnixNano"),
		fluentTimestamp(ts),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	observedTimestamp := firstNonEmpty(
		timestampValue(record, "observed_timestamp"),
		timestampValue(record, "observedTimeUnixNano"),
		timestamp,
	)

	if _, ok := resource["service.name"]; !ok {
		resource["service.name"] = serviceNameForTag(tag)
	}
	attributes["event.action"] = op
	attributes["app.component"] = component

	return logRecord{
		Timestamp:         timestamp,
		ObservedTimestamp: observedTimestamp,
		TraceID:           nullString(firstNonEmpty(stringValue(record, "trace_id"), stringValue(record, "traceId"), stringValue(attributes, "trace.id"))),
		SpanID:            nullString(firstNonEmpty(stringValue(record, "span_id"), stringValue(record, "spanId"), stringValue(attributes, "span.id"))),
		RequestID:         nullString(firstNonEmpty(stringValue(record, "request_id"), stringValue(record, "requestId"), stringValue(attributes, "request_id"), stringValue(attributes, "requestId"))),
		ProjectID:         nullString(firstNonEmpty(stringValue(record, "project_id"), stringValue(record, "projectId"), stringValue(attributes, "project_id"), stringValue(attributes, "projectId"))),
		SeverityText:      severityText,
		SeverityNumber:    severityNumber(severityText, intValue(record, "severity_number"), intValue(record, "severityNumber")),
		Body:              body,
		Component:         nullString(component),
		Op:                nullString(op),
		AttributesJSON:    mustJSON(mergeMaps(record, map[string]any{"attributes": attributes})),
		ResourceJSON:      mustJSON(resource),
		ScopeName:         nullString(firstNonEmpty(stringValue(record, "scope_name"), stringValue(record, "scopeName"), stringValue(mapValue(record, "scope"), "name"), tag)),
		ScopeVersion:      nullString(firstNonEmpty(stringValue(record, "scope_version"), stringValue(record, "scopeVersion"))),
	}
}

func expandOTLPResourceLogs(record map[string]any) []map[string]any {
	resourceLogs, ok := record["resourceLogs"].([]any)
	if !ok {
		return nil
	}

	var expanded []map[string]any
	for _, resourceLog := range resourceLogs {
		resourceLogMap, ok := resourceLog.(map[string]any)
		if !ok {
			continue
		}
		scopeLogs, _ := resourceLogMap["scopeLogs"].([]any)
		for _, scopeLog := range scopeLogs {
			scopeLogMap, ok := scopeLog.(map[string]any)
			if !ok {
				continue
			}
			logRecords, _ := scopeLogMap["logRecords"].([]any)
			for _, entry := range logRecords {
				entryMap, ok := entry.(map[string]any)
				if !ok {
					continue
				}
				expanded = append(expanded, mergeRecordContext(mergeRecordContext(resourceLogMap, scopeLogMap), entryMap))
			}
		}
	}
	return expanded
}

func mergeRecordContext(parent map[string]any, child map[string]any) map[string]any {
	merged := mergeMaps(parent, child)
	if _, ok := child["resource"]; !ok {
		if resource := mapValue(parent, "resource"); len(resource) > 0 {
			merged["resource"] = resource
		}
	}
	if _, ok := child["scope"]; !ok {
		if scope := mapValue(parent, "scope"); len(scope) > 0 {
			merged["scope"] = scope
		}
	}
	return merged
}

func isOTLPNonLogEnvelope(record map[string]any) bool {
	for _, key := range []string{"resourceSpans", "scopeSpans", "spans", "resourceMetrics", "scopeMetrics", "metrics"} {
		if _, ok := record[key]; ok {
			return !hasLogBody(record)
		}
	}
	return false
}

func hasLogBody(record map[string]any) bool {
	for _, key := range []string{"body", "message", "log", "event", "op"} {
		if stringValue(record, key) != "" {
			return true
		}
	}
	return false
}

func normalizeMap(input map[interface{}]interface{}) map[string]any {
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[fmt.Sprint(normalizeValue(key))] = normalizeValue(value)
	}
	return out
}

func normalizeValue(value any) any {
	switch v := value.(type) {
	case []byte:
		return string(v)
	case map[interface{}]interface{}:
		return normalizeMap(v)
	case []interface{}:
		out := make([]any, len(v))
		for i, entry := range v {
			out[i] = normalizeValue(entry)
		}
		return out
	default:
		return v
	}
}

func mapValue(record map[string]any, key string) map[string]any {
	value, ok := record[key]
	if !ok {
		return map[string]any{}
	}
	if mapped, ok := value.(map[string]any); ok {
		return mapped
	}
	if entries, ok := value.([]any); ok {
		return keyValueList(entries)
	}
	return map[string]any{}
}

func keyValueList(entries []any) map[string]any {
	out := make(map[string]any, len(entries))
	for _, entry := range entries {
		mapped, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		key := stringValue(mapped, "key")
		if key == "" {
			continue
		}
		if value, ok := mapped["value"]; ok {
			out[key] = normalizeOTLPAnyValue(value)
		}
	}
	return out
}

func normalizeOTLPAnyValue(value any) any {
	mapped, ok := value.(map[string]any)
	if !ok {
		return value
	}
	for _, key := range []string{"stringValue", "intValue", "doubleValue", "boolValue", "bytesValue"} {
		if scalar, ok := mapped[key]; ok {
			return scalar
		}
	}
	if nested, ok := mapped["arrayValue"]; ok {
		return nested
	}
	if nested, ok := mapped["kvlistValue"]; ok {
		return nested
	}
	return value
}

func resourceValue(record map[string]any) map[string]any {
	resource := mergeMaps(map[string]any{}, mapValue(record, "resource"))
	if attrs := mapValue(resource, "attributes"); len(attrs) > 0 {
		resource = mergeMaps(resource, attrs)
	}
	return resource
}

func stringValue(record map[string]any, key string) string {
	value, ok := record[key]
	if !ok || value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case map[string]any:
		for _, key := range []string{"stringValue", "intValue", "doubleValue", "boolValue", "bytesValue"} {
			if nested := stringValue(v, key); nested != "" {
				return nested
			}
		}
		if nested := stringValue(v, "value"); nested != "" {
			return nested
		}
		return ""
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	case json.Number:
		return v.String()
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		return fmt.Sprint(v)
	default:
		return ""
	}
}

func hasAnyKey(record map[string]any, keys ...string) bool {
	for _, key := range keys {
		if _, ok := record[key]; ok {
			return true
		}
	}
	return false
}

func timestampValue(record map[string]any, key string) string {
	value, ok := record[key]
	if !ok || value == nil {
		return ""
	}
	if text := stringValue(record, key); text != "" {
		if parsed := unixNanoTimestamp(text); parsed != "" {
			return parsed
		}
		return text
	}
	switch v := value.(type) {
	case int:
		return unixNanoTimestamp(strconv.FormatInt(int64(v), 10))
	case int64:
		return unixNanoTimestamp(strconv.FormatInt(v, 10))
	case uint64:
		return unixNanoTimestamp(strconv.FormatUint(v, 10))
	case float64:
		return unixNanoTimestamp(strconv.FormatInt(int64(v), 10))
	default:
		return ""
	}
}

func unixNanoTimestamp(value string) string {
	value = strings.TrimSpace(value)
	if len(value) < 16 {
		return ""
	}
	nanos, err := strconv.ParseInt(value, 10, 64)
	if err != nil || nanos <= 0 {
		return ""
	}
	return time.Unix(0, nanos).UTC().Format(time.RFC3339Nano)
}

func intValue(record map[string]any, key string) int {
	value, ok := record[key]
	if !ok {
		return 0
	}
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case uint64:
		return int(v)
	case float64:
		return int(v)
	case string:
		parsed, _ := strconv.Atoi(v)
		return parsed
	default:
		return 0
	}
}

func fluentTimestamp(ts any) string {
	switch value := ts.(type) {
	case output.FLBTime:
		return value.Time.UTC().Format(time.RFC3339Nano)
	case uint64:
		return time.Unix(int64(value), 0).UTC().Format(time.RFC3339Nano)
	case int64:
		return time.Unix(value, 0).UTC().Format(time.RFC3339Nano)
	default:
		return ""
	}
}

func severityNumber(severityText string, values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	switch strings.ToUpper(severityText) {
	case "TRACE":
		return 1
	case "DEBUG":
		return 5
	case "WARN", "WARNING":
		return 13
	case "ERROR":
		return 17
	case "FATAL":
		return 21
	default:
		return 9
	}
}

func serviceNameForTag(tag string) string {
	switch tag {
	case "frontend":
		return "auto-k-frontend"
	case "admin":
		return "auto-k-admin"
	case "otel":
		return "auto-k-otel"
	default:
		if tag == "" {
			return "auto-k-local"
		}
		return "auto-k-" + tag
	}
}

func nullString(value string) sql.NullString {
	value = strings.TrimSpace(value)
	return sql.NullString{String: value, Valid: value != ""}
}

func mergeMaps(base map[string]any, overlays ...map[string]any) map[string]any {
	out := make(map[string]any, len(base))
	for key, value := range base {
		out[key] = value
	}
	for _, overlay := range overlays {
		for key, value := range overlay {
			out[key] = value
		}
	}
	return out
}

func mustJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(data)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func parsePositiveInt(value string, fallback int) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}
