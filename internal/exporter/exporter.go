package exporter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/CaliLuke/autok-logal/internal/store"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	collexporter "go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var Type = component.MustNewType("logal_sqlite")

const maxPersistedRequestBytes = 64 << 20

type Config struct {
	Store string `mapstructure:"store"`
}

type logsExporter struct {
	cfg   Config
	store *store.Store
}
type tracesExporter struct {
	cfg   Config
	store *store.Store
}

func NewFactory() collexporter.Factory {
	return collexporter.NewFactory(Type, func() component.Config { return &Config{Store: "logal_store"} },
		collexporter.WithLogs(func(_ context.Context, _ collexporter.Settings, cfg component.Config) (collexporter.Logs, error) {
			return &logsExporter{cfg: *cfg.(*Config)}, nil
		}, component.StabilityLevelAlpha),
		collexporter.WithTraces(func(_ context.Context, _ collexporter.Settings, cfg component.Config) (collexporter.Traces, error) {
			return &tracesExporter{cfg: *cfg.(*Config)}, nil
		}, component.StabilityLevelAlpha),
		collexporter.WithMetrics(func(_ context.Context, _ collexporter.Settings, cfg component.Config) (collexporter.Metrics, error) {
			return &metricsExporter{cfg: *cfg.(*Config)}, nil
		}, component.StabilityLevelAlpha),
	)
}

func (e *logsExporter) Start(_ context.Context, host component.Host) error {
	var err error
	e.store, err = store.Find(host, e.cfg.Store)
	return err
}
func (e *logsExporter) Shutdown(context.Context) error { return nil }
func (e *logsExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
func (e *tracesExporter) Start(_ context.Context, host component.Host) error {
	var err error
	e.store, err = store.Find(host, e.cfg.Store)
	return err
}
func (e *tracesExporter) Shutdown(context.Context) error { return nil }
func (e *tracesExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
func (e *logsExporter) ConsumeLogs(ctx context.Context, data plog.Logs) error {
	if data.LogRecordCount() > 10000 {
		return status.Error(codes.InvalidArgument, "log request exceeds 10000 records")
	}
	now := time.Now().UnixNano()
	var records []store.LogRecord
	persistedBytes := 0
	resources := data.ResourceLogs()
	for ri := 0; ri < resources.Len(); ri++ {
		resource := resources.At(ri)
		if err := validateMap(resource.Resource().Attributes()); err != nil {
			return status.Error(codes.InvalidArgument, "invalid log resource attributes: "+err.Error())
		}
		serviceName := attributeString(resource.Resource().Attributes(), "service.name")
		if serviceName == "" {
			serviceName = "unknown_service"
		}
		scopes := resource.ScopeLogs()
		for si := 0; si < scopes.Len(); si++ {
			scopeLogs := scopes.At(si)
			if err := validateMap(scopeLogs.Scope().Attributes()); err != nil {
				return status.Error(codes.InvalidArgument, "invalid log scope attributes: "+err.Error())
			}
			logs := scopeLogs.LogRecords()
			for li := 0; li < logs.Len(); li++ {
				if err := ctx.Err(); err != nil {
					return exportError(err)
				}
				record := logs.At(li)
				if uint64(record.Timestamp()) > math.MaxInt64 {
					return status.Error(codes.InvalidArgument, "log timestamp exceeds SQLite integer range")
				}
				if err := validateValue(record.Body(), 0); err != nil {
					return status.Error(codes.InvalidArgument, "invalid log body: "+err.Error())
				}
				if err := validateMap(record.Attributes()); err != nil {
					return status.Error(codes.InvalidArgument, "invalid log attributes: "+err.Error())
				}
				payload, err := marshalSingleLog(resource, scopeLogs, record)
				if err != nil {
					return status.Error(codes.InvalidArgument, err.Error())
				}
				persistedBytes += len(payload)
				if persistedBytes > maxPersistedRequestBytes {
					return status.Error(codes.InvalidArgument, "persisted log payload exceeds 64 MiB")
				}
				fingerprint := sha256.Sum256(payload)
				traceID, spanID := record.TraceID(), record.SpanID()
				attrs := record.Attributes()
				bodyValue := pcommon.NewValueEmpty()
				record.Body().CopyTo(bodyValue)
				redactValue(bodyValue)
				body, err := json.Marshal(taggedValue(bodyValue))
				if err != nil {
					return status.Error(codes.InvalidArgument, "encode log body: "+err.Error())
				}
				persistedBytes += len(body)
				if persistedBytes > maxPersistedRequestBytes {
					return status.Error(codes.InvalidArgument, "persisted log payload exceeds 64 MiB")
				}
				records = append(records, store.LogRecord{Fingerprint: fingerprint, ReceivedAt: now, Time: int64(record.Timestamp()), ServiceName: serviceName, SeverityNumber: int32(record.SeverityNumber()), SeverityText: record.SeverityText(), TraceID: nonZeroTraceID(traceID), SpanID: nonZeroSpanID(spanID), RequestID: attributeString(attrs, "request.id"), ProductID: attributeString(attrs, "autok.product.id"), Component: attributeString(attrs, "app.component"), Op: first(attributeString(attrs, "event.name"), record.EventName()), BodyJSON: string(body), PayloadJSON: string(payload)})
			}
		}
	}
	if err := e.store.InsertLogs(ctx, records); err != nil {
		return exportError(err)
	}
	return nil
}

func (e *tracesExporter) ConsumeTraces(ctx context.Context, data ptrace.Traces) error {
	if data.SpanCount() > 5000 {
		return status.Error(codes.InvalidArgument, "trace request exceeds 5000 spans")
	}
	now := time.Now().UnixNano()
	var records []store.SpanRecord
	persistedBytes := 0
	resources := data.ResourceSpans()
	for ri := 0; ri < resources.Len(); ri++ {
		resource := resources.At(ri)
		if err := validateMap(resource.Resource().Attributes()); err != nil {
			return status.Error(codes.InvalidArgument, "invalid span resource attributes: "+err.Error())
		}
		serviceName := attributeString(resource.Resource().Attributes(), "service.name")
		if serviceName == "" {
			serviceName = "unknown_service"
		}
		scopes := resource.ScopeSpans()
		for si := 0; si < scopes.Len(); si++ {
			scopeSpans := scopes.At(si)
			if err := validateMap(scopeSpans.Scope().Attributes()); err != nil {
				return status.Error(codes.InvalidArgument, "invalid span scope attributes: "+err.Error())
			}
			spans := scopeSpans.Spans()
			for pi := 0; pi < spans.Len(); pi++ {
				if err := ctx.Err(); err != nil {
					return exportError(err)
				}
				span := spans.At(pi)
				if uint64(span.StartTimestamp()) > math.MaxInt64 || uint64(span.EndTimestamp()) > math.MaxInt64 {
					return status.Error(codes.InvalidArgument, "span timestamp exceeds SQLite integer range")
				}
				traceID, spanID := span.TraceID(), span.SpanID()
				if traceID.IsEmpty() || spanID.IsEmpty() {
					return status.Error(codes.InvalidArgument, "span trace_id and span_id are required")
				}
				attrs := span.Attributes()
				if err := validateMap(attrs); err != nil {
					return status.Error(codes.InvalidArgument, "invalid span attributes: "+err.Error())
				}
				for eventIndex := 0; eventIndex < span.Events().Len(); eventIndex++ {
					if err := validateMap(span.Events().At(eventIndex).Attributes()); err != nil {
						return status.Error(codes.InvalidArgument, "invalid span event attributes: "+err.Error())
					}
				}
				for linkIndex := 0; linkIndex < span.Links().Len(); linkIndex++ {
					if err := validateMap(span.Links().At(linkIndex).Attributes()); err != nil {
						return status.Error(codes.InvalidArgument, "invalid span link attributes: "+err.Error())
					}
				}
				payload, err := marshalSingleSpan(resource, scopeSpans, span)
				if err != nil {
					return status.Error(codes.InvalidArgument, err.Error())
				}
				persistedBytes += len(payload)
				if persistedBytes > maxPersistedRequestBytes {
					return status.Error(codes.InvalidArgument, "persisted span payload exceeds 64 MiB")
				}
				fingerprint := sha256.Sum256(payload)
				records = append(records, store.SpanRecord{Fingerprint: fingerprint, ReceivedAt: now, TraceID: traceID[:], SpanID: spanID[:], ParentSpanID: nonZeroSpanID(span.ParentSpanID()), ServiceName: serviceName, Name: span.Name(), StartTime: int64(span.StartTimestamp()), EndTime: int64(span.EndTimestamp()), RequestID: attributeString(attrs, "request.id"), ProductID: attributeString(attrs, "autok.product.id"), PayloadJSON: string(payload)})
			}
		}
	}
	if err := e.store.InsertSpans(ctx, records); err != nil {
		if errors.Is(err, store.ErrSpanConflict) {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		return exportError(err)
	}
	return nil
}

func exportError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	return status.Error(codes.Unavailable, err.Error())
}

func taggedValue(value pcommon.Value) any {
	switch value.Type() {
	case pcommon.ValueTypeEmpty:
		return map[string]any{"empty": true}
	case pcommon.ValueTypeStr:
		return map[string]any{"string": value.Str()}
	case pcommon.ValueTypeBool:
		return map[string]any{"bool": value.Bool()}
	case pcommon.ValueTypeInt:
		return map[string]any{"int": fmt.Sprint(value.Int())}
	case pcommon.ValueTypeDouble:
		number := value.Double()
		switch {
		case math.IsNaN(number):
			return map[string]any{"double": "NaN"}
		case math.IsInf(number, 1):
			return map[string]any{"double": "+Infinity"}
		case math.IsInf(number, -1):
			return map[string]any{"double": "-Infinity"}
		default:
			return map[string]any{"double": number}
		}
	case pcommon.ValueTypeBytes:
		return map[string]any{"bytes": base64.StdEncoding.EncodeToString(value.Bytes().AsRaw())}
	case pcommon.ValueTypeSlice:
		items := make([]any, 0, value.Slice().Len())
		for i := 0; i < value.Slice().Len(); i++ {
			items = append(items, taggedValue(value.Slice().At(i)))
		}
		return map[string]any{"array": items}
	case pcommon.ValueTypeMap:
		items := make([]map[string]any, 0, value.Map().Len())
		value.Map().Range(func(key string, child pcommon.Value) bool {
			items = append(items, map[string]any{"key": key, "value": taggedValue(child)})
			return true
		})
		return map[string]any{"map": items}
	}
	return map[string]any{"empty": true}
}

func validateMap(values pcommon.Map) error {
	return validateMapAtDepth(values, 0)
}

func validateValue(value pcommon.Value, depth int) error {
	if depth > 16 {
		return fmt.Errorf("nesting exceeds 16 levels")
	}
	switch value.Type() {
	case pcommon.ValueTypeStr:
		if len(value.Str()) > 1<<20 {
			return fmt.Errorf("string exceeds 1 MiB")
		}
	case pcommon.ValueTypeBytes:
		if value.Bytes().Len() > 1<<20 {
			return fmt.Errorf("bytes exceed 1 MiB")
		}
	case pcommon.ValueTypeSlice:
		for i := 0; i < value.Slice().Len(); i++ {
			if err := validateValue(value.Slice().At(i), depth+1); err != nil {
				return err
			}
		}
	case pcommon.ValueTypeMap:
		return validateMapAtDepth(value.Map(), depth+1)
	}
	return nil
}

func validateMapAtDepth(values pcommon.Map, depth int) error {
	if depth > 16 {
		return fmt.Errorf("nesting exceeds 16 levels")
	}
	var validationErr error
	values.Range(func(key string, value pcommon.Value) bool {
		if len(key) > 1<<20 {
			validationErr = errors.New("attribute key exceeds 1 MiB")
			return false
		}
		validationErr = validateValue(value, depth)
		return validationErr == nil
	})
	return validationErr
}

func marshalSingleLog(resource plog.ResourceLogs, scope plog.ScopeLogs, record plog.LogRecord) ([]byte, error) {
	single := plog.NewLogs()
	targetResource := single.ResourceLogs().AppendEmpty()
	resource.Resource().CopyTo(targetResource.Resource())
	targetResource.SetSchemaUrl(resource.SchemaUrl())
	targetScope := targetResource.ScopeLogs().AppendEmpty()
	scope.Scope().CopyTo(targetScope.Scope())
	targetScope.SetSchemaUrl(scope.SchemaUrl())
	record.CopyTo(targetScope.LogRecords().AppendEmpty())
	redactMap(targetResource.Resource().Attributes())
	redactMap(targetScope.Scope().Attributes())
	targetRecord := targetScope.LogRecords().At(0)
	redactMap(targetRecord.Attributes())
	redactValue(targetRecord.Body())
	payload, err := (&plog.JSONMarshaler{}).MarshalLogs(single)
	return canonicalPayload(payload, err)
}

func marshalSingleSpan(resource ptrace.ResourceSpans, scope ptrace.ScopeSpans, span ptrace.Span) ([]byte, error) {
	single := ptrace.NewTraces()
	targetResource := single.ResourceSpans().AppendEmpty()
	resource.Resource().CopyTo(targetResource.Resource())
	targetResource.SetSchemaUrl(resource.SchemaUrl())
	targetScope := targetResource.ScopeSpans().AppendEmpty()
	scope.Scope().CopyTo(targetScope.Scope())
	targetScope.SetSchemaUrl(scope.SchemaUrl())
	span.CopyTo(targetScope.Spans().AppendEmpty())
	redactMap(targetResource.Resource().Attributes())
	redactMap(targetScope.Scope().Attributes())
	targetSpan := targetScope.Spans().At(0)
	redactMap(targetSpan.Attributes())
	for eventIndex := 0; eventIndex < targetSpan.Events().Len(); eventIndex++ {
		redactMap(targetSpan.Events().At(eventIndex).Attributes())
	}
	for linkIndex := 0; linkIndex < targetSpan.Links().Len(); linkIndex++ {
		redactMap(targetSpan.Links().At(linkIndex).Attributes())
	}
	payload, err := (&ptrace.JSONMarshaler{}).MarshalTraces(single)
	return canonicalPayload(payload, err)
}

// Canonicalize only OTLP key/value lists. Event and array order remains intact.
// Sorting JSON avoids quadratic insertion into pdata's slice-backed maps.
func canonicalPayload(payload []byte, marshalErr error) ([]byte, error) {
	if marshalErr != nil {
		return nil, marshalErr
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber() // Preserve integers that exceed float64 precision.
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	canonicalize(value, "")
	return json.Marshal(value)
}

func canonicalize(value any, parent string) {
	switch value := value.(type) {
	case map[string]any:
		for key, child := range value {
			if key == "attributes" || key == "filteredAttributes" || key == "metadata" || (parent == "kvlistValue" && key == "values") {
				if entries, ok := child.([]any); ok {
					sort.SliceStable(entries, func(i, j int) bool {
						left, _ := entries[i].(map[string]any)["key"].(string)
						right, _ := entries[j].(map[string]any)["key"].(string)
						return left < right
					})
				}
			}
			canonicalize(child, key)
		}
	case []any:
		for _, child := range value {
			canonicalize(child, parent)
		}
	}
}

func redactMap(values pcommon.Map) {
	values.Range(func(key string, value pcommon.Value) bool {
		if isSensitiveKey(key) {
			value.SetStr("[REDACTED]")
			return true
		}
		redactValue(value)
		return true
	})
}

func redactValue(value pcommon.Value) {
	switch value.Type() {
	case pcommon.ValueTypeMap:
		redactMap(value.Map())
	case pcommon.ValueTypeSlice:
		for index := 0; index < value.Slice().Len(); index++ {
			redactValue(value.Slice().At(index))
		}
	}
}

func isSensitiveKey(key string) bool {
	// Compare whole terminal names, including common camelCase/header spellings.
	// Never match "token" as a substring: token counts are useful telemetry.
	normalized := strings.ToLower(strings.TrimSpace(key))
	segments := strings.FieldsFunc(normalized, func(r rune) bool {
		return r == '.' || r == '/' || r == ':'
	})
	if len(segments) == 0 {
		return false
	}
	terminal := strings.NewReplacer("-", "", "_", "").Replace(segments[len(segments)-1])
	switch terminal {
	case "authorization", "proxyauthorization", "cookie", "setcookie",
		"password", "passwd", "secret", "apikey", "clientsecret", "token",
		"accesstoken", "refreshtoken", "idtoken", "sessiontoken", "csrftoken":
		return true
	}
	normalized = strings.NewReplacer(".", "_", "-", "_", "/", "_", ":", "_").Replace(normalized)
	for _, credential := range []string{"password", "passwd", "secret", "api_key", "access_token", "refresh_token", "id_token", "session_token", "csrf_token"} {
		if strings.HasSuffix(normalized, "_"+credential) {
			return true
		}
	}
	return false
}

func attributeString(attrs pcommon.Map, key string) string {
	value, ok := attrs.Get(key)
	if !ok || value.Type() != pcommon.ValueTypeStr {
		return ""
	}
	return value.Str()
}
func first(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
func nonZeroTraceID(id pcommon.TraceID) []byte {
	if id.IsEmpty() {
		return nil
	}
	return append([]byte(nil), id[:]...)
}
func nonZeroSpanID(id pcommon.SpanID) []byte {
	if id.IsEmpty() {
		return nil
	}
	return append([]byte(nil), id[:]...)
}

func (cfg *Config) Validate() error {
	var id component.ID
	if err := id.UnmarshalText([]byte(cfg.Store)); err != nil || id.Type() != store.Type {
		return errors.New("store must identify a logal_store extension")
	}
	return nil
}
