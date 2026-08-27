package exporter

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/CaliLuke/autok-logal/internal/store"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	collexporter "go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var Type = component.MustNewType("logal_sqlite")

const maxPersistedRequestBytes = 64 << 20

const (
	maxMetricDescriptors = 10000
	maxMetricPoints      = 10000
)

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

type metricsExporter struct {
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
func (e *metricsExporter) Start(_ context.Context, host component.Host) error {
	var err error
	e.store, err = store.Find(host, e.cfg.Store)
	return err
}
func (e *metricsExporter) Shutdown(context.Context) error { return nil }
func (e *metricsExporter) Capabilities() consumer.Capabilities {
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
				record := logs.At(li)
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
				body, _ := json.Marshal(taggedValue(bodyValue))
				records = append(records, store.LogRecord{Fingerprint: fingerprint, ReceivedAt: now, Time: int64(record.Timestamp()), ServiceName: serviceName, SeverityNumber: int32(record.SeverityNumber()), SeverityText: record.SeverityText(), TraceID: nonZeroTraceID(traceID), SpanID: nonZeroSpanID(spanID), RequestID: attributeString(attrs, "request.id"), ProductID: attributeString(attrs, "autok.product.id"), Component: attributeString(attrs, "app.component"), Op: first(attributeString(attrs, "event.name"), record.EventName()), BodyJSON: string(body), PayloadJSON: string(payload)})
			}
		}
	}
	if err := e.store.InsertLogs(ctx, records); err != nil {
		return status.Error(codes.Unavailable, err.Error())
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
				span := spans.At(pi)
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
		if strings.Contains(err.Error(), "identity conflicts") {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		return status.Error(codes.Unavailable, err.Error())
	}
	return nil
}

func (e *metricsExporter) ConsumeMetrics(ctx context.Context, data pmetric.Metrics) error {
	if data.MetricCount() > maxMetricDescriptors {
		return status.Errorf(codes.InvalidArgument, "metric request exceeds %d descriptors", maxMetricDescriptors)
	}
	if data.DataPointCount() > maxMetricPoints {
		return status.Errorf(codes.InvalidArgument, "metric request exceeds %d points", maxMetricPoints)
	}
	receivedAt := time.Now().UnixNano()
	records := make([]store.MetricPointRecord, 0, data.DataPointCount())
	persistedBytes := 0
	resources := data.ResourceMetrics()
	for resourceIndex := range resources.Len() {
		resource := resources.At(resourceIndex)
		if err := validateMap(resource.Resource().Attributes()); err != nil {
			return status.Error(codes.InvalidArgument, "invalid metric resource attributes: "+err.Error())
		}
		serviceName := attributeString(resource.Resource().Attributes(), "service.name")
		if serviceName == "" {
			serviceName = "unknown_service"
		}
		scopes := resource.ScopeMetrics()
		for scopeIndex := range scopes.Len() {
			scopeMetrics := scopes.At(scopeIndex)
			if err := validateMap(scopeMetrics.Scope().Attributes()); err != nil {
				return status.Error(codes.InvalidArgument, "invalid metric scope attributes: "+err.Error())
			}
			metrics := scopeMetrics.Metrics()
			for metricIndex := range metrics.Len() {
				metric := metrics.At(metricIndex)
				if metric.Type() == pmetric.MetricTypeEmpty {
					return status.Error(codes.InvalidArgument, "metric data type is required")
				}
				if err := validateMap(metric.Metadata()); err != nil {
					return status.Error(codes.InvalidArgument, "invalid metric metadata: "+err.Error())
				}
				pointCount := metricDataPointCount(metric)
				for pointIndex := range pointCount {
					if err := validateMetricPoint(metric, pointIndex); err != nil {
						return status.Error(codes.InvalidArgument, err.Error())
					}
					payload, err := marshalSingleMetricPoint(resource, scopeMetrics, metric, pointIndex)
					if err != nil {
						return status.Error(codes.InvalidArgument, err.Error())
					}
					persistedBytes += len(payload)
					if persistedBytes > maxPersistedRequestBytes {
						return status.Error(codes.InvalidArgument, "persisted metric payload exceeds 64 MiB")
					}
					record := projectMetricPoint(metric, pointIndex)
					record.Fingerprint = sha256.Sum256(payload)
					record.ReceivedAt = receivedAt
					record.ServiceName = serviceName
					record.MetricName = metric.Name()
					record.PayloadJSON = string(payload)
					records = append(records, record)
				}
			}
		}
	}
	if err := e.store.InsertMetricPoints(ctx, records); err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	return nil
}

func metricDataPointCount(metric pmetric.Metric) int {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		return metric.Gauge().DataPoints().Len()
	case pmetric.MetricTypeSum:
		return metric.Sum().DataPoints().Len()
	case pmetric.MetricTypeHistogram:
		return metric.Histogram().DataPoints().Len()
	case pmetric.MetricTypeExponentialHistogram:
		return metric.ExponentialHistogram().DataPoints().Len()
	case pmetric.MetricTypeSummary:
		return metric.Summary().DataPoints().Len()
	default:
		return 0
	}
}

func validateMetricPoint(metric pmetric.Metric, pointIndex int) error {
	var attributes pcommon.Map
	var exemplars pmetric.ExemplarSlice
	hasExemplars := false
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		point := metric.Gauge().DataPoints().At(pointIndex)
		attributes, exemplars = point.Attributes(), point.Exemplars()
		hasExemplars = true
	case pmetric.MetricTypeSum:
		point := metric.Sum().DataPoints().At(pointIndex)
		attributes, exemplars = point.Attributes(), point.Exemplars()
		hasExemplars = true
	case pmetric.MetricTypeHistogram:
		point := metric.Histogram().DataPoints().At(pointIndex)
		attributes, exemplars = point.Attributes(), point.Exemplars()
		hasExemplars = true
	case pmetric.MetricTypeExponentialHistogram:
		point := metric.ExponentialHistogram().DataPoints().At(pointIndex)
		attributes, exemplars = point.Attributes(), point.Exemplars()
		hasExemplars = true
	case pmetric.MetricTypeSummary:
		attributes = metric.Summary().DataPoints().At(pointIndex).Attributes()
	default:
		return fmt.Errorf("metric data type is required")
	}
	if err := validateMap(attributes); err != nil {
		return fmt.Errorf("invalid metric point attributes: %w", err)
	}
	if hasExemplars {
		for exemplarIndex := range exemplars.Len() {
			if err := validateMap(exemplars.At(exemplarIndex).FilteredAttributes()); err != nil {
				return fmt.Errorf("invalid metric exemplar attributes: %w", err)
			}
		}
	}
	return nil
}

func projectMetricPoint(metric pmetric.Metric, pointIndex int) store.MetricPointRecord {
	record := store.MetricPointRecord{}
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		record.MetricType = "gauge"
		projectNumberPoint(metric.Gauge().DataPoints().At(pointIndex), &record)
	case pmetric.MetricTypeSum:
		record.MetricType = "sum"
		projectNumberPoint(metric.Sum().DataPoints().At(pointIndex), &record)
	case pmetric.MetricTypeHistogram:
		record.MetricType = "histogram"
		point := metric.Histogram().DataPoints().At(pointIndex)
		record.StartTime, record.Time = int64(point.StartTimestamp()), int64(point.Timestamp())
		record.AggregateCount = fmt.Sprint(point.Count())
		if point.HasSum() {
			record.AggregateSum = finiteFloat(point.Sum())
		}
		if point.HasMin() {
			record.AggregateMin = finiteFloat(point.Min())
		}
		if point.HasMax() {
			record.AggregateMax = finiteFloat(point.Max())
		}
	case pmetric.MetricTypeExponentialHistogram:
		record.MetricType = "exponential_histogram"
		point := metric.ExponentialHistogram().DataPoints().At(pointIndex)
		record.StartTime, record.Time = int64(point.StartTimestamp()), int64(point.Timestamp())
		record.AggregateCount = fmt.Sprint(point.Count())
		if point.HasSum() {
			record.AggregateSum = finiteFloat(point.Sum())
		}
		if point.HasMin() {
			record.AggregateMin = finiteFloat(point.Min())
		}
		if point.HasMax() {
			record.AggregateMax = finiteFloat(point.Max())
		}
	case pmetric.MetricTypeSummary:
		record.MetricType = "summary"
		point := metric.Summary().DataPoints().At(pointIndex)
		record.StartTime, record.Time = int64(point.StartTimestamp()), int64(point.Timestamp())
		record.AggregateCount = fmt.Sprint(point.Count())
		record.AggregateSum = finiteFloat(point.Sum())
	}
	return record
}

func projectNumberPoint(point pmetric.NumberDataPoint, record *store.MetricPointRecord) {
	record.StartTime, record.Time = int64(point.StartTimestamp()), int64(point.Timestamp())
	switch point.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		record.NumberKind = "int"
		value := point.IntValue()
		record.NumberInt = &value
	case pmetric.NumberDataPointValueTypeDouble:
		record.NumberKind = "double"
		record.NumberDouble = finiteFloat(point.DoubleValue())
	}
}

func finiteFloat(value float64) *float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil
	}
	return &value
}

func taggedValue(value pcommon.Value) any {
	return taggedValueAtDepth(value, 0)
}

func taggedValueAtDepth(value pcommon.Value, depth int) any {
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
			items = append(items, taggedValueAtDepth(value.Slice().At(i), depth+1))
		}
		return map[string]any{"array": items}
	case pcommon.ValueTypeMap:
		items := make([]map[string]any, 0, value.Map().Len())
		value.Map().Range(func(key string, child pcommon.Value) bool {
			items = append(items, map[string]any{"key": key, "value": taggedValueAtDepth(child, depth+1)})
			return true
		})
		return map[string]any{"map": items}
	}
	return map[string]any{"empty": true}
}

func validateMap(values pcommon.Map) error {
	var validationErr error
	values.Range(func(_ string, value pcommon.Value) bool {
		validationErr = validateValue(value, 0)
		return validationErr == nil
	})
	return validationErr
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
	var validationErr error
	values.Range(func(_ string, value pcommon.Value) bool {
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
	return (&plog.JSONMarshaler{}).MarshalLogs(single)
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
	return (&ptrace.JSONMarshaler{}).MarshalTraces(single)
}

func marshalSingleMetricPoint(resource pmetric.ResourceMetrics, scope pmetric.ScopeMetrics, metric pmetric.Metric, pointIndex int) ([]byte, error) {
	single := pmetric.NewMetrics()
	targetResource := single.ResourceMetrics().AppendEmpty()
	resource.Resource().CopyTo(targetResource.Resource())
	targetResource.SetSchemaUrl(resource.SchemaUrl())
	targetScope := targetResource.ScopeMetrics().AppendEmpty()
	scope.Scope().CopyTo(targetScope.Scope())
	targetScope.SetSchemaUrl(scope.SchemaUrl())
	targetMetric := targetScope.Metrics().AppendEmpty()
	targetMetric.SetName(metric.Name())
	targetMetric.SetDescription(metric.Description())
	targetMetric.SetUnit(metric.Unit())
	metric.Metadata().CopyTo(targetMetric.Metadata())
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		metric.Gauge().DataPoints().At(pointIndex).CopyTo(targetMetric.SetEmptyGauge().DataPoints().AppendEmpty())
	case pmetric.MetricTypeSum:
		source := metric.Sum()
		target := targetMetric.SetEmptySum()
		target.SetAggregationTemporality(source.AggregationTemporality())
		target.SetIsMonotonic(source.IsMonotonic())
		source.DataPoints().At(pointIndex).CopyTo(target.DataPoints().AppendEmpty())
	case pmetric.MetricTypeHistogram:
		source := metric.Histogram()
		target := targetMetric.SetEmptyHistogram()
		target.SetAggregationTemporality(source.AggregationTemporality())
		source.DataPoints().At(pointIndex).CopyTo(target.DataPoints().AppendEmpty())
	case pmetric.MetricTypeExponentialHistogram:
		source := metric.ExponentialHistogram()
		target := targetMetric.SetEmptyExponentialHistogram()
		target.SetAggregationTemporality(source.AggregationTemporality())
		source.DataPoints().At(pointIndex).CopyTo(target.DataPoints().AppendEmpty())
	case pmetric.MetricTypeSummary:
		metric.Summary().DataPoints().At(pointIndex).CopyTo(targetMetric.SetEmptySummary().DataPoints().AppendEmpty())
	default:
		return nil, fmt.Errorf("metric data type is required")
	}
	redactMap(targetResource.Resource().Attributes())
	redactMap(targetScope.Scope().Attributes())
	redactMap(targetMetric.Metadata())
	redactMetricPoint(targetMetric)
	return (&pmetric.JSONMarshaler{}).MarshalMetrics(single)
}

func redactMetricPoint(metric pmetric.Metric) {
	var attributes pcommon.Map
	var exemplars pmetric.ExemplarSlice
	hasExemplars := false
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		point := metric.Gauge().DataPoints().At(0)
		attributes, exemplars = point.Attributes(), point.Exemplars()
		hasExemplars = true
	case pmetric.MetricTypeSum:
		point := metric.Sum().DataPoints().At(0)
		attributes, exemplars = point.Attributes(), point.Exemplars()
		hasExemplars = true
	case pmetric.MetricTypeHistogram:
		point := metric.Histogram().DataPoints().At(0)
		attributes, exemplars = point.Attributes(), point.Exemplars()
		hasExemplars = true
	case pmetric.MetricTypeExponentialHistogram:
		point := metric.ExponentialHistogram().DataPoints().At(0)
		attributes, exemplars = point.Attributes(), point.Exemplars()
		hasExemplars = true
	case pmetric.MetricTypeSummary:
		attributes = metric.Summary().DataPoints().At(0).Attributes()
	}
	redactMap(attributes)
	if hasExemplars {
		for exemplarIndex := range exemplars.Len() {
			redactMap(exemplars.At(exemplarIndex).FilteredAttributes())
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
	normalized := strings.ToLower(strings.TrimSpace(key))
	segments := strings.FieldsFunc(normalized, func(r rune) bool {
		return r == '.' || r == '/' || r == ':'
	})
	terminal := normalized
	if len(segments) > 0 {
		terminal = segments[len(segments)-1]
	}
	terminal = strings.ReplaceAll(terminal, "-", "_")
	switch terminal {
	case "authorization", "cookie", "set_cookie", "password", "passwd", "secret", "api_key":
		return true
	}
	normalized = strings.NewReplacer(".", "_", "-", "_", "/", "_", ":", "_").Replace(normalized)
	for _, credential := range []string{"access_token", "refresh_token", "id_token", "session_token", "csrf_token"} {
		if normalized == credential || strings.HasSuffix(normalized, "_"+credential) {
			return true
		}
	}
	return false
}

func attributeString(attrs pcommon.Map, key string) string {
	value, ok := attrs.Get(key)
	if !ok {
		return ""
	}
	return value.AsString()
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
