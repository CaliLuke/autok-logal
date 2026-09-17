package exporter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/CaliLuke/autok-logal/internal/store"
	"go.opentelemetry.io/collector/component"
	collexporter "go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newMetricFixture(metricType pmetric.MetricType) (pmetric.Metrics, pmetric.ResourceMetrics, pmetric.ScopeMetrics, pmetric.Metric) {
	data := pmetric.NewMetrics()
	resource := data.ResourceMetrics().AppendEmpty()
	resource.SetSchemaUrl("https://resource.schema")
	resource.Resource().SetDroppedAttributesCount(2)
	resource.Resource().Attributes().PutStr("service.name", "metric-service")
	resource.Resource().Attributes().PutStr("resource.attribute", "resource-value")
	scope := resource.ScopeMetrics().AppendEmpty()
	scope.SetSchemaUrl("https://scope.schema")
	scope.Scope().SetName("metric.scope")
	scope.Scope().SetVersion("1.2.3")
	scope.Scope().SetDroppedAttributesCount(3)
	scope.Scope().Attributes().PutStr("scope.attribute", "scope-value")
	metric := scope.Metrics().AppendEmpty()
	metric.SetName("requests")
	metric.SetDescription("request metric")
	metric.SetUnit("1")
	metric.Metadata().PutStr("metadata.attribute", "metadata-value")
	switch metricType {
	case pmetric.MetricTypeGauge:
		point := metric.SetEmptyGauge().DataPoints().AppendEmpty()
		point.SetStartTimestamp(100)
		point.SetTimestamp(200)
		point.SetFlags(1)
		point.SetIntValue(42)
		point.Attributes().PutStr("point.attribute", "point-value")
		exemplar := point.Exemplars().AppendEmpty()
		exemplar.SetTimestamp(150)
		exemplar.SetDoubleValue(1.5)
		exemplar.SetTraceID(pcommon.TraceID{1})
		exemplar.SetSpanID(pcommon.SpanID{2})
		exemplar.FilteredAttributes().PutStr("exemplar.attribute", "exemplar-value")
	case pmetric.MetricTypeSum:
		sum := metric.SetEmptySum()
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		sum.SetIsMonotonic(true)
		point := sum.DataPoints().AppendEmpty()
		point.SetStartTimestamp(100)
		point.SetTimestamp(200)
		point.SetFlags(1)
		point.SetDoubleValue(math.Inf(1))
		exemplar := point.Exemplars().AppendEmpty()
		exemplar.SetTimestamp(150)
		exemplar.SetIntValue(7)
	case pmetric.MetricTypeHistogram:
		histogram := metric.SetEmptyHistogram()
		histogram.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		point := histogram.DataPoints().AppendEmpty()
		point.SetStartTimestamp(100)
		point.SetTimestamp(200)
		point.SetFlags(1)
		point.SetCount(math.MaxUint64)
		point.SetSum(12.5)
		point.SetMin(1.5)
		point.SetMax(7.5)
		point.ExplicitBounds().FromRaw([]float64{2, 4})
		point.BucketCounts().FromRaw([]uint64{1, 2, 3})
		exemplar := point.Exemplars().AppendEmpty()
		exemplar.SetTimestamp(150)
		exemplar.SetDoubleValue(3.5)
	case pmetric.MetricTypeExponentialHistogram:
		histogram := metric.SetEmptyExponentialHistogram()
		histogram.SetAggregationTemporality(pmetric.AggregationTemporalityUnspecified)
		point := histogram.DataPoints().AppendEmpty()
		point.SetStartTimestamp(100)
		point.SetTimestamp(200)
		point.SetFlags(1)
		point.SetCount(math.MaxUint64)
		point.SetSum(12.5)
		point.SetMin(1.5)
		point.SetMax(7.5)
		point.SetScale(-2)
		point.SetZeroThreshold(0.01)
		point.SetZeroCount(4)
		point.Positive().SetOffset(-1)
		point.Positive().BucketCounts().FromRaw([]uint64{2, 3})
		point.Negative().SetOffset(1)
		point.Negative().BucketCounts().FromRaw([]uint64{5, 6})
		exemplar := point.Exemplars().AppendEmpty()
		exemplar.SetTimestamp(150)
		exemplar.SetIntValue(8)
	case pmetric.MetricTypeSummary:
		point := metric.SetEmptySummary().DataPoints().AppendEmpty()
		point.SetStartTimestamp(100)
		point.SetTimestamp(200)
		point.SetFlags(1)
		point.SetCount(math.MaxUint64)
		point.SetSum(12.5)
		first := point.QuantileValues().AppendEmpty()
		first.SetQuantile(0.5)
		first.SetValue(4.5)
		second := point.QuantileValues().AppendEmpty()
		second.SetQuantile(0.99)
		second.SetValue(9.5)
	}
	return data, resource, scope, metric
}

func startMetricsExporter(t *testing.T) (*metricsExporter, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metrics.sqlite")
	factory := store.NewFactory()
	cfg := factory.CreateDefaultConfig().(*store.Config)
	cfg.Path = path
	cfg.RetentionHours = 48
	extensionInstance, err := factory.Create(context.Background(), extension.Settings{ID: component.NewID(store.Type)}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	sqliteStore := extensionInstance.(*store.Store)
	if err := sqliteStore.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sqliteStore.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return &metricsExporter{store: sqliteStore}, path
}

func readMetricRow(t *testing.T, path string, query string, args ...any) *sql.Row {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db.QueryRow(query, args...)
}

func TestFactoryCreatesNonMutatingMetricsExporter(t *testing.T) {
	factory := NewFactory()
	created, err := factory.CreateMetrics(context.Background(), collexporter.Settings{ID: component.NewID(Type)}, factory.CreateDefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if created.Capabilities().MutatesData {
		t.Fatal("metrics exporter reports caller-owned data mutation")
	}
}

func TestMetricPointRoundTripPreservesGauge(t *testing.T) {
	assertMetricRoundTrip(t, pmetric.MetricTypeGauge)
}

func TestMetricPointRoundTripPreservesSum(t *testing.T) {
	assertMetricRoundTrip(t, pmetric.MetricTypeSum)
}

func TestMetricPointRoundTripPreservesHistogram(t *testing.T) {
	assertMetricRoundTrip(t, pmetric.MetricTypeHistogram)
}

func TestMetricPointRoundTripPreservesExponentialHistogram(t *testing.T) {
	assertMetricRoundTrip(t, pmetric.MetricTypeExponentialHistogram)
}

func TestMetricPointRoundTripPreservesSummary(t *testing.T) {
	assertMetricRoundTrip(t, pmetric.MetricTypeSummary)
}

func assertMetricRoundTrip(t *testing.T, metricType pmetric.MetricType) {
	t.Helper()
	data, resource, scope, metric := newMetricFixture(metricType)
	expected, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(data)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := marshalSingleMetricPoint(resource, scope, metric, 0)
	if err != nil {
		t.Fatal(err)
	}
	expected, err = canonicalPayload(expected, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, expected) {
		t.Fatalf("single-point payload lost fidelity:\nactual:   %s\nexpected: %s", actual, expected)
	}
}

func TestMetricFingerprintDoesNotDependOnBatchPosition(t *testing.T) {
	_, standaloneResource, standaloneScope, standaloneMetric := newMetricFixture(pmetric.MetricTypeGauge)
	standalone, err := marshalSingleMetricPoint(standaloneResource, standaloneScope, standaloneMetric, 0)
	if err != nil {
		t.Fatal(err)
	}
	batched := pmetric.NewMetrics()
	batchedResource := batched.ResourceMetrics().AppendEmpty()
	standaloneResource.Resource().CopyTo(batchedResource.Resource())
	batchedResource.SetSchemaUrl(standaloneResource.SchemaUrl())
	batchedScope := batchedResource.ScopeMetrics().AppendEmpty()
	standaloneScope.Scope().CopyTo(batchedScope.Scope())
	batchedScope.SetSchemaUrl(standaloneScope.SchemaUrl())
	newMetricFixtureData, _, _, otherMetric := newMetricFixture(pmetric.MetricTypeSummary)
	_ = newMetricFixtureData
	otherMetric.CopyTo(batchedScope.Metrics().AppendEmpty())
	standaloneMetric.CopyTo(batchedScope.Metrics().AppendEmpty())
	fromBatch, err := marshalSingleMetricPoint(batchedResource, batchedScope, batchedScope.Metrics().At(1), 0)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(standalone) != sha256.Sum256(fromBatch) {
		t.Fatalf("metric fingerprint changed with batch position")
	}
}

func TestMetricFingerprintsDeduplicateRetriesButKeepDistinctContent(t *testing.T) {
	_, resource, scope, metric := newMetricFixture(pmetric.MetricTypeGauge)
	first, err := marshalSingleMetricPoint(resource, scope, metric, 0)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := marshalSingleMetricPoint(resource, scope, metric, 0)
	if err != nil {
		t.Fatal(err)
	}
	metric.Gauge().DataPoints().At(0).SetIntValue(43)
	distinct, err := marshalSingleMetricPoint(resource, scope, metric, 0)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(first) != sha256.Sum256(retry) {
		t.Fatal("exact metric retry changed fingerprint")
	}
	if sha256.Sum256(first) == sha256.Sum256(distinct) {
		t.Fatal("distinct same-time metric content collapsed")
	}
}

func TestMetricRedactionCoversEnvelopeAndPreservesTokenCounts(t *testing.T) {
	_, resource, scope, metric := newMetricFixture(pmetric.MetricTypeGauge)
	resource.Resource().Attributes().PutStr("authorization", "resource-secret")
	scope.Scope().Attributes().PutStr("password", "scope-secret")
	point := metric.Gauge().DataPoints().At(0)
	point.Attributes().PutStr("session_token", "point-secret")
	point.Attributes().PutInt("gen_ai.usage.input_tokens", 123)
	point.Exemplars().At(0).FilteredAttributes().PutStr("refresh_token", "exemplar-secret")

	payload, err := marshalSingleMetricPoint(resource, scope, metric, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"resource-secret", "scope-secret", "point-secret", "exemplar-secret"} {
		if bytes.Contains(payload, []byte(secret)) {
			t.Fatalf("credential persisted in metric payload: %s", secret)
		}
	}
	if bytes.Count(payload, []byte("[REDACTED]")) != 4 {
		t.Fatalf("redaction count mismatch: %s", payload)
	}
	if !bytes.Contains(payload, []byte(`"key":"gen_ai.usage.input_tokens","value":{"intValue":"123"}`)) {
		t.Fatalf("token count attribute was redacted: %s", payload)
	}
}

func TestMetricFingerprintUsesRedactedPayload(t *testing.T) {
	_, resource, scope, metric := newMetricFixture(pmetric.MetricTypeGauge)
	point := metric.Gauge().DataPoints().At(0)
	point.Attributes().PutStr("access_token", "first-secret")
	first, err := marshalSingleMetricPoint(resource, scope, metric, 0)
	if err != nil {
		t.Fatal(err)
	}
	point.Attributes().PutStr("access_token", "second-secret")
	second, err := marshalSingleMetricPoint(resource, scope, metric, 0)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(first) != sha256.Sum256(second) {
		t.Fatal("redacted credential changed metric fingerprint")
	}
}

func TestConsumeMetricsRejectsInvalidAttributes(t *testing.T) {
	for name, mutate := range map[string]func(pmetric.ResourceMetrics, pmetric.ScopeMetrics, pmetric.Metric){
		"oversized point scalar": func(_ pmetric.ResourceMetrics, _ pmetric.ScopeMetrics, metric pmetric.Metric) {
			metric.Gauge().DataPoints().At(0).Attributes().PutStr("large", strings.Repeat("x", (1<<20)+1))
		},
		"deep exemplar": func(_ pmetric.ResourceMetrics, _ pmetric.ScopeMetrics, metric pmetric.Metric) {
			current := metric.Gauge().DataPoints().At(0).Exemplars().At(0).FilteredAttributes().PutEmptyMap("deep")
			for range 18 {
				current = current.PutEmptyMap("next")
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, resource, scope, metric := newMetricFixture(pmetric.MetricTypeGauge)
			mutate(resource, scope, metric)
			exporter := &metricsExporter{}
			err := exporter.ConsumeMetrics(context.Background(), resourceToMetrics(resource))
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func resourceToMetrics(resource pmetric.ResourceMetrics) pmetric.Metrics {
	data := pmetric.NewMetrics()
	resource.CopyTo(data.ResourceMetrics().AppendEmpty())
	return data
}

func TestConsumeMetricsEnforcesDescriptorAndPointLimits(t *testing.T) {
	descriptors := pmetric.NewMetrics()
	scope := descriptors.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	for range maxMetricDescriptors + 1 {
		scope.Metrics().AppendEmpty().SetEmptyGauge()
	}
	if err := (&metricsExporter{}).ConsumeMetrics(context.Background(), descriptors); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("descriptor limit error=%v", err)
	}

	points := pmetric.NewMetrics()
	gauge := points.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptyGauge()
	for range maxMetricPoints + 1 {
		gauge.DataPoints().AppendEmpty().SetIntValue(1)
	}
	if err := (&metricsExporter{}).ConsumeMetrics(context.Background(), points); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("point limit error=%v", err)
	}
}

func TestConsumeMetricsRejectsEmptyTypeAndAcceptsZeroPoints(t *testing.T) {
	empty := pmetric.NewMetrics()
	empty.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	if err := (&metricsExporter{}).ConsumeMetrics(context.Background(), empty); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty metric error=%v", err)
	}

	exporter, _ := startMetricsExporter(t)
	zeroPoints := pmetric.NewMetrics()
	zeroPoints.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptyGauge()
	if err := exporter.ConsumeMetrics(context.Background(), zeroPoints); err != nil {
		t.Fatalf("zero-point descriptor failed: %v", err)
	}
}

func TestConsumeMetricsPersistsAtomicRequestAndNullableNonFiniteProjection(t *testing.T) {
	exporter, path := startMetricsExporter(t)
	data, _, _, metric := newMetricFixture(pmetric.MetricTypeSum)
	metric.Sum().DataPoints().At(0).SetDoubleValue(math.NaN())
	if err := exporter.ConsumeMetrics(context.Background(), data); err != nil {
		t.Fatal(err)
	}

	var metricType, numberKind, aggregateCount, payload string
	var numberDouble sql.NullFloat64
	if err := readMetricRow(t, path, `
		SELECT metric_type,number_kind,number_double,COALESCE(aggregate_count,''),payload_json
		FROM otel_metric_points
	`).Scan(&metricType, &numberKind, &numberDouble, &aggregateCount, &payload); err != nil {
		t.Fatal(err)
	}
	if metricType != "sum" || numberKind != "double" || numberDouble.Valid || aggregateCount != "" {
		t.Fatalf("metric projections type=%q kind=%q double=%v count=%q", metricType, numberKind, numberDouble, aggregateCount)
	}
	if !strings.Contains(payload, "NaN") {
		t.Fatalf("non-finite value lost from payload: %s", payload)
	}
}

func TestConsumeMetricsRejectsPersistedPayloadAboveLimit(t *testing.T) {
	exporter, _ := startMetricsExporter(t)
	data := pmetric.NewMetrics()
	resource := data.ResourceMetrics().AppendEmpty()
	resource.Resource().Attributes().PutStr("large", strings.Repeat("x", 1<<20))
	gauge := resource.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptyGauge()
	for range 65 {
		gauge.DataPoints().AppendEmpty().SetIntValue(1)
	}
	err := exporter.ConsumeMetrics(context.Background(), data)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("persisted byte limit error=%v", err)
	}
}

func TestMetricRetryCanonicalizesAllAttributeMapsWithoutMutatingInput(t *testing.T) {
	e, path := startMetricsExporter(t)
	data, resource, scope, metric := newMetricFixture(pmetric.MetricTypeGauge)
	point := metric.Gauge().DataPoints().At(0)
	maps := []pcommon.Map{resource.Resource().Attributes(), scope.Scope().Attributes(), metric.Metadata(), point.Attributes(), point.Exemplars().At(0).FilteredAttributes()}
	for _, attributes := range maps {
		attributes.PutStr("z-last", "last")
		attributes.PutStr("a-first", "first")
	}
	before, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.ConsumeMetrics(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	after, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(data)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("metric exporter mutated caller data")
	}
	for _, attributes := range maps {
		copy := pcommon.NewMap()
		attributes.CopyTo(copy)
		attributes.Clear()
		keys := []string{}
		copy.Range(func(key string, _ pcommon.Value) bool { keys = append(keys, key); return true })
		for i := len(keys) - 1; i >= 0; i-- {
			value, _ := copy.Get(keys[i])
			value.CopyTo(attributes.PutEmpty(keys[i]))
		}
	}
	if err := e.ConsumeMetrics(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := readMetricRow(t, path, `SELECT COUNT(*) FROM otel_metric_points`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("reordered metric retry created %d rows", count)
	}
}

func TestMetricsRejectTimestampOverflowForEveryPointType(t *testing.T) {
	for _, metricType := range []pmetric.MetricType{pmetric.MetricTypeGauge, pmetric.MetricTypeSum, pmetric.MetricTypeHistogram, pmetric.MetricTypeExponentialHistogram, pmetric.MetricTypeSummary} {
		for _, start := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/start=%t", metricType, start), func(t *testing.T) {
				data, _, _, metric := newMetricFixture(metricType)
				var point interface {
					SetTimestamp(pcommon.Timestamp)
					SetStartTimestamp(pcommon.Timestamp)
				}
				switch metricType {
				case pmetric.MetricTypeGauge:
					point = metric.Gauge().DataPoints().At(0)
				case pmetric.MetricTypeSum:
					point = metric.Sum().DataPoints().At(0)
				case pmetric.MetricTypeHistogram:
					point = metric.Histogram().DataPoints().At(0)
				case pmetric.MetricTypeExponentialHistogram:
					point = metric.ExponentialHistogram().DataPoints().At(0)
				case pmetric.MetricTypeSummary:
					point = metric.Summary().DataPoints().At(0)
				}
				if start {
					point.SetStartTimestamp(pcommon.Timestamp(math.MaxUint64))
				} else {
					point.SetTimestamp(pcommon.Timestamp(math.MaxUint64))
				}
				if err := (&metricsExporter{}).ConsumeMetrics(context.Background(), data); status.Code(err) != codes.InvalidArgument {
					t.Fatalf("overflow error=%v", err)
				}
			})
		}
	}
}

func TestMetricsHonorCancellation(t *testing.T) {
	data, _, _, _ := newMetricFixture(pmetric.MetricTypeGauge)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (&metricsExporter{}).ConsumeMetrics(ctx, data); status.Code(err) != codes.Canceled {
		t.Fatalf("cancellation error=%v", err)
	}
}
