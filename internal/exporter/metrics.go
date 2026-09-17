package exporter

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"time"

	"github.com/CaliLuke/autok-logal/internal/store"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxMetricDescriptors = 10000
	maxMetricPoints      = 10000
)

type metricsExporter struct {
	cfg   Config
	store *store.Store
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
					if err := ctx.Err(); err != nil {
						return exportError(err)
					}
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
					if record.StartTime < 0 || record.Time < 0 {
						return status.Error(codes.InvalidArgument, "metric timestamp exceeds SQLite integer range")
					}
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
		return exportError(err)
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
	payload, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(single)
	return canonicalPayload(payload, err)
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
