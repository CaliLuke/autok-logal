package query

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// The persisted OTLP attribute lists are canonicalized by the exporter. Grouping
// them retains AnyValue types and complete Resource/Scope identity. Description
// and integer-versus-double representation are not part of OTLP stream identity.
const metricIdentity = `service_name,resource_attributes,resource_schema_url,scope_name,scope_version,scope_attributes,scope_schema_url,metric_name,metric_type,unit,temporality,monotonic`
const seriesIdentity = metricIdentity + ",attributes"

// Resource IDs are display/filter fingerprints, not grouping keys. Keep the
// complete resource identity in GROUP BY and rate partitions even on collision.
func resourceID(attributes, schemaURL string) string {
	encoded, _ := json.Marshal([2]string{attributes, schemaURL})
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:16])
}

func metricSource() string {
	return `WITH envelopes AS (SELECT *,` + otelContext(metricResource, metricScope) + `,` + extract(metricRecord) + ` AS metric FROM otel_metric_points),
 data AS (SELECT *,logal_resource_id(resource_attributes,resource_schema_url) AS resource_id,
 CASE metric_type WHEN 'exponential_histogram' THEN 'exponentialHistogram' ELSE metric_type END AS data_key FROM envelopes),
 projected AS (SELECT *,json_extract(metric,'$.'||data_key||'.dataPoints[0]') AS point,
 COALESCE(json_extract(metric,'$.unit'),'') AS unit,COALESCE(json_extract(metric,'$.description'),'') AS description,
 CASE WHEN metric_type IN ('gauge','summary') THEN 'unspecified' ELSE
 CASE COALESCE(json_extract(metric,'$.'||data_key||'.aggregationTemporality'),0) WHEN 1 THEN 'delta' WHEN 2 THEN 'cumulative' ELSE 'unspecified' END END AS temporality,
 CASE WHEN metric_type='sum' THEN COALESCE(json_extract(metric,'$.sum.isMonotonic'),0) END AS monotonic FROM data),
 expanded AS (SELECT *,COALESCE(json_extract(point,'$.attributes'),'[]') AS attributes,
 COALESCE(json_extract(point,'$.flags'),0) AS flags,COALESCE(json_extract(point,'$.exemplars'),'[]') AS exemplars,
 COALESCE(json_extract(point,'$.bucketCounts'),'[]') AS bucket_counts,COALESCE(json_extract(point,'$.explicitBounds'),'[]') AS explicit_bounds,
 json_extract(point,'$.scale') AS scale,json_extract(point,'$.zeroCount') AS zero_count,json_extract(point,'$.zeroThreshold') AS zero_threshold,
 json_extract(point,'$.positive') AS positive,json_extract(point,'$.negative') AS negative,
 COALESCE(json_extract(point,'$.quantileValues'),'[]') AS quantile_values FROM projected)`
}

func MetricQuery(f Filters, now time.Time, view string) (string, []any, error) {
	where, args, err := otelWhere(f, now, "time_unix_nano")
	if err != nil {
		return "", nil, err
	}
	if f.Name != "" {
		where += " AND metric_name=?"
		args = append(args, f.Name)
	}
	if f.ResourceID != "" {
		id, err := hex.DecodeString(f.ResourceID)
		if err != nil || len(id) != 16 {
			return "", nil, fmt.Errorf("resource-id must be 32 hexadecimal digits from metric output")
		}
		where += " AND resource_id=?"
		args = append(args, strings.ToLower(f.ResourceID))
	}
	if f.MetricType != "" {
		switch f.MetricType {
		case "gauge", "sum", "histogram", "exponential_histogram", "summary":
		default:
			return "", nil, fmt.Errorf("type must be gauge, sum, histogram, exponential_histogram, or summary")
		}
		where += " AND metric_type=?"
		args = append(args, f.MetricType)
	}
	if f.Temporality != "" {
		switch f.Temporality {
		case "delta", "cumulative", "unspecified":
		default:
			return "", nil, fmt.Errorf("temporality must be delta, cumulative, or unspecified")
		}
		where += " AND temporality=?"
		args = append(args, f.Temporality)
	}
	source := metricSource() + ", selected AS (SELECT * FROM expanded WHERE " + where + ") "
	switch view {
	case "points":
		columns := `id,received_at_unix_nano AS received_at,time_unix_nano AS time,start_time_unix_nano AS start_time,resource_id,` + seriesIdentity + `,description,flags,
 number_kind,number_int,number_double,aggregate_count,aggregate_sum,aggregate_min,aggregate_max,
 bucket_counts,explicit_bounds,scale,zero_count,zero_threshold,positive,negative,quantile_values,exemplars`
		if f.Payload {
			columns += ",payload_json AS payload"
		}
		return source + "SELECT " + columns + " FROM selected ORDER BY id DESC", args, nil
	case "list", "series":
		if f.Payload {
			return "", nil, fmt.Errorf("--payload is available for metric points and rates, not discovery summaries")
		}
		identity := metricIdentity
		extra := ",COUNT(DISTINCT attributes) AS series_count"
		if view == "series" {
			identity = seriesIdentity
			extra = ""
		}
		return source + "SELECT resource_id," + identity + extra + `,COUNT(*) AS point_count,
 SUM((flags & 1)!=0) AS no_recorded_value_count,MIN(time_unix_nano) AS first_time,MAX(time_unix_nano) AS last_time,
 MAX(received_at_unix_nano) AS last_received_at FROM selected GROUP BY ` + identity + " ORDER BY " + identity, args, nil
	case "rate":
		if strings.TrimSpace(f.Name) == "" {
			return "", nil, fmt.Errorf("metrics rate requires --name to select an OTLP metric")
		}
		return source + metricRates(f.Payload), args, nil
	default:
		return "", nil, fmt.Errorf("unknown metrics view %q", view)
	}
}

func metricRates(payload bool) string {
	// Compute the window before output pagination. A cumulative rate always needs
	// a valid prior point inside the selected window: never infer a lifetime rate
	// from the first retained point or turn unknown-start data into a true reset.
	sql := `, values_for_rate AS (SELECT *,COALESCE(number_int,number_double) AS value,
 COUNT(*) OVER (PARTITION BY ` + seriesIdentity + `,time_unix_nano) AS same_time FROM selected),
 ordered AS (SELECT *,
 LAG(time_unix_nano) OVER series AS previous_time,LAG(start_time_unix_nano) OVER series AS previous_start,
 LAG(value) OVER series AS previous_value,LAG(flags) OVER series AS previous_flags,LAG(same_time) OVER series AS previous_same_time
 FROM values_for_rate WINDOW series AS (PARTITION BY ` + seriesIdentity + ` ORDER BY time_unix_nano,id)),
 checked AS (SELECT *,CASE
 WHEN metric_type!='sum' OR monotonic!=1 THEN 'not_monotonic_sum'
 WHEN temporality NOT IN ('delta','cumulative') THEN 'unspecified_temporality'
 WHEN (flags & 1)!=0 THEN 'no_recorded_value'
 WHEN value IS NULL THEN 'missing_or_nonfinite_value'
 WHEN value<0 THEN 'negative_monotonic_sum'
 WHEN time_unix_nano=0 OR start_time_unix_nano=0 THEN 'missing_timestamp'
 WHEN start_time_unix_nano>time_unix_nano THEN 'invalid_interval'
 WHEN same_time>1 THEN 'ambiguous_timestamp'
 WHEN start_time_unix_nano=time_unix_nano THEN 'zero_duration'
 WHEN temporality='delta' AND previous_time>start_time_unix_nano THEN 'overlapping_interval'
 WHEN temporality='cumulative' AND previous_time IS NULL THEN 'no_baseline'
 WHEN temporality='cumulative' AND previous_same_time>1 THEN 'ambiguous_baseline'
 WHEN temporality='cumulative' AND (previous_value IS NULL OR previous_value<0 OR (previous_flags & 1)!=0) THEN 'no_baseline'
 WHEN temporality='cumulative' AND previous_start!=start_time_unix_nano THEN 'reset'
 WHEN temporality='cumulative' AND previous_time<start_time_unix_nano THEN 'invalid_baseline'
 WHEN temporality='cumulative' AND value<previous_value THEN 'counter_decreased'
 END AS rate_reason FROM ordered),
 intervals AS (SELECT *,CASE WHEN rate_reason IS NULL THEN
 CASE temporality WHEN 'delta' THEN start_time_unix_nano ELSE previous_time END END AS interval_start,
 CASE WHEN rate_reason IS NULL THEN CASE temporality WHEN 'delta' THEN value ELSE value-previous_value END END AS delta_value
 FROM checked), calculated AS (SELECT *,delta_value/((time_unix_nano-interval_start)/1000000000.0) AS candidate_rate FROM intervals)
 SELECT id,received_at_unix_nano AS received_at,time_unix_nano AS time,start_time_unix_nano AS start_time,resource_id,` + seriesIdentity + `,
 value,flags,interval_start,time_unix_nano AS interval_end,delta_value,
 CASE WHEN abs(candidate_rate)<=1.7976931348623157e308 THEN candidate_rate END AS rate_per_second,
 CASE WHEN abs(candidate_rate)>1.7976931348623157e308 THEN 'nonfinite_rate' ELSE rate_reason END AS rate_reason,
 CASE WHEN temporality='delta' AND previous_time>0 AND start_time_unix_nano>previous_time THEN (start_time_unix_nano-previous_time)/1000000000.0 END AS gap_seconds`
	if payload {
		sql += ",payload_json AS payload"
	}
	return sql + " FROM calculated ORDER BY time_unix_nano DESC,id DESC"
}
