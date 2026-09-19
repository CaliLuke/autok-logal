package query

import (
	"fmt"
	"strings"
	"time"
)

type Filters struct {
	Service, Since, Until, TimeBasis, Level, Name                   string
	Scope, ScopeVersion, TraceID, SpanID, Kind, Status, MinDuration string
	MetricType, Temporality, ResourceID                             string
	Resource, Attribute                                             []string
	Payload                                                         bool
}

func cutoff(since string, now time.Time) (int64, error) {
	if value, err := time.ParseDuration(since); err == nil {
		if value <= 0 {
			return 0, fmt.Errorf("since duration must be positive")
		}
		return now.Add(-value).UnixNano(), nil
	}
	value, err := time.Parse(time.RFC3339Nano, since)
	if err != nil {
		return 0, fmt.Errorf("since must be a positive duration or RFC3339 timestamp")
	}
	if value.Before(time.Unix(0, 0)) || value.After(time.Unix(0, 1<<63-1)) {
		return 0, fmt.Errorf("since timestamp is outside the supported range")
	}
	return value.UnixNano(), nil
}

func Logs(f Filters, now time.Time) (string, []any, error) {
	where, args, err := otelWhere(f, now, "time_unix_nano")
	if err != nil {
		return "", nil, err
	}
	if f.Level != "" {
		number, ok := map[string]int{"trace": 1, "debug": 5, "info": 9, "warn": 13, "error": 17, "fatal": 21}[strings.ToLower(f.Level)]
		if !ok {
			return "", nil, fmt.Errorf("level must be trace, debug, info, warn, error, or fatal")
		}
		where += " AND severity_number>=?"
		args = append(args, number)
	}
	for _, filter := range []struct {
		column, value string
		size          int
	}{{"trace_id", f.TraceID, 16}, {"span_id", f.SpanID, 8}} {
		if filter.value != "" {
			id, err := decodeID(filter.value, filter.column, filter.size)
			if err != nil {
				return "", nil, err
			}
			where += " AND " + filter.column + "=?"
			args = append(args, id)
		}
	}
	columns := `id, received_at_unix_nano AS received_at, time_unix_nano AS time, service_name,
 severity_number, severity_text, trace_id, span_id, body_json AS body,
 resource_attributes,resource_schema_url,scope_name,scope_version,scope_attributes,scope_schema_url,attributes,event_name,observed_time,flags`
	if f.Payload {
		columns += ",payload_json AS payload"
	}
	source := "SELECT *," + otelContext(logResource, logScope) + "," + arrayField(logRecord+".attributes") + " AS attributes," + textField(logRecord+".eventName") + " AS event_name,CAST(" + extract(logRecord+".observedTimeUnixNano") + " AS INTEGER) AS observed_time,COALESCE(" + extract(logRecord+".flags") + ",0) AS flags FROM otel_logs"
	return "WITH source AS (" + source + ") SELECT " + columns + " FROM source WHERE " + where + " ORDER BY id DESC", args, nil
}

func Trace(id string, payload bool) (string, []any, error) {
	traceID, err := decodeID(id, "trace ID", 16)
	if err != nil {
		return "", nil, err
	}
	spanContext := "," + otelContext(spanResource, spanScope) + "," + arrayField(spanRecord+".attributes") + " AS attributes," + arrayField(spanRecord+".events") + " AS events," + arrayField(spanRecord+".links") + " AS links"
	logContext := "," + otelContext(logResource, logScope) + "," + arrayField(logRecord+".attributes") + " AS attributes,'[]' AS events,'[]' AS links"
	spanPayload, logPayload := "", ""
	if payload {
		spanPayload = ",payload_json AS payload"
		logPayload = ",payload_json AS payload"
	}
	// Event time orders the timeline; receipt time is the fallback for missing
	// timestamps. Kind and row ID break ties without implying causality.
	statement := `SELECT 'span' AS kind,id,received_at_unix_nano AS received_at,
 CASE WHEN start_time_unix_nano=0 THEN received_at_unix_nano ELSE start_time_unix_nano END AS time,
 service_name,trace_id,span_id,parent_span_id,name,NULL AS severity_text,
 start_time_unix_nano AS start_time,end_time_unix_nano AS end_time,NULL AS body,
 COALESCE(json_extract(payload_json,'$.resourceSpans[0].scopeSpans[0].spans[0].status.code'),0) AS status_code,
 COALESCE(json_extract(payload_json,'$.resourceSpans[0].scopeSpans[0].spans[0].status.message'),'') AS status_message,
 COALESCE(json_extract(payload_json,'$.resourceSpans[0].scopeSpans[0].spans[0].kind'),0) AS span_kind,
 CASE WHEN start_time_unix_nano>0 AND end_time_unix_nano>=start_time_unix_nano THEN (end_time_unix_nano-start_time_unix_nano)/1000000.0 END AS duration_ms` + spanContext + spanPayload + `
 FROM otel_spans WHERE trace_id=? UNION ALL
 SELECT 'log',id,received_at_unix_nano,
 CASE WHEN time_unix_nano=0 THEN received_at_unix_nano ELSE time_unix_nano END,
 service_name,trace_id,span_id,NULL,` + textField(logRecord+".eventName") + `,severity_text,NULL,NULL,body_json,NULL,NULL,NULL,NULL` + logContext + logPayload + `
 FROM otel_logs WHERE trace_id=?`
	statement = "WITH timeline AS (" + statement + ") SELECT *, (time-MIN(time) OVER())/1000000.0 AS offset_ms FROM timeline ORDER BY time,kind,id"
	return statement, []any{traceID, traceID}, nil
}
