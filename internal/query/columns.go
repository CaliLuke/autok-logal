package query

import (
	"fmt"
	"strings"
)

// OutputColumns describes built-in views without opening a database. Contract
// tests compare this catalog with the actual SQL projections, including empty
// results, so column discovery cannot silently drift from query output.
func OutputColumns(command, view string, payload bool) ([]string, error) {
	context := "resource_attributes,resource_schema_url,scope_name,scope_version,scope_attributes,scope_schema_url,attributes"
	var columns string
	switch command {
	case "services":
		columns = "service_name,logs,spans,metric_points,last_received_at"
	case "logs":
		columns = "id,received_at,time,service_name,severity_number,severity_text,trace_id,span_id,body," + context + ",event_name,observed_time,flags"
	case "spans":
		columns = "id,received_at,service_name,trace_id,span_id,parent_span_id,name,kind,status_code,status_message,start_time,end_time,duration_ms," + context + ",events,links"
	case "trace":
		columns = "kind,id,received_at,time,service_name,trace_id,span_id,parent_span_id,name,severity_text,start_time,end_time,body,status_code,status_message,span_kind,duration_ms," + context + ",events,links"
	case "metrics":
		switch view {
		case "points":
			columns = "id,received_at,time,start_time,resource_id," + seriesIdentity + ",description,flags,number_kind,number_int,number_double,aggregate_count,aggregate_sum,aggregate_min,aggregate_max,bucket_counts,explicit_bounds,scale,zero_count,zero_threshold,positive,negative,quantile_values,exemplars"
		case "rate":
			columns = "id,received_at,time,start_time,resource_id," + seriesIdentity + ",value,flags,interval_start,interval_end,delta_value,rate_per_second,rate_reason,gap_seconds"
		case "list", "series":
			if payload {
				return nil, fmt.Errorf("--payload is available for metric points and rates, not discovery summaries")
			}
			columns = "resource_id," + metricIdentity
			if view == "list" {
				columns += ",series_count"
			} else {
				columns += ",attributes"
			}
			columns += ",point_count,no_recorded_value_count,first_time,last_time,last_received_at"
		default:
			return nil, fmt.Errorf("unknown metrics view %q", view)
		}
	default:
		return nil, fmt.Errorf("no built-in columns for %q", command)
	}
	if payload && command != "services" {
		columns += ",payload"
	}
	if command == "trace" {
		columns += ",offset_ms"
	}
	return strings.Split(columns, ","), nil
}
