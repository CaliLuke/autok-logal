package query

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/CaliLuke/autok-logal/internal/store"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type ratePoint struct {
	start, end, value                             int64
	flags                                         pmetric.DataPointFlags
	instance, scope, attribute, description, unit string
	asDouble                                      bool
	doubleValue                                   float64
}

func TestMetricSemantics(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "metrics.sqlite")
	ext, err := store.NewFactory().Create(ctx, extension.Settings{ID: component.NewID(store.Type)}, &store.Config{Path: path, RetentionHours: 48})
	if err != nil {
		t.Fatal(err)
	}
	if err := ext.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ext.Shutdown(ctx) })
	writer := ext.(*store.Store)
	now := time.Now()
	base := now.Add(-time.Hour).UnixNano()
	timestamp := func(seconds int64) int64 {
		if seconds < 0 {
			return 0
		}
		return base + seconds*int64(time.Second)
	}
	cases := []struct {
		name        string
		temporality pmetric.AggregationTemporality
		monotonic   bool
		points      []ratePoint
		wantRate    []any // Output in reverse event-time order.
		wantReason  []any
	}{
		{"cumulative", 2, true, []ratePoint{{start: 0, end: 10, value: 100}, {start: 0, end: 20, value: 130}}, []any{float64(3), nil}, []any{nil, "no_baseline"}},
		{"reset", 2, true, []ratePoint{{start: 0, end: 10, value: 100}, {start: 11, end: 20, value: 2}, {start: 11, end: 30, value: 22}}, []any{float64(2), nil, nil}, []any{nil, "reset", "no_baseline"}},
		{"unknown_start", 2, true, []ratePoint{{start: 10, end: 10, value: 100}, {start: 10, end: 20, value: 120}}, []any{float64(2), nil}, []any{nil, "zero_duration"}},
		{"decrease", 2, true, []ratePoint{{start: 0, end: 10, value: 100}, {start: 0, end: 20, value: 2}}, []any{nil, nil}, []any{"counter_decreased", "no_baseline"}},
		{"absent", 2, true, []ratePoint{{start: 0, end: 10, value: 100, flags: 1}, {start: 0, end: 20, value: 120}}, []any{nil, nil}, []any{"no_baseline", "no_recorded_value"}},
		{"duplicate_time", 2, true, []ratePoint{{start: 0, end: 10, value: 100}, {start: 0, end: 10, value: 110}, {start: 0, end: 20, value: 120}}, []any{nil, nil, nil}, []any{"ambiguous_baseline", "ambiguous_timestamp", "ambiguous_timestamp"}},
		{"delta", 1, true, []ratePoint{{start: 0, end: 10, value: 20}, {start: 15, end: 20, value: 20}}, []any{float64(4), float64(2)}, []any{nil, nil}},
		{"overlap", 1, true, []ratePoint{{start: 0, end: 10, value: 20}, {start: 5, end: 20, value: 30}}, []any{nil, float64(2)}, []any{"overlapping_interval", nil}},
		{"nonmonotonic", 2, false, []ratePoint{{start: 0, end: 10, value: 20}}, []any{nil}, []any{"not_monotonic_sum"}},
		{"unspecified", 0, true, []ratePoint{{start: 0, end: 10, value: 20}}, []any{nil}, []any{"unspecified_temporality"}},
		{"missing_timestamp", 1, true, []ratePoint{{start: -1, end: 10, value: 20}}, []any{nil}, []any{"missing_timestamp"}},
		{"big_integer", 2, true, []ratePoint{{start: 0, end: 10, value: 9007199254740992}, {start: 0, end: 20, value: 9007199254741012}}, []any{float64(2), nil}, []any{nil, "no_baseline"}},
		{"mixed_numbers", 2, true, []ratePoint{{start: 0, end: 10, value: 10}, {start: 0, end: 20, asDouble: true, doubleValue: 30}}, []any{float64(2), nil}, []any{nil, "no_baseline"}},
		{"description", 2, true, []ratePoint{{start: 0, end: 10, value: 10, description: "one"}, {start: 0, end: 20, value: 30, description: "changed"}}, []any{float64(2), nil}, []any{nil, "no_baseline"}},
		{"resource_identity", 2, true, []ratePoint{{start: 0, end: 10, value: 10, instance: "one"}, {start: 0, end: 20, value: 30, instance: "two"}}, []any{nil, nil}, []any{"no_baseline", "no_baseline"}},
		{"scope_identity", 2, true, []ratePoint{{start: 0, end: 10, value: 10, scope: "one"}, {start: 0, end: 20, value: 30, scope: "two"}}, []any{nil, nil}, []any{"no_baseline", "no_baseline"}},
		{"point_identity", 2, true, []ratePoint{{start: 0, end: 10, value: 10, attribute: "one"}, {start: 0, end: 20, value: 30, attribute: "two"}}, []any{nil, nil}, []any{"no_baseline", "no_baseline"}},
		{"unit_identity", 2, true, []ratePoint{{start: 0, end: 10, value: 10, unit: "s"}, {start: 0, end: 20, value: 30, unit: "ms"}}, []any{nil, nil}, []any{"no_baseline", "no_baseline"}},
	}
	var records []store.MetricPointRecord
	for _, test := range cases {
		for _, p := range test.points {
			metrics := pmetric.NewMetrics()
			resource := metrics.ResourceMetrics().AppendEmpty()
			resource.Resource().Attributes().PutStr("service.name", "checkout")
			resource.Resource().Attributes().PutStr("service.instance.id", p.instance)
			scope := resource.ScopeMetrics().AppendEmpty()
			scope.Scope().SetName("http.meter")
			scope.Scope().SetVersion(p.scope)
			metric := scope.Metrics().AppendEmpty()
			metric.SetName(test.name)
			metric.SetUnit(p.unit)
			metric.SetDescription(p.description)
			sum := metric.SetEmptySum()
			sum.SetIsMonotonic(test.monotonic)
			sum.SetAggregationTemporality(test.temporality)
			point := sum.DataPoints().AppendEmpty()
			point.SetStartTimestamp(pcommon.Timestamp(timestamp(p.start)))
			point.SetTimestamp(pcommon.Timestamp(timestamp(p.end)))
			point.SetFlags(p.flags)
			point.Attributes().PutStr("route", p.attribute)
			point.Attributes().PutInt("http.response.status_code", 200)
			point.Attributes().PutBool("success", true)
			record := store.MetricPointRecord{ReceivedAt: now.UnixNano(), ServiceName: "checkout", MetricName: test.name, MetricType: "sum", StartTime: timestamp(p.start), Time: timestamp(p.end)}
			if p.asDouble {
				point.SetDoubleValue(p.doubleValue)
				value := p.doubleValue
				record.NumberDouble = &value
				record.NumberKind = "double"
			} else {
				point.SetIntValue(p.value)
				value := p.value
				record.NumberInt = &value
				record.NumberKind = "int"
			}
			payload, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(metrics)
			if err != nil {
				t.Fatal(err)
			}
			record.PayloadJSON = string(payload)
			record.Fingerprint = sha256.Sum256(payload)
			records = append(records, record)
		}
	}
	if err := writer.InsertMetricPoints(ctx, records); err != nil {
		t.Fatal(err)
	}
	options := Options{Path: path, Limit: 100, MaxBytes: 1 << 20, Timeout: time.Second}
	run := func(t *testing.T, f Filters, view string, o Options) Result {
		t.Helper()
		sql, args, err := MetricQuery(f, now, view)
		if err != nil {
			t.Fatal(err)
		}
		result, err := Read(ctx, o, sql, args, true)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			result := run(t, Filters{Since: "2h", Name: test.name}, "rate", options)
			if result.Count != len(test.wantRate) {
				t.Fatalf("%+v", result)
			}
			for i, row := range result.Rows {
				if row["rate_per_second"] != test.wantRate[i] || row["rate_reason"] != test.wantReason[i] {
					t.Fatalf("row %d: rate=%v reason=%v; want %v %v", i, row["rate_per_second"], row["rate_reason"], test.wantRate[i], test.wantReason[i])
				}
			}
			if test.name == "delta" && result.Rows[0]["gap_seconds"] != float64(5) {
				t.Fatal(result.Rows[0])
			}
		})
	}
	t.Run("pagination keeps baseline", func(t *testing.T) {
		o := options
		o.Limit = 1
		r := run(t, Filters{Since: "2h", Name: "cumulative"}, "rate", o)
		if r.Rows[0]["rate_per_second"] != float64(3) || !r.Truncated {
			t.Fatal(r)
		}
	})
	t.Run("resource identifiers distinguish and select across views", func(t *testing.T) {
		f := Filters{Since: "2h", Name: "resource_identity"}
		list := run(t, f, "list", options)
		if list.Count != 2 || list.Rows[0]["resource_id"] == list.Rows[1]["resource_id"] {
			t.Fatalf("distinct resources look identical: %+v", list)
		}
		for _, row := range list.Rows {
			id := row["resource_id"].(string)
			if len(id) != 32 {
				t.Fatalf("invalid resource ID %q", id)
			}
			f.ResourceID = strings.ToUpper(id)
			for _, view := range []string{"list", "series", "points", "rate"} {
				result := run(t, f, view, options)
				if result.Count != 1 || result.Rows[0]["resource_id"] != id || string(result.Rows[0]["resource_attributes"].(json.RawMessage)) != string(row["resource_attributes"].(json.RawMessage)) {
					t.Fatalf("resource selection drifted in %s: %+v", view, result)
				}
			}
		}
		f.ResourceID = "invalid"
		if _, _, err := MetricQuery(f, now, "points"); err == nil {
			t.Fatal("invalid resource ID accepted")
		}
		// Scope/attribute changes must not change a resource's identity.
		for _, name := range []string{"scope_identity", "point_identity"} {
			result := run(t, Filters{Since: "2h", Name: name}, "points", options)
			if result.Rows[0]["resource_id"] != result.Rows[1]["resource_id"] {
				t.Fatalf("%s changed resource identity: %+v", name, result)
			}
		}
		if resourceID("[]", "one") == resourceID("[]", "two") {
			t.Fatal("resource schema URL missing from identity")
		}
	})
	t.Run("discovery excludes description and numeric representation from identity", func(t *testing.T) {
		for _, name := range []string{"description", "mixed_numbers"} {
			r := run(t, Filters{Since: "2h", Name: name}, "series", options)
			if r.Count != 1 || r.Rows[0]["point_count"] != int64(2) {
				t.Fatal(r)
			}
		}
		r := run(t, Filters{Since: "2h", Name: "point_identity"}, "list", options)
		if r.Count != 1 || r.Rows[0]["series_count"] != int64(2) {
			t.Fatal(r)
		}
	})
	t.Run("typed attributes and resource scope filters", func(t *testing.T) {
		filters := Filters{Since: "2h", Name: "point_identity", Scope: "http.meter", Resource: []string{"service.name=checkout"}, Attribute: []string{"http.response.status_code=200", "success=true", "route=one"}}
		r := run(t, filters, "points", options)
		if r.Count != 1 {
			t.Fatal(r)
		}
		attrs, ok := r.Rows[0]["attributes"].(json.RawMessage)
		if !ok || !json.Valid(attrs) {
			t.Fatal(r)
		}
		filters.Attribute = []string{`http.response.status_code="200"`}
		r = run(t, filters, "points", options)
		if r.Count != 0 {
			t.Fatal("string matched integer", r)
		}
	})
	t.Run("service discovery bounds and filters", func(t *testing.T) {
		filters := Filters{Since: "2h", Scope: "http.meter", Resource: []string{"service.name=checkout"}}
		for _, basis := range []string{"received", "event"} {
			filters.TimeBasis = basis
			statement, args, err := Services(filters, now)
			if err != nil {
				t.Fatal(err)
			}
			result, err := Read(ctx, options, statement, args, true)
			if err != nil {
				t.Fatal(err)
			}
			if result.Count != 1 || result.Rows[0]["metric_points"] != int64(len(records)) || result.Rows[0]["logs"] != int64(0) || result.Rows[0]["spans"] != int64(0) {
				t.Fatal(result)
			}
		}
	})
	t.Run("service discovery uses receipt range without upper bound", func(t *testing.T) {
		statement, args, err := Services(Filters{Since: "1m"}, now)
		if err != nil {
			t.Fatal(err)
		}
		db, err := store.OpenReadOnly(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+statement, args...)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		plans := ""
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(detail, "SEARCH") {
				plans += detail + "\n"
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		for _, index := range []string{"idx_logs_received", "idx_spans_received", "idx_metric_points_received"} {
			if !strings.Contains(plans, index) {
				t.Fatalf("service discovery scans history instead of receipt range: %s", plans)
			}
		}
	})
	t.Run("event and receipt windows", func(t *testing.T) {
		filters := Filters{Since: "5m", Name: "cumulative"}
		if r := run(t, filters, "points", options); r.Count != 2 {
			t.Fatal(r)
		}
		filters.TimeBasis = "event"
		if r := run(t, filters, "points", options); r.Count != 0 {
			t.Fatal(r)
		}
		filters.Since = time.Unix(0, timestamp(10)).UTC().Format(time.RFC3339Nano)
		filters.Until = time.Unix(0, timestamp(20)).UTC().Format(time.RFC3339Nano)
		if r := run(t, filters, "points", options); r.Count != 1 {
			t.Fatal(r)
		}
	})
}

func TestOTelFilterValidation(t *testing.T) {
	for _, filter := range []string{"missing-equals", "=value", "value=null", "value=[1,2]", "value=9223372036854775808", "value=1e999"} {
		if _, _, err := attributeFilter("attributes", filter); err == nil {
			t.Errorf("accepted %q", filter)
		}
	}
	for _, f := range []Filters{{Since: "5m", TimeBasis: "invalid"}, {Since: "5m", Until: "bad"}, {Since: "1m", Until: time.Now().Add(-time.Hour).Format(time.RFC3339Nano)}, {Since: "5m", MetricType: "counter"}, {Since: "5m", Temporality: "instant"}} {
		if _, _, err := MetricQuery(f, time.Now(), "points"); err == nil {
			t.Errorf("accepted %+v", f)
		}
	}
	if _, _, err := MetricQuery(Filters{Since: "5m"}, time.Now(), "rate"); err == nil {
		t.Fatal("rate accepted missing name")
	}
	if _, _, err := MetricQuery(Filters{Since: "5m", Payload: true}, time.Now(), "series"); err == nil {
		t.Fatal("silently ignored payload")
	}
	for _, f := range []Filters{{Since: "5m", Kind: "invalid"}, {Since: "5m", Status: "invalid"}, {Since: "5m", MinDuration: "-1ms"}, {Since: "5m", TraceID: "bad"}} {
		if _, _, err := Spans(f, time.Now()); err == nil {
			t.Errorf("accepted %+v", f)
		}
	}
}
