package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type MetricPointRecord struct {
	Fingerprint    [32]byte
	ReceivedAt     int64
	ServiceName    string
	MetricName     string
	MetricType     string
	StartTime      int64
	Time           int64
	NumberKind     string
	NumberInt      *int64
	NumberDouble   *float64
	AggregateCount string
	AggregateSum   *float64
	AggregateMin   *float64
	AggregateMax   *float64
	PayloadJSON    string
}

func (s *Store) InsertMetricPoints(ctx context.Context, records []MetricPointRecord) error {
	if !s.ready.Load() {
		return errors.New("store is not ready")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.admissionErrorLocked(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO otel_metric_points
		(fingerprint,received_at_unix_nano,service_name,metric_name,metric_type,start_time_unix_nano,time_unix_nano,number_kind,number_int,number_double,aggregate_count,aggregate_sum,aggregate_min,aggregate_max,payload_json)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(fingerprint) DO NOTHING`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	var inserted uint64
	for _, r := range records {
		if !json.Valid([]byte(r.PayloadJSON)) {
			return errors.New("metric payload is not valid JSON")
		}
		switch r.MetricType {
		case "gauge", "sum", "histogram", "exponential_histogram", "summary":
		default:
			return fmt.Errorf("invalid metric type %q", r.MetricType)
		}
		if r.NumberKind != "" && r.NumberKind != "int" && r.NumberKind != "double" {
			return fmt.Errorf("invalid metric number kind %q", r.NumberKind)
		}
		result, err := stmt.ExecContext(ctx, r.Fingerprint[:], r.ReceivedAt, r.ServiceName, r.MetricName, r.MetricType, r.StartTime, r.Time, nullable(r.NumberKind), r.NumberInt, r.NumberDouble, nullable(r.AggregateCount), r.AggregateSum, r.AggregateMin, r.AggregateMax, r.PayloadJSON)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n > 0 {
			inserted += uint64(n)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.committedMetrics.Add(inserted)
	return nil
}
