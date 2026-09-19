package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A maintenance SQL deadline must neither disable a working collector nor
// clear a real storage fault from an earlier write.
func TestMaintenanceBudgetPreservesWriterHealth(t *testing.T) {
	for _, healthy := range []bool{true, false} {
		name := "healthy"
		if !healthy {
			name = "prior_storage_failure"
		}
		t.Run(name, func(t *testing.T) {
			s := startTestStore(t)
			if !healthy {
				s.setOperationalError(errors.New("prior storage failure"))
			}
			before := s.OperationalSnapshot()
			// Reserve the single SQL connection so maintenance acquires its
			// writer gate, then exhausts its budget waiting to execute SQL.
			connection, err := s.db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			err = s.Maintain(ctx, time.Now())
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("maintenance error=%v", err)
			}
			if after := s.OperationalSnapshot(); after.Ready != before.Ready || after.LastError != before.LastError {
				t.Fatalf("budget expiration changed writer health: before=%+v after=%+v", before, after)
			}
			if err := connection.Close(); err != nil {
				t.Fatal(err)
			}
			if err := s.Maintain(context.Background(), time.Now()); err != nil {
				t.Fatal(err)
			}
			if after := s.OperationalSnapshot(); !after.Ready || after.LastError != "" {
				t.Fatalf("successful maintenance did not recover: %+v", after)
			}
		})
	}
}
