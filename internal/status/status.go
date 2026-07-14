package status

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/CaliLuke/autok-logal/internal/store"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensioncapabilities"
	"go.opentelemetry.io/collector/extension/extensionmiddleware"
	"go.uber.org/zap"
)

var Type = component.MustNewType("logal_status")

const (
	activityInterval = 10 * time.Second
	idleInterval     = time.Minute
)

type Config struct {
	Endpoint    string `mapstructure:"endpoint"`
	MaxInFlight int    `mapstructure:"max_in_flight_requests"`
}

type Status struct {
	cfg           Config
	store         *store.Store
	server        *http.Server
	listener      net.Listener
	permits       chan struct{}
	logger        *zap.Logger
	reportStop    chan struct{}
	reportDone    chan struct{}
	reporting     atomic.Bool
	pipelineReady atomic.Bool
	inFlight      atomic.Int64
	rejected      atomic.Uint64
}

type activityState struct {
	store         store.Snapshot
	pipelineReady bool
	inFlight      int64
	rejected      uint64
}

func NewFactory() extension.Factory {
	return extension.NewFactory(Type, func() component.Config { return &Config{Endpoint: "127.0.0.1:13133", MaxInFlight: 8} }, func(_ context.Context, settings extension.Settings, cfg component.Config) (extension.Extension, error) {
		config := *cfg.(*Config)
		if config.MaxInFlight <= 0 {
			return nil, fmt.Errorf("max_in_flight_requests must be positive")
		}
		return &Status{
			cfg:        config,
			permits:    make(chan struct{}, config.MaxInFlight),
			logger:     settings.Logger,
			reportStop: make(chan struct{}),
			reportDone: make(chan struct{}),
		}, nil
	}, component.StabilityLevelAlpha)
}

func (s *Status) Start(_ context.Context, host component.Host) error {
	var err error
	s.store, err = store.Find(host, "logal_store")
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", s.handleReady)
	mux.HandleFunc("/status", s.handleStatus)
	s.server = &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	s.listener, err = net.Listen("tcp", s.cfg.Endpoint)
	if err != nil {
		return err
	}
	go func() {
		if serveErr := s.server.Serve(s.listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			s.log().Error("Logal status server stopped unexpectedly", zap.Error(serveErr))
		}
	}()
	s.reporting.Store(true)
	go s.activityLoop()
	s.log().Info("Logal operational reporting enabled",
		zap.String("status_endpoint", "http://"+s.cfg.Endpoint+"/status"),
		zap.Duration("activity_interval", activityInterval),
		zap.Duration("idle_heartbeat_interval", idleInterval),
	)
	return nil
}

func (s *Status) Shutdown(ctx context.Context) error {
	s.pipelineReady.Store(false)
	if s.reporting.CompareAndSwap(true, false) {
		close(s.reportStop)
		select {
		case <-s.reportDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

func (s *Status) Ready() error                 { s.pipelineReady.Store(true); return nil }
func (s *Status) NotReady() error              { s.pipelineReady.Store(false); return nil }
func (s *Status) Dependencies() []component.ID { return []component.ID{component.NewID(store.Type)} }

func (s *Status) GetHTTPHandler(context.Context) (extensionmiddleware.WrapHTTPHandlerFunc, error) {
	return func(_ context.Context, next http.Handler) (http.Handler, error) {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || (r.URL.Path != "/v1/logs" && r.URL.Path != "/v1/traces") {
				next.ServeHTTP(w, r)
				return
			}
			if !s.pipelineReady.Load() {
				s.rejected.Add(1)
				http.Error(w, "logal is not ready", http.StatusServiceUnavailable)
				return
			}
			select {
			case s.permits <- struct{}{}:
				s.inFlight.Add(1)
				defer func() { <-s.permits; s.inFlight.Add(-1) }()
				next.ServeHTTP(w, r)
			default:
				s.rejected.Add(1)
				http.Error(w, "logal ingestion is saturated", http.StatusServiceUnavailable)
			}
		}), nil
	}, nil
}

func (s *Status) handleReady(w http.ResponseWriter, r *http.Request) {
	snapshot := s.store.Snapshot(r.Context())
	if !s.pipelineReady.Load() || !snapshot.Ready || s.inFlight.Load() >= int64(s.cfg.MaxInFlight) {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Status) handleStatus(w http.ResponseWriter, r *http.Request) {
	snapshot := s.store.Snapshot(r.Context())
	response := map[string]any{"ready": s.pipelineReady.Load() && snapshot.Ready && s.inFlight.Load() < int64(s.cfg.MaxInFlight), "pipeline_ready": s.pipelineReady.Load(), "in_flight": s.inFlight.Load(), "limit": s.cfg.MaxInFlight, "rejected_requests": s.rejected.Load(), "store": snapshot}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (s *Status) activityLoop() {
	defer close(s.reportDone)
	ticker := time.NewTicker(activityInterval)
	defer ticker.Stop()
	previous := s.currentActivityState()
	lastReport := time.Now()
	for {
		select {
		case now := <-ticker.C:
			current := s.currentActivityState()
			if activityChanged(previous, current) || now.Sub(lastReport) >= idleInterval {
				s.logActivity(previous, current)
				previous = current
				lastReport = now
			}
		case <-s.reportStop:
			return
		}
	}
}

func (s *Status) currentActivityState() activityState {
	return activityState{
		store:         s.store.OperationalSnapshot(),
		pipelineReady: s.pipelineReady.Load(),
		inFlight:      s.inFlight.Load(),
		rejected:      s.rejected.Load(),
	}
}

func activityChanged(previous, current activityState) bool {
	return previous.store.CommittedLogs != current.store.CommittedLogs ||
		previous.store.CommittedSpans != current.store.CommittedSpans ||
		previous.store.DeletedLogs != current.store.DeletedLogs ||
		previous.store.DeletedSpans != current.store.DeletedSpans ||
		previous.store.Ready != current.store.Ready ||
		previous.store.LastError != current.store.LastError ||
		previous.pipelineReady != current.pipelineReady ||
		previous.rejected != current.rejected
}

func (s *Status) logActivity(previous, current activityState) {
	fields := []zap.Field{
		zap.Bool("ready", current.pipelineReady && current.store.Ready && current.inFlight < int64(s.cfg.MaxInFlight)),
		zap.Uint64("logs_written", counterDelta(previous.store.CommittedLogs, current.store.CommittedLogs)),
		zap.Uint64("spans_written", counterDelta(previous.store.CommittedSpans, current.store.CommittedSpans)),
		zap.Uint64("logs_total", current.store.CommittedLogs),
		zap.Uint64("spans_total", current.store.CommittedSpans),
		zap.Uint64("logs_deleted", counterDelta(previous.store.DeletedLogs, current.store.DeletedLogs)),
		zap.Uint64("spans_deleted", counterDelta(previous.store.DeletedSpans, current.store.DeletedSpans)),
		zap.Uint64("requests_rejected", counterDelta(previous.rejected, current.rejected)),
		zap.Int64("in_flight", current.inFlight),
		zap.Int64("database_bytes", current.store.DatabaseBytes),
		zap.Int64("wal_bytes", current.store.WALBytes),
		zap.Uint64("free_bytes", current.store.FreeBytes),
	}
	if current.store.LastError != "" {
		fields = append(fields, zap.String("last_error", current.store.LastError))
	}
	if !current.pipelineReady || !current.store.Ready || current.rejected > previous.rejected {
		s.log().Warn("Logal activity", fields...)
		return
	}
	s.log().Info("Logal activity", fields...)
}

func counterDelta(previous, current uint64) uint64 {
	if current < previous {
		return current
	}
	return current - previous
}

func (s *Status) log() *zap.Logger {
	if s.logger == nil {
		return zap.NewNop()
	}
	return s.logger
}

var _ extensioncapabilities.PipelineWatcher = (*Status)(nil)
var _ extensioncapabilities.Dependent = (*Status)(nil)
var _ extensionmiddleware.HTTPServer = (*Status)(nil)
