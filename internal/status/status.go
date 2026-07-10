package status

import (
	"context"
	"encoding/json"
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
)

var Type = component.MustNewType("logal_status")

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
	pipelineReady atomic.Bool
	inFlight      atomic.Int64
}

func NewFactory() extension.Factory {
	return extension.NewFactory(Type, func() component.Config { return &Config{Endpoint: "127.0.0.1:13133", MaxInFlight: 8} }, func(_ context.Context, _ extension.Settings, cfg component.Config) (extension.Extension, error) {
		config := *cfg.(*Config)
		if config.MaxInFlight <= 0 {
			return nil, fmt.Errorf("max_in_flight_requests must be positive")
		}
		return &Status{cfg: config, permits: make(chan struct{}, config.MaxInFlight)}, nil
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
	go func() { _ = s.server.Serve(s.listener) }()
	return nil
}

func (s *Status) Shutdown(ctx context.Context) error {
	s.pipelineReady.Store(false)
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
				http.Error(w, "logal is not ready", http.StatusServiceUnavailable)
				return
			}
			select {
			case s.permits <- struct{}{}:
				s.inFlight.Add(1)
				defer func() { <-s.permits; s.inFlight.Add(-1) }()
				next.ServeHTTP(w, r)
			default:
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
	response := map[string]any{"ready": s.pipelineReady.Load() && snapshot.Ready && s.inFlight.Load() < int64(s.cfg.MaxInFlight), "pipeline_ready": s.pipelineReady.Load(), "in_flight": s.inFlight.Load(), "limit": s.cfg.MaxInFlight, "store": snapshot}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

var _ extensioncapabilities.PipelineWatcher = (*Status)(nil)
var _ extensioncapabilities.Dependent = (*Status)(nil)
var _ extensionmiddleware.HTTPServer = (*Status)(nil)
