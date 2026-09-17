package status

import (
	"context"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/CaliLuke/autok-logal/internal/store"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

func newStatusStore(t *testing.T) *store.Store {
	t.Helper()
	factory := store.NewFactory()
	cfg := factory.CreateDefaultConfig().(*store.Config)
	cfg.Path = filepath.Join(t.TempDir(), "status.sqlite")
	cfg.RetentionHours = 48
	instance, err := factory.Create(context.Background(), extension.Settings{ID: component.NewID(store.Type)}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	sqliteStore := instance.(*store.Store)
	if err := sqliteStore.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqliteStore.Shutdown(context.Background()) })
	return sqliteStore
}

func wrapIngestion(t *testing.T, statusExtension *Status, next http.Handler) http.Handler {
	t.Helper()
	if statusExtension.store == nil {
		statusExtension.store = newStatusStore(t)
	}
	middleware, err := statusExtension.GetHTTPHandler(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wrapped, err := middleware(context.Background(), next)
	if err != nil {
		t.Fatal(err)
	}
	return wrapped
}

func TestLogsAdmissionFollowsPipelineReadiness(t *testing.T) {
	statusExtension := &Status{cfg: Config{MaxInFlight: 1}, permits: make(chan struct{}, 1)}
	calls := 0
	handler := wrapIngestion(t, statusExtension, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusAccepted)
	}))

	unready := httptest.NewRecorder()
	handler.ServeHTTP(unready, httptest.NewRequest(http.MethodPost, "/v1/traces", nil))
	if unready.Code != http.StatusServiceUnavailable || calls != 0 {
		t.Fatalf("unready status=%d calls=%d", unready.Code, calls)
	}

	statusExtension.pipelineReady.Store(true)
	ready := httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodPost, "/v1/traces", nil))
	if ready.Code != http.StatusAccepted || calls != 1 {
		t.Fatalf("ready status=%d calls=%d", ready.Code, calls)
	}
}

func TestSignalsShareOneSaturationPool(t *testing.T) {
	statusExtension := &Status{cfg: Config{MaxInFlight: 1}, permits: make(chan struct{}, 1)}
	statusExtension.pipelineReady.Store(true)
	entered := make(chan struct{})
	release := make(chan struct{})
	handler := wrapIngestion(t, statusExtension, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusAccepted)
	}))

	logResponse := httptest.NewRecorder()
	logDone := make(chan struct{})
	go func() {
		handler.ServeHTTP(logResponse, httptest.NewRequest(http.MethodPost, "/v1/logs", nil))
		close(logDone)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("log request did not acquire shared permit")
	}

	traceResponse := httptest.NewRecorder()
	handler.ServeHTTP(traceResponse, httptest.NewRequest(http.MethodPost, "/v1/traces", nil))
	if traceResponse.Code != http.StatusServiceUnavailable {
		t.Fatalf("saturated trace status=%d", traceResponse.Code)
	}
	close(release)
	select {
	case <-logDone:
	case <-time.After(time.Second):
		t.Fatal("admitted log request did not finish")
	}
}

func TestAdmissionPassesUnrelatedPathsAndMethods(t *testing.T) {
	statusExtension := &Status{cfg: Config{MaxInFlight: 1}, permits: make(chan struct{}, 1)}
	calls := 0
	handler := wrapIngestion(t, statusExtension, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusNoContent)
	}))

	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/v1/traces", nil),
		httptest.NewRequest(http.MethodPost, "/other", nil),
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("unrelated request status=%d", response.Code)
		}
	}
	if calls != 2 {
		t.Fatalf("unrelated next calls=%d", calls)
	}
}

func TestReadyzIncludesPipelineAndStoreReadiness(t *testing.T) {
	sqliteStore := newStatusStore(t)
	statusExtension := &Status{cfg: Config{MaxInFlight: 1}, permits: make(chan struct{}, 1), store: sqliteStore}

	pipelineUnready := httptest.NewRecorder()
	statusExtension.handleReady(pipelineUnready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if pipelineUnready.Code != http.StatusServiceUnavailable {
		t.Fatalf("pipeline-unready status=%d", pipelineUnready.Code)
	}

	statusExtension.pipelineReady.Store(true)
	ready := httptest.NewRecorder()
	statusExtension.handleReady(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("ready status=%d", ready.Code)
	}

	if err := sqliteStore.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	storeUnready := httptest.NewRecorder()
	statusExtension.handleReady(storeUnready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if storeUnready.Code != http.StatusServiceUnavailable {
		t.Fatalf("store-unready status=%d", storeUnready.Code)
	}
}

func TestActivityChangedTracksOperationalSignals(t *testing.T) {
	baseline := activityState{store: store.Snapshot{Ready: true}, pipelineReady: true}
	tests := map[string]activityState{
		"logs":            {store: store.Snapshot{Ready: true, CommittedLogs: 1}, pipelineReady: true},
		"spans":           {store: store.Snapshot{Ready: true, CommittedSpans: 1}, pipelineReady: true},
		"retention":       {store: store.Snapshot{Ready: true, DeletedLogs: 1}, pipelineReady: true},
		"store readiness": {store: store.Snapshot{Ready: false}, pipelineReady: true},
		"pipeline":        {store: store.Snapshot{Ready: true}, pipelineReady: false},
		"error":           {store: store.Snapshot{Ready: true, LastError: "disk full"}, pipelineReady: true},
		"rejection":       {store: store.Snapshot{Ready: true}, pipelineReady: true, rejected: 1},
	}
	for name, current := range tests {
		t.Run(name, func(t *testing.T) {
			if !activityChanged(baseline, current) {
				t.Fatal("operational change was not detected")
			}
		})
	}
	if activityChanged(baseline, activityState{store: store.Snapshot{Ready: true, DatabaseBytes: 4096}, pipelineReady: true, inFlight: 1}) {
		t.Fatal("transient or storage-size changes should wait for the next activity report or heartbeat")
	}
}

func TestCounterDeltaHandlesCounterReset(t *testing.T) {
	if got := counterDelta(5, 9); got != 4 {
		t.Fatalf("delta=%d", got)
	}
	if got := counterDelta(9, 2); got != 2 {
		t.Fatalf("reset delta=%d", got)
	}
}

func TestGRPCAndHTTPShareAdmissionAndReleaseOnError(t *testing.T) {
	s := &Status{cfg: Config{MaxInFlight: 1}, permits: make(chan struct{}, 1), store: newStatusStore(t)}
	s.pipelineReady.Store(true)
	info := &grpc.UnaryServerInfo{FullMethod: "/opentelemetry.proto.collector.logs.v1.LogsService/Export"}
	handler := wrapIngestion(t, s, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) }))
	_, err := s.interceptGRPC(context.Background(), nil, info, func(context.Context, any) (any, error) {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/logs", nil))
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("HTTP bypassed gRPC permit: %d", response.Code)
		}
		return nil, errors.New("handler failure")
	})
	if err == nil || s.inFlight.Load() != 0 || len(s.permits) != 0 {
		t.Fatalf("permit leaked: %v", err)
	}
	s.pipelineReady.Store(false)
	_, err = s.interceptGRPC(context.Background(), nil, info, func(context.Context, any) (any, error) { t.Fatal("unready handler called"); return nil, nil })
	if grpcstatus.Code(err) != codes.Unavailable {
		t.Fatalf("unready error=%v", err)
	}
}
