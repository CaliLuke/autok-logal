package status

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
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
	return sqliteStore
}

func wrapIngestion(t *testing.T, statusExtension *Status, next http.Handler) http.Handler {
	t.Helper()
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

func TestMetricsAdmissionFollowsPipelineReadiness(t *testing.T) {
	statusExtension := &Status{cfg: Config{MaxInFlight: 1}, permits: make(chan struct{}, 1)}
	calls := 0
	handler := wrapIngestion(t, statusExtension, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusAccepted)
	}))

	unready := httptest.NewRecorder()
	handler.ServeHTTP(unready, httptest.NewRequest(http.MethodPost, "/v1/metrics", nil))
	if unready.Code != http.StatusServiceUnavailable || calls != 0 {
		t.Fatalf("unready status=%d calls=%d", unready.Code, calls)
	}

	statusExtension.pipelineReady.Store(true)
	ready := httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodPost, "/v1/metrics", nil))
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

	metricResponse := httptest.NewRecorder()
	handler.ServeHTTP(metricResponse, httptest.NewRequest(http.MethodPost, "/v1/metrics", nil))
	if metricResponse.Code != http.StatusServiceUnavailable {
		t.Fatalf("saturated metric status=%d", metricResponse.Code)
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
		httptest.NewRequest(http.MethodGet, "/v1/metrics", nil),
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

func TestStatusReturnsMetricCounters(t *testing.T) {
	sqliteStore := newStatusStore(t)
	t.Cleanup(func() {
		if err := sqliteStore.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	record := store.MetricPointRecord{
		Fingerprint: [32]byte{1}, ReceivedAt: time.Now().UnixNano(), ServiceName: "status-test",
		MetricName: "status.metric", MetricType: "gauge", PayloadJSON: `{}`,
	}
	if err := sqliteStore.InsertMetricPoints(context.Background(), []store.MetricPointRecord{record}); err != nil {
		t.Fatal(err)
	}
	statusExtension := &Status{cfg: Config{MaxInFlight: 1}, permits: make(chan struct{}, 1), store: sqliteStore}
	statusExtension.pipelineReady.Store(true)

	response := httptest.NewRecorder()
	statusExtension.handleStatus(response, httptest.NewRequest(http.MethodGet, "/status", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status code=%d", response.Code)
	}
	if body := response.Body.String(); !strings.Contains(body, `"committed_metric_points":1`) || !strings.Contains(body, `"oldest_metric_received_unix_nano":`) {
		t.Fatalf("status body=%s", body)
	}
}

func TestActivityChangedTracksOperationalSignals(t *testing.T) {
	baseline := activityState{store: store.Snapshot{Ready: true}, pipelineReady: true}
	tests := map[string]activityState{
		"logs":             {store: store.Snapshot{Ready: true, CommittedLogs: 1}, pipelineReady: true},
		"spans":            {store: store.Snapshot{Ready: true, CommittedSpans: 1}, pipelineReady: true},
		"metrics":          {store: store.Snapshot{Ready: true, CommittedMetrics: 1}, pipelineReady: true},
		"retention":        {store: store.Snapshot{Ready: true, DeletedLogs: 1}, pipelineReady: true},
		"metric retention": {store: store.Snapshot{Ready: true, DeletedMetrics: 1}, pipelineReady: true},
		"store readiness":  {store: store.Snapshot{Ready: false}, pipelineReady: true},
		"pipeline":         {store: store.Snapshot{Ready: true}, pipelineReady: false},
		"error":            {store: store.Snapshot{Ready: true, LastError: "disk full"}, pipelineReady: true},
		"rejection":        {store: store.Snapshot{Ready: true}, pipelineReady: true, rejected: 1},
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
