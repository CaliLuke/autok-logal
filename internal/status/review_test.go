package status

import (
	"context"
	"testing"
	"time"

	"github.com/CaliLuke/autok-logal/internal/store"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
)

type reportingHost struct {
	extensions map[component.ID]component.Component
	events     chan *componentstatus.Event
}

func (h reportingHost) GetExtensions() map[component.ID]component.Component { return h.extensions }
func (h reportingHost) Report(event *componentstatus.Event)                 { h.events <- event }

func TestNamedStoreAndStatusServerFailure(t *testing.T) {
	id := component.NewIDWithName(store.Type, "project")
	host := reportingHost{map[component.ID]component.Component{id: newStatusStore(t)}, make(chan *componentstatus.Event, 1)}
	s := &Status{
		cfg:     Config{Store: id.String(), Endpoint: "127.0.0.1:0", MaxInFlight: 1},
		permits: make(chan struct{}, 1), reportStop: make(chan struct{}), reportDone: make(chan struct{}),
	}
	if err := s.cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if got := s.Dependencies(); len(got) != 1 || got[0] != id {
		t.Fatalf("dependencies=%v", got)
	}
	if err := s.Start(context.Background(), host); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := s.Ready(); err != nil {
		t.Fatal(err)
	}
	if err := s.listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-host.events:
		if event.Status() != componentstatus.StatusFatalError || event.Err() == nil {
			t.Fatalf("event=%v", event)
		}
		if s.pipelineReady.Load() {
			t.Fatal("dead status server remained ready")
		}
	case <-time.After(time.Second):
		t.Fatal("server failure was not reported to collector")
	}
}

func TestStatusConfigValidation(t *testing.T) {
	for _, cfg := range []Config{
		{Store: "logal_store", Endpoint: "127.0.0.1:13133", MaxInFlight: 0},
		{Store: "logal_store", Endpoint: "missing-port", MaxInFlight: 1},
		{Store: "other", Endpoint: "127.0.0.1:13133", MaxInFlight: 1},
	} {
		if err := cfg.Validate(); err == nil {
			t.Fatalf("invalid configuration accepted: %+v", cfg)
		}
	}
}
