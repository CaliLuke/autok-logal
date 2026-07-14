package status

import (
	"testing"

	"github.com/CaliLuke/autok-logal/internal/store"
)

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
