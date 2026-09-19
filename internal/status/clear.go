package status

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"time"

	"github.com/CaliLuke/autok-logal/internal/store"
)

func (s *Status) handleClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "clear requires POST", http.StatusMethodNotAllowed)
		return
	}
	// The status listener can be configured beyond loopback. Never expose this
	// destructive operation there, or accept browser-originated requests.
	peer, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || !net.ParseIP(peer).IsLoopback() || r.Header.Get("Origin") != "" || r.Header.Get("X-Logal-Confirm") != "clear" {
		http.Error(w, "clear requires a local CLI request", http.StatusForbidden)
		return
	}
	contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" {
		http.Error(w, "clear requires application/json", http.StatusUnsupportedMediaType)
		return
	}
	var request struct {
		Database string `json:"database"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || request.Database == "" {
		http.Error(w, "clear requires a JSON object with the database path", http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		http.Error(w, "clear requires exactly one JSON object", http.StatusBadRequest)
		return
	}
	if !s.pipelineReady.Load() || s.store == nil {
		http.Error(w, "collector is not running", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	result, err := s.store.Clear(ctx, request.Database)
	if err != nil {
		code := http.StatusServiceUnavailable
		if errors.Is(err, store.ErrDatabaseMismatch) {
			code = http.StatusConflict
		}
		http.Error(w, err.Error(), code)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
