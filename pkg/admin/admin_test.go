package admin

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()

	if opts.Host != "127.0.0.1" {
		t.Errorf("Expected host 127.0.0.1, got %s", opts.Host)
	}
	if opts.Port != 9091 {
		t.Errorf("Expected port 9091, got %d", opts.Port)
	}
}

func TestNewServer(t *testing.T) {
	s := NewServer(Options{})

	if s == nil {
		t.Fatal("NewServer returned nil")
	}
}

func TestHumanReadableSize(t *testing.T) {
	tests := []struct {
		bytes    int64
		expected string
	}{
		{0, "0 B"},
		{100, "100 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
	}

	for _, tt := range tests {
		result := humanReadableSize(tt.bytes)
		if result != tt.expected {
			t.Errorf("humanReadableSize(%d) = %s, expected %s", tt.bytes, result, tt.expected)
		}
	}
}

func TestWriteJSON(t *testing.T) {
	s := NewServer(Options{})

	w := httptest.NewRecorder()
	data := map[string]string{"key": "value"}
	s.writeJSON(w, http.StatusOK, data)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Expected Content-Type application/json, got %s", ct)
	}
}

func TestWriteSuccess(t *testing.T) {
	s := NewServer(Options{})

	w := httptest.NewRecorder()
	s.writeSuccess(w, map[string]string{"test": "data"})

	var resp Response
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if !resp.Success {
		t.Error("Expected success=true")
	}
}

func TestWriteError(t *testing.T) {
	s := NewServer(Options{})

	w := httptest.NewRecorder()
	s.writeError(w, http.StatusBadRequest, "test error")

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}

	var resp Response
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if resp.Success {
		t.Error("Expected success=false")
	}
	if resp.Error != "test error" {
		t.Errorf("Expected error 'test error', got '%s'", resp.Error)
	}
}

func TestHandlerMethodNotAllowed(t *testing.T) {
	s := NewServer(Options{})

	tests := []struct {
		handler func(http.ResponseWriter, *http.Request)
		method  string
	}{
		{s.handleCompact, http.MethodGet},
		{s.handleFlush, http.MethodGet},
		{s.handleSSTables, http.MethodPost},
		{s.handleLevels, http.MethodPost},
		{s.handleConfig, http.MethodGet},
		{s.handleProperties, http.MethodPost},
		{s.handleRange, http.MethodGet},
		{s.handleStats, http.MethodPost},
	}

	for _, tt := range tests {
		req := httptest.NewRequest(tt.method, "/test", nil)
		w := httptest.NewRecorder()
		tt.handler(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("Expected 405, got %d for method %s", w.Code, tt.method)
		}
	}
}

func TestHandlerNoEngine(t *testing.T) {
	s := NewServer(Options{}) // No engine

	handlers := []struct {
		name    string
		handler func(http.ResponseWriter, *http.Request)
		method  string
	}{
		{"compact", s.handleCompact, http.MethodPost},
		{"flush", s.handleFlush, http.MethodPost},
		{"sstables", s.handleSSTables, http.MethodGet},
		{"levels", s.handleLevels, http.MethodGet},
		{"properties", s.handleProperties, http.MethodGet},
		{"stats", s.handleStats, http.MethodGet},
	}

	for _, tt := range handlers {
		req := httptest.NewRequest(tt.method, "/admin/"+tt.name, nil)
		w := httptest.NewRecorder()
		tt.handler(w, req)

		if w.Code != http.StatusServiceUnavailable {
			t.Errorf("%s: Expected 503, got %d", tt.name, w.Code)
		}
	}
}

func TestHandleConfigNotConfigured(t *testing.T) {
	s := NewServer(Options{})

	req := httptest.NewRequest(http.MethodPost, "/admin/config", nil)
	w := httptest.NewRecorder()
	s.handleConfig(w, req)

	if w.Code != http.StatusNotImplemented {
		t.Errorf("Expected 501, got %d", w.Code)
	}
}

func TestHandleRangeInvalidBody(t *testing.T) {
	s := NewServer(Options{
		Engine: nil, // Will fail at engine check first
	})

	// First test with no engine
	req := httptest.NewRequest(http.MethodDelete, "/admin/range", bytes.NewBufferString("{}"))
	w := httptest.NewRecorder()
	s.handleRange(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503 with no engine, got %d", w.Code)
	}
}

func TestAuthMiddleware(t *testing.T) {
	s := NewServer(Options{
		AuthToken: "secret-token",
	})

	handler := s.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Test without token
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 without token, got %d", w.Code)
	}

	// Test with wrong token
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 with wrong token, got %d", w.Code)
	}

	// Test with correct token
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200 with correct token, got %d", w.Code)
	}
}

func TestAuthMiddlewareNoToken(t *testing.T) {
	s := NewServer(Options{}) // No auth token

	handler := s.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Should pass through without auth
	if w.Code != http.StatusOK {
		t.Errorf("Expected 200 when no auth configured, got %d", w.Code)
	}
}

func TestSetConfigReloadFn(t *testing.T) {
	s := NewServer(Options{})

	s.SetConfigReloadFn(func() error {
		return nil
	})

	// Trigger through handler (will fail because no engine, but function should be set)
	if s.configReloadFn == nil {
		t.Error("configReloadFn should be set")
	}
}
