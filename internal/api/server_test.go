package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPSKMiddleware_Disabled(t *testing.T) {
	mw := PSKMiddleware("")
	called := false
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
	if !called {
		t.Error("handler should be called when PSK is disabled")
	}
}

func TestPSKMiddleware_ValidKey(t *testing.T) {
	mw := PSKMiddleware("secret")
	called := false
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "psk secret")
	handler.ServeHTTP(httptest.NewRecorder(), req)
	if !called {
		t.Error("handler should be called for valid PSK")
	}
}

func TestPSKMiddleware_InvalidKey(t *testing.T) {
	mw := PSKMiddleware("secret")
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler should not be called for invalid PSK")
	}))
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "psk wrong")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rr.Code)
	}
}

func TestHandleReadings_MissingStart(t *testing.T) {
	s := &Server{logger: noopLogger()}
	req := httptest.NewRequest("GET", "/api/v1/readings", nil)
	rr := httptest.NewRecorder()
	s.handleReadings(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestHandleReadings_InvalidStart(t *testing.T) {
	s := &Server{logger: noopLogger()}
	req := httptest.NewRequest("GET", "/api/v1/readings?start=notadate", nil)
	rr := httptest.NewRecorder()
	s.handleReadings(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestHandleReadings_InvalidResolution(t *testing.T) {
	s := &Server{logger: noopLogger()}
	req := httptest.NewRequest("GET", "/api/v1/readings?start=2024-01-01T00:00:00Z&resolution=bad", nil)
	rr := httptest.NewRecorder()
	s.handleReadings(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestHandleReadings_InvalidField(t *testing.T) {
	s := &Server{logger: noopLogger()}
	req := httptest.NewRequest("GET", "/api/v1/readings?start=2024-01-01T00:00:00Z&fields=evil_field", nil)
	rr := httptest.NewRecorder()
	s.handleReadings(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for unknown field, got %d", rr.Code)
	}
}

