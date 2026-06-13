package api

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rmrobinson/weather-server/internal/hub"
	"github.com/rmrobinson/weather-server/internal/ingester"
	"github.com/rmrobinson/weather-server/internal/store"
	"go.uber.org/zap"
)

// validReadingFields is the allowlist of field names accepted in ?fields=.
// Prevents Flux injection from untrusted query parameters.
var validReadingFields = map[string]bool{
	// Outdoor
	"temp_c": true, "humidity_pct": true,
	// Indoor
	"temp_in_c": true, "humidity_in_pct": true,
	// Pressure
	"pressure_hpa": true, "pressure_abs_hpa": true,
	// Wind
	"wind_speed_ms": true, "wind_gust_ms": true, "max_daily_gust_ms": true, "wind_dir_deg": true,
	// Rain
	"rain_mm_hr": true, "rain_event_mm": true, "rain_hourly_mm": true,
	"rain_daily_mm": true, "rain_weekly_mm": true, "rain_monthly_mm": true,
	"rain_season_mm": true, "rain_yearly_mm": true,
	// Derived atmospheric
	"dew_point_c": true, "feels_like_c": true,
	// Solar / UV + derived cloud cover
	"uv_index": true, "solar_wm2": true,
	"clear_sky_wm2": true, "clear_sky_index": true, "cloud_cover_pct": true,
	// Sensor health
	"battery_v": true, "capacitor_v": true,
}

type Server struct {
	store    *store.Store
	hub      *hub.Hub
	ingester *ingester.Ingester
	psk      string
	logger   *zap.Logger
}

func New(s *store.Store, h *hub.Hub, ing *ingester.Ingester, psk string, logger *zap.Logger) *Server {
	return &Server{store: s, hub: h, ingester: ing, psk: psk, logger: logger}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	protected := PSKMiddleware(s.psk)

	mux.Handle("GET /api/v1/readings", protected(http.HandlerFunc(s.handleReadings)))
	mux.Handle("GET /api/v1/readings/latest", protected(http.HandlerFunc(s.handleLatest)))
	mux.Handle("GET /api/v1/stream/sse", protected(http.HandlerFunc(s.handleSSE)))
	mux.HandleFunc("GET /healthz", s.handleHealth)

	return mux
}

func (s *Server) handleReadings(w http.ResponseWriter, r *http.Request) {
	startStr := r.URL.Query().Get("start")
	if startStr == "" {
		http.Error(w, "missing required param: start", http.StatusBadRequest)
		return
	}
	start, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		http.Error(w, "invalid start: must be RFC3339", http.StatusBadRequest)
		return
	}

	end := time.Now()
	if endStr := r.URL.Query().Get("end"); endStr != "" {
		end, err = time.Parse(time.RFC3339, endStr)
		if err != nil {
			http.Error(w, "invalid end: must be RFC3339", http.StatusBadRequest)
			return
		}
	}

	res := store.Resolution(r.URL.Query().Get("resolution"))
	if res == "" {
		res = store.ResolutionRaw
	}
	switch res {
	case store.ResolutionRaw, store.Resolution1h, store.Resolution1d:
	default:
		http.Error(w, fmt.Sprintf("invalid resolution: %q (must be raw|1h|1d)", res), http.StatusBadRequest)
		return
	}

	var fields []string
	if f := r.URL.Query().Get("fields"); f != "" {
		fields = strings.Split(f, ",")
		for _, field := range fields {
			if !validReadingFields[field] {
				http.Error(w, fmt.Sprintf("invalid field: %q", field), http.StatusBadRequest)
				return
			}
		}
	}

	readings, err := s.store.QueryReadings(r.Context(), store.Query{
		Start:      start,
		End:        end,
		Resolution: res,
		Fields:     fields,
	})
	if err != nil {
		s.logger.Error("query readings failed", zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(readings); err != nil {
		s.logger.Error("encode readings failed", zap.Error(err))
	}
}

func (s *Server) handleLatest(w http.ResponseWriter, r *http.Request) {
	reading, err := s.store.QueryLatest(r.Context())
	if err != nil {
		s.logger.Error("query latest failed", zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if reading == nil {
		http.Error(w, "no data", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(reading); err != nil {
		s.logger.Error("encode latest failed", zap.Error(err))
	}
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	id := uuid.New().String()
	// Subscribe before querying latest so no reading is missed between the
	// query and the loop.
	sub := s.hub.Subscribe(id)
	defer s.hub.Unsubscribe(id)

	// Send the most recent stored reading immediately so the client doesn't
	// have to wait for the next MQTT cycle.
	if latest, err := s.store.QueryLatest(r.Context()); err != nil {
		s.logger.Warn("sse: could not fetch latest reading", zap.Error(err))
	} else if latest != nil {
		if data, err := json.Marshal(latest); err == nil {
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}

	for {
		select {
		case reading := <-sub.Ch:
			data, err := json.Marshal(reading)
			if err != nil {
				s.logger.Error("sse marshal failed", zap.Error(err))
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

type healthResponse struct {
	Status string                 `json:"status"`
	Checks map[string]checkResult `json:"checks"`
}

type checkResult struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	checks := make(map[string]checkResult)
	degraded := false

	if s.ingester != nil {
		lr := s.ingester.LastReceived()
		switch {
		case lr.IsZero():
			// No reading yet — service is starting up, not degraded.
			checks["mqtt"] = checkResult{Status: "ok", Message: "waiting for first reading"}
		case time.Since(lr) > 5*time.Minute:
			checks["mqtt"] = checkResult{Status: "fail", Message: "no reading in last 5 minutes"}
			degraded = true
		default:
			checks["mqtt"] = checkResult{Status: "ok"}
		}
	}

	if err := s.store.LastWriteError(); err != nil {
		checks["influx_write"] = checkResult{Status: "fail", Message: err.Error()}
		degraded = true
	} else {
		checks["influx_write"] = checkResult{Status: "ok"}
	}

	taskHealth, err := s.store.CheckTaskHealth(r.Context())
	if err != nil {
		checks["downsample_1h"] = checkResult{Status: "fail", Message: err.Error()}
		checks["downsample_1d"] = checkResult{Status: "fail", Message: err.Error()}
		degraded = true
	} else {
		for _, name := range []string{"downsample_1h", "downsample_1d"} {
			h := taskHealth[name]
			if !h.Exists || !h.Active || !h.WithinThreshold {
				msg := "task not running or overdue"
				if !h.Exists {
					msg = "task not found"
				}
				checks[name] = checkResult{Status: "fail", Message: msg}
				degraded = true
			} else {
				checks[name] = checkResult{Status: "ok"}
			}
		}
	}

	resp := healthResponse{
		Status: "ok",
		Checks: checks,
	}
	if degraded {
		resp.Status = "degraded"
	}

	// Set Content-Type and status code before writing body.
	w.Header().Set("Content-Type", "application/json")
	if degraded {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error("encode health failed", zap.Error(err))
	}
}

func PSKMiddleware(psk string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if psk == "" {
				next.ServeHTTP(w, r)
				return
			}
			want := []byte("psk " + psk)
			got := []byte(r.Header.Get("Authorization"))
			if subtle.ConstantTimeCompare(got, want) != 1 {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
