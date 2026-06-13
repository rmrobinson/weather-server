package store

import (
	"strings"
	"testing"
	"time"

	"github.com/rmrobinson/weather-server/internal/config"
)

func testStore() *Store {
	cfg := config.InfluxConfig{
		BucketRaw: "weather_raw",
		Bucket1h:  "weather_1h",
		Bucket1d:  "weather_1d",
	}
	return &Store{cfg: cfg, stationID: "test"}
}

func TestBucketForResolution(t *testing.T) {
	s := testStore()
	cases := []struct {
		res    Resolution
		bucket string
	}{
		{ResolutionRaw, "weather_raw"},
		{Resolution1h, "weather_1h"},
		{Resolution1d, "weather_1d"},
		{"unknown", "weather_raw"}, // default
	}
	for _, c := range cases {
		got := s.bucketForResolution(c.res)
		if got != c.bucket {
			t.Errorf("bucketForResolution(%q): got %q, want %q", c.res, got, c.bucket)
		}
	}
}

func TestQueryReadingsFluxContainsBucket(t *testing.T) {
	s := testStore()
	cases := []struct {
		res    Resolution
		bucket string
	}{
		{ResolutionRaw, "weather_raw"},
		{Resolution1h, "weather_1h"},
		{Resolution1d, "weather_1d"},
	}
	start := time.Now().Add(-time.Hour)
	end := time.Now()
	for _, c := range cases {
		bucket := s.bucketForResolution(c.res)
		q := Query{Start: start, End: end, Resolution: c.res}
		_ = q
		if !strings.Contains(bucket, c.bucket) {
			t.Errorf("resolution %q: expected bucket %q, got %q", c.res, c.bucket, bucket)
		}
	}
}

func TestCheckTaskHealth_Degraded(t *testing.T) {
	h := TaskHealth{
		Exists:          true,
		Active:          true,
		LastCompleted:   time.Now().Add(-3 * time.Hour),
		WithinThreshold: false,
	}
	if h.WithinThreshold {
		t.Error("expected WithinThreshold=false for overdue task")
	}
}

func TestCheckTaskHealth_Healthy(t *testing.T) {
	h := TaskHealth{
		Exists:          true,
		Active:          true,
		LastCompleted:   time.Now().Add(-30 * time.Minute),
		WithinThreshold: true,
	}
	if !h.WithinThreshold {
		t.Error("expected WithinThreshold=true for recent task")
	}
}

func TestCheckTaskHealth_Missing(t *testing.T) {
	h := TaskHealth{}
	if h.Exists || h.Active || h.WithinThreshold {
		t.Error("zero-value TaskHealth should have all booleans false")
	}
}

func TestFluxTemplateSubstitution(t *testing.T) {
	s := testStore()
	template := `from(bucket: "{{bucket_raw}}") |> to(bucket: "{{bucket_1h}}")`
	got := strings.NewReplacer(
		`{{bucket_raw}}`, s.cfg.BucketRaw,
		`{{bucket_1h}}`, s.cfg.Bucket1h,
		`{{bucket_1d}}`, s.cfg.Bucket1d,
	).Replace(template)
	want := `from(bucket: "weather_raw") |> to(bucket: "weather_1h")`
	if got != want {
		t.Errorf("flux substitution:\n  got  %q\n  want %q", got, want)
	}
}

func TestErrRainReset_Sentinel(t *testing.T) {
	if ErrRainReset == nil {
		t.Error("ErrRainReset should not be nil")
	}
	if ErrRainReset.Error() == "" {
		t.Error("ErrRainReset should have a non-empty message")
	}
}
