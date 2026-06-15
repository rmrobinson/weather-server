package ingester

import (
	"testing"

	"github.com/rmrobinson/weather-server/internal/types"
	"go.uber.org/zap"
)

func TestValidateReading_InBoundsPassThrough(t *testing.T) {
	r := types.WeatherReading{
		TempC: 14.1, HumidityPct: 81,
		TempInC: 22.0, HumidityInPct: 55,
		PressureHPa: 1013.0, PressureAbsHPa: 1012.0,
		WindSpeedMs: 1.0, WindGustMs: 2.0, MaxDailyGustMs: 5.0, WindDirDeg: 180,
		RainMmHr: 0, RainEventMm: 5.0, RainHourlyMm: 1.0,
		RainDailyMm: 3.0, RainWeeklyMm: 10.0, RainMonthlyMm: 24.0, RainYearlyMm: 247.0,
		UVIndex: 3, SolarWm2: 400,
		BatteryV: 3.28, CapacitorV: 5.3,
		ReceivedFields: map[string]bool{
			"temp_c": true, "humidity_pct": true,
			"temp_in_c": true, "humidity_in_pct": true,
			"pressure_hpa": true, "pressure_abs_hpa": true,
			"wind_speed_ms": true, "wind_gust_ms": true, "max_daily_gust_ms": true, "wind_dir_deg": true,
			"rain_mm_hr": true, "rain_event_mm": true, "rain_hourly_mm": true,
			"rain_daily_mm": true, "rain_weekly_mm": true, "rain_monthly_mm": true, "rain_yearly_mm": true,
			"uv_index": true, "solar_wm2": true,
			"battery_v": true, "capacitor_v": true,
		},
	}
	before := len(r.ReceivedFields)
	validateReading(&r, zap.NewNop())
	if len(r.ReceivedFields) != before {
		t.Errorf("in-bounds reading lost %d fields after validation", before-len(r.ReceivedFields))
	}
}

func TestValidateReading_OutOfBoundsDropped(t *testing.T) {
	cases := []struct {
		name      string
		field     string
		badVal    float64
		setup     func(*types.WeatherReading)
		getVal    func(*types.WeatherReading) float64
	}{
		{
			name: "humidity zero (sensor failure floor)",
			field: "humidity_pct", badVal: 0,
			setup:  func(r *types.WeatherReading) { r.HumidityPct = 0 },
			getVal: func(r *types.WeatherReading) float64 { return r.HumidityPct },
		},
		{
			name: "temp below physical minimum",
			field: "temp_c", badVal: -80,
			setup:  func(r *types.WeatherReading) { r.TempC = -80 },
			getVal: func(r *types.WeatherReading) float64 { return r.TempC },
		},
		{
			name: "wind direction out of range",
			field: "wind_dir_deg", badVal: 400,
			setup:  func(r *types.WeatherReading) { r.WindDirDeg = 400 },
			getVal: func(r *types.WeatherReading) float64 { return r.WindDirDeg },
		},
		{
			name: "pressure too low",
			field: "pressure_hpa", badVal: 500,
			setup:  func(r *types.WeatherReading) { r.PressureHPa = 500 },
			getVal: func(r *types.WeatherReading) float64 { return r.PressureHPa },
		},
		{
			name: "indoor humidity zero",
			field: "humidity_in_pct", badVal: 0,
			setup:  func(r *types.WeatherReading) { r.HumidityInPct = 0 },
			getVal: func(r *types.WeatherReading) float64 { return r.HumidityInPct },
		},
		{
			name: "rain rate above physical maximum",
			field: "rain_mm_hr", badVal: 9999,
			setup:  func(r *types.WeatherReading) { r.RainMmHr = 9999 },
			getVal: func(r *types.WeatherReading) float64 { return r.RainMmHr },
		},
		{
			name: "uv index above maximum",
			field: "uv_index", badVal: 50,
			setup:  func(r *types.WeatherReading) { r.UVIndex = 50 },
			getVal: func(r *types.WeatherReading) float64 { return r.UVIndex },
		},
		{
			name: "battery voltage too low",
			field: "battery_v", badVal: 0,
			setup:  func(r *types.WeatherReading) { r.BatteryV = 0 },
			getVal: func(r *types.WeatherReading) float64 { return r.BatteryV },
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := types.WeatherReading{
				ReceivedFields: map[string]bool{c.field: true},
			}
			c.setup(&r)
			validateReading(&r, zap.NewNop())
			if r.ReceivedFields[c.field] {
				t.Errorf("field %q still in ReceivedFields after out-of-bounds value %.2f", c.field, c.badVal)
			}
			if got := c.getVal(&r); got != 0 {
				t.Errorf("field %q struct value not zeroed: got %.2f", c.field, got)
			}
		})
	}
}

func TestValidateReading_NotReceivedFieldIgnored(t *testing.T) {
	// A field with an out-of-range value that was never received must not be touched.
	r := types.WeatherReading{
		TempC:          -80, // would be out of bounds if received
		ReceivedFields: map[string]bool{},
	}
	validateReading(&r, zap.NewNop())
	if r.TempC != -80 {
		t.Error("validateReading modified a field that was not in ReceivedFields")
	}
}

func TestValidateReading_IncidentReplay(t *testing.T) {
	// Reproduces the 2026-06-15 01:13Z WH90 sensor failure:
	// temp_c = -17.8 (0°F floor), humidity_pct = 0 (sensor failure floor).
	r := types.WeatherReading{
		TempC:       -17.8,
		HumidityPct: 0,
		ReceivedFields: map[string]bool{
			"temp_c":       true,
			"humidity_pct": true,
		},
	}
	validateReading(&r, zap.NewNop())

	// humidity_pct = 0 is outside [1, 100] → must be dropped.
	if r.ReceivedFields["humidity_pct"] {
		t.Error("humidity_pct should have been dropped (value = 0)")
	}
	if r.HumidityPct != 0 {
		t.Errorf("HumidityPct not zeroed: got %v", r.HumidityPct)
	}

	// temp_c = -17.8 is within [-65, 65] → must be retained.
	if !r.ReceivedFields["temp_c"] {
		t.Error("temp_c should have been retained (value -17.8 is within physical bounds)")
	}
	if r.TempC != -17.8 {
		t.Errorf("TempC was modified unexpectedly: got %v", r.TempC)
	}

	// With humidity_pct absent, flush() will not compute dew_point_c.
	// Verify the guard condition directly.
	if r.ReceivedFields["temp_c"] && r.ReceivedFields["humidity_pct"] {
		t.Error("both temp_c and humidity_pct are marked received — dew point would be computed from bad inputs")
	}
}
