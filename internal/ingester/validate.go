package ingester

import (
	"github.com/rmrobinson/weather-server/internal/types"
	"go.uber.org/zap"
)

type fieldBounds struct{ min, max float64 }

// fieldLimits maps each storage field name to its physical plausibility range.
// Values outside these bounds indicate a sensor failure, not an extreme reading.
var fieldLimits = map[string]fieldBounds{
	// Outdoor — world record extremes with margin
	"temp_c":       {-65, 65},
	"humidity_pct": {1, 100}, // 0 is a sensor failure floor; RH never reaches 0% outdoors

	// Indoor
	"temp_in_c":       {-10, 60},
	"humidity_in_pct": {1, 100},

	// Pressure — lowest ever ~870 hPa, highest ~1084 hPa
	"pressure_hpa":     {850, 1100},
	"pressure_abs_hpa": {850, 1100},

	// Wind — highest gust ever recorded ~113 m/s; 120 m/s gives margin
	"wind_speed_ms":     {0, 120},
	"wind_gust_ms":      {0, 120},
	"max_daily_gust_ms": {0, 120},
	"wind_dir_deg":      {0, 360},

	// Rain — floors are 0 (no rain is valid); ceilings are generous physical maxima
	"rain_mm_hr":      {0, 500},   // highest recorded rate ~305 mm/hr
	"rain_event_mm":   {0, 2000},
	"rain_hourly_mm":  {0, 500},
	"rain_daily_mm":   {0, 2000}, // highest ever single-day ~1825 mm
	"rain_weekly_mm":  {0, 5000},
	"rain_monthly_mm": {0, 10000},
	"rain_yearly_mm":  {0, 25000}, // wettest place on Earth ~12 000 mm/yr

	// Solar / UV
	"uv_index":  {0, 20},   // practical max ~16 at high-altitude tropics
	"solar_wm2": {0, 1500}, // solar constant ~1361 W/m²

	// Sensor health — wide margins to tolerate hardware variation
	"battery_v":   {0.5, 6.0},
	"capacitor_v": {0.5, 7.0},
}

// validateReading checks each received field against its physical bounds.
// Fields outside their bounds are removed from r.ReceivedFields and zeroed on
// the struct so that neither the store nor hub consumers see the bad value.
func validateReading(r *types.WeatherReading, logger *zap.Logger) {
	check := func(key string, val float64, zero func()) {
		if !r.ReceivedFields[key] {
			return
		}
		b, ok := fieldLimits[key]
		if !ok || (val >= b.min && val <= b.max) {
			return
		}
		logger.Warn("sensor value out of bounds, dropping field",
			zap.String("field", key),
			zap.Float64("value", val),
			zap.Float64("min", b.min),
			zap.Float64("max", b.max))
		delete(r.ReceivedFields, key)
		zero()
	}

	// Outdoor
	check("temp_c", r.TempC, func() { r.TempC = 0 })
	check("humidity_pct", r.HumidityPct, func() { r.HumidityPct = 0 })
	// Indoor
	check("temp_in_c", r.TempInC, func() { r.TempInC = 0 })
	check("humidity_in_pct", r.HumidityInPct, func() { r.HumidityInPct = 0 })
	// Pressure
	check("pressure_hpa", r.PressureHPa, func() { r.PressureHPa = 0 })
	check("pressure_abs_hpa", r.PressureAbsHPa, func() { r.PressureAbsHPa = 0 })
	// Wind
	check("wind_speed_ms", r.WindSpeedMs, func() { r.WindSpeedMs = 0 })
	check("wind_gust_ms", r.WindGustMs, func() { r.WindGustMs = 0 })
	check("max_daily_gust_ms", r.MaxDailyGustMs, func() { r.MaxDailyGustMs = 0 })
	check("wind_dir_deg", r.WindDirDeg, func() { r.WindDirDeg = 0 })
	// Rain
	check("rain_mm_hr", r.RainMmHr, func() { r.RainMmHr = 0 })
	check("rain_event_mm", r.RainEventMm, func() { r.RainEventMm = 0 })
	check("rain_hourly_mm", r.RainHourlyMm, func() { r.RainHourlyMm = 0 })
	check("rain_daily_mm", r.RainDailyMm, func() { r.RainDailyMm = 0 })
	check("rain_weekly_mm", r.RainWeeklyMm, func() { r.RainWeeklyMm = 0 })
	check("rain_monthly_mm", r.RainMonthlyMm, func() { r.RainMonthlyMm = 0 })
	check("rain_yearly_mm", r.RainYearlyMm, func() { r.RainYearlyMm = 0 })
	// Solar / UV
	check("uv_index", r.UVIndex, func() { r.UVIndex = 0 })
	check("solar_wm2", r.SolarWm2, func() { r.SolarWm2 = 0 })
	// Sensor health
	check("battery_v", r.BatteryV, func() { r.BatteryV = 0 })
	check("capacitor_v", r.CapacitorV, func() { r.CapacitorV = 0 })
}
