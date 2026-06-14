package types

import "time"

type WeatherReading struct {
	Timestamp time.Time `json:"timestamp"`

	// Outdoor conditions
	TempC       float64 `json:"temp_c"`
	HumidityPct float64 `json:"humidity_pct"`

	// Indoor conditions
	TempInC      float64 `json:"temp_in_c"`
	HumidityInPct float64 `json:"humidity_in_pct"`

	// Pressure
	PressureHPa    float64 `json:"pressure_hpa"`
	PressureAbsHPa float64 `json:"pressure_abs_hpa"`

	// Wind
	WindSpeedMs    float64 `json:"wind_speed_ms"`
	WindGustMs     float64 `json:"wind_gust_ms"`
	MaxDailyGustMs float64 `json:"max_daily_gust_ms"`
	WindDirDeg     float64 `json:"wind_dir_deg"`

	// Rain
	RainMmHr      float64 `json:"rain_mm_hr"`
	RainEventMm   float64 `json:"rain_event_mm"`
	RainHourlyMm  float64 `json:"rain_hourly_mm"`
	RainDailyMm   float64 `json:"rain_daily_mm"`
	RainWeeklyMm  float64 `json:"rain_weekly_mm"`
	RainMonthlyMm float64 `json:"rain_monthly_mm"`
	RainYearlyMm  float64 `json:"rain_yearly_mm"`

	// Derived atmospheric
	DewPointC float64 `json:"dew_point_c"` // Magnus formula from temp + humidity

	// Solar / UV
	UVIndex     float64 `json:"uv_index"`
	SolarWm2    float64 `json:"solar_wm2"`
	ClearSkyWm2 float64 `json:"clear_sky_wm2"`  // theoretical clear-sky GHI
	ClearSkyIdx float64 `json:"clear_sky_index"` // Kt = measured / clear-sky; -1 at night
	CloudCovPct float64 `json:"cloud_cover_pct"` // 0–100 %; -1 when sun below horizon

	// Sensor health
	BatteryV   float64 `json:"battery_v"`
	CapacitorV float64 `json:"capacitor_v"`

	// Derived situational
	Condition   string  `json:"condition"`    // DeriveCondition; never persisted to InfluxDB
	FeelsLikeC  float64 `json:"feels_like_c"` // EC wind-chill / humidex; stored
}
