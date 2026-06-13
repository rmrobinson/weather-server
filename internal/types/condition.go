package types

// DeriveCondition returns a human-readable summary of the current weather
// situation based on the other fields in the reading.
//
// Priority:
//  1. Precipitation (rain rate > 0) — classified by intensity and temperature.
//  2. Night (sun below the horizon — ClearSkyWm2 < 1 W/m²).
//  3. Daytime sky — classified by the clear-sky index (Kt).
func DeriveCondition(r WeatherReading) string {
	// Precipitation takes priority over sky cover.
	if r.RainMmHr > 0 {
		switch {
		case r.TempC < 0:
			return "Snow"
		case r.TempC < 2:
			return "Freezing Rain"
		case r.RainMmHr >= 10:
			return "Heavy Rain"
		case r.RainMmHr >= 2:
			return "Rain"
		default:
			return "Light Rain"
		}
	}

	// Sun is below the effective horizon; solar-based sky cover is unavailable.
	if r.ClearSkyWm2 < 1 {
		return "Night"
	}

	// Daytime sky conditions via the clear-sky index (Kt).
	// Breakpoints follow the Kasten-Czeplak thresholds used in CloudCover.
	switch {
	case r.ClearSkyIdx > 0.8:
		return "Sunny"
	case r.ClearSkyIdx > 0.6:
		return "Mostly Sunny"
	case r.ClearSkyIdx > 0.3:
		return "Partly Cloudy"
	case r.ClearSkyIdx > 0.1:
		return "Mostly Cloudy"
	default:
		return "Overcast"
	}
}
