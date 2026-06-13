package types

import "testing"

func TestDeriveCondition(t *testing.T) {
	cases := []struct {
		name string
		r    WeatherReading
		want string
	}{
		// Precipitation — priority over sky cover.
		{"snow: rain + sub-zero", WeatherReading{RainMmHr: 0.5, TempC: -3}, "Snow"},
		{"freezing rain: rain + near-freezing", WeatherReading{RainMmHr: 0.5, TempC: 1}, "Freezing Rain"},
		{"freezing rain: boundary at 2°C", WeatherReading{RainMmHr: 1, TempC: 1.9}, "Freezing Rain"},
		{"light rain: < 2 mm/hr", WeatherReading{RainMmHr: 0.8, TempC: 15}, "Light Rain"},
		{"rain: 2–10 mm/hr", WeatherReading{RainMmHr: 5, TempC: 15}, "Rain"},
		{"heavy rain: ≥ 10 mm/hr", WeatherReading{RainMmHr: 15, TempC: 15}, "Heavy Rain"},
		{"heavy rain: boundary at 10 mm/hr", WeatherReading{RainMmHr: 10, TempC: 20}, "Heavy Rain"},

		// Night — sun below the effective horizon.
		{"night: ClearSkyWm2 = 0", WeatherReading{ClearSkyWm2: 0}, "Night"},
		{"night: ClearSkyWm2 < 1", WeatherReading{ClearSkyWm2: 0.5}, "Night"},

		// Daytime sky conditions via clear-sky index.
		{"sunny: Kt > 0.8", WeatherReading{ClearSkyWm2: 600, ClearSkyIdx: 0.9}, "Sunny"},
		{"sunny: Kt exactly 0.81", WeatherReading{ClearSkyWm2: 600, ClearSkyIdx: 0.81}, "Sunny"},
		{"mostly sunny: Kt 0.6–0.8", WeatherReading{ClearSkyWm2: 600, ClearSkyIdx: 0.7}, "Mostly Sunny"},
		{"partly cloudy: Kt 0.3–0.6", WeatherReading{ClearSkyWm2: 600, ClearSkyIdx: 0.5}, "Partly Cloudy"},
		{"mostly cloudy: Kt 0.1–0.3", WeatherReading{ClearSkyWm2: 600, ClearSkyIdx: 0.2}, "Mostly Cloudy"},
		{"overcast: Kt ≤ 0.1", WeatherReading{ClearSkyWm2: 600, ClearSkyIdx: 0.05}, "Overcast"},
		{"overcast: Kt = 0", WeatherReading{ClearSkyWm2: 600, ClearSkyIdx: 0}, "Overcast"},

		// Precipitation beats night.
		{"rain at night", WeatherReading{RainMmHr: 3, TempC: 10, ClearSkyWm2: 0}, "Rain"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := DeriveCondition(c.r)
			if got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}
