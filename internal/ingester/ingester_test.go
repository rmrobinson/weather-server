package ingester

import (
	"math"
	"testing"
	"time"

	"github.com/rmrobinson/weather-server/internal/config"
	"github.com/rmrobinson/weather-server/internal/types"
)

// fakePublisher captures published readings for assertions.
type fakePublisher struct{ ch chan types.WeatherReading }

func (f *fakePublisher) Publish(r types.WeatherReading) { f.ch <- r }

// fakeMsg implements mqtt.Message for tests.
type fakeMsg struct {
	topic   string
	payload []byte
}

func (m *fakeMsg) Duplicate() bool   { return false }
func (m *fakeMsg) Qos() byte         { return 0 }
func (m *fakeMsg) Retained() bool    { return false }
func (m *fakeMsg) Topic() string     { return m.topic }
func (m *fakeMsg) MessageID() uint16 { return 0 }
func (m *fakeMsg) Payload() []byte   { return m.payload }
func (m *fakeMsg) Ack()              {}

func newTestIngester(prefix string) (*Ingester, *fakePublisher) {
	pub := &fakePublisher{ch: make(chan types.WeatherReading, 8)}
	ing := &Ingester{
		cfg:    config.MQTTConfig{TopicPrefix: prefix},
		hub:    pub,
		logger: noopLogger(),
	}
	return ing, pub
}

// send delivers a single topic/value pair to the ingester.
func send(ing *Ingester, field, val string) {
	topic := ing.cfg.TopicPrefix + "/" + field
	ing.handleMessage(nil, &fakeMsg{topic: topic, payload: []byte(val)})
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestApplyField_KnownFields(t *testing.T) {
	r := &types.WeatherReading{}
	fields := map[string]string{
		// Outdoor
		"tempOutC":    "24.0",
		"humidityOut": "53",
		// Indoor
		"tempInC":    "26.4",
		"humidityIn": "49",
		// Pressure
		"baromRelHpa": "971.6",
		"baromAbsHpa": "971.5",
		// Wind
		"windSpdMps":      "1.4",
		"windGustMps":     "2.8",
		"maxDailyGustMps": "7.6",
		"windDir":         "266",
		// Rain
		"rainRealTime": "0.500",
		"rainEvent":    "3.200",
		"rainHourly":   "0.800",
		"rainDaily":    "1.400",
		"rainWeekly":   "5.600",
		"rainMonthly":  "22.100",
		"rainSeason":   "45.000",
		"rainYearly":   "9.748",
		// Solar / UV
		"uvIndex":        "3",
		"solarRadiation": "76.24",
		// Sensor health
		"wh90Battery": "3.28",
		"capacVolt":   "5.30",
	}
	for f, v := range fields {
		if err := applyField(r, f, v); err != nil {
			t.Errorf("applyField(%q, %q): unexpected error: %v", f, v, err)
		}
	}

	expectClose(t, "TempC", r.TempC, 24.0)
	expectClose(t, "HumidityPct", r.HumidityPct, 53.0)
	expectClose(t, "TempInC", r.TempInC, 26.4)
	expectClose(t, "HumidityInPct", r.HumidityInPct, 49.0)
	expectClose(t, "PressureHPa", r.PressureHPa, 971.6)
	expectClose(t, "PressureAbsHPa", r.PressureAbsHPa, 971.5)
	expectClose(t, "WindSpeedMs", r.WindSpeedMs, 1.4)
	expectClose(t, "WindGustMs", r.WindGustMs, 2.8)
	expectClose(t, "MaxDailyGustMs", r.MaxDailyGustMs, 7.6)
	expectClose(t, "WindDirDeg", r.WindDirDeg, 266.0)
	expectClose(t, "RainMmHr", r.RainMmHr, 0.5)
	expectClose(t, "RainEventMm", r.RainEventMm, 3.2)
	expectClose(t, "RainHourlyMm", r.RainHourlyMm, 0.8)
	expectClose(t, "RainDailyMm", r.RainDailyMm, 1.4)
	expectClose(t, "RainWeeklyMm", r.RainWeeklyMm, 5.6)
	expectClose(t, "RainMonthlyMm", r.RainMonthlyMm, 22.1)
	expectClose(t, "RainSeasonMm", r.RainSeasonMm, 45.0)
	expectClose(t, "RainYearlyMm", r.RainYearlyMm, 9.748)
	expectClose(t, "UVIndex", r.UVIndex, 3.0)
	expectClose(t, "SolarWm2", r.SolarWm2, 76.24)
	expectClose(t, "BatteryV", r.BatteryV, 3.28)
	expectClose(t, "CapacitorV", r.CapacitorV, 5.30)
}

func TestApplyField_UnknownField_ReturnsError(t *testing.T) {
	if err := applyField(&types.WeatherReading{}, "passkey", "ABC123"); err == nil {
		t.Error("expected error for unknown field")
	}
}

func TestApplyField_InvalidFloat_ReturnsError(t *testing.T) {
	if err := applyField(&types.WeatherReading{}, "tempOutC", "notanumber"); err == nil {
		t.Error("expected error for non-numeric value")
	}
}

func TestDebounce_EmitsAfterBurst(t *testing.T) {
	ing, pub := newTestIngester("ws90")

	send(ing, "time", "2026-06-12 10:00:00")
	send(ing, "tempOutC", "24.0")
	send(ing, "humidityOut", "53")
	send(ing, "baromRelHpa", "971.6")

	// Nothing emitted yet — debounce hasn't fired.
	select {
	case r := <-pub.ch:
		t.Fatalf("unexpected early emit: %+v", r)
	case <-time.After(50 * time.Millisecond):
	}

	// Wait for debounce to fire (debounceDelay + slack).
	select {
	case r := <-pub.ch:
		expectClose(t, "TempC", r.TempC, 24.0)
		expectClose(t, "HumidityPct", r.HumidityPct, 53.0)
		expectClose(t, "PressureHPa", r.PressureHPa, 971.6)
		if r.Timestamp.IsZero() {
			t.Error("Timestamp should not be zero")
		}
	case <-time.After(debounceDelay + time.Second):
		t.Fatal("timed out waiting for debounced reading")
	}
}

func TestDebounce_SecondBurstProducesSecondReading(t *testing.T) {
	ing, pub := newTestIngester("ws90")

	send(ing, "time", "2026-06-12 10:00:00")
	send(ing, "tempOutC", "20.0")

	// Wait for first emit.
	select {
	case <-pub.ch:
	case <-time.After(debounceDelay + time.Second):
		t.Fatal("timed out waiting for first reading")
	}

	// Second burst.
	send(ing, "time", "2026-06-12 10:01:00")
	send(ing, "tempOutC", "21.0")

	select {
	case r := <-pub.ch:
		expectClose(t, "TempC from second burst", r.TempC, 21.0)
	case <-time.After(debounceDelay + time.Second):
		t.Fatal("timed out waiting for second reading")
	}
}

func TestFeelsLike(t *testing.T) {
	cases := []struct {
		name          string
		tempC, dewC   float64
		windMs        float64
		wantApprox    float64
		toleranceC    float64
	}{
		// Wind chill (T ≤ 0 °C, V > 4.8 km/h — EC published range).
		// EC table: -10 °C, 20 km/h → -18 °C wind chill.
		{"wind chill -10C 20kmh", -10, -15, 20.0 / 3.6, -18.0, 0.5},
		// EC formula: 0 °C, 30 km/h → 13.12 - 11.37×(30^0.16) ≈ -6.5 °C.
		{"wind chill 0C 30kmh", 0, -5, 30.0 / 3.6, -6.5, 0.5},
		// Above 0 °C, no wind chill regardless of wind speed → returns T.
		{"above freezing no wind chill", 5, 0, 20.0 / 3.6, 5.0, 0.01},
		// No wind chill when wind ≤ 4.8 km/h.
		{"calm wind returns T", -5, -10, 1.0 / 3.6, -5.0, 0.01},

		// Humidex (T ≥ 20 °C).
		// At 30 °C, dew point 20 °C: humidex ≈ 37 °C.
		{"humidex 30C dewp20", 30, 20, 0, 37.4, 0.5},
		// At 25 °C, dew point 15 °C: humidex ≈ 29 °C.
		{"humidex 25C dewp15", 25, 15, 0, 29.0, 1.0},
		// Dry air: humidex ≤ temp → returns temp unchanged.
		{"humidex dry air returns T", 20, -5, 0, 20.0, 0.1},

		// Neither regime → returns T.
		{"neutral zone 15C", 15, 10, 10.0 / 3.6, 15.0, 0.01},
		{"neutral zone 11C no wind", 11, 5, 0, 11.0, 0.01},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := feelsLike(c.tempC, c.dewC, c.windMs)
			if math.Abs(got-c.wantApprox) > c.toleranceC {
				t.Errorf("feelsLike(%.1f°C, dp=%.1f°C, wind=%.2fm/s) = %.2f, want ≈%.1f±%.1f",
					c.tempC, c.dewC, c.windMs, got, c.wantApprox, c.toleranceC)
			}
		})
	}
}

func TestDewPoint(t *testing.T) {
	cases := []struct{ tempC, rh, wantDp float64 }{
		{25, 60, 16.7},  // warm, moderate humidity
		{0, 100, 0.0},   // freezing, saturated → dew point = air temp
		{20, 50, 9.3},   // typical indoor conditions
		{30, 90, 28.2},  // hot and humid
	}
	for _, c := range cases {
		got := dewPoint(c.tempC, c.rh)
		if math.Abs(got-c.wantDp) > 0.2 {
			t.Errorf("dewPoint(%.0f°C, %.0f%%) = %.2f, want %.1f", c.tempC, c.rh, got, c.wantDp)
		}
	}
}

func expectClose(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 0.001 {
		t.Errorf("%s: got %.4f, want %.4f", name, got, want)
	}
}
