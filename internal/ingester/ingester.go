package ingester

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rmrobinson/weather-server/internal/config"
	"github.com/rmrobinson/weather-server/internal/hub"
	"github.com/rmrobinson/weather-server/internal/solar"
	"github.com/rmrobinson/weather-server/internal/types"
	"go.uber.org/zap"
)

// publisher is satisfied by *hub.Hub; defined here to allow test injection.
type publisher interface {
	Publish(types.WeatherReading)
}

const debounceDelay = 2 * time.Second

type Ingester struct {
	hub    publisher
	cfg    config.MQTTConfig
	lat    float64
	lon    float64
	logger *zap.Logger

	mu           sync.Mutex
	lastReceived time.Time
	pending      types.WeatherReading // accumulates fields from the current burst
	hasPending   bool
	debounce     *time.Timer
	debounceGen  uint64 // incremented on each reset; stale timer callbacks are no-ops
}

func New(cfg config.MQTTConfig, lat, lon float64, h *hub.Hub, logger *zap.Logger) *Ingester {
	return &Ingester{hub: h, cfg: cfg, lat: lat, lon: lon, logger: logger}
}

func (i *Ingester) LastReceived() time.Time {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.lastReceived
}

func (i *Ingester) Run(ctx context.Context) {
	delay := 2 * time.Second
	const maxDelay = 60 * time.Second

	for {
		if ctx.Err() != nil {
			return
		}
		connLost := make(chan error, 1)
		client, err := i.connect(ctx, connLost)
		if err != nil {
			i.logger.Warn("mqtt connect failed, retrying", zap.Error(err), zap.Duration("delay", delay))
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}
			continue
		}
		delay = 2 * time.Second

		select {
		case <-ctx.Done():
			client.Disconnect(250)
			return
		case err := <-connLost:
			i.logger.Warn("mqtt connection lost, reconnecting", zap.Error(err))
			client.Disconnect(0)
			// loop back to reconnect with fresh backoff
		}
	}
}

func (i *Ingester) connect(ctx context.Context, connLost chan<- error) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().
		AddBroker(i.cfg.Broker).
		SetClientID(i.cfg.ClientID).
		SetAutoReconnect(false).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			select {
			case connLost <- err:
			default:
			}
		})

	client := mqtt.NewClient(opts)
	connectToken := client.Connect()
	select {
	case <-connectToken.Done():
		if err := connectToken.Error(); err != nil {
			return nil, err
		}
	case <-ctx.Done():
		client.Disconnect(0)
		return nil, ctx.Err()
	}

	topic := i.cfg.TopicPrefix + "/#"
	subToken := client.Subscribe(topic, 0, i.handleMessage)
	select {
	case <-subToken.Done():
		if err := subToken.Error(); err != nil {
			client.Disconnect(250)
			return nil, fmt.Errorf("subscribe: %w", err)
		}
	case <-ctx.Done():
		client.Disconnect(0)
		return nil, ctx.Err()
	}

	i.logger.Info("mqtt connected", zap.String("broker", i.cfg.Broker), zap.String("topic", topic))
	return client, nil
}

// handleMessage is called for each incoming MQTT message. Each message carries
// one field. All messages in a burst are accumulated into a single reading;
// a debounce timer fires 2 seconds after the last message and emits the reading.
func (i *Ingester) handleMessage(_ mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	field := strings.TrimPrefix(topic, i.cfg.TopicPrefix)
	field = strings.TrimPrefix(field, "/")
	val := strings.TrimSpace(string(msg.Payload()))

	i.mu.Lock()
	defer i.mu.Unlock()

	// Update receipt time on every message for health check accuracy.
	i.lastReceived = time.Now()

	if !i.hasPending {
		i.pending = types.WeatherReading{Timestamp: time.Now()}
		i.hasPending = true
	}

	if field == "time" {
		if t, err := time.Parse("2006-01-02 15:04:05", val); err == nil {
			i.pending.Timestamp = t
		}
	} else if err := applyField(&i.pending, field, val); err != nil {
		i.logger.Debug("unknown mqtt field", zap.String("field", field), zap.String("val", val))
	}

	// Increment generation so any in-flight timer callback becomes a no-op.
	i.debounceGen++
	gen := i.debounceGen
	if i.debounce != nil {
		i.debounce.Stop()
	}
	i.debounce = time.AfterFunc(debounceDelay, func() { i.flush(gen) })
}

// flush emits the accumulated reading. Called by the debounce timer with the
// generation value current at the time the timer was created. If a newer timer
// has since been created (gen mismatch), the call is a no-op.
func (i *Ingester) flush(gen uint64) {
	i.mu.Lock()
	if !i.hasPending || i.debounceGen != gen {
		i.mu.Unlock()
		return
	}
	r := i.pending
	i.pending = types.WeatherReading{}
	i.hasPending = false
	i.mu.Unlock()

	// Derived fields — computed outside the lock.
	r.DewPointC = dewPoint(r.TempC, r.HumidityPct)
	r.FeelsLikeC = feelsLike(r.TempC, r.DewPointC, r.WindSpeedMs)
	clearSky := solar.ClearSkyGHI(i.lat, i.lon, r.Timestamp.Unix())
	r.ClearSkyWm2 = clearSky
	r.ClearSkyIdx, r.CloudCovPct = solar.CloudCover(r.SolarWm2, clearSky)
	r.Condition = types.DeriveCondition(r)

	i.hub.Publish(r)
}

// applyField sets the WeatherReading field corresponding to the topic suffix.
func applyField(r *types.WeatherReading, field, val string) error {
	f, err := parseFloat(val)
	if err != nil {
		return err
	}
	switch field {
	// Outdoor
	case "tempOutC":
		r.TempC = f
	case "humidityOut":
		r.HumidityPct = f
	// Indoor
	case "tempInC":
		r.TempInC = f
	case "humidityIn":
		r.HumidityInPct = f
	// Pressure
	case "baromRelHpa":
		r.PressureHPa = f
	case "baromAbsHpa":
		r.PressureAbsHPa = f
	// Wind
	case "windSpdMps":
		r.WindSpeedMs = f
	case "windGustMps":
		r.WindGustMs = f
	case "maxDailyGustMps":
		r.MaxDailyGustMs = f
	case "windDir":
		r.WindDirDeg = f
	// Rain
	case "rainRealTime":
		r.RainMmHr = f
	case "rainEvent":
		r.RainEventMm = f
	case "rainHourly":
		r.RainHourlyMm = f
	case "rainDaily":
		r.RainDailyMm = f
	case "rainWeekly":
		r.RainWeeklyMm = f
	case "rainMonthly":
		r.RainMonthlyMm = f
	case "rainSeason":
		r.RainSeasonMm = f
	case "rainYearly":
		r.RainYearlyMm = f
	// Solar / UV
	case "uvIndex":
		r.UVIndex = f
	case "solarRadiation":
		r.SolarWm2 = f
	// Sensor health
	case "wh90Battery":
		r.BatteryV = f
	case "capacVolt":
		r.CapacitorV = f
	default:
		return fmt.Errorf("unknown field %q", field)
	}
	return nil
}

func parseFloat(s string) (float64, error) {
	return strconv.ParseFloat(strings.TrimSpace(s), 64)
}

// dewPoint returns the dew point temperature (°C) using the Magnus formula.
// Accurate to ±0.35 °C for -45 °C < T < 60 °C and 1 % < RH < 100 %.
// Returns tempC unchanged if humidityPct is out of the valid range [1, 100]
// to avoid math.Log(0) → NaN propagating into InfluxDB writes.
func dewPoint(tempC, humidityPct float64) float64 {
	if humidityPct <= 0 || humidityPct > 100 {
		return tempC
	}
	const b, c = 17.62, 243.12
	gamma := math.Log(humidityPct/100.0) + b*tempC/(c+tempC)
	return c * gamma / (b - gamma)
}

// feelsLike returns the apparent temperature (°C) using Environment Canada
// formulas, mirroring the algorithm used by Ecowitt collectors.
//
//   - Wind chill (EC 2001): applied when T ≤ 0 °C and wind > 4.8 km/h.
//   - Humidex (EC):         applied when T ≥ 20 °C; vapour pressure derived
//     from dew point via Magnus.
//   - Otherwise: returns T unchanged.
func feelsLike(tempC, dewPointC, windSpeedMs float64) float64 {
	windKmH := windSpeedMs * 3.6

	switch {
	case tempC <= 0 && windKmH > 4.8:
		// EC wind chill (2001 revision). EC only publishes wind chill at T ≤ 0 °C
		// because above freezing, evaporative cooling dominates and the formula
		// no longer represents perceived temperature accurately.
		v016 := math.Pow(windKmH, 0.16)
		wc := 13.12 + 0.6215*tempC - 11.37*v016 + 0.3965*tempC*v016
		if wc > tempC {
			return tempC // guard: wind chill can't feel warmer than still air
		}
		return wc

	case tempC >= 20:
		// EC humidex. Vapour pressure (hPa) from Magnus via dew point.
		e := 6.112 * math.Exp(17.67 * dewPointC / (dewPointC + 243.5))
		h := tempC + (5.0/9.0)*(e-10.0)
		if h < tempC {
			return tempC // guard: humidex can't feel cooler than dry-bulb
		}
		return h

	default:
		return tempC
	}
}
