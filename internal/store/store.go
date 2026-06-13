package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/query"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/influxdata/influxdb-client-go/v2/domain"
	"github.com/rmrobinson/weather-server/internal/config"
	"github.com/rmrobinson/weather-server/internal/types"
	"go.uber.org/zap"
)

//go:embed flux
var fluxFS embed.FS

type Resolution string

const (
	ResolutionRaw Resolution = "raw"
	Resolution1h  Resolution = "1h"
	Resolution1d  Resolution = "1d"
)

type Query struct {
	Start      time.Time
	End        time.Time
	Resolution Resolution
	Fields     []string
}

type TaskHealth struct {
	Exists          bool
	Active          bool
	LastCompleted   time.Time
	WithinThreshold bool
}

type Store struct {
	client     influxdb2.Client
	writeAPI   api.WriteAPIBlocking
	queryAPI   api.QueryAPI
	tasksAPI   api.TasksAPI
	bucketsAPI api.BucketsAPI
	cfg        config.InfluxConfig
	stationID  string
	logger     *zap.Logger

	mu             sync.Mutex
	lastWriteError error
}

func New(cfg config.InfluxConfig, stationID string, logger *zap.Logger) (*Store, error) {
	client := influxdb2.NewClient(cfg.URL, cfg.Token)
	// The write and query APIs accept either an org name or org ID.
	orgRef := cfg.Org
	if orgRef == "" {
		orgRef = cfg.OrgID
	}
	return &Store{
		client:     client,
		writeAPI:   client.WriteAPIBlocking(orgRef, cfg.BucketRaw),
		queryAPI:   client.QueryAPI(orgRef),
		tasksAPI:   client.TasksAPI(),
		bucketsAPI: client.BucketsAPI(),
		cfg:        cfg,
		stationID:  stationID,
		logger:     logger,
	}, nil
}

func (s *Store) Bootstrap(ctx context.Context) error {
	if err := s.ensureBucket(ctx, s.cfg.BucketRaw, time.Duration(s.cfg.BucketRawRetentionDays)*24*time.Hour); err != nil {
		return fmt.Errorf("bucket %s: %w", s.cfg.BucketRaw, err)
	}
	if err := s.ensureBucket(ctx, s.cfg.Bucket1h, 2*365*24*time.Hour); err != nil {
		return fmt.Errorf("bucket %s: %w", s.cfg.Bucket1h, err)
	}
	if err := s.ensureBucket(ctx, s.cfg.Bucket1d, 0); err != nil {
		return fmt.Errorf("bucket %s: %w", s.cfg.Bucket1d, err)
	}
	return s.ensureFluxTasks(ctx)
}

// resolveOrgID returns the org ID without requiring read:orgs permission when
// org_id is set in config. Falls back to FindOrganizationByName otherwise.
func (s *Store) resolveOrgID(ctx context.Context) (string, error) {
	if s.cfg.OrgID != "" {
		return s.cfg.OrgID, nil
	}
	org, err := s.client.OrganizationsAPI().FindOrganizationByName(ctx, s.cfg.Org)
	if err != nil {
		return "", fmt.Errorf("find org %q: %w (set influx.org_id in config to avoid requiring read:orgs permission)", s.cfg.Org, err)
	}
	return *org.Id, nil
}

func (s *Store) ensureBucket(ctx context.Context, name string, retention time.Duration) error {
	if _, err := s.bucketsAPI.FindBucketByName(ctx, name); err == nil {
		return nil // already exists
	}
	orgID, err := s.resolveOrgID(ctx)
	if err != nil {
		return err
	}
	org := &domain.Organization{Id: &orgID}
	var rules []domain.RetentionRule
	if retention > 0 {
		ruleType := domain.RetentionRuleTypeExpire
		rules = []domain.RetentionRule{{
			EverySeconds: int64(retention.Seconds()),
			Type:         &ruleType,
		}}
	}
	if _, err = s.bucketsAPI.CreateBucketWithName(ctx, org, name, rules...); err != nil {
		// Tolerate a create-after-find race: verify the bucket exists now.
		if _, verifyErr := s.bucketsAPI.FindBucketByName(ctx, name); verifyErr == nil {
			return nil
		}
		return err
	}
	return nil
}

// ErrRainReset is returned by QueryRainAccumulation when the rain_yearly_mm
// counter appears to have reset (i.e. the last value is less than the first),
// which happens when the query window spans January 1st.
var ErrRainReset = errors.New("rain accumulation data spans a yearly counter reset; split the query at the year boundary")

// taskSchedule maps each embedded task name to the InfluxDB scheduler interval.
var taskSchedule = map[string]string{
	"downsample_1h": "1h",
	"downsample_1d": "1d",
}

func (s *Store) ensureFluxTasks(ctx context.Context) error {
	orgID, err := s.resolveOrgID(ctx)
	if err != nil {
		return err
	}
	return fs.WalkDir(fluxFS, "flux", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".flux") {
			return err
		}
		data, err := fluxFS.ReadFile(path)
		if err != nil {
			return err
		}
		taskName := strings.TrimSuffix(d.Name(), ".flux")
		tasks, err := s.tasksAPI.FindTasks(ctx, &api.TaskFilter{Name: taskName})
		if err != nil {
			return fmt.Errorf("find task %s: %w", taskName, err)
		}
		if len(tasks) > 0 {
			return nil
		}
		every, ok := taskSchedule[taskName]
		if !ok {
			every = "1h"
		}
		// Substitute configured bucket names so the task works regardless of
		// the names chosen in config.yaml.
		script := strings.NewReplacer(
			`{{bucket_raw}}`, s.cfg.BucketRaw,
			`{{bucket_1h}}`, s.cfg.Bucket1h,
			`{{bucket_1d}}`, s.cfg.Bucket1d,
		).Replace(string(data))
		_, err = s.tasksAPI.CreateTaskWithEvery(ctx, taskName, script, every, orgID)
		if err != nil {
			return fmt.Errorf("create task %s: %w", taskName, err)
		}
		s.logger.Info("created flux task", zap.String("task", taskName))
		return nil
	})
}

func (s *Store) WriteReading(ctx context.Context, r types.WeatherReading) error {
	p := write.NewPoint("weather",
		map[string]string{"station": s.stationID},
		map[string]any{
			// Outdoor
			"temp_c":       r.TempC,
			"humidity_pct": r.HumidityPct,
			// Indoor
			"temp_in_c":       r.TempInC,
			"humidity_in_pct": r.HumidityInPct,
			// Pressure
			"pressure_hpa":     r.PressureHPa,
			"pressure_abs_hpa": r.PressureAbsHPa,
			// Wind
			"wind_speed_ms":    r.WindSpeedMs,
			"wind_gust_ms":     r.WindGustMs,
			"max_daily_gust_ms": r.MaxDailyGustMs,
			"wind_dir_deg":     r.WindDirDeg,
			// Rain
			"rain_mm_hr":      r.RainMmHr,
			"rain_event_mm":   r.RainEventMm,
			"rain_hourly_mm":  r.RainHourlyMm,
			"rain_daily_mm":   r.RainDailyMm,
			"rain_weekly_mm":  r.RainWeeklyMm,
			"rain_monthly_mm": r.RainMonthlyMm,
			"rain_season_mm":  r.RainSeasonMm,
			"rain_yearly_mm":  r.RainYearlyMm,
			// Derived atmospheric
			"dew_point_c":  r.DewPointC,
			"feels_like_c": r.FeelsLikeC,
			// Solar / UV
			"uv_index":    r.UVIndex,
			"solar_wm2":       r.SolarWm2,
			"clear_sky_wm2":   r.ClearSkyWm2,
			"clear_sky_index": r.ClearSkyIdx,
			"cloud_cover_pct": r.CloudCovPct,
			// Sensor health
			"battery_v":   r.BatteryV,
			"capacitor_v": r.CapacitorV,
		},
		r.Timestamp,
	)
	err := s.writeAPI.WritePoint(ctx, p)
	s.mu.Lock()
	s.lastWriteError = err
	s.mu.Unlock()
	return err
}

func (s *Store) LastWriteError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastWriteError
}

func (s *Store) QueryReadings(ctx context.Context, q Query) ([]types.WeatherReading, error) {
	bucket := s.bucketForResolution(q.Resolution)
	end := q.End
	if end.IsZero() {
		end = time.Now()
	}

	fieldFilter := ""
	if len(q.Fields) > 0 {
		quoted := make([]string, len(q.Fields))
		for i, f := range q.Fields {
			quoted[i] = fmt.Sprintf("%q", f)
		}
		fieldFilter = fmt.Sprintf(`|> filter(fn: (r) => contains(value: r._field, set: [%s]))`, strings.Join(quoted, ", "))
	}

	flux := fmt.Sprintf(`
from(bucket: %q)
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._measurement == "weather" and r.station == %q)
  %s
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"])
`,
		bucket,
		q.Start.UTC().Format(time.RFC3339),
		end.UTC().Format(time.RFC3339),
		s.stationID,
		fieldFilter,
	)

	result, err := s.queryAPI.Query(ctx, flux)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	// Initialise to empty slice so JSON encodes as [] not null.
	readings := make([]types.WeatherReading, 0)
	for result.Next() {
		readings = append(readings, recordToReading(result.Record()))
	}
	return readings, result.Err()
}

func (s *Store) QueryLatest(ctx context.Context) (*types.WeatherReading, error) {
	// Use a 1-year range so the query works on fresh installs and after
	// extended downtime. last() per-field then pivot collapses to one row
	// because all fields in a reading share the same timestamp.
	flux := fmt.Sprintf(`
from(bucket: %q)
  |> range(start: -365d)
  |> filter(fn: (r) => r._measurement == "weather" and r.station == %q)
  |> last()
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
`, s.cfg.BucketRaw, s.stationID)

	result, err := s.queryAPI.Query(ctx, flux)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	if result.Next() {
		r := recordToReading(result.Record())
		return &r, result.Err()
	}
	return nil, result.Err()
}

// RainAccumulationResult holds the computed accumulation and the timestamps of
// the bounding data points actually found.
type RainAccumulationResult struct {
	RainMm      float64
	ActualStart time.Time
	ActualEnd   time.Time
}

// QueryRainAccumulation returns the total rainfall (mm) over [start, end] by
// computing last(rain_yearly_mm) - first(rain_yearly_mm) within the window.
func (s *Store) QueryRainAccumulation(ctx context.Context, start, end time.Time) (*RainAccumulationResult, error) {
	if end.IsZero() {
		end = time.Now()
	}
	rangeClause := fmt.Sprintf("start: %s, stop: %s",
		start.UTC().Format(time.RFC3339),
		end.UTC().Format(time.RFC3339),
	)
	filter := fmt.Sprintf(`r._measurement == "weather" and r.station == %q and r._field == "rain_yearly_mm"`, s.stationID)

	getEdge := func(fn string) (float64, time.Time, error) {
		q := fmt.Sprintf(`
from(bucket: %q)
  |> range(%s)
  |> filter(fn: (r) => %s)
  |> %s()
`, s.cfg.BucketRaw, rangeClause, filter, fn)
		res, err := s.queryAPI.Query(ctx, q)
		if err != nil {
			return 0, time.Time{}, err
		}
		defer res.Close()
		if res.Next() {
			rec := res.Record()
			v, _ := rec.Value().(float64)
			return v, rec.Time(), res.Err()
		}
		return 0, time.Time{}, res.Err()
	}

	firstVal, firstTime, err := getEdge("first")
	if err != nil {
		return nil, fmt.Errorf("query first: %w", err)
	}
	lastVal, lastTime, err := getEdge("last")
	if err != nil {
		return nil, fmt.Errorf("query last: %w", err)
	}
	if firstTime.IsZero() {
		return nil, nil // no data in range
	}
	rainMm := lastVal - firstVal
	if rainMm < 0 {
		// rain_yearly_mm resets to 0 on January 1st; a negative difference
		// means the query window straddles the reset and the result is meaningless.
		return nil, ErrRainReset
	}
	return &RainAccumulationResult{
		RainMm:      rainMm,
		ActualStart: firstTime,
		ActualEnd:   lastTime,
	}, nil
}

func (s *Store) CheckTaskHealth(ctx context.Context) (map[string]TaskHealth, error) {
	taskIntervals := map[string]time.Duration{
		"downsample_1h": time.Hour,
		"downsample_1d": 24 * time.Hour,
	}
	result := make(map[string]TaskHealth, len(taskIntervals))

	for name, interval := range taskIntervals {
		h := TaskHealth{}
		tasks, err := s.tasksAPI.FindTasks(ctx, &api.TaskFilter{Name: name})
		if err != nil || len(tasks) == 0 {
			result[name] = h
			continue
		}
		h.Exists = true
		task := tasks[0]
		h.Active = task.Status != nil && string(*task.Status) == "active"

		runs, err := s.tasksAPI.FindRunsWithID(ctx, task.Id, &api.RunFilter{Limit: 1})
		if err == nil && len(runs) > 0 && runs[0].FinishedAt != nil {
			h.LastCompleted = *runs[0].FinishedAt
			h.WithinThreshold = time.Since(h.LastCompleted) < 2*interval
		}
		result[name] = h
	}
	return result, nil
}

func (s *Store) bucketForResolution(r Resolution) string {
	switch r {
	case Resolution1h:
		return s.cfg.Bucket1h
	case Resolution1d:
		return s.cfg.Bucket1d
	default:
		return s.cfg.BucketRaw
	}
}

func recordToReading(rec *query.FluxRecord) types.WeatherReading {
	getFloat := func(key string) float64 {
		v := rec.ValueByKey(key)
		if v == nil {
			return 0
		}
		switch f := v.(type) {
		case float64:
			return f
		case float32:
			return float64(f)
		}
		return 0
	}
	r := types.WeatherReading{
		Timestamp: rec.Time(),
		// Outdoor
		TempC:       getFloat("temp_c"),
		HumidityPct: getFloat("humidity_pct"),
		// Indoor
		TempInC:       getFloat("temp_in_c"),
		HumidityInPct: getFloat("humidity_in_pct"),
		// Pressure
		PressureHPa:    getFloat("pressure_hpa"),
		PressureAbsHPa: getFloat("pressure_abs_hpa"),
		// Wind
		WindSpeedMs:    getFloat("wind_speed_ms"),
		WindGustMs:     getFloat("wind_gust_ms"),
		MaxDailyGustMs: getFloat("max_daily_gust_ms"),
		WindDirDeg:     getFloat("wind_dir_deg"),
		// Rain
		RainMmHr:      getFloat("rain_mm_hr"),
		RainEventMm:   getFloat("rain_event_mm"),
		RainHourlyMm:  getFloat("rain_hourly_mm"),
		RainDailyMm:   getFloat("rain_daily_mm"),
		RainWeeklyMm:  getFloat("rain_weekly_mm"),
		RainMonthlyMm: getFloat("rain_monthly_mm"),
		RainSeasonMm:  getFloat("rain_season_mm"),
		RainYearlyMm:  getFloat("rain_yearly_mm"),
		// Derived atmospheric
		DewPointC:  getFloat("dew_point_c"),
		FeelsLikeC: getFloat("feels_like_c"),
		// Solar / UV
		UVIndex:     getFloat("uv_index"),
		SolarWm2:    getFloat("solar_wm2"),
		ClearSkyWm2: getFloat("clear_sky_wm2"),
		ClearSkyIdx: getFloat("clear_sky_index"),
		CloudCovPct: getFloat("cloud_cover_pct"),
		// Sensor health
		BatteryV:   getFloat("battery_v"),
		CapacitorV: getFloat("capacitor_v"),
	}
	r.Condition = types.DeriveCondition(r)
	return r
}
