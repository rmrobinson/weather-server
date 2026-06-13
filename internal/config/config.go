package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	StationID string  `yaml:"station_id"`
	Latitude  float64 `yaml:"latitude"`
	Longitude float64 `yaml:"longitude"`
	MQTT      MQTTConfig   `yaml:"mqtt"`
	Influx    InfluxConfig `yaml:"influx"`
	GRPC      GRPCConfig   `yaml:"grpc"`
	HTTP      HTTPConfig   `yaml:"http"`
	Auth      AuthConfig   `yaml:"auth"`
}

type MQTTConfig struct {
	Broker      string `yaml:"broker"`
	TopicPrefix string `yaml:"topic_prefix"`
	ClientID    string `yaml:"client_id"`
}

type InfluxConfig struct {
	URL                    string `yaml:"url"`
	Token                  string `yaml:"-"` // set from INFLUX_TOKEN env var only; never read from config file
	Org                    string `yaml:"org"`
	OrgID                  string `yaml:"org_id"` // optional; avoids requiring read:orgs token permission
	BucketRaw              string `yaml:"bucket_raw"`
	BucketRawRetentionDays int    `yaml:"bucket_raw_retention_days"`
	Bucket1h               string `yaml:"bucket_1h"`
	Bucket1d               string `yaml:"bucket_1d"`
}

type GRPCConfig struct {
	Addr string `yaml:"addr"`
}

type HTTPConfig struct {
	Addr string `yaml:"addr"`
}

type AuthConfig struct {
	PSK string `yaml:"-"` // set from WEATHER_PSK env var only; never read from config file
}

func LoadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var cfg Config
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, err
	}

	if tok := os.Getenv("INFLUX_TOKEN"); tok != "" {
		cfg.Influx.Token = tok
	}
	if psk := os.Getenv("WEATHER_PSK"); psk != "" {
		cfg.Auth.PSK = psk
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) validate() error {
	var errs []string
	if c.StationID == "" {
		errs = append(errs, "station_id is required")
	}
	if c.MQTT.Broker == "" {
		errs = append(errs, "mqtt.broker is required")
	}
	if c.MQTT.TopicPrefix == "" {
		errs = append(errs, "mqtt.topic_prefix is required")
	}
	if c.Influx.URL == "" {
		errs = append(errs, "influx.url is required")
	}
	if c.Influx.Org == "" && c.Influx.OrgID == "" {
		errs = append(errs, "influx.org or influx.org_id is required")
	}
	if c.Influx.Token == "" {
		errs = append(errs, "influx.token is required (set INFLUX_TOKEN env var)")
	}
	if c.Influx.BucketRaw == "" {
		errs = append(errs, "influx.bucket_raw is required")
	}
	if c.Influx.Bucket1h == "" {
		errs = append(errs, "influx.bucket_1h is required")
	}
	if c.Influx.Bucket1d == "" {
		errs = append(errs, "influx.bucket_1d is required")
	}
	if c.GRPC.Addr == "" {
		errs = append(errs, "grpc.addr is required")
	}
	if c.HTTP.Addr == "" {
		errs = append(errs, "http.addr is required")
	}
	if len(errs) > 0 {
		return fmt.Errorf("invalid config:\n  %s", strings.Join(errs, "\n  "))
	}
	return nil
}
