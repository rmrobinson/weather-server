# weather-server

Ingests Ecowitt WS90 / GW2000 sensor data over MQTT, stores it in InfluxDB v2 with automatic hourly and daily downsampling, and serves:

- Live readings via gRPC server-streaming and HTTP SSE
- A single-page dashboard at `/`
- A REST API at `/api/v1/`
- A gRPC `QueryRainAccumulation` RPC for historical rainfall over any time window

## Requirements

- Go 1.22+
- An Ecowitt GW2000 (or compatible) base station publishing to MQTT
- InfluxDB v2 (OSS or Cloud)
- An MQTT broker (e.g. Mosquitto)

## Setup

### 1. Config file

```bash
cp config.yaml.example config.yaml
```

Edit `config.yaml` with your MQTT broker address, InfluxDB URL, and org name.
The InfluxDB token is **not** stored in the config file — it is read from the
`INFLUX_TOKEN` environment variable at runtime (see step 3).

### 2. Create an InfluxDB API token

The server needs write access to `weather_raw` and read access to all three
weather buckets, plus read access to tasks (for the `/healthz` task health checks).

**Option A — InfluxDB UI**

1. Open your InfluxDB instance → **Load Data → API Tokens → Generate API Token**
2. Choose **Custom API Token**
3. Grant the following permissions for your org:
   - Buckets: **Read** + **Write** on `weather_raw`
   - Buckets: **Read** on `weather_1h`, `weather_1d`
   - Tasks: **Read**
4. Copy the generated token

**Option B — InfluxDB CLI**

```bash
influx auth create \
  --org your-org-name \
  --description "weather-server" \
  --read-bucket weather_raw \
  --write-bucket weather_raw \
  --read-bucket weather_1h \
  --read-bucket weather_1d
```

> **Note:** Run `store.Bootstrap()` once with an all-access token to create the
> buckets and Flux downsampling tasks. Once they exist you can switch to the
> restricted token above. Bootstrap warns and continues if it lacks permissions —
> normal operation is unaffected.

### 3. Run

```bash
export INFLUX_TOKEN='your-token-here'
go run ./cmd/weather-server/ -config ./config.yaml
```

Or build first:

```bash
go build -o weather-server ./cmd/weather-server/
INFLUX_TOKEN='your-token-here' ./weather-server -config ./config.yaml
```

The server will:
- Connect to your MQTT broker and subscribe to `<topic_prefix>/#`
- Create InfluxDB buckets and Flux downsampling tasks on first run (requires broader token)
- Serve the dashboard at `http://localhost:8080`
- Serve gRPC at `localhost:9090`

## API

### HTTP

| Endpoint | Description |
|---|---|
| `GET /` | Dashboard (SSE-driven live view + charts) |
| `GET /healthz` | Health check — MQTT, InfluxDB write, downsampling tasks |
| `GET /api/v1/readings?start=&end=&resolution=raw\|1h\|1d&fields=` | Historical readings |
| `GET /api/v1/readings/latest` | Most recent reading |
| `GET /api/v1/stream/sse` | Live SSE stream |

### gRPC (`weather.v1.WeatherService`)

| RPC | Type | Description |
|---|---|---|
| `StreamReadings` | server-streaming | Live readings as they arrive |
| `QueryRainAccumulation` | unary | Total rainfall (mm) over a time range |

**Example — rainfall in the last 7 days:**

```bash
grpcurl -plaintext -d '{
  "start": {"seconds": '"$(date -v-7d +%s)"'}
}' localhost:9090 weather.v1.WeatherService/QueryRainAccumulation
```

If a PSK is configured, add `-H 'authorization: psk <your-secret>'` to all gRPC calls and set `Authorization: psk <your-secret>` on HTTP requests.

## Docker

Docker files live in `deploy/`. Run from that directory:

```bash
cd deploy
docker compose up
```

InfluxDB is not included in the compose file — point `config.yaml` at your existing instance. The Mosquitto container in the compose file is optional if you already have an MQTT broker running.

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `INFLUX_TOKEN` | Yes | InfluxDB API token |
| `WEATHER_PSK` | No | Pre-shared key for API auth (overrides `auth.psk` in config) |
