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

Edit `config.yaml` with your MQTT broker address, InfluxDB URL, org name, and org ID.
The InfluxDB token is **not** stored in the config file — it is read from the
`INFLUX_TOKEN` environment variable at runtime (see step 3).

**Find your org ID:**

```bash
influx org list
# ID                    Name
# db0874614fb9e07b      your-org-name
```

Set `influx.org_id` in `config.yaml` to the ID shown above. This lets the server
create buckets and Flux tasks on first run without requiring the `read:orgs`
token permission.

### 2. Create an InfluxDB API token

On first run the server creates the InfluxDB buckets and Flux downsampling tasks
automatically. The token needs two distinct classes of permission:

- **Bucket management** (resource-level) — to list and create the three buckets on startup
- **Bucket data** (per-bucket) — to write incoming readings, query historical data, and
  allow the hourly/daily downsampling tasks to read from one bucket and write to another.
  The tasks run under this same token, so missing write access to `weather_1h` or
  `weather_1d` will cause them to fail silently with "not authorized to write".
- **Tasks** — to create and health-check the downsampling tasks

The full set of required permissions:

| Resource | Permission | Why |
|---|---|---|
| Buckets (all, resource-level) | Read | `FindBucketByName` on startup |
| Buckets (all, resource-level) | Write | `CreateBucketWithName` on first run |
| `weather_raw` data | Read | Historical queries, `QueryLatest`, rain accumulation |
| `weather_raw` data | Write | Persist incoming MQTT readings |
| `weather_1h` data | Read | 7d / 30d chart queries; input for `downsample_1d` task |
| `weather_1h` data | Write | Output of `downsample_1h` task |
| `weather_1d` data | Read | 1-year chart queries |
| `weather_1d` data | Write | Output of `downsample_1d` task |
| Tasks | Read | Health checks (`/healthz`), task existence check on startup |
| Tasks | Write | Create downsampling tasks on first run |

> If `influx.org_id` is set in `config.yaml` (recommended), the server never calls
> `FindOrganizationByName`, so Organizations Read is not required.

**Option A — InfluxDB UI**

1. Open your InfluxDB instance → **Load Data → API Tokens → Generate API Token**
2. Choose **Custom API Token**
3. Grant the following:
   - **Buckets** (resource): **Read** + **Write** — enables bucket listing and creation
   - **`weather_raw`**: **Read** + **Write**
   - **`weather_1h`**: **Read** + **Write**
   - **`weather_1d`**: **Read** + **Write**
   - **Tasks**: **Read** + **Write**
4. Copy the generated token

Note: the three named buckets only appear in the picker after they have been created.
On a fresh install, grant **All Buckets: Read + Write** for the data rows above — you can
narrow it to the three named buckets after the server has run once and created them.

**Option B — InfluxDB CLI**

After the server has run once and created the buckets, look up their IDs:

```bash
influx bucket list --org your-org-name --token your-token
# ID                    Name            ...
# a1b2c3d4e5f60001      weather_raw
# a1b2c3d4e5f60002      weather_1h
# a1b2c3d4e5f60003      weather_1d
```

Then create the token with explicit per-bucket data permissions:

```bash
influx auth create \
  --org your-org-name \
  --user your-username \
  --description "weather-server" \
  --read-buckets \
  --write-buckets \
  --read-bucket a1b2c3d4e5f60001 \
  --write-bucket a1b2c3d4e5f60001 \
  --read-bucket a1b2c3d4e5f60002 \
  --write-bucket a1b2c3d4e5f60002 \
  --read-bucket a1b2c3d4e5f60003 \
  --write-bucket a1b2c3d4e5f60003 \
  --read-tasks \
  --write-tasks
```

Replace the IDs with the actual values from `influx bucket list`.

> `--read-buckets` / `--write-buckets` grant resource-level bucket management only
> (list, create, delete). They do **not** grant data read/write — that requires the
> per-bucket `--read-bucket` / `--write-bucket` flags shown above.

### 3. Run

```bash
export INFLUX_TOKEN='your-token-here'
go run ./cmd/weatherd/ --config ./config.yaml
```

Or build first:

```bash
go build -o weatherd ./cmd/weatherd/
INFLUX_TOKEN='your-token-here' ./weatherd --config ./config.yaml
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

## Systemd deployment

`deploy/install.sh` automates the full installation as a systemd service.

```bash
# Build and install in one step (requires Go on the target machine)
sudo ./deploy/install.sh --build

# Or install a binary you built elsewhere
sudo ./deploy/install.sh --binary ./weatherd
```

The script:
1. Creates a `weatherd` system user and group
2. Installs the binary to `/opt/weatherd/weatherd`
3. Installs the service unit to `/etc/systemd/system/weatherd.service`
4. Copies `config.yaml.example` → `/etc/weatherd/config.yaml` (only on first run)
5. Copies `weatherd.env.example` → `/etc/weatherd/weatherd.env` (only on first run)
6. Enables and starts the service

**After installation**, before starting the service, edit the two files it installed:

```bash
# Set your station details, coordinates, MQTT broker, and InfluxDB connection
sudo editor /etc/weatherd/config.yaml

# Set the InfluxDB token (and optionally a PSK)
sudo editor /etc/weatherd/weatherd.env
```

Then start (or restart if already running):

```bash
sudo systemctl enable --now weatherd   # first time
sudo systemctl restart weatherd        # after config changes
sudo journalctl -u weatherd -f         # follow logs
```

Pass `--no-start` to install files without starting the service — useful when
you want to edit config before the first run:

```bash
sudo ./deploy/install.sh --build --no-start
sudo editor /etc/weatherd/config.yaml
sudo editor /etc/weatherd/weatherd.env
sudo systemctl enable --now weatherd
```

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
