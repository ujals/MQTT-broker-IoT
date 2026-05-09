# Pure-Python MQTT Broker + Integration Platform

A production-ready, pure-Python MQTT broker (no Mosquitto, no external broker binary) with a built-in integration service that fans out messages to HTTP endpoints, ChirpStack, The Things Network (TTN), and any other MQTT broker.

---

## Architecture

```
IoT Devices / LoRa Gateways / Any MQTT Client
              │
              ▼  port 1883
   ┌─────────────────────┐
   │   mqtt_broker       │  Pure-Python asyncio broker
   │   MQTT 3.1/3.1.1/5  │  QoS 0/1/2 · Retain · Will
   │   Auth · TLS · ACL  │  Persistent sessions
   └──────────┬──────────┘
              │  subscribes internally
              ▼
   ┌─────────────────────┐
   │   integration       │  FastAPI + async MQTT subscriber
   │   service           │  port 8000
   └──────┬──────┬───────┘
          │      │      │
          ▼      ▼      ▼
       HTTP    MQTT    MQTT
      endpoint bridge  bridge
    (your API) (ChirpStack) (TTN / AWS / HiveMQ)
```

Everything runs in a single `docker-compose up` command. Edit YAML config files to change behaviour — no rebuild required.

---

## Repository Structure

```
.
├── Dockerfile                  # broker container image
├── docker-compose.yml          # runs broker + integration together
├── config.yaml                 # broker configuration (listeners, auth, forwarders, limits)
├── requirements.txt            # broker Python deps
│
├── mqtt_broker/                # core broker package
│   ├── __main__.py             # entry point:  python -m mqtt_broker
│   ├── broker.py               # asyncio server, client handler, routing
│   ├── protocol.py             # MQTT 3.1/3.1.1/5.0 wire-format codec
│   ├── session.py              # session state, offline queue, QoS-2 flow
│   ├── router.py               # topic-filter matching (+, #), subscription store
│   ├── auth.py                 # username/password auth (plain or sha256)
│   └── forwarder.py            # built-in HTTP / UDP / MQTT forwarders
│
├── mqtt_bridge.py              # standalone bridge script (no broker needed)
│
└── integration/                # fan-out integration service (Docker)
    ├── Dockerfile
    ├── config.yaml             # integration configuration
    ├── requirements.txt
    ├── main.py                 # FastAPI app + MQTT subscriber loop
    └── integrations/
        ├── base.py             # BaseIntegration abstract class
        ├── http_post.py        # async HTTP/HTTPS with retries
        └── mqtt_bridge.py      # MQTT bridge (ChirpStack / TTN / any broker)
```

---

## Quick Start — Docker (Recommended)

**Requirements:** Docker Desktop installed and running.

```bash
git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
cd YOUR_REPO
docker-compose up --build
```

That's it. The broker is now accepting MQTT connections on **port 1883** and the integration API is on **port 8000**.

---

## Quick Start — Local (No Docker)

**Requirements:** Python 3.10+

```bash
pip install -r requirements.txt
python -m mqtt_broker --config config.yaml
```

---

## Testing with MQTTX

1. Download [MQTTX](https://mqttx.emqx.io) (free desktop MQTT client)
2. Create a new connection:
   - Host: `127.0.0.1`
   - Port: `1883`
   - No username/password (auth is disabled by default)
3. Click **New Subscription** → topic `test/#` → QoS 0
4. Publish to topic `test/hello` with payload `{"msg": "hello world"}`
5. The message appears instantly in the subscription panel

---

## Broker Configuration — `config.yaml`

### Listeners

```yaml
listeners:
  - host: 0.0.0.0
    port: 1883          # plain MQTT

  # TLS on port 8883 (uncomment when certs are ready)
  # - host: 0.0.0.0
  #   port: 8883
  #   tls:
  #     certfile: certs/server.crt
  #     keyfile:  certs/server.key
  #     ca_certs: certs/ca.crt    # optional: require client certificates
```

### Authentication

```yaml
auth:
  enabled: false          # set true to enforce credentials
  allow_anonymous: true   # allow clients with no username

  users:
    alice: "secret123"                          # plain-text password
    device1: "sha256:e3b0c44298fc1c149a..."    # sha256-hashed password
```

To generate a sha256 hash:
```bash
python -c "import hashlib; print('sha256:' + hashlib.sha256(b'mypassword').hexdigest())"
```

### Resource Limits

```yaml
limits:
  max_connections: 10000    # maximum simultaneous TCP connections
  max_packet_size: 1048576  # 1 MB — reject packets larger than this
  forwarder_threads: 10     # thread pool for built-in HTTP/UDP forwarders
```

### Built-in Forwarders (optional)

These run inside the broker process and forward every published message:

```yaml
forwarders:
  # HTTP endpoint
  - name: my_api
    type: http
    url: http://192.168.1.100:8080/api/ingest
    method: POST
    timeout: 10
    headers:
      Content-Type: application/json
      Authorization: Bearer YOUR_TOKEN

  # Bridge to another MQTT broker
  - name: cloud
    type: mqtt
    host: broker.example.com
    port: 1883
    topic_prefix: "lns/"    # "dev/01" becomes "lns/dev/01"

  # UDP sink
  - name: local_udp
    type: udp
    host: 127.0.0.1
    port: 5005
```

### Logging

```yaml
logging:
  level: INFO               # DEBUG | INFO | WARNING | ERROR
  file: mqtt_broker.log     # omit to log to stdout only
  max_bytes: 10485760       # 10 MB per file
  backup_count: 5           # keep 5 rotated files
```

---

## Integration Service Configuration — `integration/config.yaml`

This service subscribes to the local broker and fans out to external systems. Edit this file and restart the integration container — **no rebuild needed**.

### Broker Connection

```yaml
broker:
  host: mqtt_broker     # Docker service name; use 'localhost' for local dev
  port: 1883
  client_id: integration-service

subscriptions:
  - topic: "#"          # subscribe to all topics
    qos: 1
```

### Integrations

#### HTTP / REST API

Post every MQTT message as JSON to your backend. Use `{topic}` in the URL to include the MQTT topic.

```yaml
integrations:
  - name: my_backend
    type: http
    enabled: true
    url: "http://192.168.1.100:8080/api/ingest"
    method: POST
    timeout: 10
    retry_attempts: 3
    ssl_verify: true
    headers:
      Content-Type: application/json
      Authorization: "Bearer YOUR_TOKEN"
```

#### ChirpStack (MQTT Bridge)

Bridge messages to a ChirpStack MQTT broker.

```yaml
  - name: chirpstack
    type: mqtt
    enabled: true
    host: 192.168.1.50      # your ChirpStack server IP
    port: 1883
    username: ""            # leave empty if ChirpStack auth is disabled
    password: ""
    topic_prefix: ""        # e.g. "lns/" to prefix all outgoing topics
    qos: 1
    retain: false
```

ChirpStack listens for uplinks on topics like `application/{id}/device/{eui}/event/up`. Set `topic_prefix` to match your ChirpStack application routing.

#### The Things Network v3 (MQTT)

```yaml
  - name: ttn
    type: mqtt
    enabled: true
    host: eu1.cloud.thethings.network    # or nam1 / au1
    port: 8883
    tls: true                            # TTN requires TLS
    username: "your-app-id@ttn"
    password: "YOUR_TTN_API_KEY"         # generate in TTN console → API Keys
    topic_prefix: "v3/"
    qos: 0
```

To get your TTN API key:
1. Go to TTN Console → Your Application → API Keys
2. Create a key with **Write downlink traffic** + **Read uplink traffic** permissions
3. Paste it in `password` above

#### Any Other MQTT Broker (AWS IoT, HiveMQ, EMQX, etc.)

```yaml
  - name: aws_iot
    type: mqtt
    enabled: true
    host: xxxx.iot.us-east-1.amazonaws.com
    port: 8883
    tls:
      ca_certs: certs/AmazonRootCA1.pem
      certfile: certs/device-cert.pem
      keyfile:  certs/device-key.pem
    topic_prefix: "devices/"
    qos: 1
```

---

## Integration Service API

Once running, the integration service exposes a simple REST API on **port 8000**:

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Liveness probe — returns `{"status": "ok"}` |
| GET | `/metrics` | Message counters — received, forwarded, errors |
| GET | `/integrations` | List all integrations and their enabled state |

Example:
```bash
curl http://localhost:8000/metrics
# {"received": 142, "forwarded": 284, "errors": 0}

curl http://localhost:8000/integrations
# [{"name": "chirpstack", "type": "mqtt", "enabled": true}, ...]
```

---

## Standalone Bridge Script — `mqtt_bridge.py`

If you don't need the full broker (e.g. you already have Mosquitto or ChirpStack running), use `mqtt_bridge.py` to subscribe to any MQTT broker and forward messages to HTTP / UDP / MQTT destinations.

```bash
pip install paho-mqtt requests PyYAML
python mqtt_bridge.py --config config.yaml
```

Bridge-specific config fields:

```yaml
source:
  host: localhost
  port: 1883
  topics:
    - "application/#"
    - "devices/#"
  qos: 1
  client_id: mqtt_bridge_in
  # username: user
  # password: pass
  # tls: true

add_metadata: false     # set true to inject _bridge.topic / timestamp into payload

forwarders:
  - name: my_api
    type: http
    url: http://192.168.1.100:8080/api/ingest
    ...
```

The bridge fans out to all forwarders **in parallel threads** so a slow HTTP endpoint never blocks UDP or MQTT delivery.

---

## MQTT Protocol Support

| Feature | Supported |
|---------|-----------|
| MQTT 3.1 | ✅ |
| MQTT 3.1.1 | ✅ |
| MQTT 5.0 | ✅ |
| QoS 0 (at most once) | ✅ |
| QoS 1 (at least once) | ✅ |
| QoS 2 (exactly once) | ✅ |
| Retained messages | ✅ |
| Will (LWT) messages | ✅ |
| Persistent sessions | ✅ |
| Offline message queue | ✅ |
| Wildcard subscriptions (`+`, `#`) | ✅ |
| Topic aliases (MQTT 5.0) | ✅ |
| No-local flag (MQTT 5.0) | ✅ |
| TLS / mTLS | ✅ |
| Username / password auth | ✅ |
| SHA-256 hashed passwords | ✅ |
| WebSocket transport | ❌ (planned) |
| Shared subscriptions | ❌ (planned) |

---

## Running in Production

### Systemd service (Linux, no Docker)

```ini
# /etc/systemd/system/mqtt-broker.service
[Unit]
Description=Pure-Python MQTT Broker
After=network.target

[Service]
WorkingDirectory=/opt/mqtt-broker
ExecStart=/usr/bin/python3 -m mqtt_broker --config config.yaml
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable mqtt-broker
sudo systemctl start mqtt-broker
```

### Docker on a remote server

```bash
# On your server
git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
cd YOUR_REPO
docker-compose up -d        # -d = detached (background)
docker-compose logs -f      # watch live logs
```

To update after a code change:
```bash
git pull
docker-compose up --build -d
```

To change config without rebuilding:
```bash
# Edit config.yaml or integration/config.yaml, then:
docker-compose restart
```

---

## Adding a New Integration Type

1. Create `integration/integrations/my_integration.py`:

```python
from .base import BaseIntegration

class MyIntegration(BaseIntegration):
    def __init__(self, cfg: dict):
        super().__init__(cfg)
        # read your config fields here

    async def forward(self, topic: str, payload, raw: str) -> None:
        # send the message wherever you need
        pass

    async def close(self) -> None:
        # clean up connections
        pass
```

2. Register it in `integration/integrations/__init__.py`:

```python
from .my_integration import MyIntegration

def build_integrations(cfgs):
    ...
    elif t == "mytype":
        result.append(MyIntegration(cfg))
```

3. Add it to `integration/config.yaml`:

```yaml
integrations:
  - name: my_thing
    type: mytype
    enabled: true
    my_custom_field: value
```

4. Rebuild the integration container:

```bash
docker-compose up --build integration
```

---

## Troubleshooting

**Port 1883 already in use**
```bash
# Find and kill the process using port 1883
# Windows:
netstat -ano | findstr :1883
taskkill /PID <PID> /F

# Linux/Mac:
lsof -i :1883
kill -9 <PID>
```

**Integration service can't connect to broker**

The integration container connects to `mqtt_broker` (the Docker service name). If running locally outside Docker, change `broker.host` in `integration/config.yaml` to `localhost`.

**TLS certificate errors with TTN / AWS**

Make sure `tls: true` is set. For custom certificates, pass a dict:
```yaml
tls:
  ca_certs: certs/ca.pem
  certfile: certs/client.crt
  keyfile:  certs/client.key
```

**Messages not arriving at integration**

Check `/metrics` endpoint:
```bash
curl http://localhost:8000/metrics
```
If `received` is 0, the integration service isn't connected to the broker. If `errors` > 0, check `docker-compose logs integration`.

---

## Dependencies

### Broker (`requirements.txt`)
| Package | Purpose |
|---------|---------|
| `paho-mqtt` | MQTT forwarder / bridge client |
| `requests` | HTTP forwarder |
| `PyYAML` | Config file parsing |

### Integration service (`integration/requirements.txt`)
| Package | Purpose |
|---------|---------|
| `aiomqtt` | Async MQTT subscriber |
| `aiohttp` | Async HTTP client with retries |
| `fastapi` | REST API (`/health`, `/metrics`) |
| `uvicorn` | ASGI server for FastAPI |
| `paho-mqtt` | Outbound MQTT bridge |
| `PyYAML` | Config file parsing |

---

## License

MIT
