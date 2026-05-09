# MQTT Broker + ChirpStack LoRaWAN Integration

A pure-Python asyncio MQTT broker with a FastAPI integration service that
bridges LoRaWAN gateway frames to one or more ChirpStack LNS instances and
mirrors OTAA session keys to secondary targets automatically.

---

## Architecture

```
                         +--------------------------------------+
 RAK Gateway             |         This Project                |
 (or any MQTT            |                                     |
  forwarder)             |  +----------------------+           |
       |                 |  |   Pure-Python MQTT   |           |
       |  eu868/gateway/ |  |      Broker          |           |
       +---------------->|  |   (port 1883)        |           |
                         |  +----------+-----------+           |
                         |             | subscribe #           |
                         |  +----------v-----------+           |
                         |  |  Integration Service  |          |
                         |  |  FastAPI (port 8000)  |          |
                         |  +----------+------------+          |
                         |             |                       |
                         +-------------+-----------------------+
                                       |
                    +------------------+---------------------+
                    |                  |                     |
                    v                  v                     v
          +--------------+   +-----------------+   +-----------------+
          |   Primary    |   |  Secondary      |   |  Secondary      |
          |  ChirpStack  |   |  ChirpStack #1  |   |  ChirpStack #2  |
          |  (OTAA join  |   |  (mirrored ABP) |   |  (mirrored ABP) |
          |   handling)  |   |                 |   |                 |
          +--------------+   +-----------------+   +-----------------+
                |
                | join event (MQTT)
                v
          Integration watches primary Mosquitto
          -> fetches session keys via REST API
          -> registers device as ABP on all secondaries
```

---

## Features

- Pure-Python asyncio MQTT broker (no Mosquitto dependency)
- MQTT 3.1 / 3.1.1 / 5.0 auto-detection per client
- QoS 0, 1, and 2 support
- Retained messages, will messages, persistent sessions
- Wildcard subscriptions (`+` and `#`)
- Optional TLS on port 8883
- Optional username/password authentication
- External forwarders: HTTP, MQTT bridge, UDP (broker-level)
- FastAPI integration service with CORS-enabled REST API
- ChirpStack integration:
  - Bridges raw gateway frames to primary and secondary ChirpStack instances
  - Watches primary Mosquitto for OTAA join events
  - Fetches session keys from primary ChirpStack REST API
  - Mirrors device as ABP activation to all secondaries
- Web UI for configuration (`frontend/index.html`)
- Broker monitor dashboard (`broker_ui/index.html`)
- Simulated MQTT devices for testing (`test_devices/mqtt_devices.py`)
- LoRaWAN gateway + ABP device simulator (`test_devices/lorawan_sim.py`)

---

## Prerequisites

- Python 3.11 or later
- pip

---

## Installation

```bash
git clone https://github.com/ujals/MQTT-broker-IoT.git
cd "MQTT-broker-IoT"

# Install all dependencies
pip install -r requirements.txt
```

---

## Requirements

All pip packages needed across the project:

| Package       | Purpose                                             |
|---------------|-----------------------------------------------------|
| `aiomqtt`     | Async MQTT client (integration service, simulators) |
| `fastapi`     | REST API framework for integration service          |
| `uvicorn`     | ASGI server for FastAPI                             |
| `paho-mqtt`   | Sync MQTT client (broker forwarder, CS targets)     |
| `aiohttp`     | Async HTTP client (ChirpStack REST API calls)       |
| `pyyaml`      | YAML config parsing                                 |
| `cryptography`| LoRaWAN AES-128 encryption and CMAC MIC             |
| `requests`    | Sync HTTP forwarder in broker                       |

Install everything at once:

```bash
pip install aiomqtt fastapi uvicorn paho-mqtt aiohttp pyyaml cryptography requests
```

---

## Quick Start

**Terminal 1 — Start the MQTT Broker**

```bash
python -m mqtt_broker --config config.yaml
```

**Terminal 2 — Start the Integration Service**

```bash
cd integration
python main.py
```

**Terminal 3 — Open the Config UI**

Open `frontend/index.html` in your browser, or:

```bash
# Windows
start frontend\index.html

# macOS
open frontend/index.html

# Linux
xdg-open frontend/index.html
```

---

## Configuration

### `config.yaml` — Broker

| Field                       | Description                                            |
|-----------------------------|--------------------------------------------------------|
| `listeners[].host`          | Bind address (`0.0.0.0` = all interfaces)              |
| `listeners[].port`          | TCP port (default 1883)                                |
| `listeners[].tls`           | Optional TLS block (`certfile`, `keyfile`, `ca_certs`) |
| `auth.enabled`              | Enforce username/password auth                         |
| `auth.allow_anonymous`      | Allow clients with no credentials                      |
| `auth.users`                | Map of `username: password` (plain or `sha256:<hex>`)  |
| `forwarders`                | List of HTTP / MQTT / UDP forwarder configs            |
| `limits.max_connections`    | Maximum simultaneous TCP connections                   |
| `limits.max_packet_size`    | Maximum MQTT packet size in bytes                      |
| `limits.forwarder_threads`  | Thread pool size for blocking forwarders               |
| `logging.level`             | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR`         |
| `logging.file`              | Log file path (omit to log to stdout only)             |

### `integration/config.yaml` — Integration Service

| Field            | Description                                          |
|------------------|------------------------------------------------------|
| `broker.host`    | Your broker's LAN IP (e.g. `192.168.0.104`)          |
| `broker.port`    | Broker port (default 1883)                           |
| `broker.client_id` | MQTT client ID for the integration service         |
| `subscriptions`  | List of `{topic, qos}` to subscribe                 |
| `integrations`   | List of HTTP / MQTT / ChirpStack integrations        |

### `integration/settings.json` — UI-Configured Settings

Written by `frontend/index.html`, read at startup.

| Field                          | Description                                      |
|--------------------------------|--------------------------------------------------|
| `chirpstack.api_url`           | Primary ChirpStack REST API URL                  |
| `chirpstack.api_key`           | Primary ChirpStack API Bearer token              |
| `chirpstack.application_id`    | Default Application UUID for new devices         |
| `chirpstack.device_profile_id` | Default Device Profile UUID for new devices      |
| `chirpstack.mqtt_host`         | Primary ChirpStack Mosquitto host                |
| `chirpstack.mqtt_port`         | Primary ChirpStack Mosquitto port                |
| `chirpstack.mqtt_username`     | Primary Mosquitto username (optional)            |
| `chirpstack.mqtt_password`     | Primary Mosquitto password (optional)            |
| `targets`                      | Array of secondary ChirpStack target configs     |
| `targets[].name`               | Display name                                     |
| `targets[].host`               | Secondary Mosquitto host                         |
| `targets[].port`               | Secondary Mosquitto port                         |
| `targets[].api_url`            | Secondary ChirpStack REST API URL                |
| `targets[].api_key`            | Secondary API Bearer token                       |
| `targets[].application_id`     | Application UUID on secondary                    |
| `targets[].profile_id`         | ABP Device Profile UUID on secondary             |
| `targets[].enabled`            | Enable/disable without deleting                  |

---

## Device Whitelist

The integration has a built-in device whitelist to control which devices are auto-created in ChirpStack when a JOIN REQUEST is detected.

### How it works

| Whitelist state | Behaviour |
|---|---|
| **Empty** (default) | All devices allowed — any JOIN REQUEST auto-creates the device |
| **Has entries** | Only listed DevEUIs are allowed — unknown devices are rejected and logged |

### Managing devices via UI

Open `frontend/index.html` → **Devices** tab:

- **Add Device** — enter Name, DevEUI (16 hex chars), optional AppKey (stored for reference)
- **Disable** — temporarily block a device without removing it
- **Remove** — delete from whitelist

### settings.json structure

```json
{
  "devices": [
    {
      "name": "Temp Sensor 01",
      "dev_eui": "0807060504030201",
      "app_key": "00000000000000000000000000000001",
      "enabled": true
    }
  ]
}
```

> **Note:** AppKey is stored for reference only. LoRaWAN JOIN REQUEST frames do not transmit the AppKey over the air — it cannot be verified from the frame. Frame verification is handled by ChirpStack using the AppKey you entered when registering the device.

---

## ChirpStack Setup

### Primary ChirpStack

1. Create a **Network Server** pointing to your ChirpStack's internal address.
2. Create a **Gateway** with EUI matching your hardware (e.g. `aabbccddee000001`).
3. Create an **Application**.
4. Create a **Device Profile** with:
   - LoRaWAN MAC version: 1.0.3
   - Regional parameters: EU868 (or your region)
   - Activation: OTAA
5. Create a **Device** under that application with your device's DevEUI and AppKey.
6. Generate an **API Key** (Tenant API key with device read/write permissions).
7. Enter all these values in the web UI Settings tab.

### Secondary ChirpStack Instances

On each secondary ChirpStack:

1. Create an **Application** (note its UUID).
2. Create an **ABP Device Profile**:
   - Activation: ABP
   - Check "Skip frame-counter validation"
3. Get an **API Key** with device create/activate permissions.
4. Add the secondary as a **Target** in the web UI Targets tab.

The integration service will automatically register devices as ABP and set
session keys whenever an OTAA join is seen on the primary.

---

## Real Device Setup

### RAK Wireless Gateway (MQTT Forwarder)

Configure the RAK gateway's `chirpstack-mqtt-forwarder`:

```toml
# /etc/chirpstack-mqtt-forwarder/chirpstack-mqtt-forwarder.toml

[mqtt]
  server = "tcp://192.168.0.104:1883"    # point to this broker's LAN IP
  topic_prefix = "eu868"                 # must match your region

[backend]
  enabled = "concentratord"
```

Restart the forwarder service:

```bash
sudo systemctl restart chirpstack-mqtt-forwarder
```

The gateway will now publish uplink frames to:
```
eu868/gateway/{gateway_eui}/event/up
```

---

## Test Devices

### Simulated MQTT Devices

Runs 10 virtual sensors, each publishing JSON telemetry every 30 seconds.
Also exposes a status HTTP server on port 8081.

```bash
# Default broker at 192.168.0.104:1883, 30s interval
python test_devices/mqtt_devices.py

# Custom options
python test_devices/mqtt_devices.py --host 192.168.0.104 --port 1883 --interval 10
```

**Topics used:**
```
devices/sensor-001/telemetry
devices/sensor-002/telemetry
...
devices/sensor-010/telemetry
```

**Control server:**
```
GET  http://localhost:8081/status   — JSON status of all devices
POST http://localhost:8081/stop     — stop all devices
```

**Payload example:**
```json
{
  "deviceId": "sensor-001",
  "location": "Room A",
  "temperature": 21.4,
  "humidity": 52.0,
  "battery": 3.92,
  "rssi": -73,
  "timestamp": "2026-01-01T12:00:00+00:00"
}
```

### LoRaWAN Gateway Simulator

Simulates a RAK-style gateway publishing valid LoRaWAN Unconfirmed Data Up
frames (AES-128 encrypted payload, CMAC MIC).

```bash
# Default broker at 192.168.0.104:1883, 30s interval
python test_devices/lorawan_sim.py

# Custom options
python test_devices/lorawan_sim.py --host 192.168.0.104 --port 1883 --interval 15
```

**Simulator parameters:**
```
Gateway EUI : aabbccddee000001
Device EUI  : 0807060504030201
DevAddr     : 01ab23cd
AppSKey     : 00000000000000000000000000000001
NwkSKey     : 00000000000000000000000000000002
FPort       : 1
Payload     : int16 temperature (x100) + uint8 humidity, AES-128 encrypted
MIC         : AES-128-CMAC with NwkSKey
```

**Topic:**
```
eu868/gateway/aabbccddee000001/event/up
```

---

## MQTTX Testing

See `test_devices/mqttx_tests.md` for step-by-step manual test cases covering:

- Simple pub/sub verification
- Gateway frame bridging
- LoRaWAN JOIN REQUEST simulation
- ChirpStack join event mirror flow

---

## API Reference

The integration service (`http://localhost:8000`) exposes:

| Method | Path                  | Description                                             |
|--------|-----------------------|---------------------------------------------------------|
| GET    | `/health`             | Liveness probe — returns `{"status": "ok"}`             |
| GET    | `/metrics`            | Message counters: received, forwarded, errors           |
| GET    | `/integrations`       | List active integrations with name, type, enabled state |
| GET    | `/settings`           | Load current settings (from `settings.json`)            |
| POST   | `/settings`           | Save settings JSON body — reloads targets immediately   |
| GET    | `/chirpstack/test`    | Test primary ChirpStack API connectivity                |
| GET    | `/chirpstack/targets` | List MQTT target connection statuses                    |
| GET    | `/docs`               | Swagger UI (auto-generated by FastAPI)                  |

---

## Repository Structure

```
.
+-- config.yaml                    # broker config (port 1883, auth, forwarders)
+-- requirements.txt               # broker Python deps
+-- mqtt_broker/                   # pure-Python asyncio MQTT broker package
|   +-- __main__.py                # entry point: python -m mqtt_broker
|   +-- broker.py                  # asyncio server, client handler, routing
|   +-- protocol.py                # MQTT 3.1/3.1.1/5.0 codec
|   +-- session.py                 # session state, offline queue, QoS-2 flow
|   +-- router.py                  # topic-filter matching (+, #)
|   +-- auth.py                    # username/password auth
|   +-- forwarder.py               # built-in HTTP / UDP / MQTT forwarders
+-- integration/                   # FastAPI integration service
|   +-- config.yaml                # integration config (broker host, subscriptions)
|   +-- main.py                    # FastAPI app + MQTT subscriber loop
|   +-- settings.json              # UI-saved settings (ChirpStack API, targets)
|   +-- integrations/
|       +-- base.py                # BaseIntegration abstract class
|       +-- chirpstack.py          # ChirpStack MQTT bridge + OTAA mirror
|       +-- http_post.py           # async HTTP/HTTPS with retries
|       +-- mqtt_bridge.py         # generic MQTT bridge
+-- frontend/
|   +-- index.html                 # integration config UI (dark theme)
+-- broker_ui/
|   +-- index.html                 # broker monitor dashboard
+-- test_devices/
|   +-- mqtt_devices.py            # 10 simulated MQTT sensor devices
|   +-- lorawan_sim.py             # LoRaWAN gateway + ABP device simulator
|   +-- mqttx_tests.md             # manual MQTTX test instructions
+-- Dockerfile                     # broker container image
+-- docker-compose.yml             # runs broker + integration together
```

---

## Troubleshooting

### Cannot connect from WSL / Docker — use LAN IP, not localhost

The broker binds to `0.0.0.0:1883`. Clients inside WSL or Docker containers
**cannot** reach it via `localhost` or `127.0.0.1` — use the machine's LAN IP:

```bash
# Find your LAN IP (Windows)
ipconfig | findstr "IPv4"
```

Update `integration/config.yaml` -> `broker.host` to your LAN IP (e.g. `192.168.0.104`).

### Windows asyncio "Event loop closed" Error

All async scripts in this project already include the fix at the top:

```python
import sys, asyncio
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
```

If you see `RuntimeError: Event loop is closed` in a custom script, add these
lines before any `asyncio.run()` call.

### Port 1883 Already in Use

Another MQTT broker (Mosquitto, etc.) may be running on port 1883.

```bash
# Windows — find what is using port 1883
netstat -ano | findstr :1883

# Kill by PID
taskkill /PID <pid> /F

# Or change the broker port in config.yaml listeners section
```

### Port 8000 Already in Use (Integration Service)

```bash
# Windows
netstat -ano | findstr :8000

# Or run on a different port:
uvicorn integration.main:app --port 8001
```

### Integration Service Cannot Connect to Broker

- Ensure the broker is running: `python -m mqtt_broker --config config.yaml`
- Check `integration/config.yaml` -> `broker.host` matches your broker's LAN IP
- Verify no firewall blocks port 1883

### ChirpStack API Test Returns 401 Unauthorized

- Regenerate the API key in ChirpStack (Tenant -> API Keys -> Add API Key)
- Paste the full Bearer token string (without the `Bearer ` prefix) into the
  Settings UI API Key field, then click Save

### LoRaWAN Frames Not Appearing in ChirpStack

1. Verify the gateway EUI in the simulator matches a registered gateway in ChirpStack
2. Check that the integration service has a primary MQTT target configured
3. Check ChirpStack -> Network Server -> Gateway -> Frames tab (may take 30s)
4. Ensure the region prefix (`eu868`) matches ChirpStack's configured region

---

## MQTT Protocol Support

| Feature                           | Supported |
|-----------------------------------|-----------|
| MQTT 3.1                          | Yes       |
| MQTT 3.1.1                        | Yes       |
| MQTT 5.0                          | Yes       |
| QoS 0 (at most once)              | Yes       |
| QoS 1 (at least once)             | Yes       |
| QoS 2 (exactly once)              | Yes       |
| Retained messages                 | Yes       |
| Will (LWT) messages               | Yes       |
| Persistent sessions               | Yes       |
| Offline message queue             | Yes       |
| Wildcard subscriptions (+, #)     | Yes       |
| Topic aliases (MQTT 5.0)          | Yes       |
| No-local flag (MQTT 5.0)          | Yes       |
| TLS / mTLS                        | Yes       |
| Username / password auth          | Yes       |
| SHA-256 hashed passwords          | Yes       |
| WebSocket transport               | Planned   |
| Shared subscriptions              | Planned   |

---

## License

MIT
