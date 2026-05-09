# MQTTX Testing Guide

Manual test cases for verifying the MQTT broker and LoRaWAN integration using
[MQTTX](https://mqttx.app/) (desktop or CLI).

---

## Connection Settings

| Field         | Value             |
|---------------|-------------------|
| Host          | `192.168.0.104`   |
| Port          | `1883`            |
| Protocol      | MQTT 3.1.1        |
| Client ID     | `mqttx-test-001`  |
| Username      | *(leave blank)*   |
| Password      | *(leave blank)*   |
| TLS           | Off               |
| Clean Session | On                |

Click **Connect** — you should see the green "Connected" indicator.

---

## Test 1 — Simple MQTT Pub/Sub

Verify the broker routes messages between clients.

### Step 1 — Subscribe

- **Topic**: `devices/#`
- **QoS**: 0
- Click **Subscribe**

### Step 2 — Publish

| Field   | Value                                                                 |
|---------|-----------------------------------------------------------------------|
| Topic   | `devices/test/data`                                                   |
| QoS     | 0                                                                     |
| Payload | `{"deviceId": "test", "temperature": 22.5, "humidity": 55, "battery": 3.8, "rssi": -72, "timestamp": "2026-01-01T00:00:00Z"}` |

Click **Publish**.

### Expected Result

- The subscription pane immediately shows the message on topic `devices/test/data`.
- The integration service log (stdout) shows:  
  `[integration] subscriber_loop — received devices/test/data`
- No ChirpStack forwarding occurs for this topic (it is not a gateway frame).

---

## Test 2 — Gateway Frame Bridging

Publish a minimal gateway uplink frame and verify the integration bridges it to
configured ChirpStack targets.

### Publish

| Field   | Value                                      |
|---------|--------------------------------------------|
| Topic   | `eu868/gateway/aabbccddee000001/event/up`  |
| QoS     | 0                                          |
| Payload | (see JSON below)                           |

```json
{
  "phyPayload": "QM0jqwEAAQB3dpU=",
  "txInfo": {
    "frequency": 868100000,
    "modulation": {
      "lora": {
        "bandwidth": 125000,
        "spreadingFactor": 7,
        "codeRate": "CR_4_5"
      }
    }
  },
  "rxInfo": {
    "gatewayId": "aabbccddee000001",
    "rssi": -65,
    "snr": 9.5,
    "channel": 0,
    "context": "AAAAAAA=",
    "crcStatus": "CRC_OK",
    "uplinkId": 1
  }
}
```

### Expected Result

- Integration service log shows:  
  `ChirpStack integration — forwarding gateway frame on eu868/gateway/aabbccddee000001/event/up`
- Each enabled MQTT target in your settings receives the same JSON on the same topic.
- If a primary ChirpStack host is configured, the frame appears in ChirpStack's
  "LoRa Frames" tab for gateway `aabbccddee000001`.
- If no targets are configured the integration still receives it but silently drops
  the forwarding step (no targets to publish to).

---

## Test 3 — LoRaWAN JOIN Simulation

Publish a JOIN REQUEST frame and observe the integration's auto-provisioning flow.

### Publish

| Field   | Value                                      |
|---------|--------------------------------------------|
| Topic   | `eu868/gateway/aabbccddee000001/event/up`  |
| QoS     | 0                                          |
| Payload | (see JSON below)                           |

```json
{
  "phyPayload": "AAQBAQEBAQEBAgMEBQYHCAoAd6tvu8=",
  "txInfo": {
    "frequency": 868100000,
    "modulation": {
      "lora": {
        "bandwidth": 125000,
        "spreadingFactor": 7,
        "codeRate": "CR_4_5"
      }
    }
  },
  "rxInfo": {
    "gatewayId": "aabbccddee000001",
    "rssi": -80,
    "snr": 7.0,
    "channel": 0,
    "context": "AAAAAAA=",
    "crcStatus": "CRC_OK",
    "uplinkId": 42
  }
}
```

> The `phyPayload` value `AAQBAQEBAQEBAgMEBQYHCAoAd6tvu8=` is a valid LoRaWAN 1.0
> JOIN REQUEST with:
> - AppEUI: `0102030405060708`
> - DevEUI: `0807060504030201`
> - DevNonce: `0x0A00`

### What Happens

1. The broker receives the publish and routes it to the integration service.
2. The integration detects the topic contains `gateway` — it bridges the raw JSON
   to all configured ChirpStack MQTT targets.
3. The integration's `_extract_dev_eui_from_join` parses the `phyPayload`:
   - MHDR byte = `0x00` → JOIN REQUEST message type (bits 7..5 = `000`).
   - DevEUI bytes 1..8 (reversed) = `0807060504030201`.
4. If a primary ChirpStack API is configured and the device does not already exist,
   the integration calls `POST /api/devices` to create it with the configured
   Application ID and Device Profile ID.
5. ChirpStack processes the JOIN REQUEST and sends a JOIN ACCEPT if the device is
   registered with an OTAA profile and matching AppKey.
6. Integration log:  
   ```
   Join detected: 0807060504030201 — fetching session keys
   Got session keys for 0807060504030201 (DevAddr=...)
   Mirrored 0807060504030201 as ABP on http://secondary-cs:8091
   ```

### Expected Result (no ChirpStack configured)

- Frame is forwarded to MQTT targets (if any).
- Integration log shows it parsed a JOIN REQUEST frame.
- No API calls are made (primary API URL not set).

---

## Test 4 — ChirpStack Join Event Mirror

This test verifies the **primary watcher** loop: the integration connects to the
primary ChirpStack's Mosquitto broker, listens for join events, then mirrors the
device session keys as ABP activations to all secondary ChirpStack targets.

### Prerequisites

- Primary ChirpStack Mosquitto host/port configured in the integration settings UI
  (`frontend/index.html` → Settings → Primary ChirpStack MQTT).
- At least one secondary target configured with a working ChirpStack REST API URL
  and API key.

### Trigger

Publish the following JSON directly to the **primary ChirpStack's Mosquitto**
(not your broker) using a separate MQTTX connection:

| Field         | Value                                              |
|---------------|----------------------------------------------------|
| Host          | `<primary ChirpStack IP>`                          |
| Port          | `1883`                                             |
| Topic         | `application/<app-uuid>/device/<dev-eui>/event/join` |
| Payload       | (see JSON below)                                   |

```json
{
  "deduplicationId": "test-dedup-001",
  "time": "2026-01-01T12:00:00Z",
  "deviceInfo": {
    "tenantId": "your-tenant-id",
    "tenantName": "Your Tenant",
    "applicationId": "your-app-uuid",
    "applicationName": "Your App",
    "deviceProfileId": "your-profile-uuid",
    "deviceProfileName": "OTAA Profile",
    "deviceName": "TestDevice",
    "devEui": "0807060504030201"
  },
  "devAddr": "01ab23cd"
}
```

### What Happens

1. The integration's `primary_watcher_loop` receives the join event on the
   primary ChirpStack Mosquitto.
2. It extracts `devEui = "0807060504030201"` from `deviceInfo`.
3. It calls `handle_join("0807060504030201", ...)` which:
   a. Fetches session keys from primary API:
      `GET /api/devices/0807060504030201/activation`
   b. For each secondary API target:
      - Creates the device if it does not exist:
        `POST /api/devices`
      - Sets ABP activation (session keys):
        `POST /api/devices/0807060504030201/activate`
4. The device now appears in all secondary ChirpStack instances with the same
   session keys and can decode subsequent uplinks without another OTAA exchange.

### Expected Integration Logs

```
Primary watcher connected to <primary-host>:1883
Join detected: 0807060504030201 — fetching session keys
Got session keys for 0807060504030201 (DevAddr=01ab23cd)
Mirrored 0807060504030201 as ABP on http://secondary-cs-1:8091
Mirrored 0807060504030201 as ABP on http://secondary-cs-2:8091
```

### Expected Result

- The device appears in each secondary ChirpStack with DevAddr `01ab23cd`.
- Subsequent uplinks (forwarded to secondary Mosquitto brokers) are decoded and
  displayed in the secondary ChirpStack Live Device Data tab.

---

## Quick Reference — Topic Patterns

| Purpose                        | Topic pattern                                       |
|--------------------------------|-----------------------------------------------------|
| Generic device telemetry       | `devices/{device_id}/telemetry`                     |
| Any device topic               | `devices/#`                                         |
| Gateway uplink frame           | `eu868/gateway/{gw_eui}/event/up`                   |
| Gateway downlink ack           | `eu868/gateway/{gw_eui}/event/ack`                  |
| Gateway stats                  | `eu868/gateway/{gw_eui}/event/stats`                |
| ChirpStack app join event      | `application/{app_id}/device/{dev_eui}/event/join`  |
| ChirpStack app uplink event    | `application/{app_id}/device/{dev_eui}/event/up`    |
| All topics                     | `#`                                                 |
