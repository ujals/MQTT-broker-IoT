"""
mqtt_devices.py — 10 simulated MQTT devices
============================================
Each device publishes JSON telemetry every --interval seconds to:
  devices/{device_id}/telemetry

Payload:
  {"deviceId": "...", "temperature": ..., "humidity": ...,
   "battery": ..., "rssi": ..., "timestamp": "..."}

Also exposes a lightweight HTTP control server on port 8081:
  GET  /status  — list all devices + last publish time
  POST /stop    — stop all devices and exit

Usage:
  python test_devices/mqtt_devices.py --host 192.168.0.104 --port 1883 --interval 30
"""

import argparse
import asyncio
import json
import random
import sys
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

try:
    import aiomqtt
except ImportError:
    print("ERROR: aiomqtt not installed.  Run: pip install aiomqtt")
    sys.exit(1)

# ── Device definitions ────────────────────────────────────────────────────────

DEVICES = [
    {"id": "sensor-001", "location": "Room A",      "floor": 1},
    {"id": "sensor-002", "location": "Room B",      "floor": 1},
    {"id": "sensor-003", "location": "Hallway",     "floor": 1},
    {"id": "sensor-004", "location": "Room C",      "floor": 2},
    {"id": "sensor-005", "location": "Room D",      "floor": 2},
    {"id": "sensor-006", "location": "Lab",         "floor": 2},
    {"id": "sensor-007", "location": "Server Room", "floor": 3},
    {"id": "sensor-008", "location": "Rooftop",     "floor": 4},
    {"id": "sensor-009", "location": "Basement",    "floor": 0},
    {"id": "sensor-010", "location": "Lobby",       "floor": 1},
]

# Shared state for HTTP status server
_device_state: dict[str, dict] = {
    d["id"]: {
        "location":       d["location"],
        "floor":          d["floor"],
        "last_publish":   None,
        "last_payload":   None,
        "publish_count":  0,
        "status":         "starting",
    }
    for d in DEVICES
}

_stop_event = asyncio.Event()
_http_stop  = False


# ── Telemetry generator ───────────────────────────────────────────────────────

def _make_telemetry(device: dict) -> dict:
    """Generate realistic-ish sensor telemetry."""
    device_id = device["id"]
    floor     = device["floor"]

    # Vary readings slightly per device for realism
    base_temp = 18.0 + floor * 1.5 + hash(device_id) % 5
    base_hum  = 40 + hash(device_id) % 30

    return {
        "deviceId":    device_id,
        "location":    device["location"],
        "temperature": round(base_temp + random.uniform(-2.0, 2.0), 2),
        "humidity":    round(max(10, min(95, base_hum + random.uniform(-5, 5))), 1),
        "battery":     round(random.uniform(3.0, 4.2), 2),
        "rssi":        random.randint(-110, -45),
        "timestamp":   datetime.now(timezone.utc).isoformat(),
    }


# ── Per-device publish loop ───────────────────────────────────────────────────

async def device_loop(device: dict, host: str, port: int, interval: int) -> None:
    device_id = device["id"]
    topic     = f"devices/{device_id}/telemetry"
    cid       = f"sim-{device_id}"

    _device_state[device_id]["status"] = "connecting"

    while not _stop_event.is_set():
        try:
            async with aiomqtt.Client(hostname=host, port=port, identifier=cid) as client:
                _device_state[device_id]["status"] = "connected"
                print(f"[{device_id}] Connected to {host}:{port}")

                while not _stop_event.is_set():
                    payload = _make_telemetry(device)
                    json_payload = json.dumps(payload)

                    await client.publish(topic, json_payload, qos=0)

                    now_str = datetime.now(timezone.utc).isoformat()
                    _device_state[device_id]["last_publish"] = now_str
                    _device_state[device_id]["last_payload"] = payload
                    _device_state[device_id]["publish_count"] += 1
                    _device_state[device_id]["status"] = "publishing"

                    count = _device_state[device_id]["publish_count"]
                    print(
                        f"[{device_id}] #{count:04d}  "
                        f"temp={payload['temperature']:+.1f}C  "
                        f"hum={payload['humidity']:.0f}%  "
                        f"bat={payload['battery']:.2f}V  "
                        f"rssi={payload['rssi']}dBm  "
                        f"-> {topic}"
                    )

                    # Wait for next interval (interruptible)
                    try:
                        await asyncio.wait_for(_stop_event.wait(), timeout=interval)
                    except asyncio.TimeoutError:
                        pass  # Normal — interval elapsed, publish again

        except aiomqtt.MqttError as exc:
            _device_state[device_id]["status"] = "disconnected"
            print(f"[{device_id}] MQTT error: {exc} — retrying in 10s")
            try:
                await asyncio.wait_for(_stop_event.wait(), timeout=10)
            except asyncio.TimeoutError:
                pass
        except asyncio.CancelledError:
            break

    _device_state[device_id]["status"] = "stopped"
    print(f"[{device_id}] Stopped.")


# ── HTTP control server (runs in a background thread) ────────────────────────

class _Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # Silence default access logs

    def do_GET(self):
        if self.path == "/status":
            body = json.dumps({
                "devices": _device_state,
                "server_time": datetime.now(timezone.utc).isoformat(),
            }, indent=2).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        global _http_stop
        if self.path == "/stop":
            _http_stop = True
            body = b'{"ok": true, "message": "stopping all devices"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            # Signal the async event loop to stop
            try:
                loop = asyncio.get_event_loop()
                loop.call_soon_threadsafe(_stop_event.set)
            except RuntimeError:
                pass
        else:
            self.send_response(404)
            self.end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()


def _run_http_server(port: int = 8081) -> HTTPServer:
    server = HTTPServer(("0.0.0.0", port), _Handler)
    t = Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server


# ── Main ──────────────────────────────────────────────────────────────────────

async def main(host: str, port: int, interval: int) -> None:
    print("=" * 60)
    print(f"  MQTT Device Simulator — {len(DEVICES)} devices")
    print(f"  Broker : {host}:{port}")
    print(f"  Interval: {interval}s")
    print(f"  Control : http://localhost:8081/status")
    print("=" * 60)

    http_server = _run_http_server(8081)
    print("HTTP control server started on :8081")

    tasks = [
        asyncio.create_task(device_loop(d, host, port, interval))
        for d in DEVICES
    ]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        _stop_event.set()
        for t in tasks:
            t.cancel()
        http_server.shutdown()
        print("\nAll devices stopped. Goodbye.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulated MQTT device publisher")
    parser.add_argument("--host",     default="192.168.0.104", help="MQTT broker host")
    parser.add_argument("--port",     default=1883, type=int,  help="MQTT broker port")
    parser.add_argument("--interval", default=30,  type=int,  help="Publish interval (seconds)")
    args = parser.parse_args()

    try:
        asyncio.run(main(args.host, args.port, args.interval))
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
