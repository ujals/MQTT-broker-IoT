"""
Integration Service — MQTT fan-out + ChirpStack device auto-provisioning

API
---
  GET  /health            liveness probe
  GET  /metrics           message counters
  GET  /integrations      list integrations
  GET  /settings          load saved settings
  POST /settings          save settings (JSON body)
  GET  /chirpstack/test   test ChirpStack API connectivity
"""

import argparse
import asyncio
import json
import logging
import logging.handlers
import os
import sys
import time

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
from contextlib import asynccontextmanager
from typing import Any, Dict, List

import aiomqtt
import uvicorn
import yaml
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from integrations import build_integrations
from integrations.base import BaseIntegration
from integrations.chirpstack import ChirpStackIntegration

log = logging.getLogger("integration")

SETTINGS_FILE = os.path.join(os.path.dirname(__file__), "settings.json")

_cfg:            Dict[str, Any]        = {}
_integrations:   List[BaseIntegration] = []
_metrics                               = {"received": 0, "forwarded": 0, "errors": 0}
_shared_settings: Dict[str, Any]      = {}   # live settings — updated via API
_topic_stats:    Dict[str, int]        = {}   # topic → message count
_message_times:  List[float]           = []   # timestamps for rate calculation


# ── Settings persistence ──────────────────────────────────────────────────────

def _load_settings() -> Dict[str, Any]:
    if os.path.exists(SETTINGS_FILE):
        with open(SETTINGS_FILE, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    return {"chirpstack": {
        "api_url": "", "api_key": "",
        "application_id": "", "device_profile_id": "",
        "mqtt_host": "", "mqtt_port": 1883,
    }}


def _save_settings(data: dict) -> None:
    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


# ── MQTT subscriber loop ──────────────────────────────────────────────────────

async def primary_watcher_loop() -> None:
    """Subscribe to primary ChirpStack Mosquitto for join events, then mirror to secondaries."""
    while True:
        cs = _shared_settings.get("chirpstack", {})
        host = cs.get("mqtt_host", "")
        port = int(cs.get("mqtt_port", 1883))
        username = cs.get("mqtt_username", "")
        password = cs.get("mqtt_password", "")
        if not host:
            await asyncio.sleep(15)
            continue
        try:
            kwargs = {"hostname": host, "port": port, "identifier": "intg-primary-watcher"}
            if username:
                kwargs["username"] = username
                kwargs["password"] = password
            async with aiomqtt.Client(**kwargs) as client:
                log.info("Primary watcher connected to %s:%d", host, port)
                await client.subscribe("application/+/device/+/event/join")
                async for msg in client.messages:
                    try:
                        payload = json.loads(bytes(msg.payload))
                        info = payload.get("deviceInfo", {})
                        dev_eui = info.get("devEui", "")
                        if not dev_eui:
                            continue
                        cs_intg = next((i for i in _integrations if isinstance(i, ChirpStackIntegration)), None)
                        if cs_intg:
                            await cs_intg.handle_join(
                                dev_eui,
                                app_id=info.get("applicationId", ""),
                                profile_id=info.get("deviceProfileId", ""),
                                device_name=info.get("deviceName", ""),
                            )
                    except Exception as exc:
                        log.error("primary_watcher msg error: %s", exc)
        except aiomqtt.MqttError as exc:
            log.warning("Primary watcher lost (%s) — retry in 10s", exc)
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            log.error("Primary watcher error: %s — retry in 10s", exc)
            await asyncio.sleep(10)


async def subscriber_loop() -> None:
    broker = _cfg.get("broker", {})
    host   = broker.get("host", "localhost")
    port   = int(broker.get("port", 1883))
    cid    = broker.get("client_id", "integration-service")
    subs   = _cfg.get("subscriptions", [{"topic": "#", "qos": 0}])

    retry = 5
    while True:
        try:
            async with aiomqtt.Client(hostname=host, port=port, identifier=cid) as client:
                log.info("Connected to broker %s:%d", host, port)
                for s in subs:
                    await client.subscribe(s["topic"], qos=s.get("qos", 0))

                async for msg in client.messages:
                    _metrics["received"] += 1
                    topic     = str(msg.topic)
                    raw_bytes = bytes(msg.payload)

                    # Track topic stats and message rate
                    _topic_stats[topic] = _topic_stats.get(topic, 0) + 1
                    now = time.time()
                    _message_times.append(now)
                    # Trim to last 60 seconds
                    while _message_times and _message_times[0] < now - 60:
                        _message_times.pop(0)

                    try:
                        payload = json.loads(raw_bytes.decode("utf-8", errors="replace"))
                    except Exception:
                        payload = {"raw": raw_bytes.decode("utf-8", errors="replace")}

                    raw_str = json.dumps(payload)

                    for intg in _integrations:
                        if not intg.enabled:
                            continue
                        try:
                            await intg.forward(topic, payload, raw_str)
                            _metrics["forwarded"] += 1
                        except Exception as exc:
                            _metrics["errors"] += 1
                            log.error("[%s] forward error: %s", intg.name, exc)

        except aiomqtt.MqttError as exc:
            log.warning("Broker lost (%s) — retry in %ds", exc, retry)
            await asyncio.sleep(retry)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            log.error("Subscriber error: %s — retry in %ds", exc, retry)
            await asyncio.sleep(retry)


# ── FastAPI ───────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    _shared_settings.update(_load_settings())
    task1 = asyncio.create_task(subscriber_loop())
    task2 = asyncio.create_task(primary_watcher_loop())
    yield
    task1.cancel()
    task2.cancel()
    for t in (task1, task2):
        try:
            await t
        except asyncio.CancelledError:
            pass
    for intg in _integrations:
        await intg.close()


app = FastAPI(title="MQTT Integration Service", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def get_metrics():
    return _metrics


@app.get("/integrations")
def list_integrations():
    return [{"name": i.name, "type": i.type, "enabled": i.enabled} for i in _integrations]


@app.get("/settings")
def get_settings():
    return _shared_settings


@app.post("/settings")
async def save_settings(body: dict):
    _shared_settings.update(body)
    _save_settings(_shared_settings)
    cs_intg = next((i for i in _integrations if isinstance(i, ChirpStackIntegration)), None)
    if cs_intg:
        cs_intg.reload_targets()
    log.info("Settings updated")
    return {"ok": True}


@app.get("/chirpstack/test")
async def chirpstack_test():
    cs_intg = next((i for i in _integrations if isinstance(i, ChirpStackIntegration)), None)
    if cs_intg is None:
        return JSONResponse({"ok": False, "error": "ChirpStack integration not loaded"})
    result = await cs_intg.test_connection()
    return JSONResponse(result)


@app.get("/broker/stats")
def broker_stats():
    now    = time.time()
    recent = [t for t in _message_times if now - t < 10]
    top    = sorted(_topic_stats.items(), key=lambda x: -x[1])[:20]
    return {
        "message_rate":   round(len(recent) / 10.0, 2),
        "total_messages": _metrics["received"],
        "active_topics":  len(_topic_stats),
        "errors":         _metrics["errors"],
        "top_topics":     [{"topic": t, "count": c} for t, c in top],
    }


@app.get("/chirpstack/targets")
def get_targets():
    cs_intg = next((i for i in _integrations if isinstance(i, ChirpStackIntegration)), None)
    if cs_intg is None:
        return []
    return cs_intg.targets_status()


# ── Entry point ───────────────────────────────────────────────────────────────

def setup_logging(cfg: dict) -> None:
    lc    = cfg.get("logging", {})
    level = getattr(logging, lc.get("level", "INFO").upper(), logging.INFO)
    fmt   = "%(asctime)s [%(levelname)-8s] %(name)s – %(message)s"
    logging.basicConfig(level=level, format=fmt, handlers=[logging.StreamHandler(sys.stdout)])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", default="config.yaml")
    args = parser.parse_args()

    with open(args.config, "r", encoding="utf-8") as f:
        _cfg.update(yaml.safe_load(f))

    setup_logging(_cfg)
    _shared_settings.update(_load_settings())
    _integrations.extend(build_integrations(_cfg.get("integrations", []), _shared_settings))

    log.info("=== Integration Service — ChirpStack integration active ===")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
