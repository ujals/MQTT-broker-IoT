"""
Integration Service
===================
Subscribes to the local MQTT broker and fans out every message to all
configured integrations (HTTP endpoints, ChirpStack, TTN, other brokers).

API endpoints
-------------
  GET /health          liveness probe
  GET /metrics         message counters (received / forwarded / errors)
  GET /integrations    list of configured integrations + enabled state
"""

import argparse
import asyncio
import json
import logging
import logging.handlers
import sys
from contextlib import asynccontextmanager
from typing import Any, Dict, List

import aiomqtt
import uvicorn
import yaml
from fastapi import FastAPI

from integrations import build_integrations
from integrations.base import BaseIntegration

log = logging.getLogger("integration")

# ── Shared state (set before uvicorn.run) ─────────────────────────────────────
_cfg:          Dict[str, Any]      = {}
_integrations: List[BaseIntegration] = []
_metrics = {"received": 0, "forwarded": 0, "errors": 0}


# ── MQTT subscriber loop ──────────────────────────────────────────────────────

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
                    log.info("Subscribed to '%s' QoS%d", s["topic"], s.get("qos", 0))

                async for msg in client.messages:
                    _metrics["received"] += 1
                    topic     = str(msg.topic)
                    raw_bytes = bytes(msg.payload)

                    try:
                        payload = json.loads(raw_bytes.decode("utf-8", errors="replace"))
                    except Exception:
                        payload = {"raw": raw_bytes.decode("utf-8", errors="replace")}

                    raw_str = json.dumps(payload)
                    log.debug("RX '%s'  %s", topic, raw_str[:120])

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
            log.warning("Broker connection lost (%s) — retry in %ds", exc, retry)
            await asyncio.sleep(retry)
        except asyncio.CancelledError:
            log.info("Subscriber stopped.")
            return
        except Exception as exc:
            log.error("Unexpected subscriber error: %s — retry in %ds", exc, retry)
            await asyncio.sleep(retry)


# ── FastAPI app ───────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(subscriber_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    for intg in _integrations:
        await intg.close()
    log.info("Integration service stopped.")


app = FastAPI(title="MQTT Integration Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def get_metrics():
    return _metrics


@app.get("/integrations")
def list_integrations():
    return [
        {"name": i.name, "type": i.type, "enabled": i.enabled}
        for i in _integrations
    ]


# ── Entry point ───────────────────────────────────────────────────────────────

def setup_logging(cfg: dict) -> None:
    lc    = cfg.get("logging", {})
    level = getattr(logging, lc.get("level", "INFO").upper(), logging.INFO)
    fmt   = "%(asctime)s [%(levelname)-8s] %(name)s – %(message)s"
    logging.basicConfig(level=level, format=fmt,
                        handlers=[logging.StreamHandler(sys.stdout)])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MQTT Integration Service")
    parser.add_argument("--config", "-c", default="config.yaml")
    args = parser.parse_args()

    with open(args.config, "r", encoding="utf-8") as f:
        _cfg.update(yaml.safe_load(f))

    setup_logging(_cfg)
    _integrations.extend(build_integrations(_cfg.get("integrations", [])))

    enabled = [i.name for i in _integrations if i.enabled]
    log.info("=== Integration Service v1.0.0 — %d integration(s) active: %s ===",
             len(enabled), enabled or "none")

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
