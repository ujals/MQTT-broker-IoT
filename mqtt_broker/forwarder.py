"""
mqtt_broker.forwarder
=====================
Fan-out forwarding: HTTP/HTTPS, UDP, MQTT.

Each forwarder's forward() is called synchronously from an asyncio task,
so keep network calls short-circuiting or move to a thread pool if needed.
"""

import logging
import socket
import threading
import time
from typing import Any

import paho.mqtt.client as mqtt
import requests

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# HTTP / HTTPS
# ──────────────────────────────────────────────────────────────────────────────

class HttpForwarder:
    def __init__(self, cfg: dict, logger: logging.Logger):
        self.name       = cfg.get("name", "http")
        self.url        = cfg["url"]
        self.method     = cfg.get("method", "POST").upper()
        self.headers    = cfg.get("headers", {"Content-Type": "application/json"})
        self.timeout    = cfg.get("timeout", 10)
        self.ssl_verify = cfg.get("ssl_verify", True)
        self.logger     = logger.getChild(f"http[{self.name}]")

    def forward(self, topic: str, payload: Any, raw: str) -> None:
        url = self.url.replace("{topic}", topic)
        try:
            resp = requests.request(
                self.method, url,
                data=raw, headers=self.headers,
                timeout=self.timeout, verify=self.ssl_verify,
            )
            self.logger.info("→ %s %s [%d]", self.method, url, resp.status_code)
            if resp.status_code >= 400:
                self.logger.warning("Non-OK: %s", resp.text[:200])
        except requests.RequestException as exc:
            self.logger.error("Request failed: %s", exc)


# ──────────────────────────────────────────────────────────────────────────────
# UDP
# ──────────────────────────────────────────────────────────────────────────────

class UdpForwarder:
    def __init__(self, cfg: dict, logger: logging.Logger):
        self.name   = cfg.get("name", "udp")
        self.host   = cfg["host"]
        self.port   = int(cfg["port"])
        self.logger = logger.getChild(f"udp[{self.name}]")
        self._sock  = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def forward(self, topic: str, payload: Any, raw: str) -> None:
        data = raw.encode("utf-8")
        try:
            self._sock.sendto(data, (self.host, self.port))
            self.logger.info("→ UDP %s:%d (%d bytes)", self.host, self.port, len(data))
        except OSError as exc:
            self.logger.error("UDP send failed: %s", exc)

    def close(self):
        self._sock.close()


# ──────────────────────────────────────────────────────────────────────────────
# MQTT (outbound to another broker)
# ──────────────────────────────────────────────────────────────────────────────

class MqttForwarder:
    def __init__(self, cfg: dict, logger: logging.Logger):
        self.name         = cfg.get("name", "mqtt_out")
        self.host         = cfg["host"]
        self.port         = int(cfg.get("port", 1883))
        self.username     = cfg.get("username")
        self.password     = cfg.get("password")
        self.topic_prefix = cfg.get("topic_prefix", "")
        self.topic_static = cfg.get("topic")
        self.qos          = int(cfg.get("qos", 0))
        self.retain       = bool(cfg.get("retain", False))
        self.logger       = logger.getChild(f"mqtt[{self.name}]")

        self._connected = threading.Event()
        client_id = cfg.get("client_id", f"mqtt_broker_fwd_{self.name}")
        self._client = mqtt.Client(client_id=client_id, clean_session=True)

        if self.username:
            self._client.username_pw_set(self.username, self.password)

        tls = cfg.get("tls")
        if tls:
            tls_cfg = tls if isinstance(tls, dict) else {}
            self._client.tls_set(
                ca_certs = tls_cfg.get("ca_certs"),
                certfile = tls_cfg.get("certfile"),
                keyfile  = tls_cfg.get("keyfile"),
            )

        self._client.on_connect    = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.connect_async(self.host, self.port, keepalive=60)
        self._client.loop_start()

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info("Connected to %s:%d", self.host, self.port)
            self._connected.set()
        else:
            self.logger.error("Connect error rc=%d", rc)

    def _on_disconnect(self, client, userdata, rc):
        self._connected.clear()
        if rc:
            self.logger.warning("Disconnected rc=%d — will reconnect", rc)

    def forward(self, topic: str, payload: Any, raw: str) -> None:
        if not self._connected.is_set():
            self.logger.warning("Not connected — dropping message")
            return
        out_topic = self.topic_static or (self.topic_prefix + topic)
        result    = self._client.publish(out_topic, raw, qos=self.qos, retain=self.retain)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            self.logger.info("→ MQTT %s:%d topic='%s'", self.host, self.port, out_topic)
        else:
            self.logger.error("Publish failed rc=%d", result.rc)

    def close(self):
        self._client.loop_stop()
        self._client.disconnect()


# ──────────────────────────────────────────────────────────────────────────────
# Factory
# ──────────────────────────────────────────────────────────────────────────────

def build_forwarder(cfg: dict, logger: logging.Logger):
    ftype = cfg.get("type", "").lower()
    if ftype in ("http", "https"):
        return HttpForwarder(cfg, logger)
    elif ftype == "udp":
        return UdpForwarder(cfg, logger)
    elif ftype == "mqtt":
        return MqttForwarder(cfg, logger)
    else:
        raise ValueError(f"Unknown forwarder type: '{ftype}'")
