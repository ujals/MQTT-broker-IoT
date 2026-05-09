#!/usr/bin/env python3
"""
mqtt_bridge.py  –  Universal LNS MQTT JSON Forwarder
======================================================
Subscribes to one or more topics on a source MQTT broker,
parses every incoming message as JSON, then fans-out to any
combination of:
  • HTTP / HTTPS   (POST)
  • UDP            (raw JSON bytes)
  • MQTT           (publish to another broker / IoT platform)

Configuration is read from  config.yaml  (or pass --config <path>).

Usage:
    python mqtt_bridge.py [--config config.yaml]
"""

import argparse
import json
import logging
import logging.handlers
import socket
import sys
import threading
import time
from typing import Any, Dict, List, Optional

import paho.mqtt.client as mqtt
import requests
import yaml

# ─────────────────────────────────────────────────────────────────────────────
# Logging setup
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging(cfg: dict) -> logging.Logger:
    log_cfg   = cfg.get("logging", {})
    level     = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    log_file  = log_cfg.get("file")          # optional path
    max_bytes = log_cfg.get("max_bytes", 10 * 1024 * 1024)   # 10 MB
    backups   = log_cfg.get("backup_count", 5)

    fmt = "%(asctime)s [%(levelname)s] %(name)s – %(message)s"
    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    if log_file:
        handlers.append(
            logging.handlers.RotatingFileHandler(
                log_file, maxBytes=max_bytes, backupCount=backups, encoding="utf-8"
            )
        )

    logging.basicConfig(level=level, format=fmt, handlers=handlers)
    return logging.getLogger("mqtt_bridge")


# ─────────────────────────────────────────────────────────────────────────────
# Forwarder classes
# ─────────────────────────────────────────────────────────────────────────────

class HttpForwarder:
    """POST JSON payload to an HTTP/HTTPS endpoint."""

    def __init__(self, cfg: dict, logger: logging.Logger):
        self.name    = cfg.get("name", "http")
        self.url     = cfg["url"]
        self.method  = cfg.get("method", "POST").upper()
        self.headers = cfg.get("headers", {"Content-Type": "application/json"})
        self.timeout = cfg.get("timeout", 10)
        self.verify  = cfg.get("ssl_verify", True)   # set False to skip TLS verify
        self.logger  = logger.getChild(f"http[{self.name}]")

        # Optional topic → URL template substitution
        # e.g. url: "https://host/ingest/{topic}"
        self._url_has_topic = "{topic}" in self.url

    def forward(self, topic: str, payload: Any, raw: str):
        url = self.url.replace("{topic}", topic) if self._url_has_topic else self.url
        try:
            resp = requests.request(
                self.method,
                url,
                data=raw,
                headers=self.headers,
                timeout=self.timeout,
                verify=self.verify,
            )
            self.logger.info("→ %s %s  [%d]", self.method, url, resp.status_code)
            if resp.status_code >= 400:
                self.logger.warning("Non-OK response: %s", resp.text[:200])
        except requests.RequestException as exc:
            self.logger.error("Request failed: %s", exc)


class UdpForwarder:
    """Send raw JSON bytes over UDP."""

    def __init__(self, cfg: dict, logger: logging.Logger):
        self.name   = cfg.get("name", "udp")
        self.host   = cfg["host"]
        self.port   = int(cfg["port"])
        self.logger = logger.getChild(f"udp[{self.name}]")
        self._sock  = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def forward(self, topic: str, payload: Any, raw: str):
        data = raw.encode("utf-8")
        try:
            self._sock.sendto(data, (self.host, self.port))
            self.logger.info("→ UDP %s:%d  (%d bytes)", self.host, self.port, len(data))
        except OSError as exc:
            self.logger.error("UDP send failed: %s", exc)

    def close(self):
        self._sock.close()


class MqttForwarder:
    """Publish JSON payload to another MQTT broker / IoT platform."""

    def __init__(self, cfg: dict, logger: logging.Logger):
        self.name          = cfg.get("name", "mqtt_out")
        self.host          = cfg["host"]
        self.port          = int(cfg.get("port", 1883))
        self.username      = cfg.get("username")
        self.password      = cfg.get("password")
        self.topic_prefix  = cfg.get("topic_prefix", "")   # prepend to incoming topic
        self.topic_static  = cfg.get("topic")              # override: publish to fixed topic
        self.qos           = int(cfg.get("qos", 0))
        self.retain        = bool(cfg.get("retain", False))
        self.tls           = cfg.get("tls", False)
        self.logger        = logger.getChild(f"mqtt[{self.name}]")

        client_id = cfg.get("client_id", f"mqtt_bridge_out_{self.name}")
        self._client = mqtt.Client(client_id=client_id, clean_session=True)

        if self.username:
            self._client.username_pw_set(self.username, self.password)

        if self.tls:
            tls_cfg = self.tls if isinstance(self.tls, dict) else {}
            self._client.tls_set(
                ca_certs   = tls_cfg.get("ca_certs"),
                certfile   = tls_cfg.get("certfile"),
                keyfile    = tls_cfg.get("keyfile"),
            )

        self._client.on_connect    = self._on_connect
        self._client.on_disconnect = self._on_disconnect

        self._connected = threading.Event()
        self._connect()

    def _connect(self):
        try:
            self._client.connect_async(self.host, self.port, keepalive=60)
            self._client.loop_start()
        except Exception as exc:
            self.logger.error("Connection failed: %s", exc)

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info("Connected to %s:%d", self.host, self.port)
            self._connected.set()
        else:
            self.logger.error("Connect error rc=%d", rc)

    def _on_disconnect(self, client, userdata, rc):
        self._connected.clear()
        if rc != 0:
            self.logger.warning("Unexpected disconnect rc=%d – will auto-reconnect", rc)

    def forward(self, topic: str, payload: Any, raw: str):
        if not self._connected.is_set():
            self.logger.warning("Not connected – dropping message on topic '%s'", topic)
            return

        out_topic = self.topic_static or (self.topic_prefix + topic)
        result = self._client.publish(out_topic, raw, qos=self.qos, retain=self.retain)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            self.logger.info("→ MQTT %s:%d  topic='%s'", self.host, self.port, out_topic)
        else:
            self.logger.error("Publish failed rc=%d", result.rc)

    def close(self):
        self._client.loop_stop()
        self._client.disconnect()


# ─────────────────────────────────────────────────────────────────────────────
# Forwarder factory
# ─────────────────────────────────────────────────────────────────────────────

def build_forwarder(cfg: dict, logger: logging.Logger):
    ftype = cfg.get("type", "").lower()
    if ftype in ("http", "https"):
        return HttpForwarder(cfg, logger)
    elif ftype == "udp":
        return UdpForwarder(cfg, logger)
    elif ftype == "mqtt":
        return MqttForwarder(cfg, logger)
    else:
        raise ValueError(f"Unknown forwarder type: '{ftype}'. Use http, https, udp, or mqtt.")


# ─────────────────────────────────────────────────────────────────────────────
# Main bridge class
# ─────────────────────────────────────────────────────────────────────────────

class MqttBridge:
    def __init__(self, cfg: dict, logger: logging.Logger):
        self.cfg        = cfg
        self.logger     = logger
        self.forwarders = [build_forwarder(f, logger) for f in cfg.get("forwarders", [])]

        if not self.forwarders:
            self.logger.warning("No forwarders configured – messages will only be logged.")

        src = cfg["source"]
        self._topics    = src.get("topics", ["#"])
        self._qos       = int(src.get("qos", 0))
        self._reconnect_delay = int(src.get("reconnect_delay", 5))

        client_id = src.get("client_id", "mqtt_bridge_in")
        self._client = mqtt.Client(client_id=client_id, clean_session=True)

        username = src.get("username")
        password = src.get("password")
        if username:
            self._client.username_pw_set(username, password)

        tls = src.get("tls")
        if tls:
            tls_cfg = tls if isinstance(tls, dict) else {}
            self._client.tls_set(
                ca_certs = tls_cfg.get("ca_certs"),
                certfile = tls_cfg.get("certfile"),
                keyfile  = tls_cfg.get("keyfile"),
            )

        self._client.on_connect    = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message    = self._on_message

    # ── MQTT callbacks ──────────────────────────────────────────────────────

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info(
                "Source broker connected. Subscribing to: %s", self._topics
            )
            for topic in self._topics:
                client.subscribe(topic, qos=self._qos)
        else:
            self.logger.error("Source connect error rc=%d", rc)

    def _on_disconnect(self, client, userdata, rc):
        if rc != 0:
            self.logger.warning(
                "Source broker disconnected rc=%d – retrying in %ds …",
                rc, self._reconnect_delay,
            )

    def _on_message(self, client, userdata, msg: mqtt.MQTTMessage):
        raw = msg.payload.decode("utf-8", errors="replace")
        self.logger.debug("← topic='%s'  payload=%s", msg.topic, raw[:200])

        # Parse JSON
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            self.logger.warning(
                "Non-JSON payload on topic '%s': %s – forwarding as-is", msg.topic, exc
            )
            payload = {"raw": raw}
            raw     = json.dumps(payload)

        # Add bridge metadata if configured
        if self.cfg.get("add_metadata", False):
            payload["_bridge"] = {
                "topic"     : msg.topic,
                "qos"       : msg.qos,
                "retain"    : msg.retain,
                "timestamp" : time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            raw = json.dumps(payload)

        self.logger.info("RX topic='%s' (%d bytes) → %d forwarder(s)",
                         msg.topic, len(raw), len(self.forwarders))

        # Fan-out in parallel threads so one slow forwarder can't block others
        threads = []
        for fwd in self.forwarders:
            t = threading.Thread(
                target=fwd.forward,
                args=(msg.topic, payload, raw),
                daemon=True,
            )
            t.start()
            threads.append(t)
        for t in threads:
            t.join(timeout=30)

    # ── Run ─────────────────────────────────────────────────────────────────

    def run(self):
        src  = self.cfg["source"]
        host = src.get("host", "localhost")
        port = int(src.get("port", 1883))

        self.logger.info("Connecting to source broker %s:%d …", host, port)

        self._client.reconnect_delay_set(
            min_delay=1, max_delay=self._reconnect_delay
        )
        self._client.connect(host, port, keepalive=60)

        try:
            self._client.loop_forever(retry_first_connection=True)
        except KeyboardInterrupt:
            self.logger.info("Interrupted by user – shutting down …")
        finally:
            self._client.disconnect()
            for fwd in self.forwarders:
                if hasattr(fwd, "close"):
                    fwd.close()
            self.logger.info("Bridge stopped.")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser(
        description="Universal LNS → MQTT/HTTP/UDP JSON forwarder"
    )
    parser.add_argument(
        "--config", "-c",
        default="config.yaml",
        help="Path to YAML config file (default: config.yaml)",
    )
    args = parser.parse_args()

    try:
        cfg = load_config(args.config)
    except FileNotFoundError:
        print(f"ERROR: Config file '{args.config}' not found.", file=sys.stderr)
        sys.exit(1)
    except yaml.YAMLError as exc:
        print(f"ERROR: Invalid YAML: {exc}", file=sys.stderr)
        sys.exit(1)

    logger = setup_logging(cfg)
    logger.info("=== MQTT Bridge starting (config: %s) ===", args.config)

    bridge = MqttBridge(cfg, logger)
    bridge.run()


if __name__ == "__main__":
    main()
