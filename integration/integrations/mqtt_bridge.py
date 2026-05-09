import logging
import threading
from typing import Any

import paho.mqtt.client as mqtt

from .base import BaseIntegration

log = logging.getLogger(__name__)


class MqttBridgeIntegration(BaseIntegration):
    """
    Bridges messages to a remote MQTT broker (ChirpStack, TTN, HiveMQ, AWS IoT…).
    Uses paho-mqtt with loop_start() so it runs in its own thread and never
    blocks the asyncio event loop.
    """

    def __init__(self, cfg: dict):
        super().__init__(cfg)
        self.host         = cfg["host"]
        self.port         = int(cfg.get("port", 1883))
        self.username     = cfg.get("username")
        self.password     = cfg.get("password")
        self.topic_prefix = cfg.get("topic_prefix", "")
        self.topic_static = cfg.get("topic")       # if set, always publish to this topic
        self.qos          = int(cfg.get("qos", 0))
        self.retain       = bool(cfg.get("retain", False))

        self._ready = threading.Event()
        cid = cfg.get("client_id", f"intg-bridge-{self.name}")
        self._client = mqtt.Client(client_id=cid, clean_session=True)

        if self.username:
            self._client.username_pw_set(self.username, self.password)

        tls = cfg.get("tls")
        if tls:
            if isinstance(tls, dict):
                self._client.tls_set(
                    ca_certs = tls.get("ca_certs"),
                    certfile = tls.get("certfile"),
                    keyfile  = tls.get("keyfile"),
                )
            else:
                self._client.tls_set()   # use system CA bundle (for TTN, AWS, etc.)

        self._client.on_connect    = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.connect_async(self.host, self.port, keepalive=60)
        self._client.loop_start()

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self._ready.set()
            log.info("[%s] Connected to %s:%d", self.name, self.host, self.port)
        else:
            log.error("[%s] Connect failed rc=%d", self.name, rc)

    def _on_disconnect(self, client, userdata, rc):
        self._ready.clear()
        if rc:
            log.warning("[%s] Disconnected rc=%d — paho will reconnect", self.name, rc)

    async def forward(self, topic: str, payload: Any, raw: str) -> None:
        if not self._ready.is_set():
            log.warning("[%s] Not connected — dropping '%s'", self.name, topic)
            return

        out_topic = self.topic_static or (self.topic_prefix + topic)
        result = self._client.publish(out_topic, raw, qos=self.qos, retain=self.retain)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"paho publish failed rc={result.rc}")
        log.info("[%s] → %s:%d  topic='%s'", self.name, self.host, self.port, out_topic)

    async def close(self) -> None:
        self._client.loop_stop()
        self._client.disconnect()
