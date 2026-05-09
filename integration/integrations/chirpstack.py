"""
ChirpStack Mirror Integration
=============================
Flow:
  1. Raw gateway frames bridged to ALL targets (primary + secondaries)
  2. Primary handles OTAA join normally
  3. Integration watches primary's Mosquitto for join events
  4. On join: fetches session keys from primary API
  5. Registers device as ABP (skip_fcnt_check) on all secondaries
  6. All secondaries can now decrypt and show the same uplink data
"""

import asyncio
import base64
import json
import logging
import re
import threading

_UUID_RE = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)

def _is_uuid(s: str) -> bool:
    return bool(s and _UUID_RE.match(s))


def _normalise_eui(s: str) -> str:
    return s.lower().replace(":", "").replace("-", "").strip()


def _check_whitelist(dev_eui: str, devices: list) -> tuple[bool, str]:
    """Return (allowed, device_name). If whitelist empty, allow all."""
    if not devices:
        return True, f"device-{dev_eui}"
    for d in devices:
        if not d.get("enabled", True):
            continue
        if _normalise_eui(d.get("dev_eui", "")) == dev_eui:
            return True, d.get("name", f"device-{dev_eui}")
    return False, ""

import aiohttp
import paho.mqtt.client as mqtt

from .base import BaseIntegration

log = logging.getLogger(__name__)


# ── LoRa frame helpers ────────────────────────────────────────────────────────

def _extract_dev_eui_from_join(phy_b64: str) -> str | None:
    try:
        data = base64.b64decode(phy_b64)
        if (data[0] >> 5) & 0x07 == 0x00 and len(data) >= 17:
            return data[9:17][::-1].hex()
    except Exception:
        pass
    return None


def _gateway_eui_from_topic(topic: str) -> str | None:
    parts = topic.split("/")
    if "gateway" in parts:
        idx = parts.index("gateway")
        if idx + 1 < len(parts):
            return parts[idx + 1].lower().replace(":", "").replace("-", "")
    return None


# ── MQTT bridge target (one paho connection) ──────────────────────────────────

class MQTTTarget:
    def __init__(self, cfg: dict):
        self.name     = cfg.get("name", "target")
        self.host     = cfg.get("host", "")
        self.port     = int(cfg.get("port", 1883))
        self.username = cfg.get("username", "")
        self.password = cfg.get("password", "")
        self._ready   = threading.Event()

        self._client = mqtt.Client(client_id=f"intg-cs-{self.name}", clean_session=True)
        if self.username:
            self._client.username_pw_set(self.username, self.password)
        self._client.on_connect    = lambda c, u, f, rc: (self._ready.set() if rc == 0 else log.error("[%s] MQTT rc=%d", self.name, rc)) or log.info("[%s] MQTT connected %s:%d", self.name, self.host, self.port)
        self._client.on_disconnect = lambda c, u, rc: self._ready.clear()
        self._client.connect_async(self.host, self.port, keepalive=60)
        self._client.loop_start()

    def publish(self, topic: str, payload: bytes, qos: int = 0):
        if self._ready.is_set():
            self._client.publish(topic, payload, qos=qos)
        else:
            log.warning("[%s] not ready — drop %s", self.name, topic)

    def stop(self):
        self._client.loop_stop()
        self._client.disconnect()

    def status(self) -> dict:
        return {"name": self.name, "host": self.host,
                "port": self.port, "connected": self._ready.is_set()}


# ── REST API client (one aiohttp session per target) ─────────────────────────

class CSApiClient:
    def __init__(self, api_url: str, api_key: str,
                 app_id: str = "", profile_id: str = ""):
        self.api_url    = api_url.rstrip("/")
        self.app_id     = app_id
        self.profile_id = profile_id
        self._key       = api_key
        self._session: aiohttp.ClientSession | None = None

    def _h(self):
        return {"Authorization": f"Bearer {self._key}",
                "Content-Type": "application/json"}

    async def _s(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def get_activation(self, dev_eui: str) -> dict | None:
        try:
            s = await self._s()
            async with s.get(f"{self.api_url}/api/devices/{dev_eui}/activation",
                             headers=self._h(), ssl=False,
                             timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    data = await r.json()
                    return data.get("deviceActivation") or data
        except Exception as exc:
            log.error("get_activation %s: %s", dev_eui, exc)
        return None

    async def device_exists(self, dev_eui: str) -> bool:
        try:
            s = await self._s()
            async with s.get(f"{self.api_url}/api/devices/{dev_eui}",
                             headers=self._h(), ssl=False) as r:
                return r.status == 200
        except Exception:
            return False

    async def create_device(self, dev_eui: str, name: str,
                             app_id: str = "", profile_id: str = "",
                             skip_fcnt: bool = False) -> bool:
        app_id     = app_id     or self.app_id
        profile_id = profile_id or self.profile_id
        if not app_id or not profile_id:
            log.warning("create_device %s — missing app/profile id", dev_eui)
            return False
        s = await self._s()
        body = {"device": {"devEui": dev_eui, "name": name,
                            "applicationId": app_id,
                            "deviceProfileId": profile_id,
                            "skipFcntCheck": skip_fcnt}}
        async with s.post(f"{self.api_url}/api/devices",
                          headers=self._h(), json=body, ssl=False) as r:
            if r.status in (200, 201):
                return True
            text = await r.text()
            if "duplicate key" in text or r.status == 409:
                log.debug("create_device %s — already exists", dev_eui)
                return True
            log.error("create_device %s → %d %s", dev_eui, r.status, text)
            return False

    async def set_activation(self, dev_eui: str, activation: dict) -> bool:
        s = await self._s()
        body = {"deviceActivation": {**activation, "devEui": dev_eui}}
        async with s.post(f"{self.api_url}/api/devices/{dev_eui}/activate",
                          headers=self._h(), json=body, ssl=False) as r:
            ok = r.status in (200, 201)
            if not ok:
                log.error("set_activation %s → %d %s", dev_eui, r.status, await r.text())
            return ok

    async def test(self) -> dict:
        if not self.api_url:
            return {"ok": False, "error": "API URL not set"}
        try:
            s = await self._s()
            async with s.get(f"{self.api_url}/api/tenants?limit=1",
                             headers=self._h(), ssl=False,
                             timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    return {"ok": True}
                return {"ok": False, "error": f"HTTP {r.status}"}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


# ── Main integration ──────────────────────────────────────────────────────────

class ChirpStackIntegration(BaseIntegration):

    def __init__(self, settings: dict):
        super().__init__({"name": "chirpstack", "type": "chirpstack", "enabled": True})
        self._settings       = settings
        self._seen_gw:  set  = set()
        self._seen_dev: set  = set()
        self._mqtt_targets:  list[MQTTTarget]  = []
        self._api_targets:   list[CSApiClient] = []
        self._primary_api:   CSApiClient | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._rebuild()

    # ── Build / rebuild from settings ─────────────────────────────────────────

    def _rebuild(self):
        for t in self._mqtt_targets:
            t.stop()
        self._mqtt_targets = []
        self._api_targets  = []

        cs = self._settings.get("chirpstack", {})
        if cs.get("api_url"):
            self._primary_api = CSApiClient(
                cs["api_url"], cs.get("api_key", ""),
                cs.get("application_id", ""), cs.get("device_profile_id", "")
            )

        # Bridge gateway frames to primary ChirpStack Mosquitto
        if cs.get("mqtt_host"):
            self._mqtt_targets.append(MQTTTarget({
                "name": "primary",
                "host": cs["mqtt_host"],
                "port": cs.get("mqtt_port", 1883),
                "username": cs.get("mqtt_username", ""),
                "password": cs.get("mqtt_password", ""),
            }))

        for cfg in self._settings.get("targets", []):
            if not cfg.get("enabled", True):
                continue
            if cfg.get("host"):
                self._mqtt_targets.append(MQTTTarget(cfg))
            if cfg.get("api_url"):
                self._api_targets.append(CSApiClient(
                    cfg["api_url"], cfg.get("api_key", ""),
                    cfg.get("application_id", ""), cfg.get("device_profile_id", "")
                ))

    def reload_targets(self):
        self._rebuild()

    def set_loop(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop

    # ── Status ────────────────────────────────────────────────────────────────

    def targets_status(self) -> list:
        return [t.status() for t in self._mqtt_targets]

    # ── Join handler (called from primary watcher loop) ───────────────────────

    async def handle_join(self, dev_eui: str, app_id: str = "",
                           profile_id: str = "", device_name: str = ""):
        if not self._primary_api:
            return
        if dev_eui in self._seen_dev:
            return

        log.info("Join detected: %s — fetching session keys", dev_eui)
        activation = await self._primary_api.get_activation(dev_eui)
        if not activation:
            log.warning("No activation found for %s — will retry on next join", dev_eui)
            return

        log.info("Got session keys for %s (DevAddr=%s)", dev_eui, activation.get("devAddr"))

        name = device_name or f"device-{dev_eui}"
        for api in self._api_targets:
            if not await api.device_exists(dev_eui):
                created = await api.create_device(
                    dev_eui, name,
                    app_id or api.app_id,
                    profile_id or api.profile_id,
                    skip_fcnt=True
                )
                if not created:
                    continue
            ok = await api.set_activation(dev_eui, activation)
            if ok:
                log.info("Mirrored %s as ABP on %s", dev_eui, api.api_url)

        self._seen_dev.add(dev_eui)

    # ── forward (called for every message on your broker) ────────────────────

    async def forward(self, topic: str, payload: dict, raw: str) -> None:
        parts = topic.split("/")

        # Gateway frame → bridge + parse JOIN REQUEST
        if "gateway" in parts:
            raw_bytes = raw.encode()
            for t in self._mqtt_targets:
                t.publish(topic, raw_bytes)

            # Auto-register gateway
            gw_eui = _gateway_eui_from_topic(topic)
            if gw_eui and gw_eui not in self._seen_gw and self._primary_api:
                self._seen_gw.add(gw_eui)

            # Try extract DevEUI from JOIN REQUEST
            phy = payload.get("phyPayload")
            if phy:
                dev_eui = _extract_dev_eui_from_join(phy)
                if dev_eui and dev_eui not in self._seen_dev and self._primary_api:
                    allowed, dev_name = _check_whitelist(
                        dev_eui, self._settings.get("devices", []))
                    if not allowed:
                        log.warning("JOIN REJECTED — DevEUI %s not in whitelist", dev_eui)
                        return
                    cs = self._settings.get("chirpstack", {})
                    if not await self._primary_api.device_exists(dev_eui):
                        await self._primary_api.create_device(
                            dev_eui, dev_name,
                            cs.get("application_id", ""),
                            cs.get("device_profile_id", "")
                        )
            return

        # Non-gateway plain JSON (MQTTX tests etc.)
        if not self._primary_api or not self._primary_api.api_url:
            return
        eui = (payload.get("deviceInfo", {}).get("devEui") or
               payload.get("devEUI") or payload.get("dev_eui"))
        if not eui:
            if "device" in parts:
                idx = parts.index("device")
                eui = parts[idx + 1] if idx + 1 < len(parts) else None
        if not eui:
            return
        eui = eui.lower().replace(":", "").replace("-", "")
        if eui not in self._seen_dev:
            allowed, dev_name = _check_whitelist(eui, self._settings.get("devices", []))
            if not allowed:
                log.warning("DEVICE REJECTED — DevEUI %s not in whitelist", eui)
                return
            info = payload.get("deviceInfo", {})
            cs   = self._settings.get("chirpstack", {})
            app_id     = info.get("applicationId",  "") if _is_uuid(info.get("applicationId",  "")) else cs.get("application_id",    "")
            profile_id = info.get("deviceProfileId","") if _is_uuid(info.get("deviceProfileId","")) else cs.get("device_profile_id", "")
            await self._primary_api.create_device(
                eui,
                info.get("deviceName", dev_name),
                app_id,
                profile_id,
            )
            self._seen_dev.add(eui)

    async def close(self) -> None:
        for t in self._mqtt_targets:
            t.stop()
        if self._primary_api:
            await self._primary_api.close()
        for a in self._api_targets:
            await a.close()

    async def test_connection(self) -> dict:
        if not self._primary_api:
            return {"ok": False, "error": "Primary API not configured"}
        return await self._primary_api.test()
