import logging
from typing import List

from .base import BaseIntegration
from .http_post import HttpIntegration
from .mqtt_bridge import MqttBridgeIntegration
from .chirpstack import ChirpStackIntegration

_log = logging.getLogger(__name__)


def build_integrations(cfgs: list, shared_settings: dict) -> List[BaseIntegration]:
    result: List[BaseIntegration] = []

    # Always add ChirpStack integration (reads live settings)
    result.append(ChirpStackIntegration(shared_settings))

    for cfg in (cfgs or []):
        if not cfg.get("enabled", True):
            continue
        t = cfg.get("type", "").lower()
        name = cfg.get("name", "?")
        try:
            if t in ("http", "https"):
                result.append(HttpIntegration(cfg))
            elif t == "mqtt":
                result.append(MqttBridgeIntegration(cfg))
            elif t == "chirpstack":
                pass  # handled above
            else:
                _log.warning("Unknown integration type '%s' for '%s' — skipped", t, name)
        except Exception as exc:
            _log.error("Failed to build integration '%s': %s", name, exc)
    return result
