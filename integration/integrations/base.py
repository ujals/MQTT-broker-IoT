from abc import ABC, abstractmethod
from typing import Any


class BaseIntegration(ABC):
    def __init__(self, cfg: dict):
        self.name    = cfg.get("name", "unnamed")
        self.type    = cfg.get("type", "unknown")
        self.enabled = cfg.get("enabled", True)

    @abstractmethod
    async def forward(self, topic: str, payload: Any, raw: str) -> None: ...

    async def close(self) -> None:
        pass
