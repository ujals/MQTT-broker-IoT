import asyncio
import logging
from typing import Any

import aiohttp

from .base import BaseIntegration

log = logging.getLogger(__name__)


class HttpIntegration(BaseIntegration):
    def __init__(self, cfg: dict):
        super().__init__(cfg)
        self.url      = cfg["url"]
        self.method   = cfg.get("method", "POST").upper()
        self.headers  = cfg.get("headers", {"Content-Type": "application/json"})
        self.timeout  = aiohttp.ClientTimeout(total=cfg.get("timeout", 10))
        self.ssl      = cfg.get("ssl_verify", True)
        self.retries  = int(cfg.get("retry_attempts", 3))
        self._session: aiohttp.ClientSession | None = None

    async def _session_get(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def forward(self, topic: str, payload: Any, raw: str) -> None:
        url = self.url.replace("{topic}", topic)
        session = await self._session_get()
        last_exc: Exception | None = None

        for attempt in range(self.retries):
            try:
                async with session.request(
                    self.method, url,
                    data=raw,
                    headers=self.headers,
                    timeout=self.timeout,
                    ssl=self.ssl if isinstance(self.ssl, bool) else None,
                ) as resp:
                    if resp.status >= 400:
                        body = await resp.text()
                        log.warning("[%s] HTTP %d: %s", self.name, resp.status, body[:200])
                    else:
                        log.info("[%s] → %s %s [%d]", self.name, self.method, url, resp.status)
                    return
            except Exception as exc:
                last_exc = exc
                if attempt < self.retries - 1:
                    await asyncio.sleep(2 ** attempt)

        raise last_exc  # type: ignore[misc]

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
