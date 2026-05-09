"""
mqtt_broker.auth
================
Username / password authentication.

Configuration (from config.yaml)
---------------------------------
  auth:
    enabled: true
    allow_anonymous: false    # allow clients with no credentials
    users:
      alice: "secret123"
      bob:   "hunter2"

Extension points
----------------
  Subclass Authenticator and override check() to plug in a database,
  LDAP, JWT validation, etc.
"""

import hashlib
import hmac
import logging
from typing import Dict, Optional

log = logging.getLogger(__name__)


class Authenticator:
    """Simple in-memory username/password authenticator."""

    def __init__(self, cfg: dict):
        self.enabled         = cfg.get("enabled", False)
        self.allow_anonymous = cfg.get("allow_anonymous", not self.enabled)
        # Store passwords as plain text or hashed (sha256 hex)
        raw_users: Dict[str, str] = cfg.get("users", {})
        self._users: Dict[str, str] = {k: str(v) for k, v in raw_users.items()}

        if self.enabled:
            log.info(
                "Auth enabled — %d user(s) configured, anonymous=%s",
                len(self._users), self.allow_anonymous,
            )
        else:
            log.info("Auth disabled — all clients accepted")

    def check(self, username: Optional[str], password: Optional[bytes]) -> bool:
        """
        Return True if the client should be admitted.

        Rules
        -----
        1. If auth is disabled → always True.
        2. If username is None/empty and allow_anonymous → True.
        3. Look up username; compare password.
        """
        if not self.enabled:
            return True

        if not username:
            if self.allow_anonymous:
                return True
            log.warning("Rejected anonymous client (allow_anonymous=false)")
            return False

        stored = self._users.get(username)
        if stored is None:
            log.warning("Rejected unknown user '%s'", username)
            return False

        pw_str = (password or b"").decode("utf-8", errors="replace")

        # Support both plain-text and "sha256:<hex>" stored passwords
        if stored.startswith("sha256:"):
            digest = hashlib.sha256(pw_str.encode()).hexdigest()
            ok = hmac.compare_digest(digest, stored[7:])
        else:
            ok = hmac.compare_digest(pw_str, stored)

        if not ok:
            log.warning("Rejected bad password for user '%s'", username)
        return ok
