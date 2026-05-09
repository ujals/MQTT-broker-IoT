"""
mqtt_broker.router
==================
Topic filter matching (MQTT §4.7) with wildcard support.

Wildcards
---------
  +   single-level: matches exactly one topic level
  #   multi-level:  matches the rest of the topic (must be the last character)

Special rules
-------------
  • Topics / filters starting with '$' are not matched by '#' or '+' at the
    root level (system topics like $SYS are isolated).
  • An empty topic level is valid: "sport//player1" has three levels.
"""

from typing import Dict, Iterable, List, Optional, Set, Tuple


def topic_matches(topic_filter: str, topic: str) -> bool:
    """
    Return True if *topic* matches *topic_filter*.

    Examples
    --------
    >>> topic_matches("#", "a/b/c")            True
    >>> topic_matches("a/+/c", "a/b/c")        True
    >>> topic_matches("a/+/c", "a/b/d")        False
    >>> topic_matches("$SYS/#", "$SYS/stats")  True
    >>> topic_matches("#", "$SYS/stats")        False
    """
    # System topics ($…) are not caught by bare '#' or '+' at the first level
    if topic.startswith("$") and not topic_filter.startswith("$"):
        return False

    f_parts = topic_filter.split("/")
    t_parts = topic.split("/")

    fi = ti = 0
    while fi < len(f_parts) and ti < len(t_parts):
        f = f_parts[fi]
        if f == "#":
            return True
        elif f == "+":
            fi += 1
            ti += 1
        elif f == t_parts[ti]:
            fi += 1
            ti += 1
        else:
            return False

    # Both exhausted → exact match
    if fi == len(f_parts) and ti == len(t_parts):
        return True
    # Filter ends with '#' after a '/'
    if fi < len(f_parts) and f_parts[fi] == "#":
        return True
    return False


def is_valid_topic_filter(f: str) -> bool:
    """Return True if *f* is a syntactically valid MQTT topic filter."""
    if not f:
        return False
    parts = f.split("/")
    for i, p in enumerate(parts):
        if "#" in p and (len(p) != 1 or i != len(parts) - 1):
            return False
        if "+" in p and len(p) != 1:
            return False
    return True


def is_valid_topic(t: str) -> bool:
    """Return True if *t* is a valid publish topic (no wildcards)."""
    return bool(t) and "+" not in t and "#" not in t


# ──────────────────────────────────────────────────────────────────────────────
# Subscription registry
# ──────────────────────────────────────────────────────────────────────────────

class SubscriptionStore:
    """
    Keeps track of which clients subscribe to which topic filters.

    Data model
    ----------
    _subs : { topic_filter -> { client_id -> (qos, no_local) } }
    """

    def __init__(self):
        # { topic_filter: { client_id: (qos, no_local) } }
        self._subs: Dict[str, Dict[str, Tuple[int, bool]]] = {}

    # ── Mutation ─────────────────────────────────────────────────────────────

    def add(self, client_id: str, topic_filter: str, qos: int,
            no_local: bool = False) -> None:
        self._subs.setdefault(topic_filter, {})[client_id] = (qos, no_local)

    def remove(self, client_id: str, topic_filter: str) -> None:
        bucket = self._subs.get(topic_filter)
        if bucket:
            bucket.pop(client_id, None)
            if not bucket:
                del self._subs[topic_filter]

    def remove_all(self, client_id: str) -> None:
        """Remove *all* subscriptions for a client (used on clean disconnect)."""
        for bucket in self._subs.values():
            bucket.pop(client_id, None)
        self._subs = {f: b for f, b in self._subs.items() if b}

    # ── Query ────────────────────────────────────────────────────────────────

    def matching_subscribers(self, topic: str) -> List[Tuple[str, int, bool]]:
        """
        Return list of (client_id, effective_qos, no_local) for all subscribers
        whose filter matches *topic*. If a client matches via multiple filters,
        the highest QoS wins and no_local is OR'd.
        """
        result: Dict[str, Tuple[int, bool]] = {}
        for filt, clients in self._subs.items():
            if topic_matches(filt, topic):
                for cid, (qos, no_local) in clients.items():
                    if cid not in result or qos > result[cid][0]:
                        result[cid] = (qos, no_local)
        return [(cid, qos, nl) for cid, (qos, nl) in result.items()]

    def filters_for_client(self, client_id: str) -> Dict[str, int]:
        """Return {filter: qos} for a given client."""
        out: Dict[str, int] = {}
        for filt, clients in self._subs.items():
            if client_id in clients:
                out[filt] = clients[client_id][0]
        return out

    def restore(self, client_id: str, filters: Dict[str, int]) -> None:
        """Restore subscriptions from a persisted session (no_local not persisted)."""
        for filt, qos in filters.items():
            self.add(client_id, filt, qos, no_local=False)
