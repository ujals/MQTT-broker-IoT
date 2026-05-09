"""
mqtt_broker.session
===================
Session state per connected (or persisted) client.

Tracks
------
  • Protocol version and negotiated options
  • Subscriptions (delegated to SubscriptionStore)
  • Inflight QoS-1 messages  (packet_id → PublishPacket)
  • Inflight QoS-2 flow state (packet_id → stage)
  • Offline message queue for persistent sessions
  • Last Will and Testament
  • Session expiry (MQTT 5.0)
"""

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


# ──────────────────────────────────────────────────────────────────────────────
# Data classes
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class WillMessage:
    topic:      str
    payload:    bytes
    qos:        int  = 0
    retain:     bool = False
    properties: Dict = field(default_factory=dict)
    delay:      int  = 0     # MQTT 5.0 Will-Delay-Interval (seconds)


@dataclass
class QoS2State:
    """Tracks the four-packet QoS-2 exchange."""
    # Incoming (publish → broker):   PUBLISH → PUBREC → PUBREL → PUBCOMP
    # Outgoing (broker → subscriber): PUBLISH → PUBREC → PUBREL → PUBCOMP
    stage: str   # "pubrec_sent" | "pubrel_sent" | "pubcomp_sent"
    pub:   object = None   # the original PublishPacket (incoming only)


@dataclass
class ClientSession:
    client_id:        str
    protocol_level:   int           # 3 | 4 | 5
    clean_session:    bool
    keepalive:        int
    connected:        bool  = False

    # Will message (cleared after delivery)
    will: Optional[WillMessage] = None

    # Inflight QoS-1 outbound  {packet_id: PublishPacket}
    inflight_qos1: Dict[int, object] = field(default_factory=dict)

    # QoS-2 state  {packet_id: QoS2State}
    qos2_in:  Dict[int, QoS2State] = field(default_factory=dict)   # incoming
    qos2_out: Dict[int, QoS2State] = field(default_factory=dict)   # outgoing

    # Offline queue (for persistent sessions while disconnected)
    offline_queue: List[object] = field(default_factory=list)       # List[PublishPacket]

    # Subscription filters  {filter: qos}  — mirrored copy for restore
    subscriptions: Dict[str, int] = field(default_factory=dict)

    # Packet-ID counter (1–65535, wraps)
    _next_pid: int = 1

    # MQTT 5.0
    session_expiry:   int   = 0    # 0 = end on disconnect; 0xFFFFFFFF = never
    receive_maximum:  int   = 65535
    topic_aliases_in: Dict[int, str] = field(default_factory=dict)

    # Housekeeping
    connected_at:    float = field(default_factory=time.time)
    disconnected_at: Optional[float] = None

    def next_packet_id(self) -> int:
        pid = self._next_pid
        self._next_pid = (self._next_pid % 65535) + 1
        return pid


# ──────────────────────────────────────────────────────────────────────────────
# Session store
# ──────────────────────────────────────────────────────────────────────────────

class SessionStore:
    """
    In-memory registry of all client sessions.

    MQTT 3.1.1 §3.1.2.4  / MQTT 5.0 §3.1.2.11.2:
      clean_session=True  → discard any previous session on connect; no
                            persistence after disconnect.
      clean_session=False → resume previous session if it exists; persist
                            subscriptions and offline queue.
    """

    def __init__(self):
        # client_id → ClientSession
        self._sessions: Dict[str, ClientSession] = {}

    # ── Connect / disconnect ─────────────────────────────────────────────────

    def on_connect(self, connect_pkt, handler) -> Tuple[bool, "ClientSession"]:
        """
        Called when a CONNECT packet is accepted.

        Returns (session_present, session).
        session_present == True means a previous persistent session was resumed.
        """
        from .protocol import ConnectPacket, Prop  # local import to avoid cycle
        cid = connect_pkt.client_id
        clean = connect_pkt.clean_session

        existing = self._sessions.get(cid)
        session_present = False

        if existing and existing.protocol_level == connect_pkt.protocol_level:
            if clean:
                # Discard old session
                self._discard(cid)
            else:
                # Resume
                existing.connected      = True
                existing.keepalive      = connect_pkt.keepalive
                existing.connected_at   = time.time()
                existing.disconnected_at = None
                session_present = True
                return True, existing
        elif existing:
            self._discard(cid)

        # MQTT 5.0 session expiry
        expiry = 0
        if connect_pkt.protocol_level == 5:
            expiry = connect_pkt.properties.get(Prop.SESSION_EXPIRY_INTERVAL, 0)
            recv_max = connect_pkt.properties.get(Prop.RECEIVE_MAXIMUM, 65535)
        else:
            recv_max = 65535

        sess = ClientSession(
            client_id       = cid,
            protocol_level  = connect_pkt.protocol_level,
            clean_session   = clean,
            keepalive       = connect_pkt.keepalive,
            connected       = True,
            session_expiry  = expiry,
            receive_maximum = recv_max,
        )

        # Store will
        if connect_pkt.will_topic:
            delay = 0
            if connect_pkt.protocol_level == 5:
                from .protocol import Prop as P
                delay = connect_pkt.will_properties.get(P.WILL_DELAY_INTERVAL, 0)
            sess.will = WillMessage(
                topic      = connect_pkt.will_topic,
                payload    = connect_pkt.will_payload or b"",
                qos        = connect_pkt.will_qos,
                retain     = connect_pkt.will_retain,
                properties = connect_pkt.will_properties,
                delay      = delay,
            )

        self._sessions[cid] = sess
        return False, sess

    def on_disconnect(self, client_id: str, clean: bool) -> Optional[WillMessage]:
        """
        Mark session as disconnected.
        Returns the WillMessage if it should be sent (unclean disconnect),
        or None if no will or clean disconnect.
        """
        sess = self._sessions.get(client_id)
        if not sess:
            return None

        sess.connected       = False
        sess.disconnected_at = time.time()

        will = sess.will
        sess.will = None   # consume it

        # Discard session entirely on clean disconnect with no expiry
        if sess.clean_session and (sess.protocol_level < 5 or sess.session_expiry == 0):
            self._discard(client_id)

        # Only send will on unclean disconnect
        return will if (not clean) else None

    def _discard(self, client_id: str) -> None:
        self._sessions.pop(client_id, None)

    # ── Lookup ───────────────────────────────────────────────────────────────

    def get(self, client_id: str) -> Optional[ClientSession]:
        return self._sessions.get(client_id)

    def all_sessions(self) -> List[ClientSession]:
        return list(self._sessions.values())

    # ── QoS 2 flow helpers ───────────────────────────────────────────────────

    def store_qos2_incoming(self, client_id: str, packet_id: int, pub) -> None:
        sess = self._sessions.get(client_id)
        if sess:
            sess.qos2_in[packet_id] = QoS2State(stage="pubrec_sent", pub=pub)

    def release_qos2_incoming(self, client_id: str, packet_id: int):
        """Called when PUBREL received. Returns stored pub or None."""
        sess = self._sessions.get(client_id)
        if sess:
            state = sess.qos2_in.pop(packet_id, None)
            return state.pub if state else None
        return None

    # ── Offline queue ────────────────────────────────────────────────────────

    def enqueue_offline(self, client_id: str, pub, max_queue: int = 1000) -> bool:
        """Queue a message for a disconnected persistent-session client."""
        sess = self._sessions.get(client_id)
        if not sess or sess.connected or sess.clean_session:
            return False
        if len(sess.offline_queue) < max_queue:
            sess.offline_queue.append(pub)
            return True
        return False

    def drain_offline(self, client_id: str) -> List:
        """Return and clear the offline queue."""
        sess = self._sessions.get(client_id)
        if not sess:
            return []
        q = list(sess.offline_queue)
        sess.offline_queue.clear()
        return q
