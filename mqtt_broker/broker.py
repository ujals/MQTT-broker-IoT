"""
mqtt_broker.broker
==================
Pure-Python asyncio MQTT broker.

Features
--------
  • MQTT 3.1 / 3.1.1 / 5.0  (auto-detected per client)
  • QoS 0, 1, 2
  • Wildcard subscriptions  (+, #)
  • Retained messages
  • Will messages
  • Persistent sessions (clean_session=False)
  • Offline message queuing
  • Username / password auth
  • TLS on port 8883
  • MQTT 5.0: topic aliases, reason codes, properties, session expiry
  • Built-in forwarder fan-out (HTTP, UDP, MQTT)
"""

import asyncio
import logging
import ssl
import time
from typing import Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor

from .auth     import Authenticator
from .protocol import (
    PINGRESP, ConnectPacket, PacketType, Prop, PublishPacket, RC,
    RawPacket, SubscribePacket,
    decode_varint, encode_connack, encode_disconnect, encode_puback,
    encode_pubcomp, encode_pubrel, encode_pubrec, encode_publish,
    encode_suback, encode_unsuback, parse_connect, parse_disconnect,
    parse_publish, parse_subscribe, parse_unsubscribe, parse_packet_id,
)
from .router  import SubscriptionStore, is_valid_topic, is_valid_topic_filter
from .session import ClientSession, SessionStore, WillMessage

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Per-client connection handler
# ──────────────────────────────────────────────────────────────────────────────

class ClientHandler:
    """Handles the full MQTT lifecycle for one TCP connection."""

    def __init__(
        self,
        reader:  asyncio.StreamReader,
        writer:  asyncio.StreamWriter,
        broker:  "MQTTBroker",
    ):
        self.reader = reader
        self.writer = writer
        self.broker = broker

        self.peer            = writer.get_extra_info("peername", ("?", 0))
        self.client_id: Optional[str] = None
        self.session: Optional[ClientSession] = None
        self.protocol_level: int = 4        # updated on CONNECT
        self.connected       = False
        self._write_lock     = asyncio.Lock()
        self._topic_aliases: Dict[int, str] = {}   # alias → topic (MQTT 5.0)

    # ── Raw I/O ──────────────────────────────────────────────────────────────

    async def _read_packet(self, timeout: float = 30.0) -> Optional[RawPacket]:
        """Read one complete MQTT packet. Returns None on disconnect/timeout."""
        try:
            # Fixed header byte 1
            hdr = await asyncio.wait_for(self.reader.readexactly(1), timeout=timeout)
        except (asyncio.TimeoutError, asyncio.IncompleteReadError, ConnectionResetError):
            return None

        ptype = (hdr[0] >> 4) & 0x0F
        flags = hdr[0] & 0x0F

        # Remaining length  (variable-length encoding)
        remaining = 0
        multiplier = 1
        while True:
            try:
                b = await asyncio.wait_for(self.reader.readexactly(1), timeout=10.0)
            except (asyncio.TimeoutError, asyncio.IncompleteReadError):
                return None
            remaining  += (b[0] & 0x7F) * multiplier
            multiplier *= 128
            if not (b[0] & 0x80):
                break
            if multiplier > 128 ** 3:
                log.warning("%s: malformed remaining length", self.peer)
                return None

        # Enforce max packet size to prevent memory exhaustion
        max_pkt = self.broker.cfg.get("limits", {}).get("max_packet_size", 256 * 1024 * 1024)
        if remaining > max_pkt:
            log.warning("%s: packet too large (%d bytes > %d limit), dropping",
                        self.peer, remaining, max_pkt)
            return None

        payload = b""
        if remaining > 0:
            try:
                payload = await asyncio.wait_for(
                    self.reader.readexactly(remaining), timeout=30.0
                )
            except (asyncio.TimeoutError, asyncio.IncompleteReadError):
                return None

        return RawPacket(ptype, flags, payload)

    async def _send(self, data: bytes) -> None:
        async with self._write_lock:
            try:
                self.writer.write(data)
                await self.writer.drain()
            except (ConnectionResetError, BrokenPipeError, OSError):
                self.connected = False

    # ── Main loop ────────────────────────────────────────────────────────────

    async def run(self) -> None:
        log.info("New connection from %s:%d", *self.peer)
        try:
            # The FIRST packet must be CONNECT (30 s timeout)
            raw = await self._read_packet(timeout=30.0)
            if raw is None or raw.ptype != PacketType.CONNECT:
                log.warning("%s: did not send CONNECT", self.peer)
                return
            await self._handle_connect(raw)
            if not self.connected:
                return

            # Keepalive timeout = 1.5 × negotiated keepalive (§3.1.2.10)
            ka = (self.session.keepalive * 1.5) if self.session.keepalive else 300.0

            # Drain any queued offline messages first
            for pub in self.broker.sessions.drain_offline(self.client_id):
                pub.packet_id = self.session.next_packet_id()
                await self._send(encode_publish(pub, self.protocol_level))

            while self.connected:
                raw = await self._read_packet(timeout=ka)
                if raw is None:
                    log.info("%s (%s): connection lost / keepalive timeout",
                             self.peer, self.client_id)
                    break
                await self._dispatch(raw)

        except Exception as exc:
            log.exception("%s: unhandled error: %s", self.peer, exc)
        finally:
            await self._teardown(clean=False)

    # ── CONNECT ──────────────────────────────────────────────────────────────

    async def _handle_connect(self, raw: RawPacket) -> None:
        try:
            pkt = parse_connect(raw.payload)
        except Exception as exc:
            log.warning("%s: malformed CONNECT: %s", self.peer, exc)
            return

        level = pkt.protocol_level

        # Protocol version check
        if level not in (3, 4, 5):
            await self._send(encode_connack(False, RC.UNSUPPORTED_PROTOCOL_VERSION, level))
            return

        # Auth
        if not self.broker.auth.check(pkt.username, pkt.password):
            await self._send(encode_connack(False, RC.BAD_USER_OR_PASSWORD, level))
            return

        # Client ID validation
        if not pkt.client_id:
            if level < 5:
                # MQTT 3.1.1 §3.1.3.1: zero-length ID only allowed with clean session
                if not pkt.clean_session:
                    await self._send(encode_connack(False, RC.CLIENT_ID_NOT_VALID, level))
                    return
                pkt.client_id = f"auto-{id(self):x}"
            else:
                pkt.client_id = f"auto-{id(self):x}"

        # Take over existing connection with same client ID
        self.broker.kick_existing(pkt.client_id)

        self.protocol_level = level
        self.client_id      = pkt.client_id

        session_present, self.session = self.broker.sessions.on_connect(pkt, self)
        self.broker.register(self)

        # Restore subscriptions into the routing table
        self.broker.sub_store.restore(
            pkt.client_id, self.session.subscriptions
        )

        # Build CONNACK properties for MQTT 5.0
        props = None
        if level == 5:
            props = {
                Prop.RETAIN_AVAILABLE:          1,
                Prop.WILDCARD_SUB_AVAILABLE:     1,
                Prop.SUBSCRIPTION_ID_AVAILABLE:  1,
                Prop.SHARED_SUB_AVAILABLE:       0,
                Prop.TOPIC_ALIAS_MAXIMUM:        65535,
                Prop.RECEIVE_MAXIMUM:            65535,
                Prop.MAX_QOS:                    2,
            }
            if pkt.client_id.startswith("auto-"):
                props[Prop.ASSIGNED_CLIENT_ID] = pkt.client_id

        await self._send(encode_connack(session_present, RC.SUCCESS, level, props))
        self.connected = True
        log.info("%s (%s): CONNECTED  v%s  clean=%s",
                 self.peer, self.client_id,
                 {3: "3.1", 4: "3.1.1", 5: "5.0"}.get(level, level),
                 pkt.clean_session)

    # ── Packet dispatcher ─────────────────────────────────────────────────────

    async def _dispatch(self, raw: RawPacket) -> None:
        t = raw.ptype
        if   t == PacketType.PUBLISH:
            await self._handle_publish(raw)
        elif t == PacketType.PUBACK:
            self._handle_puback(raw)
        elif t == PacketType.PUBREC:
            await self._handle_pubrec(raw)
        elif t == PacketType.PUBREL:
            await self._handle_pubrel(raw)
        elif t == PacketType.PUBCOMP:
            self._handle_pubcomp(raw)
        elif t == PacketType.SUBSCRIBE:
            await self._handle_subscribe(raw)
        elif t == PacketType.UNSUBSCRIBE:
            await self._handle_unsubscribe(raw)
        elif t == PacketType.PINGREQ:
            await self._send(PINGRESP)
        elif t == PacketType.DISCONNECT:
            await self._handle_disconnect_packet(raw)
        elif t == PacketType.AUTH and self.protocol_level == 5:
            pass   # Enhanced auth not yet implemented
        else:
            log.debug("%s: unexpected packet type %d", self.client_id, t)

    # ── PUBLISH ──────────────────────────────────────────────────────────────

    async def _handle_publish(self, raw: RawPacket) -> None:
        try:
            pub = parse_publish(raw.flags, raw.payload, self.protocol_level)
        except Exception as exc:
            log.warning("%s: malformed PUBLISH: %s", self.client_id, exc)
            return

        # MQTT 5.0 topic-alias resolution
        if self.protocol_level == 5:
            alias = pub.properties.get(Prop.TOPIC_ALIAS)
            if alias is not None:
                if pub.topic:
                    self._topic_aliases[alias] = pub.topic
                elif alias in self._topic_aliases:
                    pub.topic = self._topic_aliases[alias]
                else:
                    await self._send(encode_disconnect(RC.TOPIC_ALIAS_INVALID, 5))
                    self.connected = False
                    return

        if not is_valid_topic(pub.topic):
            if self.protocol_level == 5:
                await self._send(encode_disconnect(RC.TOPIC_NAME_INVALID, 5))
            self.connected = False
            return

        # QoS 1 → PUBACK
        if pub.qos == 1:
            await self._send(encode_puback(pub.packet_id, RC.SUCCESS, self.protocol_level))

        # QoS 2 → PUBREC, wait for PUBREL before routing
        elif pub.qos == 2:
            self.broker.sessions.store_qos2_incoming(self.client_id, pub.packet_id, pub)
            await self._send(encode_pubrec(pub.packet_id, RC.SUCCESS, self.protocol_level))
            return  # Do NOT route yet

        # Retained message handling
        if pub.retain:
            if pub.payload:
                self.broker.retained[pub.topic] = pub
                log.debug("Retained: topic='%s'", pub.topic)
            else:
                self.broker.retained.pop(pub.topic, None)

        # Route to matching subscribers
        await self.broker.route(pub, sender_id=self.client_id)

        # Fan-out to external forwarders (non-blocking)
        asyncio.create_task(self.broker.forward(pub))

    # ── QoS 1 PUBACK (inbound ACK for outbound QoS-1 we sent) ───────────────

    def _handle_puback(self, raw: RawPacket) -> None:
        if not self.session or len(raw.payload) < 2:
            return
        pid = parse_packet_id(raw.payload)
        self.session.inflight_qos1.pop(pid, None)

    # ── QoS 2 flow ───────────────────────────────────────────────────────────

    async def _handle_pubrec(self, raw: RawPacket) -> None:
        if len(raw.payload) < 2:
            return
        pid = parse_packet_id(raw.payload)
        if self.session:
            self.session.qos2_out[pid] = None
        await self._send(encode_pubrel(pid, RC.SUCCESS, self.protocol_level))

    async def _handle_pubrel(self, raw: RawPacket) -> None:
        if len(raw.payload) < 2:
            return
        pid = parse_packet_id(raw.payload)
        pub = self.broker.sessions.release_qos2_incoming(self.client_id, pid)
        await self._send(encode_pubcomp(pid, RC.SUCCESS, self.protocol_level))
        if pub:
            if pub.retain:
                self.broker.retained[pub.topic] = pub if pub.payload else None
            await self.broker.route(pub, sender_id=self.client_id)
            asyncio.create_task(self.broker.forward(pub))

    def _handle_pubcomp(self, raw: RawPacket) -> None:
        if len(raw.payload) < 2 or not self.session:
            return
        pid = parse_packet_id(raw.payload)
        self.session.qos2_out.pop(pid, None)

    # ── SUBSCRIBE ────────────────────────────────────────────────────────────

    async def _handle_subscribe(self, raw: RawPacket) -> None:
        try:
            pkt = parse_subscribe(raw.payload, self.protocol_level)
        except Exception as exc:
            log.warning("%s: malformed SUBSCRIBE: %s", self.client_id, exc)
            return

        reason_codes = []
        for sub in pkt.subscriptions:
            if not is_valid_topic_filter(sub.topic_filter):
                reason_codes.append(RC.TOPIC_FILTER_INVALID)
                continue

            granted_qos = min(sub.qos, 2)   # broker supports QoS 0/1/2
            self.broker.sub_store.add(self.client_id, sub.topic_filter, granted_qos,
                                      no_local=sub.no_local)
            if self.session:
                self.session.subscriptions[sub.topic_filter] = granted_qos
            reason_codes.append(granted_qos)

            log.info("%s: SUBSCRIBE '%s' QoS%d", self.client_id, sub.topic_filter, granted_qos)

            # Send matching retained messages
            for topic, ret_pub in list(self.broker.retained.items()):
                from .router import topic_matches
                if topic_matches(sub.topic_filter, topic):
                    clone = PublishPacket(
                        topic     = ret_pub.topic,
                        payload   = ret_pub.payload,
                        qos       = min(sub.qos, ret_pub.qos),
                        retain    = True,
                        packet_id = self.session.next_packet_id() if sub.qos > 0 else None,
                        properties= ret_pub.properties,
                    )
                    await self._send(encode_publish(clone, self.protocol_level))

        await self._send(encode_suback(pkt.packet_id, reason_codes, self.protocol_level))

    # ── UNSUBSCRIBE ──────────────────────────────────────────────────────────

    async def _handle_unsubscribe(self, raw: RawPacket) -> None:
        try:
            pkt = parse_unsubscribe(raw.payload, self.protocol_level)
        except Exception as exc:
            log.warning("%s: malformed UNSUBSCRIBE: %s", self.client_id, exc)
            return

        reason_codes = []
        for tf in pkt.topic_filters:
            if self.broker.sub_store.filters_for_client(self.client_id).get(tf) is not None:
                self.broker.sub_store.remove(self.client_id, tf)
                if self.session:
                    self.session.subscriptions.pop(tf, None)
                reason_codes.append(RC.SUCCESS)
                log.info("%s: UNSUBSCRIBE '%s'", self.client_id, tf)
            else:
                reason_codes.append(RC.NO_SUBSCRIPTION_FOUND)

        await self._send(encode_unsuback(pkt.packet_id, reason_codes, self.protocol_level))

    # ── DISCONNECT ───────────────────────────────────────────────────────────

    async def _handle_disconnect_packet(self, raw: RawPacket) -> None:
        rc, props = parse_disconnect(raw.payload, self.protocol_level)
        log.info("%s (%s): DISCONNECT  rc=0x%02x", self.peer, self.client_id, rc)
        self.connected = False
        # Clean disconnect → suppress will
        if rc == RC.NORMAL_DISCONNECTION or rc == RC.DISCONNECT_WITH_WILL:
            await self._teardown(clean=(rc == RC.NORMAL_DISCONNECTION))
        else:
            await self._teardown(clean=False)

    # ── Teardown ──────────────────────────────────────────────────────────────

    async def _teardown(self, clean: bool) -> None:
        self.connected = False
        if self.client_id:
            self.broker.unregister(self.client_id)
            will = self.broker.sessions.on_disconnect(self.client_id, clean)
            if will:
                asyncio.create_task(self._send_will(will))
            # Clean up subscriptions only if clean session
            if self.session and self.session.clean_session:
                self.broker.sub_store.remove_all(self.client_id)
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:
            pass
        log.info("%s (%s): disconnected (clean=%s)", self.peer, self.client_id, clean)

    async def _send_will(self, will: WillMessage) -> None:
        if will.delay:
            await asyncio.sleep(will.delay)
        pub = PublishPacket(
            topic      = will.topic,
            payload    = will.payload,
            qos        = will.qos,
            retain     = will.retain,
            properties = will.properties,
        )
        if will.retain:
            self.broker.retained[will.topic] = pub
        await self.broker.route(pub, sender_id=None)
        asyncio.create_task(self.broker.forward(pub))

    # ── Called by the broker to push a message to this client ────────────────

    async def deliver(self, pub: PublishPacket, effective_qos: int) -> None:
        """Deliver a routed message to this client."""
        if not self.connected:
            # Queue for persistent sessions
            if self.session and not self.session.clean_session:
                clone = PublishPacket(
                    topic=pub.topic, payload=pub.payload,
                    qos=effective_qos, retain=False, properties=pub.properties,
                )
                self.broker.sessions.enqueue_offline(self.client_id, clone)
            return

        out = PublishPacket(
            topic      = pub.topic,
            payload    = pub.payload,
            qos        = effective_qos,
            retain     = False,
            properties = pub.properties,
        )
        if effective_qos > 0 and self.session:
            out.packet_id = self.session.next_packet_id()
            if effective_qos == 1:
                self.session.inflight_qos1[out.packet_id] = out
            elif effective_qos == 2:
                self.session.qos2_out[out.packet_id] = None

        await self._send(encode_publish(out, self.protocol_level))


# ──────────────────────────────────────────────────────────────────────────────
# Broker
# ──────────────────────────────────────────────────────────────────────────────

class MQTTBroker:
    """
    The central MQTT broker.  Holds shared state and coordinates routing.
    """

    def __init__(self, cfg: dict):
        self.cfg        = cfg
        self.auth       = Authenticator(cfg.get("auth", {}))
        self.sessions   = SessionStore()
        self.sub_store  = SubscriptionStore()
        self.retained:  Dict[str, PublishPacket] = {}   # topic → retained pub

        # Connected client handlers  {client_id → ClientHandler}
        self._clients:  Dict[str, ClientHandler] = {}
        self._lock      = asyncio.Lock()

        # Active TCP connection counter (includes pre-auth connections)
        self._connection_count: int = 0

        # Thread pool for blocking forwarder calls (HTTP, etc.)
        fwd_workers = cfg.get("limits", {}).get("forwarder_threads", 10)
        self._executor = ThreadPoolExecutor(max_workers=fwd_workers,
                                            thread_name_prefix="fwd")

        fwd_cfgs = cfg.get("forwarders", []) or []
        if fwd_cfgs:
            from .forwarder import build_forwarder
            self._forwarders = [build_forwarder(f, log) for f in fwd_cfgs]
        else:
            self._forwarders = []

    # ── Client registry ──────────────────────────────────────────────────────

    def register(self, handler: ClientHandler) -> None:
        self._clients[handler.client_id] = handler

    def unregister(self, client_id: str) -> None:
        self._clients.pop(client_id, None)

    def kick_existing(self, client_id: str) -> None:
        """Disconnect an already-connected client with the same ID."""
        old = self._clients.get(client_id)
        if old and old.connected:
            log.info("Kicking existing session for client_id='%s'", client_id)
            old.connected = False
            asyncio.create_task(old._teardown(clean=False))

    # ── Routing ──────────────────────────────────────────────────────────────

    async def route(self, pub: PublishPacket, sender_id: Optional[str]) -> None:
        """Deliver pub to all matching subscribers."""
        matches = self.sub_store.matching_subscribers(pub.topic)
        for cid, eff_qos, no_local in matches:
            # Honor MQTT 5.0 no_local: skip delivery back to the publishing client
            if no_local and cid == sender_id:
                continue
            handler = self._clients.get(cid)
            if handler:
                effective_qos = min(pub.qos, eff_qos)
                asyncio.create_task(handler.deliver(pub, effective_qos))

    # ── External forwarding ───────────────────────────────────────────────────

    async def forward(self, pub: PublishPacket) -> None:
        if not self._forwarders:
            return
        import json
        try:
            body = json.loads(pub.payload.decode("utf-8", errors="replace"))
        except Exception:
            body = {"raw": pub.payload.decode("utf-8", errors="replace")}
        raw_str = json.dumps(body)

        loop = asyncio.get_running_loop()
        tasks = [
            loop.run_in_executor(self._executor, fwd.forward, pub.topic, body, raw_str)
            for fwd in self._forwarders
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for fwd, result in zip(self._forwarders, results):
            if isinstance(result, Exception):
                log.error("Forwarder %s error: %s", getattr(fwd, "name", "?"), result)

    # ── Server entry point ────────────────────────────────────────────────────

    async def serve(self) -> None:
        listener_cfgs = self.cfg.get("listeners", [{"host": "0.0.0.0", "port": 1883}])
        servers = []

        for lcfg in listener_cfgs:
            host = lcfg.get("host", "0.0.0.0")
            port = int(lcfg.get("port", 1883))
            tls_cfg = lcfg.get("tls")

            ssl_ctx = None
            if tls_cfg:
                ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                ssl_ctx.load_cert_chain(
                    certfile = tls_cfg["certfile"],
                    keyfile  = tls_cfg["keyfile"],
                )
                ca = tls_cfg.get("ca_certs")
                if ca:
                    ssl_ctx.load_verify_locations(ca)
                    ssl_ctx.verify_mode = ssl.CERT_REQUIRED
                log.info("TLS enabled on %s:%d", host, port)

            srv = await asyncio.start_server(
                self._client_connected,
                host=host, port=port,
                ssl=ssl_ctx,
                limit=2 ** 20,   # 1 MB read buffer
            )
            servers.append(srv)
            tls_tag = " (TLS)" if ssl_ctx else ""
            log.info("Listening on %s:%d%s", host, port, tls_tag)

        await asyncio.gather(*[srv.serve_forever() for srv in servers])

    async def _client_connected(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        max_conn = self.cfg.get("limits", {}).get("max_connections", 10000)
        self._connection_count += 1
        if self._connection_count > max_conn:
            peer = writer.get_extra_info("peername", ("?", 0))
            log.warning("Max connections (%d) reached, rejecting %s:%d", max_conn, *peer)
            self._connection_count -= 1
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            return
        try:
            handler = ClientHandler(reader, writer, self)
            await handler.run()
        finally:
            self._connection_count -= 1
