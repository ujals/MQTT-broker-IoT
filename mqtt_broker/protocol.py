"""
mqtt_broker.protocol
====================
MQTT wire-format encoder / decoder.

Covers MQTT 3.1 (level 3), 3.1.1 (level 4), and 5.0 (level 5).

Packet types handled
--------------------
  CONNECT / CONNACK
  PUBLISH / PUBACK / PUBREC / PUBREL / PUBCOMP
  SUBSCRIBE / SUBACK
  UNSUBSCRIBE / UNSUBACK
  PINGREQ / PINGRESP
  DISCONNECT
  AUTH  (MQTT 5.0 only)
"""

import struct
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Dict, List, Optional, Tuple


# ──────────────────────────────────────────────────────────────────────────────
# Enumerations
# ──────────────────────────────────────────────────────────────────────────────

class PacketType(IntEnum):
    CONNECT     = 1
    CONNACK     = 2
    PUBLISH     = 3
    PUBACK      = 4
    PUBREC      = 5
    PUBREL      = 6
    PUBCOMP     = 7
    SUBSCRIBE   = 8
    SUBACK      = 9
    UNSUBSCRIBE = 10
    UNSUBACK    = 11
    PINGREQ     = 12
    PINGRESP    = 13
    DISCONNECT  = 14
    AUTH        = 15   # MQTT 5.0 only


class RC(IntEnum):
    """MQTT 5.0 reason codes (also used internally for 3.x mapping)."""
    SUCCESS                      = 0x00
    NORMAL_DISCONNECTION         = 0x00
    GRANTED_QOS_0                = 0x00
    GRANTED_QOS_1                = 0x01
    GRANTED_QOS_2                = 0x02
    DISCONNECT_WITH_WILL         = 0x04
    NO_MATCHING_SUBSCRIBERS      = 0x10
    NO_SUBSCRIPTION_FOUND        = 0x11
    UNSPECIFIED_ERROR            = 0x80
    MALFORMED_PACKET             = 0x81
    PROTOCOL_ERROR               = 0x82
    IMPL_SPECIFIC_ERROR          = 0x83
    UNSUPPORTED_PROTOCOL_VERSION = 0x84
    CLIENT_ID_NOT_VALID          = 0x85
    BAD_USER_OR_PASSWORD         = 0x86
    NOT_AUTHORIZED               = 0x87
    SERVER_UNAVAILABLE           = 0x88
    SERVER_BUSY                  = 0x89
    BANNED                       = 0x8A
    SERVER_SHUTTING_DOWN         = 0x8B
    BAD_AUTH_METHOD              = 0x8C
    KEEP_ALIVE_TIMEOUT           = 0x8D
    SESSION_TAKEN_OVER           = 0x8E
    TOPIC_FILTER_INVALID         = 0x8F
    TOPIC_NAME_INVALID           = 0x90
    PACKET_ID_IN_USE             = 0x91
    RECEIVE_MAX_EXCEEDED         = 0x93
    TOPIC_ALIAS_INVALID          = 0x94
    PACKET_TOO_LARGE             = 0x95
    QUOTA_EXCEEDED               = 0x97
    PAYLOAD_FORMAT_INVALID       = 0x99
    RETAIN_NOT_SUPPORTED         = 0x9A
    QOS_NOT_SUPPORTED            = 0x9B
    USE_ANOTHER_SERVER           = 0x9C
    SERVER_MOVED                 = 0x9D
    SHARED_SUBS_NOT_SUPPORTED    = 0x9E
    CONNECTION_RATE_EXCEEDED     = 0x9F
    TOPIC_ALIAS_MAXIMUM          = 0xA0   # reused locally


class Prop(IntEnum):
    """MQTT 5.0 property identifiers."""
    PAYLOAD_FORMAT_INDICATOR   = 0x01
    MESSAGE_EXPIRY_INTERVAL    = 0x02
    CONTENT_TYPE               = 0x03
    RESPONSE_TOPIC             = 0x08
    CORRELATION_DATA           = 0x09
    SUBSCRIPTION_IDENTIFIER    = 0x0B
    SESSION_EXPIRY_INTERVAL    = 0x11
    ASSIGNED_CLIENT_ID         = 0x12
    SERVER_KEEP_ALIVE          = 0x13
    AUTH_METHOD                = 0x15
    AUTH_DATA                  = 0x16
    REQUEST_PROBLEM_INFO       = 0x17
    WILL_DELAY_INTERVAL        = 0x18
    REQUEST_RESPONSE_INFO      = 0x19
    RESPONSE_INFO              = 0x1A
    SERVER_REFERENCE           = 0x1C
    REASON_STRING              = 0x1F
    RECEIVE_MAXIMUM            = 0x21
    TOPIC_ALIAS_MAXIMUM        = 0x22
    TOPIC_ALIAS                = 0x23
    MAX_QOS                    = 0x24
    RETAIN_AVAILABLE           = 0x25
    USER_PROPERTY              = 0x26
    MAX_PACKET_SIZE            = 0x27
    WILDCARD_SUB_AVAILABLE     = 0x28
    SUBSCRIPTION_ID_AVAILABLE  = 0x29
    SHARED_SUB_AVAILABLE       = 0x2A


# ──────────────────────────────────────────────────────────────────────────────
# Low-level helpers
# ──────────────────────────────────────────────────────────────────────────────

def encode_varint(n: int) -> bytes:
    """Encode a variable-length integer (remaining length, property length)."""
    if n == 0:
        return b"\x00"
    result = []
    while n > 0:
        b = n % 128
        n //= 128
        if n > 0:
            b |= 0x80
        result.append(b)
    return bytes(result)


def decode_varint(data: bytes, offset: int) -> Tuple[int, int]:
    """Return (value, new_offset). Raises ValueError on malformed input."""
    multiplier = 1
    value = 0
    while True:
        if offset >= len(data):
            raise ValueError("Truncated variable-length integer")
        b = data[offset]
        offset += 1
        value += (b & 0x7F) * multiplier
        multiplier *= 128
        if not (b & 0x80):
            break
        if multiplier > 128 ** 3:
            raise ValueError("Variable-length integer overflow")
    return value, offset


def encode_utf8(s: str) -> bytes:
    enc = s.encode("utf-8")
    return struct.pack("!H", len(enc)) + enc


def decode_utf8(data: bytes, offset: int) -> Tuple[str, int]:
    if offset + 2 > len(data):
        raise ValueError("Buffer too short for string length prefix")
    length = struct.unpack_from("!H", data, offset)[0]
    offset += 2
    if offset + length > len(data):
        raise ValueError("Buffer too short for string data")
    return data[offset: offset + length].decode("utf-8", errors="replace"), offset + length


def encode_binary(b: bytes) -> bytes:
    return struct.pack("!H", len(b)) + b


def decode_binary(data: bytes, offset: int) -> Tuple[bytes, int]:
    if offset + 2 > len(data):
        raise ValueError("Buffer too short for binary length prefix")
    length = struct.unpack_from("!H", data, offset)[0]
    offset += 2
    if offset + length > len(data):
        raise ValueError("Buffer too short for binary data")
    return data[offset: offset + length], offset + length


def build_packet(ptype: int, flags: int, body: bytes) -> bytes:
    """Assemble a complete MQTT packet."""
    return bytes([(ptype << 4) | (flags & 0x0F)]) + encode_varint(len(body)) + body


# ──────────────────────────────────────────────────────────────────────────────
# MQTT 5.0 Property codec
# ──────────────────────────────────────────────────────────────────────────────

# (prop_id) -> type-tag used by encoder/decoder
_PROP_TYPE: Dict[int, str] = {
    0x01: "byte",    # PAYLOAD_FORMAT_INDICATOR
    0x02: "u32",     # MESSAGE_EXPIRY_INTERVAL
    0x03: "utf8",    # CONTENT_TYPE
    0x08: "utf8",    # RESPONSE_TOPIC
    0x09: "bin",     # CORRELATION_DATA
    0x0B: "varint",  # SUBSCRIPTION_IDENTIFIER
    0x11: "u32",     # SESSION_EXPIRY_INTERVAL
    0x12: "utf8",    # ASSIGNED_CLIENT_ID
    0x13: "u16",     # SERVER_KEEP_ALIVE
    0x15: "utf8",    # AUTH_METHOD
    0x16: "bin",     # AUTH_DATA
    0x17: "byte",    # REQUEST_PROBLEM_INFO
    0x18: "u32",     # WILL_DELAY_INTERVAL
    0x19: "byte",    # REQUEST_RESPONSE_INFO
    0x1A: "utf8",    # RESPONSE_INFO
    0x1C: "utf8",    # SERVER_REFERENCE
    0x1F: "utf8",    # REASON_STRING
    0x21: "u16",     # RECEIVE_MAXIMUM
    0x22: "u16",     # TOPIC_ALIAS_MAXIMUM
    0x23: "u16",     # TOPIC_ALIAS
    0x24: "byte",    # MAX_QOS
    0x25: "byte",    # RETAIN_AVAILABLE
    0x26: "pair",    # USER_PROPERTY  (repeatable)
    0x27: "u32",     # MAX_PACKET_SIZE
    0x28: "byte",    # WILDCARD_SUB_AVAILABLE
    0x29: "byte",    # SUBSCRIPTION_ID_AVAILABLE
    0x2A: "byte",    # SHARED_SUB_AVAILABLE
}


def encode_properties(props: Optional[Dict[int, Any]]) -> bytes:
    """Encode a {Prop: value} dict into MQTT 5.0 property bytes (length-prefixed)."""
    if not props:
        return b"\x00"
    body = b""
    for pid, val in props.items():
        tag = _PROP_TYPE.get(pid, "byte")
        if tag == "pair":
            pairs = val if isinstance(val, list) else [val]
            for k, v in pairs:
                body += bytes([pid]) + encode_utf8(k) + encode_utf8(v)
        else:
            body += bytes([pid])
            if tag == "byte":
                body += bytes([int(val)])
            elif tag == "u16":
                body += struct.pack("!H", int(val))
            elif tag == "u32":
                body += struct.pack("!I", int(val))
            elif tag == "utf8":
                body += encode_utf8(str(val))
            elif tag == "bin":
                body += encode_binary(bytes(val))
            elif tag == "varint":
                body += encode_varint(int(val))
    return encode_varint(len(body)) + body


def decode_properties(data: bytes, offset: int) -> Tuple[Dict[int, Any], int]:
    """Decode MQTT 5.0 properties. Returns ({prop_id: value}, new_offset)."""
    prop_len, offset = decode_varint(data, offset)
    end = offset + prop_len
    props: Dict[int, Any] = {}
    while offset < end:
        pid = data[offset]
        offset += 1
        tag = _PROP_TYPE.get(pid, "byte")
        if tag == "byte":
            props[pid] = data[offset]; offset += 1
        elif tag == "u16":
            props[pid] = struct.unpack_from("!H", data, offset)[0]; offset += 2
        elif tag == "u32":
            props[pid] = struct.unpack_from("!I", data, offset)[0]; offset += 4
        elif tag == "utf8":
            props[pid], offset = decode_utf8(data, offset)
        elif tag == "bin":
            props[pid], offset = decode_binary(data, offset)
        elif tag == "varint":
            props[pid], offset = decode_varint(data, offset)
        elif tag == "pair":
            k, offset = decode_utf8(data, offset)
            v, offset = decode_utf8(data, offset)
            props.setdefault(pid, []).append((k, v))
    return props, end


# ──────────────────────────────────────────────────────────────────────────────
# Raw packet container (returned by the async reader in broker.py)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class RawPacket:
    ptype:   int    # PacketType value
    flags:   int    # lower 4 bits of fixed header byte
    payload: bytes  # everything after the fixed header + remaining length


# ──────────────────────────────────────────────────────────────────────────────
# CONNECT
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class ConnectPacket:
    protocol_name:    str
    protocol_level:   int           # 3 = 3.1 | 4 = 3.1.1 | 5 = 5.0
    clean_session:    bool          # "clean start" in 5.0
    keepalive:        int           # seconds
    client_id:        str
    will_topic:       Optional[str]   = None
    will_payload:     Optional[bytes] = None
    will_qos:         int             = 0
    will_retain:      bool            = False
    username:         Optional[str]   = None
    password:         Optional[bytes] = None
    properties:       Dict            = field(default_factory=dict)
    will_properties:  Dict            = field(default_factory=dict)


def parse_connect(data: bytes) -> ConnectPacket:
    offset = 0
    protocol_name, offset = decode_utf8(data, offset)
    protocol_level = data[offset]; offset += 1
    flags          = data[offset]; offset += 1

    username_flag = bool(flags & 0x80)
    password_flag = bool(flags & 0x40)
    will_retain   = bool(flags & 0x20)
    will_qos      = (flags >> 3) & 0x03
    will_flag     = bool(flags & 0x04)
    clean_session = bool(flags & 0x02)

    keepalive = struct.unpack_from("!H", data, offset)[0]; offset += 2

    properties: Dict = {}
    if protocol_level == 5:
        properties, offset = decode_properties(data, offset)

    client_id, offset = decode_utf8(data, offset)

    will_topic = will_payload = None
    will_properties: Dict = {}
    if will_flag:
        if protocol_level == 5:
            will_properties, offset = decode_properties(data, offset)
        will_topic,   offset = decode_utf8(data, offset)
        will_payload, offset = decode_binary(data, offset)

    username = None
    if username_flag:
        username, offset = decode_utf8(data, offset)

    password = None
    if password_flag:
        password, offset = decode_binary(data, offset)

    return ConnectPacket(
        protocol_name   = protocol_name,
        protocol_level  = protocol_level,
        clean_session   = clean_session,
        keepalive       = keepalive,
        client_id       = client_id,
        will_topic      = will_topic,
        will_payload    = will_payload,
        will_qos        = will_qos,
        will_retain     = will_retain,
        username        = username,
        password        = password,
        properties      = properties,
        will_properties = will_properties,
    )


# Map MQTT-5 reason code → MQTT-3.1.1 CONNACK return code (Table 3-1)
_RC5_TO_RC311 = {
    RC.SUCCESS:                      0,
    RC.UNSUPPORTED_PROTOCOL_VERSION: 1,
    RC.CLIENT_ID_NOT_VALID:          2,
    RC.SERVER_UNAVAILABLE:           3,
    RC.BAD_USER_OR_PASSWORD:         4,
    RC.NOT_AUTHORIZED:               5,
}


def encode_connack(
    session_present: bool,
    rc: int,
    protocol_level: int = 4,
    properties: Optional[Dict] = None,
) -> bytes:
    if protocol_level == 5:
        body = bytes([int(session_present), rc]) + encode_properties(properties)
    else:
        rc311 = _RC5_TO_RC311.get(rc, 5)
        body  = bytes([int(session_present), rc311])
    return build_packet(PacketType.CONNACK, 0, body)


# ──────────────────────────────────────────────────────────────────────────────
# PUBLISH
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class PublishPacket:
    topic:      str
    payload:    bytes
    qos:        int  = 0
    retain:     bool = False
    dup:        bool = False
    packet_id:  Optional[int] = None
    properties: Dict = field(default_factory=dict)


def parse_publish(flags: int, data: bytes, protocol_level: int) -> PublishPacket:
    dup    = bool(flags & 0x08)
    qos    = (flags >> 1) & 0x03
    retain = bool(flags & 0x01)

    offset = 0
    topic, offset = decode_utf8(data, offset)

    packet_id = None
    if qos > 0:
        packet_id = struct.unpack_from("!H", data, offset)[0]; offset += 2

    properties: Dict = {}
    if protocol_level == 5:
        properties, offset = decode_properties(data, offset)

    payload = data[offset:]
    return PublishPacket(
        topic=topic, payload=payload, qos=qos, retain=retain,
        dup=dup, packet_id=packet_id, properties=properties,
    )


def encode_publish(pub: PublishPacket, protocol_level: int = 4) -> bytes:
    flags = (int(pub.dup) << 3) | (pub.qos << 1) | int(pub.retain)
    body  = encode_utf8(pub.topic)
    if pub.qos > 0:
        body += struct.pack("!H", pub.packet_id or 0)
    if protocol_level == 5:
        body += encode_properties(pub.properties)
    body += pub.payload
    return build_packet(PacketType.PUBLISH, flags, body)


# ──────────────────────────────────────────────────────────────────────────────
# QoS ACK packets (PUBACK, PUBREC, PUBREL, PUBCOMP)
# ──────────────────────────────────────────────────────────────────────────────

def _encode_qos_ack(ptype: int, flags: int, packet_id: int,
                    rc: int = 0, protocol_level: int = 4,
                    properties: Optional[Dict] = None) -> bytes:
    body = struct.pack("!H", packet_id)
    if protocol_level == 5:
        body += bytes([rc]) + encode_properties(properties)
    return build_packet(ptype, flags, body)


def encode_puback(packet_id: int, rc: int = 0,
                  protocol_level: int = 4, properties: Optional[Dict] = None) -> bytes:
    return _encode_qos_ack(PacketType.PUBACK, 0, packet_id, rc, protocol_level, properties)


def encode_pubrec(packet_id: int, rc: int = 0,
                  protocol_level: int = 4, properties: Optional[Dict] = None) -> bytes:
    return _encode_qos_ack(PacketType.PUBREC, 0, packet_id, rc, protocol_level, properties)


def encode_pubrel(packet_id: int, rc: int = 0,
                  protocol_level: int = 4, properties: Optional[Dict] = None) -> bytes:
    return _encode_qos_ack(PacketType.PUBREL, 0b0010, packet_id, rc, protocol_level, properties)


def encode_pubcomp(packet_id: int, rc: int = 0,
                   protocol_level: int = 4, properties: Optional[Dict] = None) -> bytes:
    return _encode_qos_ack(PacketType.PUBCOMP, 0, packet_id, rc, protocol_level, properties)


def parse_packet_id(data: bytes) -> int:
    """Extract packet ID from the first two bytes of a payload (PUBACK/PUBREC/PUBREL/PUBCOMP)."""
    return struct.unpack_from("!H", data, 0)[0]


# ──────────────────────────────────────────────────────────────────────────────
# SUBSCRIBE / SUBACK
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class Subscription:
    topic_filter:       str
    qos:                int
    no_local:           bool = False
    retain_as_published: bool = False
    retain_handling:    int  = 0


@dataclass
class SubscribePacket:
    packet_id:     int
    subscriptions: List[Subscription]
    properties:    Dict = field(default_factory=dict)


def parse_subscribe(data: bytes, protocol_level: int) -> SubscribePacket:
    offset    = 0
    packet_id = struct.unpack_from("!H", data, offset)[0]; offset += 2

    properties: Dict = {}
    if protocol_level == 5:
        properties, offset = decode_properties(data, offset)

    subs: List[Subscription] = []
    while offset < len(data):
        topic_filter, offset = decode_utf8(data, offset)
        opts = data[offset]; offset += 1
        qos              = opts & 0x03
        no_local         = bool(opts & 0x04) if protocol_level == 5 else False
        retain_as_pub    = bool(opts & 0x08) if protocol_level == 5 else False
        retain_handling  = ((opts >> 4) & 0x03) if protocol_level == 5 else 0
        subs.append(Subscription(topic_filter, qos, no_local, retain_as_pub, retain_handling))

    return SubscribePacket(packet_id, subs, properties)


def encode_suback(packet_id: int, reason_codes: List[int],
                  protocol_level: int = 4, properties: Optional[Dict] = None) -> bytes:
    body = struct.pack("!H", packet_id)
    if protocol_level == 5:
        body += encode_properties(properties)
    body += bytes(reason_codes)
    return build_packet(PacketType.SUBACK, 0, body)


# ──────────────────────────────────────────────────────────────────────────────
# UNSUBSCRIBE / UNSUBACK
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class UnsubscribePacket:
    packet_id:     int
    topic_filters: List[str]
    properties:    Dict = field(default_factory=dict)


def parse_unsubscribe(data: bytes, protocol_level: int) -> UnsubscribePacket:
    offset    = 0
    packet_id = struct.unpack_from("!H", data, offset)[0]; offset += 2

    properties: Dict = {}
    if protocol_level == 5:
        properties, offset = decode_properties(data, offset)

    filters: List[str] = []
    while offset < len(data):
        tf, offset = decode_utf8(data, offset)
        filters.append(tf)

    return UnsubscribePacket(packet_id, filters, properties)


def encode_unsuback(packet_id: int, reason_codes: List[int],
                    protocol_level: int = 4, properties: Optional[Dict] = None) -> bytes:
    body = struct.pack("!H", packet_id)
    if protocol_level == 5:
        body += encode_properties(properties)
        body += bytes(reason_codes)
    # MQTT 3.x UNSUBACK has no payload beyond packet ID
    return build_packet(PacketType.UNSUBACK, 0, body)


# ──────────────────────────────────────────────────────────────────────────────
# PINGRESP
# ──────────────────────────────────────────────────────────────────────────────

PINGRESP: bytes = build_packet(PacketType.PINGRESP, 0, b"")


# ──────────────────────────────────────────────────────────────────────────────
# DISCONNECT
# ──────────────────────────────────────────────────────────────────────────────

def encode_disconnect(rc: int = 0, protocol_level: int = 4,
                      properties: Optional[Dict] = None) -> bytes:
    if protocol_level == 5:
        body = bytes([rc]) + encode_properties(properties)
    else:
        body = b""
    return build_packet(PacketType.DISCONNECT, 0, body)


def parse_disconnect(data: bytes, protocol_level: int) -> Tuple[int, Dict]:
    """Returns (reason_code, properties). Handles empty payload (normal disconnect)."""
    if not data or protocol_level < 5:
        return RC.NORMAL_DISCONNECTION, {}
    rc = data[0]
    props: Dict = {}
    if len(data) > 1:
        props, _ = decode_properties(data, 1)
    return rc, props


# ──────────────────────────────────────────────────────────────────────────────
# AUTH  (MQTT 5.0 §3.15)
# ──────────────────────────────────────────────────────────────────────────────

def encode_auth(rc: int = RC.SUCCESS, properties: Optional[Dict] = None) -> bytes:
    body = bytes([rc]) + encode_properties(properties)
    return build_packet(PacketType.AUTH, 0, body)
