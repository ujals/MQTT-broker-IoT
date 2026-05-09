#!/usr/bin/env python3
"""
test_client.py — Paho test clients for the custom MQTT broker.

Tests
-----
  1. MQTT 3.1.1  subscribe + publish  (QoS 0 and 1)
  2. MQTT 5.0    subscribe + publish  (with properties)
  3. Wildcard subscription
  4. Retained message
  5. Will message

Usage
-----
  # In terminal 1 — start the broker:
  python -m mqtt_broker --config config.yaml

  # In terminal 2 — run all tests:
  python test_client.py

  # Run a specific test:
  python test_client.py --test will
"""

import argparse
import json
import sys
import time
import threading

import paho.mqtt.client as mqtt

HOST   = "localhost"
PORT   = 1883
WAIT   = 3          # seconds to wait for messages


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

class Collector:
    """Receives messages and lets the test check them."""
    def __init__(self):
        self.messages  = []
        self.connected = threading.Event()
        self._lock     = threading.Lock()

    def on_connect(self, client, userdata, flags, rc, *args):
        if rc == 0:
            self.connected.set()

    def on_message(self, client, userdata, msg):
        with self._lock:
            self.messages.append({
                "topic":   msg.topic,
                "payload": msg.payload.decode("utf-8", errors="replace"),
                "qos":     msg.qos,
                "retain":  msg.retain,
            })

    def wait_for(self, count: int, timeout: float = WAIT) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                if len(self.messages) >= count:
                    return True
            time.sleep(0.1)
        return False


def make_client_v311(client_id: str) -> tuple:
    c = Collector()
    try:
        cl = mqtt.Client(
            client_id         = client_id,
            clean_session     = True,
            callback_api_version = mqtt.CallbackAPIVersion.VERSION2,
        )
    except AttributeError:
        # paho < 2.0
        cl = mqtt.Client(client_id=client_id, clean_session=True)
    cl.on_connect = c.on_connect
    cl.on_message = c.on_message
    cl.connect(HOST, PORT, keepalive=10)
    cl.loop_start()
    c.connected.wait(timeout=5)
    return cl, c


def make_client_v5(client_id: str) -> tuple:
    c = Collector()
    try:
        cl = mqtt.Client(
            client_id            = client_id,
            protocol             = mqtt.MQTTv5,
            callback_api_version = mqtt.CallbackAPIVersion.VERSION2,
        )
    except AttributeError:
        cl = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv5)
    cl.on_connect = c.on_connect
    cl.on_message = c.on_message
    cl.connect(HOST, PORT, keepalive=10)
    cl.loop_start()
    c.connected.wait(timeout=5)
    return cl, c


def ok(msg: str): print(f"  ✔  {msg}")
def fail(msg: str): print(f"  ✘  {msg}")


# ──────────────────────────────────────────────────────────────────────────────
# Test cases
# ──────────────────────────────────────────────────────────────────────────────

def test_basic_311():
    print("\n[TEST] MQTT 3.1.1 — basic subscribe/publish")
    sub_cl, sub_c = make_client_v311("test-sub-311")
    pub_cl, _     = make_client_v311("test-pub-311")

    sub_cl.subscribe("test/hello", qos=0)
    time.sleep(0.3)

    payload = json.dumps({"device": "lora-001", "temp": 23.5, "unit": "C"})
    pub_cl.publish("test/hello", payload, qos=0)

    if sub_c.wait_for(1):
        m = sub_c.messages[0]
        ok(f"Received on '{m['topic']}': {m['payload']}")
    else:
        fail("No message received within timeout")

    sub_cl.loop_stop(); sub_cl.disconnect()
    pub_cl.loop_stop(); pub_cl.disconnect()


def test_qos1():
    print("\n[TEST] MQTT 3.1.1 — QoS 1")
    sub_cl, sub_c = make_client_v311("test-sub-qos1")
    pub_cl, _     = make_client_v311("test-pub-qos1")

    sub_cl.subscribe("test/qos1", qos=1)
    time.sleep(0.3)

    payload = json.dumps({"msg": "guaranteed delivery"})
    pub_cl.publish("test/qos1", payload, qos=1)

    if sub_c.wait_for(1):
        ok(f"QoS-1 message received: {sub_c.messages[0]['payload']}")
    else:
        fail("QoS-1 message not received")

    sub_cl.loop_stop(); sub_cl.disconnect()
    pub_cl.loop_stop(); pub_cl.disconnect()


def test_wildcard():
    print("\n[TEST] MQTT 3.1.1 — Wildcard subscription (#)")
    sub_cl, sub_c = make_client_v311("test-sub-wild")
    pub_cl, _     = make_client_v311("test-pub-wild")

    sub_cl.subscribe("lns/#", qos=0)
    time.sleep(0.3)

    for dev in ["lns/device/001/up", "lns/device/002/up", "lns/gateway/stats"]:
        pub_cl.publish(dev, json.dumps({"device": dev}), qos=0)

    if sub_c.wait_for(3):
        ok(f"Received {len(sub_c.messages)} messages via wildcard 'lns/#'")
        for m in sub_c.messages:
            print(f"       topic: {m['topic']}")
    else:
        fail(f"Only got {len(sub_c.messages)}/3 messages")

    sub_cl.loop_stop(); sub_cl.disconnect()
    pub_cl.loop_stop(); pub_cl.disconnect()


def test_retained():
    print("\n[TEST] Retained message")
    pub_cl, _ = make_client_v311("test-pub-retain")
    pub_cl.publish(
        "test/retained",
        json.dumps({"status": "online"}),
        qos=0, retain=True,
    )
    time.sleep(0.3)

    sub_cl, sub_c = make_client_v311("test-sub-retain")
    sub_cl.subscribe("test/retained", qos=0)

    if sub_c.wait_for(1):
        m = sub_c.messages[0]
        if m["retain"]:
            ok(f"Retained message received: {m['payload']}")
        else:
            fail("Message received but retain flag not set")
    else:
        fail("No retained message received")

    # Clear retained
    pub_cl.publish("test/retained", b"", qos=0, retain=True)

    pub_cl.loop_stop(); pub_cl.disconnect()
    sub_cl.loop_stop(); sub_cl.disconnect()


def test_will():
    print("\n[TEST] Will message")
    # Subscriber
    sub_cl, sub_c = make_client_v311("test-sub-will")
    sub_cl.subscribe("test/will/+", qos=0)
    time.sleep(0.3)

    # Publisher with a will
    try:
        will_cl = mqtt.Client(
            client_id            = "test-will-publisher",
            clean_session        = True,
            callback_api_version = mqtt.CallbackAPIVersion.VERSION2,
        )
    except AttributeError:
        will_cl = mqtt.Client(client_id="test-will-publisher", clean_session=True)

    will_cl.will_set(
        "test/will/gone",
        json.dumps({"device": "test-will-publisher", "status": "offline"}),
        qos=0, retain=False,
    )
    will_cl.connect(HOST, PORT)
    will_cl.loop_start()
    time.sleep(0.5)

    # Force ungraceful disconnect — yank the socket so the broker triggers the will
    try:
        will_cl._sock.close()
    except Exception:
        pass
    will_cl.loop_stop()

    if sub_c.wait_for(1, timeout=5):
        ok(f"Will message received: {sub_c.messages[0]['payload']}")
    else:
        fail("Will message not received (broker may need a moment)")

    sub_cl.loop_stop(); sub_cl.disconnect()


def test_mqtt5():
    print("\n[TEST] MQTT 5.0 — subscribe/publish with properties")
    try:
        sub_cl, sub_c = make_client_v5("test-sub-v5")
        pub_cl, _     = make_client_v5("test-pub-v5")
    except Exception as exc:
        print(f"  ⚠  Skipped (paho may not support MQTTv5): {exc}")
        return

    from paho.mqtt.properties import Properties
    from paho.mqtt.packettypes import PacketTypes

    sub_cl.subscribe("test/v5", qos=1)
    time.sleep(0.3)

    props = Properties(PacketTypes.PUBLISH)
    props.ContentType     = "application/json"
    props.UserProperty    = [("source", "lora-gw-01"), ("region", "IN")]
    payload               = json.dumps({"rssi": -85, "snr": 7.2})

    pub_cl.publish("test/v5", payload, qos=1, properties=props)

    if sub_c.wait_for(1):
        ok(f"MQTT 5.0 message received: {sub_c.messages[0]['payload']}")
    else:
        fail("MQTT 5.0 message not received")

    sub_cl.loop_stop(); sub_cl.disconnect()
    pub_cl.loop_stop(); pub_cl.disconnect()


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

ALL_TESTS = {
    "basic":    test_basic_311,
    "qos1":     test_qos1,
    "wildcard": test_wildcard,
    "retained": test_retained,
    "will":     test_will,
    "mqtt5":    test_mqtt5,
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",  default=HOST)
    parser.add_argument("--port",  default=PORT, type=int)
    parser.add_argument("--test",  default="all", choices=list(ALL_TESTS) + ["all"])
    args = parser.parse_args()

    HOST = args.host
    PORT = args.port

    tests = list(ALL_TESTS.values()) if args.test == "all" else [ALL_TESTS[args.test]]

    print(f"Connecting to broker at {HOST}:{PORT}")
    for t in tests:
        try:
            t()
        except Exception as exc:
            fail(f"Exception: {exc}")
        time.sleep(0.5)

    print("\nDone.")
