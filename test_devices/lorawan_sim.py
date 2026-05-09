"""
lorawan_sim.py — LoRaWAN gateway + ABP device simulator
========================================================
Simulates a RAK-style gateway publishing chirpstack-mqtt-forwarder format
uplink frames to the local MQTT broker.

Gateway EUI : aabbccddee000001
Device EUI  : 0807060504030201
DevAddr     : 01ab23cd
AppSKey     : 00000000000000000000000000000001
NwkSKey     : 00000000000000000000000000000002

Publishes to:
  eu868/gateway/aabbccddee000001/event/up

Frame format: Unconfirmed Data Up (MHDR=0x40)
  - FRMPayload: int16 temperature (x100) + uint8 humidity, encrypted with AppSKey
  - MIC: AES-128-CMAC over B0||frame with NwkSKey

Usage:
  python test_devices/lorawan_sim.py --host 192.168.0.104 --port 1883 --interval 30

Requirements:
  pip install aiomqtt cryptography
"""

import argparse
import asyncio
import base64
import json
import random
import struct
import sys
import time
from datetime import datetime, timezone

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

try:
    import aiomqtt
except ImportError:
    print("ERROR: aiomqtt not installed.  Run: pip install aiomqtt")
    sys.exit(1)

try:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.primitives.cmac import CMAC
    from cryptography.hazmat.backends import default_backend
except ImportError:
    print("ERROR: cryptography not installed.  Run: pip install cryptography")
    sys.exit(1)

# ── LoRaWAN constants ─────────────────────────────────────────────────────────

GATEWAY_EUI = "aabbccddee000001"
DEVICE_EUI  = "0807060504030201"
DEV_ADDR    = bytes.fromhex("01ab23cd")     # big-endian for display, LE in frame
APP_SKEY    = bytes.fromhex("00000000000000000000000000000001")
NWK_SKEY    = bytes.fromhex("00000000000000000000000000000002")

MHDR        = 0x40   # Unconfirmed Data Up
FPORT       = 1

TOPIC       = f"eu868/gateway/{GATEWAY_EUI}/event/up"


# ── LoRaWAN crypto ────────────────────────────────────────────────────────────

def _aes128_ecb_encrypt(key: bytes, block: bytes) -> bytes:
    """Encrypt a single 16-byte block with AES-128-ECB."""
    cipher = Cipher(algorithms.AES(key), modes.ECB(), backend=default_backend())
    enc = cipher.encryptor()
    return enc.update(block) + enc.finalize()


def _lorawan_encrypt_frmpayload(key: bytes, dev_addr: bytes, fcnt: int,
                                 direction: int, plaintext: bytes) -> bytes:
    """
    LoRaWAN AES-128 counter-mode encryption (TS001-1.0.4 §4.3.3).

    Ai = 0x01 || 0x00*4 || dir || DevAddr(LE) || FCnt(LE,4B) || 0x00 || i
    Si = AES(key, Ai)
    ciphertext = plaintext XOR (S1 || S2 || ...)
    """
    if not plaintext:
        return b""

    num_blocks = (len(plaintext) + 15) // 16
    keystream   = b""

    dev_addr_le = dev_addr[::-1]   # LE order

    for i in range(1, num_blocks + 1):
        a_i = (
            b"\x01"
            + b"\x00" * 4
            + bytes([direction & 0xFF])
            + dev_addr_le
            + struct.pack("<I", fcnt)
            + b"\x00"
            + bytes([i & 0xFF])
        )
        keystream += _aes128_ecb_encrypt(key, a_i)

    return bytes(p ^ k for p, k in zip(plaintext, keystream[:len(plaintext)]))


def _lorawan_compute_mic(key: bytes, dev_addr: bytes, fcnt: int,
                          direction: int, frame_without_mic: bytes) -> bytes:
    """
    LoRaWAN MIC = first 4 bytes of AES-128-CMAC(NwkSKey, B0 || frame).

    B0 = 0x49 || 0x00*4 || dir || DevAddr(LE) || FCnt(LE,4B) || 0x00 || len(frame)
    """
    dev_addr_le = dev_addr[::-1]
    msg_len     = len(frame_without_mic)

    b0 = (
        b"\x49"
        + b"\x00" * 4
        + bytes([direction & 0xFF])
        + dev_addr_le
        + struct.pack("<I", fcnt)
        + b"\x00"
        + bytes([msg_len & 0xFF])
    )

    c = CMAC(algorithms.AES(key), backend=default_backend())
    c.update(b0 + frame_without_mic)
    full_mac = c.finalize()
    return full_mac[:4]


def build_lorawan_frame(fcnt: int, temperature: float, humidity: int) -> bytes:
    """
    Build a valid LoRaWAN Unconfirmed Data Up PHY frame (ABP).

    Frame structure:
      MHDR(1) | DevAddr(4,LE) | FCtrl(1) | FCnt(2,LE) | FPort(1) | FRMPayload | MIC(4)
    """
    # Encode sensor data: temp as int16 (x100), humidity as uint8
    temp_raw = int(round(temperature * 100))
    plaintext = struct.pack(">hB", temp_raw, humidity & 0xFF)

    # Encrypt FRMPayload
    frm_payload = _lorawan_encrypt_frmpayload(APP_SKEY, DEV_ADDR, fcnt, 0, plaintext)

    # Build frame without MIC
    dev_addr_le = DEV_ADDR[::-1]   # LE in frame
    frame = (
        bytes([MHDR])
        + dev_addr_le
        + b"\x00"                          # FCtrl = 0x00
        + struct.pack("<H", fcnt & 0xFFFF) # FCnt LE 2 bytes
        + bytes([FPORT])
        + frm_payload
    )

    # Compute and append MIC
    mic = _lorawan_compute_mic(NWK_SKEY, DEV_ADDR, fcnt, 0, frame)
    return frame + mic


# ── Uplink JSON builder ───────────────────────────────────────────────────────

def build_uplink_json(phy_bytes: bytes, fcnt: int, uplink_id: int) -> dict:
    """Build the chirpstack-mqtt-forwarder uplink JSON envelope."""
    return {
        "phyPayload": base64.b64encode(phy_bytes).decode(),
        "txInfo": {
            "frequency": 868100000,
            "modulation": {
                "lora": {
                    "bandwidth":      125000,
                    "spreadingFactor": 7,
                    "codeRate":       "CR_4_5",
                },
            },
        },
        "rxInfo": {
            "gatewayId":  GATEWAY_EUI,
            "rssi":       -65,
            "snr":        9.5,
            "channel":    0,
            "context":    "AAAAAAA=",
            "crcStatus":  "CRC_OK",
            "uplinkId":   uplink_id,
        },
    }


# ── Main loop ─────────────────────────────────────────────────────────────────

async def main(host: str, port: int, interval: int) -> None:
    print("=" * 64)
    print("  LoRaWAN Gateway Simulator")
    print(f"  Broker    : {host}:{port}")
    print(f"  Gateway   : {GATEWAY_EUI}")
    print(f"  DevEUI    : {DEVICE_EUI}")
    print(f"  DevAddr   : {DEV_ADDR.hex()}")
    print(f"  Topic     : {TOPIC}")
    print(f"  Interval  : {interval}s")
    print("=" * 64)

    fcnt      = 0
    uplink_id = 1
    stop_evt  = asyncio.Event()

    while not stop_evt.is_set():
        try:
            async with aiomqtt.Client(
                hostname=host,
                port=port,
                identifier="lorawan-sim-gw",
            ) as client:
                print(f"Connected to {host}:{port}")

                while not stop_evt.is_set():
                    # Simulate sensor readings
                    temperature = round(20.0 + random.uniform(-5.0, 10.0), 2)
                    humidity    = random.randint(30, 90)

                    # Build LoRaWAN frame
                    phy = build_lorawan_frame(fcnt, temperature, humidity)
                    envelope = build_uplink_json(phy, fcnt, uplink_id)
                    json_payload = json.dumps(envelope)

                    # Publish
                    await client.publish(TOPIC, json_payload, qos=0)

                    ts = datetime.now(timezone.utc).isoformat()
                    print(
                        f"[{ts}] FCnt={fcnt:05d}  uplinkId={uplink_id}  "
                        f"temp={temperature:+.2f}C  hum={humidity}%  "
                        f"len={len(phy)}B  -> {TOPIC}"
                    )

                    fcnt      += 1
                    uplink_id += 1

                    # Wait for next interval
                    try:
                        await asyncio.wait_for(stop_evt.wait(), timeout=interval)
                    except asyncio.TimeoutError:
                        pass

        except aiomqtt.MqttError as exc:
            print(f"MQTT error: {exc} — retrying in 10s")
            try:
                await asyncio.wait_for(stop_evt.wait(), timeout=10)
            except asyncio.TimeoutError:
                pass
        except asyncio.CancelledError:
            break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LoRaWAN gateway + ABP device simulator")
    parser.add_argument("--host",     default="192.168.0.104", help="MQTT broker host")
    parser.add_argument("--port",     default=1883, type=int,  help="MQTT broker port")
    parser.add_argument("--interval", default=30,  type=int,  help="Uplink interval (seconds)")
    args = parser.parse_args()

    try:
        asyncio.run(main(args.host, args.port, args.interval))
    except KeyboardInterrupt:
        print("\nStopped by user.")
