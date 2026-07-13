#!/usr/bin/env python3
import json
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

HOST = os.environ["MQTT_HOST"]
PORT = int(os.environ.get("MQTT_PORT", "1883"))
USERNAME = os.environ.get("MQTT_USERNAME", "")
PASSWORD = os.environ.get("MQTT_PASSWORD", "")
BASE = os.environ.get("MQTT_BASE_TOPIC", "pager").strip("/")
DISCOVERY = os.environ.get("MQTT_DISCOVERY_PREFIX", "homeassistant").strip("/")
RETAIN = os.environ.get("RETAIN_LAST_MESSAGE", "true").lower() == "true"
FREQUENCY = os.environ.get("GATEWAY_FREQUENCY", "171.300M")

STATE_TOPIC = f"{BASE}/state"
STATUS_TOPIC = f"{BASE}/status"
RAW_TOPIC = f"{BASE}/raw"

PATTERN = re.compile(
    r"POCSAG(?P<baud>\\d+):\\s+Address:\\s*(?P<address>\\d+)"
    r"(?:\\s+Function:\\s*(?P<function>\\d+))?"
    r"(?:\\s+(?P<kind>Alpha|Numeric):\\s*(?P<message>.*))?",
    re.IGNORECASE,
)

DEVICE = {
    "identifiers": ["pocsag_gateway"],
    "name": "POCSAG Gateway",
    "manufacturer": "Jacob / Home Assistant",
    "model": "RTL-SDR Blog V4",
    "sw_version": "0.2.0",
}

client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="pocsag_gateway",
    clean_session=True,
)
if USERNAME:
    client.username_pw_set(USERNAME, PASSWORD)

client.will_set(STATUS_TOPIC, "offline", qos=1, retain=True)

while True:
    try:
        client.connect(HOST, PORT, 60)
        break
    except Exception as exc:
        print(f"[mqtt] Forbindelse fejlede: {exc}. Ny prøve om 5 sek.", flush=True)
        time.sleep(5)

client.loop_start()

def publish_discovery() -> None:
    entities = {
        "status": {
            "name": "Pager Gateway status",
            "state_topic": STATUS_TOPIC,
            "icon": "mdi:radio-tower",
        },
        "message": {
            "name": "Seneste pagerbesked",
            "state_topic": STATE_TOPIC,
            "value_template": "{{ value_json.message }}",
            "json_attributes_topic": STATE_TOPIC,
            "icon": "mdi:message-alert",
        },
        "address": {
            "name": "Seneste pager capcode",
            "state_topic": STATE_TOPIC,
            "value_template": "{{ value_json.address }}",
            "icon": "mdi:identifier",
        },
        "timestamp": {
            "name": "Seneste pager tidspunkt",
            "state_topic": STATE_TOPIC,
            "value_template": "{{ value_json.timestamp }}",
            "device_class": "timestamp",
            "icon": "mdi:clock-alert",
        },
    }

    for object_id, cfg in entities.items():
        payload = {
            **cfg,
            "unique_id": f"pocsag_gateway_{object_id}",
            "device": DEVICE,
            "availability_topic": STATUS_TOPIC,
            "payload_available": "online",
            "payload_not_available": "offline",
        }
        topic = f"{DISCOVERY}/sensor/pocsag_gateway_{object_id}/config"
        client.publish(topic, json.dumps(payload, ensure_ascii=False), qos=1, retain=True)

publish_discovery()
client.publish(STATUS_TOPIC, "online", qos=1, retain=True)
print(f"[mqtt] Forbundet til {HOST}:{PORT}; gateway online", flush=True)

def shutdown(*_args) -> None:
    client.publish(STATUS_TOPIC, "offline", qos=1, retain=True)
    client.loop_stop()
    client.disconnect()
    raise SystemExit(0)

signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)

for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue

    print(f"[decoder] {line}", flush=True)
    match = PATTERN.search(line)
    now = datetime.now(timezone.utc).isoformat()

    if not match:
        client.publish(
            RAW_TOPIC,
            json.dumps({"timestamp": now, "raw": line}, ensure_ascii=False),
            qos=0,
            retain=False,
        )
        continue

    message = (match.group("message") or "").strip()
    payload = {
        "timestamp": now,
        "protocol": "POCSAG",
        "baud": int(match.group("baud")),
        "frequency": FREQUENCY,
        "address": match.group("address"),
        "function": match.group("function"),
        "format": (match.group("kind") or "").lower(),
        "message": message,
        "raw": line,
    }

    result = client.publish(
        STATE_TOPIC,
        json.dumps(payload, ensure_ascii=False),
        qos=1,
        retain=RETAIN,
    )
    result.wait_for_publish()
    print(f"[alarm] Capcode {payload['address']}: {payload['message']}", flush=True)
