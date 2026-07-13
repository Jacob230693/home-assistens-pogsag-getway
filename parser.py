#!/usr/bin/env python3
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

host = os.environ["MQTT_HOST"]
port = int(os.environ.get("MQTT_PORT", "1883"))
username = os.environ.get("MQTT_USERNAME", "")
password = os.environ.get("MQTT_PASSWORD", "")
topic = os.environ.get("MQTT_TOPIC", "pager/alarm")
retain = os.environ.get("RETAIN", "false").lower() == "true"

pattern = re.compile(
    r"POCSAG(?P<baud>\d+):\s+Address:\s*(?P<address>\d+)"
    r"(?:\s+Function:\s*(?P<function>\d+))?"
    r"(?:\s+(?P<kind>Alpha|Numeric):\s*(?P<message>.*))?",
    re.IGNORECASE,
)

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="pocsag_gateway")
if username:
    client.username_pw_set(username, password)

while True:
    try:
        client.connect(host, port, 60)
        break
    except Exception as exc:
        print(f"[mqtt] Kunne ikke forbinde: {exc}. Prøver igen om 5 sek.", flush=True)
        time.sleep(5)

client.loop_start()
print(f"[mqtt] Forbundet til {host}:{port}", flush=True)

for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue

    print(f"[decoder] {line}", flush=True)
    now = datetime.now(timezone.utc).isoformat()
    match = pattern.search(line)

    if not match:
        client.publish(
            topic + "/raw",
            json.dumps({"timestamp": now, "raw": line}, ensure_ascii=False),
            qos=0,
            retain=False,
        )
        continue

    payload = {
        "timestamp": now,
        "protocol": "POCSAG",
        "baud": int(match.group("baud")),
        "address": match.group("address"),
        "function": match.group("function"),
        "format": (match.group("kind") or "").lower(),
        "message": (match.group("message") or "").strip(),
        "raw": line,
    }

    result = client.publish(
        topic,
        json.dumps(payload, ensure_ascii=False),
        qos=1,
        retain=retain,
    )
    result.wait_for_publish()
    print(f"[mqtt] Publiceret på {topic}: {payload}", flush=True)
