#!/usr/bin/env python3

import json
import os
import re
import signal
import sys
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt


# ============================================================
# POCSAG Gateway 4.0
# ============================================================

VERSION = "4.0.0"

HOST = os.environ["MQTT_HOST"]
PORT = int(os.environ.get("MQTT_PORT", "1883"))
USERNAME = os.environ.get("MQTT_USERNAME", "")
PASSWORD = os.environ.get("MQTT_PASSWORD", "")

BASE = os.environ.get("MQTT_BASE_TOPIC", "pager").strip("/")
DISCOVERY = os.environ.get(
    "MQTT_DISCOVERY_PREFIX",
    "homeassistant",
).strip("/")

RETAIN = (
    os.environ.get("RETAIN_LAST_MESSAGE", "true").lower()
    == "true"
)

FREQUENCY = os.environ.get(
    "GATEWAY_FREQUENCY",
    "171.300M",
)

LOG_FILE = Path(
    os.environ.get(
        "POCSAG_LOG_FILE",
        "/data/pocsag_events.jsonl",
    )
)

HISTORY_LIMIT = int(
    os.environ.get(
        "POCSAG_HISTORY_LIMIT",
        "100",
    )
)

SKAELSKOER_CAPCODES = {
    "1300": "Allekald",
    "1320": "Gruppe 1",
    "1340": "Gruppe 2",
}


# ============================================================
# MQTT-emner
# ============================================================

STATE_TOPIC = f"{BASE}/state"
STATUS_TOPIC = f"{BASE}/status"
RAW_TOPIC = f"{BASE}/raw"

EVENT_TOPIC = f"{BASE}/events"
HISTORY_TOPIC = f"{BASE}/history"

SKAELSKOER_TOPIC = f"{BASE}/skaelskoer"
SKAELSKOER_LAST_TOPIC = f"{BASE}/skaelskoer/last"

COUNT_TOPIC = f"{BASE}/count"
SKAELSKOER_COUNT_TOPIC = f"{BASE}/skaelskoer/count"


# ============================================================
# Decoder-mønster
# ============================================================

PATTERN = re.compile(
    r"POCSAG(?P<baud>\d+):\s+Address:\s*(?P<address>\d+)"
    r"(?:\s+Function:\s*(?P<function>\d+))?"
    r"(?:\s+(?P<kind>Alpha|Numeric):\s*(?P<message>.*))?",
    re.IGNORECASE,
)


# ============================================================
# Home Assistant-enhed
# ============================================================

DEVICE = {
    "identifiers": ["pocsag_gateway"],
    "name": "POCSAG Gateway",
    "manufacturer": "Jacob / Home Assistant",
    "model": "RTL-SDR Blog V4",
    "sw_version": VERSION,
}


# ============================================================
# Hjælpefunktioner
# ============================================================

def utc_now() -> str:
    """Returnerer aktuelt tidspunkt i ISO-format."""
    return datetime.now(timezone.utc).isoformat()


def clean_message(message: str) -> str:
    """Fjerner nogle af decoderens kontroltegn."""
    if not message:
        return ""

    replacements = {
        "<NUL>": "",
        "<CR>": " ",
        "<LF>": " ",
        "\\r": " ",
        "\\n": " ",
    }

    cleaned = message

    for old, new in replacements.items():
        cleaned = cleaned.replace(old, new)

    cleaned = re.sub(r"\s+", " ", cleaned)

    return cleaned.strip()


def get_station(address: str) -> tuple[str, str]:
    """
    Finder station og gruppe ud fra capcode.
    Ukendte capcodes gemmes stadig.
    """
    if address in SKAELSKOER_CAPCODES:
        return (
            "Skælskør",
            SKAELSKOER_CAPCODES[address],
        )

    return ("Ukendt", "Ukendt")


def append_event_to_log(event: dict) -> None:
    """Gemmer én hændelse som én JSON-linje."""
    try:
        LOG_FILE.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with LOG_FILE.open(
            "a",
            encoding="utf-8",
        ) as file_handle:
            file_handle.write(
                json.dumps(
                    event,
                    ensure_ascii=False,
                )
                + "\n"
            )

    except Exception as exc:
        print(
            f"[log] Kunne ikke gemme hændelsen: {exc}",
            flush=True,
        )


def load_history() -> deque:
    """Indlæser de seneste hændelser fra logfilen."""
    history = deque(maxlen=HISTORY_LIMIT)

    if not LOG_FILE.exists():
        return history

    try:
        with LOG_FILE.open(
            "r",
            encoding="utf-8",
        ) as file_handle:
            for line in file_handle:
                line = line.strip()

                if not line:
                    continue

                try:
                    history.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    except Exception as exc:
        print(
            f"[log] Kunne ikke læse historikken: {exc}",
            flush=True,
        )

    return history


history = load_history()

total_count = len(history)
skaelskoer_count = sum(
    1
    for event in history
    if event.get("station") == "Skælskør"
)


# ============================================================
# MQTT
# ============================================================

client = mqtt.Client(
    client_id="pocsag_gateway",
    clean_session=True,
)

if USERNAME:
    client.username_pw_set(
        USERNAME,
        PASSWORD,
    )

client.will_set(
    STATUS_TOPIC,
    "offline",
    qos=1,
    retain=True,
)


def connect_mqtt() -> None:
    """Forsøger forbindelse til MQTT, indtil den lykkes."""
    while True:
        try:
            client.connect(
                HOST,
                PORT,
                60,
            )
            return

        except Exception as exc:
            print(
                f"[mqtt] Forbindelse fejlede: {exc}. "
                "Ny prøve om 5 sek.",
                flush=True,
            )
            time.sleep(5)


connect_mqtt()
client.loop_start()


# ============================================================
# Home Assistant MQTT Discovery
# ============================================================

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

        "station": {
            "name": "Seneste pager station",
            "state_topic": STATE_TOPIC,
            "value_template": "{{ value_json.station }}",
            "icon": "mdi:fire-station",
        },

        "group": {
            "name": "Seneste pager gruppe",
            "state_topic": STATE_TOPIC,
            "value_template": "{{ value_json.group }}",
            "icon": "mdi:account-group",
        },

        "timestamp": {
            "name": "Seneste pager tidspunkt",
            "state_topic": STATE_TOPIC,
            "value_template": "{{ value_json.timestamp }}",
            "device_class": "timestamp",
            "icon": "mdi:clock-alert",
        },

        "skaelskoer_message": {
            "name": "Seneste Skælskør alarm",
            "state_topic": SKAELSKOER_LAST_TOPIC,
            "value_template": "{{ value_json.message }}",
            "json_attributes_topic": SKAELSKOER_LAST_TOPIC,
            "icon": "mdi:alarm-light",
        },

        "total_count": {
            "name": "POCSAG modtagne kald",
            "state_topic": COUNT_TOPIC,
            "icon": "mdi:counter",
            "state_class": "total_increasing",
        },

        "skaelskoer_count": {
            "name": "Skælskør modtagne kald",
            "state_topic": SKAELSKOER_COUNT_TOPIC,
            "icon": "mdi:fire-truck",
            "state_class": "total_increasing",
        },
    }

    for object_id, config in entities.items():
        payload = {
            **config,
            "unique_id": f"pocsag_gateway_{object_id}",
            "device": DEVICE,
            "availability_topic": STATUS_TOPIC,
            "payload_available": "online",
            "payload_not_available": "offline",
        }

        discovery_topic = (
            f"{DISCOVERY}/sensor/"
            f"pocsag_gateway_{object_id}/config"
        )

        client.publish(
            discovery_topic,
            json.dumps(
                payload,
                ensure_ascii=False,
            ),
            qos=1,
            retain=True,
        )


def publish_history() -> None:
    """Sender de seneste hændelser på MQTT."""
    client.publish(
        HISTORY_TOPIC,
        json.dumps(
            list(history),
            ensure_ascii=False,
        ),
        qos=1,
        retain=True,
    )


def publish_counts() -> None:
    client.publish(
        COUNT_TOPIC,
        str(total_count),
        qos=1,
        retain=True,
    )

    client.publish(
        SKAELSKOER_COUNT_TOPIC,
        str(skaelskoer_count),
        qos=1,
        retain=True,
    )


publish_discovery()

client.publish(
    STATUS_TOPIC,
    "online",
    qos=1,
    retain=True,
)

publish_history()
publish_counts()

print(
    f"[mqtt] Forbundet til {HOST}:{PORT}; "
    f"POCSAG Gateway {VERSION} online",
    flush=True,
)

print(
    f"[log] Logfil: {LOG_FILE}",
    flush=True,
)

print(
    f"[filter] Skælskør-capcodes: "
    f"{', '.join(sorted(SKAELSKOER_CAPCODES))}",
    flush=True,
)


# ============================================================
# Luk systemet korrekt
# ============================================================

def shutdown(*_args) -> None:
    print(
        "[system] Lukker POCSAG Gateway...",
        flush=True,
    )

    client.publish(
        STATUS_TOPIC,
        "offline",
        qos=1,
        retain=True,
    )

    client.loop_stop()
    client.disconnect()

    raise SystemExit(0)


signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)


# ============================================================
# Behandling af decoder-data
# ============================================================

for raw_line in sys.stdin:
    line = raw_line.strip()

    if not line:
        continue

    print(
        f"[decoder] {line}",
        flush=True,
    )

    match = PATTERN.search(line)
    now = utc_now()

    if not match:
        client.publish(
            RAW_TOPIC,
            json.dumps(
                {
                    "timestamp": now,
                    "raw": line,
                },
                ensure_ascii=False,
            ),
            qos=0,
            retain=False,
        )
        continue

    address = match.group("address")
    station, group = get_station(address)

    message = clean_message(
        match.group("message") or ""
    )

    event = {
        "event_id": uuid.uuid4().hex,
        "timestamp": now,
        "protocol": "POCSAG",
        "baud": int(match.group("baud")),
        "frequency": FREQUENCY,
        "address": address,
        "capcode": address,
        "function": match.group("function"),
        "format": (
            match.group("kind") or ""
        ).lower(),
        "station": station,
        "group": group,
        "is_skaelskoer": station == "Skælskør",
        "message": message,
        "raw": line,
        "accepted": False,
    }

    # Gem alle afkodede kald permanent
    append_event_to_log(event)

    # Opdater historikken
    history.append(event)

    total_count += 1

    if station == "Skælskør":
        skaelskoer_count += 1

    event_json = json.dumps(
        event,
        ensure_ascii=False,
    )

    # Seneste generelle kald
    state_result = client.publish(
        STATE_TOPIC,
        event_json,
        qos=1,
        retain=RETAIN,
    )

    state_result.wait_for_publish()

    # Eventstrøm med alle kald
    client.publish(
        EVENT_TOPIC,
        event_json,
        qos=1,
        retain=False,
    )

    # Skælskør-kald sendes på eget MQTT-topic
    if station == "Skælskør":
        client.publish(
            SKAELSKOER_TOPIC,
            event_json,
            qos=1,
            retain=False,
        )

        client.publish(
            SKAELSKOER_LAST_TOPIC,
            event_json,
            qos=1,
            retain=True,
        )

        print(
            f"[skaelskoer] {group} – "
            f"Capcode {address}: {message}",
            flush=True,
        )

    # Opdater Home Assistant-historikken og tællerne
    publish_history()
    publish_counts()

    print(
        f"[alarm] Capcode {address} "
        f"({station} / {group}): {message}",
        flush=True,
    )