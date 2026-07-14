#!/usr/bin/env python3
"""
POCSAG Gateway 4.2.0

Funktioner:
- Modtager POCSAG-linjer fra multimon-ng via stdin.
- Gemmer alle afkodede kald i SQLite.
- Publicerer seneste kald, historik og statistik via MQTT.
- Publicerer Skælskør-kald på pager/skaelskoer.
- Modtager deltagelsessvar på pager/response:
    {"event_id":"...", "participation":"driving|not_driving"}
- Opdaterer den konkrete alarm og genudsender statistik/historik.
"""

from __future__ import annotations

import json
import os
import re
import signal
import sqlite3
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import paho.mqtt.client as mqtt


VERSION = "4.2.0"

HOST = os.environ["MQTT_HOST"]
PORT = int(os.environ.get("MQTT_PORT", "1883"))
USERNAME = os.environ.get("MQTT_USERNAME", "")
PASSWORD = os.environ.get("MQTT_PASSWORD", "")

BASE = os.environ.get("MQTT_BASE_TOPIC", "pager").strip("/")
DISCOVERY = os.environ.get("MQTT_DISCOVERY_PREFIX", "homeassistant").strip("/")
RETAIN = os.environ.get("RETAIN_LAST_MESSAGE", "true").lower() == "true"
FREQUENCY = os.environ.get("GATEWAY_FREQUENCY", "171.300M")

DB_FILE = Path(os.environ.get("POCSAG_DB_FILE", "/data/pocsag_gateway.db"))
HISTORY_LIMIT = max(10, int(os.environ.get("POCSAG_HISTORY_LIMIT", "100")))

SKAELSKOER_CAPCODES = {
    "1300": "Allekald",
    "1320": "Gruppe 1",
    "1340": "Gruppe 2",
}

STATE_TOPIC = f"{BASE}/state"
STATUS_TOPIC = f"{BASE}/status"
RAW_TOPIC = f"{BASE}/raw"
EVENT_TOPIC = f"{BASE}/events"
HISTORY_TOPIC = f"{BASE}/history"
RESPONSE_TOPIC = f"{BASE}/response"
RESPONSE_RESULT_TOPIC = f"{BASE}/response/result"
SKAELSKOER_TOPIC = f"{BASE}/skaelskoer"
SKAELSKOER_LAST_TOPIC = f"{BASE}/skaelskoer/last"

COUNT_TOPIC = f"{BASE}/count"
SKAELSKOER_COUNT_TOPIC = f"{BASE}/skaelskoer/count"
DRIVING_COUNT_TOPIC = f"{BASE}/participation/driving"
NOT_DRIVING_COUNT_TOPIC = f"{BASE}/participation/not_driving"
PENDING_COUNT_TOPIC = f"{BASE}/participation/pending"
PARTICIPATION_PERCENT_TOPIC = f"{BASE}/participation/percent"

PATTERN = re.compile(
    r"POCSAG(?P<baud>\d+):\s+Address:\s*(?P<address>\d+)"
    r"(?:\s+Function:\s*(?P<function>\d+))?"
    r"(?:\s+(?P<kind>Alpha|Numeric):\s*(?P<message>.*))?",
    re.IGNORECASE,
)

DEVICE = {
    "identifiers": ["pocsag_gateway"],
    "name": "POCSAG Gateway",
    "manufacturer": "Jacob / Home Assistant",
    "model": "RTL-SDR Blog V4",
    "sw_version": VERSION,
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_message(message: str) -> str:
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
    return re.sub(r"\s+", " ", cleaned).strip()


def get_station(address: str) -> tuple[str, str]:
    if address in SKAELSKOER_CAPCODES:
        return "Skælskør", SKAELSKOER_CAPCODES[address]
    return "Ukendt", "Ukendt"


def classify_message(message: str) -> str:
    """Enkel, lokal kategorisering uden adresseudledning."""
    text = message.upper()

    if "ABA" in text or "AUTOMATISK BRANDALARM" in text:
        return "ABA"
    if any(word in text for word in ("FUH", "FÆRDSELSUHELD", "FÆRDSELS UHELD", "TRAFIKUHELD")):
        return "Færdselsuheld"
    if any(word in text for word in ("BYGNINGSBRAND", "VILLABRAND", "BRAND I", "ILDEBRAND")):
        return "Brand"
    if any(word in text for word in ("NATURBRAND", "MARKBRAND", "GRÆSBRAND", "SKOVBRAND")):
        return "Naturbrand"
    if any(word in text for word in ("REDNING", "FASTKLEMT", "DRUKNE", "PERSON I VAND")):
        return "Redning"
    if "DAGENS PRØVE" in text or "PRØVE" in text:
        return "Prøvekald"
    return "Andet"


def db_connect() -> sqlite3.Connection:
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_FILE, timeout=10)
    connection.row_factory = sqlite3.Row
    return connection


def init_database() -> None:
    with db_connect() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                protocol TEXT NOT NULL,
                baud INTEGER NOT NULL,
                frequency TEXT NOT NULL,
                address TEXT NOT NULL,
                function TEXT,
                format TEXT,
                station TEXT NOT NULL,
                alarm_group TEXT NOT NULL,
                category TEXT NOT NULL,
                is_skaelskoer INTEGER NOT NULL DEFAULT 0,
                message TEXT NOT NULL,
                raw TEXT NOT NULL,
                participation TEXT NOT NULL DEFAULT 'pending',
                response_timestamp TEXT
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp DESC)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_station ON events(station)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_participation ON events(participation)"
        )
        connection.commit()


def insert_event(event: dict[str, Any]) -> None:
    with db_connect() as connection:
        connection.execute(
            """
            INSERT INTO events (
                event_id, timestamp, protocol, baud, frequency, address,
                function, format, station, alarm_group, category,
                is_skaelskoer, message, raw, participation, response_timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event["event_id"],
                event["timestamp"],
                event["protocol"],
                event["baud"],
                event["frequency"],
                event["address"],
                event["function"],
                event["format"],
                event["station"],
                event["group"],
                event["category"],
                1 if event["is_skaelskoer"] else 0,
                event["message"],
                event["raw"],
                event["participation"],
                event["response_timestamp"],
            ),
        )
        connection.commit()


def update_participation(
    event_id: str,
    participation: str,
    response_timestamp: str,
) -> bool:
    if participation not in {"driving", "not_driving", "pending"}:
        raise ValueError("Ugyldig participation-værdi")

    with db_connect() as connection:
        cursor = connection.execute(
            """
            UPDATE events
            SET participation = ?, response_timestamp = ?
            WHERE event_id = ?
            """,
            (participation, response_timestamp, event_id),
        )
        connection.commit()
        return cursor.rowcount == 1


def row_to_event(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "event_id": row["event_id"],
        "timestamp": row["timestamp"],
        "protocol": row["protocol"],
        "baud": row["baud"],
        "frequency": row["frequency"],
        "address": row["address"],
        "capcode": row["address"],
        "function": row["function"],
        "format": row["format"],
        "station": row["station"],
        "group": row["alarm_group"],
        "category": row["category"],
        "is_skaelskoer": bool(row["is_skaelskoer"]),
        "message": row["message"],
        "raw": row["raw"],
        "participation": row["participation"],
        "response_timestamp": row["response_timestamp"],
    }


def get_history(limit: int = HISTORY_LIMIT) -> list[dict[str, Any]]:
    with db_connect() as connection:
        rows = connection.execute(
            "SELECT * FROM events ORDER BY timestamp DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [row_to_event(row) for row in rows]


def get_event(event_id: str) -> dict[str, Any] | None:
    with db_connect() as connection:
        row = connection.execute(
            "SELECT * FROM events WHERE event_id = ?",
            (event_id,),
        ).fetchone()
    return row_to_event(row) if row else None


def get_statistics() -> dict[str, Any]:
    with db_connect() as connection:
        total = connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        skaelskoer = connection.execute(
            "SELECT COUNT(*) FROM events WHERE is_skaelskoer = 1"
        ).fetchone()[0]
        driving = connection.execute(
            """
            SELECT COUNT(*) FROM events
            WHERE is_skaelskoer = 1 AND participation = 'driving'
            """
        ).fetchone()[0]
        not_driving = connection.execute(
            """
            SELECT COUNT(*) FROM events
            WHERE is_skaelskoer = 1 AND participation = 'not_driving'
            """
        ).fetchone()[0]
        pending = connection.execute(
            """
            SELECT COUNT(*) FROM events
            WHERE is_skaelskoer = 1 AND participation = 'pending'
            """
        ).fetchone()[0]

    answered = driving + not_driving
    percentage = round((driving / answered) * 100, 1) if answered else 0.0

    return {
        "total": total,
        "skaelskoer": skaelskoer,
        "driving": driving,
        "not_driving": not_driving,
        "pending": pending,
        "participation_percent": percentage,
    }


init_database()

client = mqtt.Client(client_id="pocsag_gateway", clean_session=True)
if USERNAME:
    client.username_pw_set(USERNAME, PASSWORD)

client.will_set(STATUS_TOPIC, "offline", qos=1, retain=True)


def publish_json(topic: str, payload: Any, qos: int = 1, retain: bool = False) -> None:
    client.publish(
        topic,
        json.dumps(payload, ensure_ascii=False),
        qos=qos,
        retain=retain,
    )


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
        "category": {
            "name": "Seneste pager kategori",
            "state_topic": STATE_TOPIC,
            "value_template": "{{ value_json.category }}",
            "icon": "mdi:shape",
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
        "driving_count": {
            "name": "Skælskør ture jeg kørte",
            "state_topic": DRIVING_COUNT_TOPIC,
            "icon": "mdi:fire-truck",
        },
        "not_driving_count": {
            "name": "Skælskør ture jeg ikke kørte",
            "state_topic": NOT_DRIVING_COUNT_TOPIC,
            "icon": "mdi:close-circle",
        },
        "pending_count": {
            "name": "Skælskør ubesvarede kald",
            "state_topic": PENDING_COUNT_TOPIC,
            "icon": "mdi:help-circle",
        },
        "participation_percent": {
            "name": "Skælskør deltagelsesprocent",
            "state_topic": PARTICIPATION_PERCENT_TOPIC,
            "unit_of_measurement": "%",
            "icon": "mdi:percent",
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
        publish_json(
            f"{DISCOVERY}/sensor/pocsag_gateway_{object_id}/config",
            payload,
            qos=1,
            retain=True,
        )


def publish_history() -> None:
    publish_json(HISTORY_TOPIC, get_history(), qos=1, retain=True)


def publish_statistics() -> None:
    stats = get_statistics()
    client.publish(COUNT_TOPIC, str(stats["total"]), qos=1, retain=True)
    client.publish(SKAELSKOER_COUNT_TOPIC, str(stats["skaelskoer"]), qos=1, retain=True)
    client.publish(DRIVING_COUNT_TOPIC, str(stats["driving"]), qos=1, retain=True)
    client.publish(NOT_DRIVING_COUNT_TOPIC, str(stats["not_driving"]), qos=1, retain=True)
    client.publish(PENDING_COUNT_TOPIC, str(stats["pending"]), qos=1, retain=True)
    client.publish(
        PARTICIPATION_PERCENT_TOPIC,
        str(stats["participation_percent"]),
        qos=1,
        retain=True,
    )


def on_connect(
    mqtt_client: mqtt.Client,
    _userdata: Any,
    _flags: dict[str, Any],
    return_code: int,
) -> None:
    if return_code == 0:
        mqtt_client.subscribe(RESPONSE_TOPIC, qos=1)
        print(f"[mqtt] Lytter efter svar på {RESPONSE_TOPIC}", flush=True)
    else:
        print(f"[mqtt] Forbindelsesfejl, kode {return_code}", flush=True)


def on_message(
    _mqtt_client: mqtt.Client,
    _userdata: Any,
    message: mqtt.MQTTMessage,
) -> None:
    if message.topic != RESPONSE_TOPIC:
        return

    try:
        payload = json.loads(message.payload.decode("utf-8"))
        event_id = str(payload.get("event_id", "")).strip()
        participation = str(payload.get("participation", "")).strip()
        response_timestamp = str(
            payload.get("response_timestamp") or utc_now()
        ).strip()

        if not event_id:
            raise ValueError("event_id mangler")

        if participation not in {"driving", "not_driving"}:
            raise ValueError("participation skal være driving eller not_driving")

        updated = update_participation(
            event_id=event_id,
            participation=participation,
            response_timestamp=response_timestamp,
        )

        result = {
            "event_id": event_id,
            "participation": participation,
            "response_timestamp": response_timestamp,
            "updated": updated,
        }

        if updated:
            result["event"] = get_event(event_id)
            publish_history()
            publish_statistics()
            print(
                f"[response] {event_id}: {participation}",
                flush=True,
            )
        else:
            result["error"] = "event_id blev ikke fundet"
            print(
                f"[response] Ukendt event_id: {event_id}",
                flush=True,
            )

        publish_json(RESPONSE_RESULT_TOPIC, result, qos=1, retain=False)

    except Exception as exc:
        error = {"updated": False, "error": str(exc)}
        publish_json(RESPONSE_RESULT_TOPIC, error, qos=1, retain=False)
        print(f"[response] Fejl: {exc}", flush=True)


client.on_connect = on_connect
client.on_message = on_message

while True:
    try:
        client.connect(HOST, PORT, 60)
        break
    except Exception as exc:
        print(
            f"[mqtt] Forbindelse fejlede: {exc}. Ny prøve om 5 sek.",
            flush=True,
        )
        time.sleep(5)

client.loop_start()

publish_discovery()
client.publish(STATUS_TOPIC, "online", qos=1, retain=True)
publish_history()
publish_statistics()

print(
    f"[mqtt] Forbundet til {HOST}:{PORT}; POCSAG Gateway {VERSION} online",
    flush=True,
)
print(f"[database] SQLite: {DB_FILE}", flush=True)
print(
    f"[filter] Skælskør-capcodes: {', '.join(sorted(SKAELSKOER_CAPCODES))}",
    flush=True,
)


def shutdown(*_args: Any) -> None:
    print("[system] Lukker POCSAG Gateway...", flush=True)
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
    now = utc_now()

    if not match:
        publish_json(
            RAW_TOPIC,
            {"timestamp": now, "raw": line},
            qos=0,
            retain=False,
        )
        continue

    address = match.group("address")
    station, group = get_station(address)
    message_text = clean_message(match.group("message") or "")

    event = {
        "event_id": uuid.uuid4().hex,
        "timestamp": now,
        "protocol": "POCSAG",
        "baud": int(match.group("baud")),
        "frequency": FREQUENCY,
        "address": address,
        "capcode": address,
        "function": match.group("function"),
        "format": (match.group("kind") or "").lower(),
        "station": station,
        "group": group,
        "category": classify_message(message_text),
        "is_skaelskoer": station == "Skælskør",
        "message": message_text,
        "raw": line,
        "participation": "pending",
        "response_timestamp": None,
    }

    try:
        insert_event(event)
    except sqlite3.IntegrityError:
        print(
            f"[database] Dublet event_id ignoreret: {event['event_id']}",
            flush=True,
        )
        continue
    except Exception as exc:
        print(f"[database] Kunne ikke gemme alarm: {exc}", flush=True)
        continue

    event_json = json.dumps(event, ensure_ascii=False)

    result = client.publish(
        STATE_TOPIC,
        event_json,
        qos=1,
        retain=RETAIN,
    )
    result.wait_for_publish()

    client.publish(EVENT_TOPIC, event_json, qos=1, retain=False)

    if event["is_skaelskoer"]:
        client.publish(SKAELSKOER_TOPIC, event_json, qos=1, retain=False)
        client.publish(SKAELSKOER_LAST_TOPIC, event_json, qos=1, retain=True)
        print(
            f"[skaelskoer] {group} – Capcode {address}: {message_text}",
            flush=True,
        )

    publish_history()
    publish_statistics()

    print(
        f"[alarm] Capcode {address} "
        f"({station} / {group} / {event['category']}): {message_text}",
        flush=True,
    )
