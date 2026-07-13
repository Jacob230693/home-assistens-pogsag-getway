#!/usr/bin/with-contenv bashio
set -euo pipefail

FREQUENCY="$(bashio::config 'frequency')"
SAMPLE_RATE="$(bashio::config 'sample_rate')"
GAIN="$(bashio::config 'gain')"
PPM="$(bashio::config 'ppm')"
SQUELCH="$(bashio::config 'squelch')"
MQTT_TOPIC="$(bashio::config 'mqtt_topic')"
RETAIN="$(bashio::config 'retain')"

MQTT_HOST="$(bashio::services mqtt 'host')"
MQTT_PORT="$(bashio::services mqtt 'port')"
MQTT_USERNAME="$(bashio::services mqtt 'username')"
MQTT_PASSWORD="$(bashio::services mqtt 'password')"

bashio::log.info "Starter POCSAG Gateway"
bashio::log.info "Frekvens ${FREQUENCY}, POCSAG 2400, sample rate ${SAMPLE_RATE}"
bashio::log.info "MQTT topic: ${MQTT_TOPIC}"

RTL_ARGS=(-M fm -f "${FREQUENCY}" -s "${SAMPLE_RATE}" -p "${PPM}")

if [[ "${GAIN}" != "auto" ]]; then
  RTL_ARGS+=(-g "${GAIN}")
fi

if [[ "${SQUELCH}" -gt 0 ]]; then
  RTL_ARGS+=(-l "${SQUELCH}")
fi

export MQTT_HOST MQTT_PORT MQTT_USERNAME MQTT_PASSWORD MQTT_TOPIC RETAIN

rtl_fm "${RTL_ARGS[@]}" 2> >(sed -u 's/^/[rtl_fm] /' >&2) \
  | multimon-ng -q -t raw -a POCSAG2400 -f alpha /dev/stdin \
  | python3 -u /parser.py
