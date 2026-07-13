#!/usr/bin/with-contenv bashio
set -euo pipefail

FREQUENCY="$(bashio::config 'frequency')"
SAMPLE_RATE="$(bashio::config 'sample_rate')"
GAIN="$(bashio::config 'gain')"
PPM="$(bashio::config 'ppm')"
SQUELCH="$(bashio::config 'squelch')"
MQTT_BASE_TOPIC="$(bashio::config 'mqtt_base_topic')"
MQTT_DISCOVERY_PREFIX="$(bashio::config 'mqtt_discovery_prefix')"
RETAIN_LAST_MESSAGE="$(bashio::config 'retain_last_message')"

MQTT_HOST="$(bashio::services mqtt 'host')"
MQTT_PORT="$(bashio::services mqtt 'port')"
MQTT_USERNAME="$(bashio::services mqtt 'username')"
MQTT_PASSWORD="$(bashio::services mqtt 'password')"

bashio::log.info "POCSAG Gateway 0.2.0 starter"
bashio::log.info "RTL-SDR: ${FREQUENCY}, POCSAG 2400, sample rate ${SAMPLE_RATE}"
bashio::log.info "MQTT: ${MQTT_HOST}:${MQTT_PORT}/${MQTT_BASE_TOPIC}"

RTL_ARGS=(-M fm -A fast -E dc -f "${FREQUENCY}" -s "${SAMPLE_RATE}" -p "${PPM}")

if [[ "${GAIN}" != "auto" ]]; then
  RTL_ARGS+=(-g "${GAIN}")
fi

if [[ "${SQUELCH}" -gt 0 ]]; then
  RTL_ARGS+=(-l "${SQUELCH}")
fi

export MQTT_HOST MQTT_PORT MQTT_USERNAME MQTT_PASSWORD
export MQTT_BASE_TOPIC MQTT_DISCOVERY_PREFIX RETAIN_LAST_MESSAGE
export GATEWAY_FREQUENCY="${FREQUENCY}"

rtl_fm "${RTL_ARGS[@]}" 2> >(sed's/^/[rtl_fm] /' >&2) \
  | multimon-ng -q -t raw -a POCSAG2400 -f alpha /dev/stdin \
  | python3 -u /parser.py
