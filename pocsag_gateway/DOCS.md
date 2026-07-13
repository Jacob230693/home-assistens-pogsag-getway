# POCSAG Gateway

Denne app er kodet til:

- RTL-SDR Blog V4
- Frekvens: **171,300 MHz**
- Protokol: **POCSAG**
- Hastighed: **2400 baud**
- Mosquitto Broker i Home Assistant

## Automatisk oprettede entiteter

Når appen starter, opretter MQTT Discovery:

- Pager Gateway status
- Seneste pagerbesked
- Seneste pager capcode
- Seneste pager tidspunkt

## Første test

1. Start appen.
2. Åbn **Log**.
3. Kontroller, at RTL-SDR findes, og at MQTT viser `gateway online`.
4. Vent på en lovlig test- eller stationsbesked.

Du kan også lytte manuelt på MQTT-emnet:

`pager/#`

## Justering

Start med:

- gain: `auto`
- ppm: `0`
- squelch: `0`
