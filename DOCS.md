# POCSAG Gateway

Denne lokale Home Assistant-app er lavet til:

- RTL-SDR Blog V4
- 171,300 MHz
- POCSAG 2400
- MQTT-emnet `pager/alarm`

## Første start

Start appen og åbn **Log**. De første linjer skal vise, at RTL-SDR-enheden er fundet.
Når der modtages en POCSAG-besked, vises den i loggen og publiceres som JSON på
`pager/alarm`.

## Test i Home Assistant

Gå til:

**Indstillinger → Enheder og tjenester → MQTT → Konfigurer → Lyt til et emne**

Lyt til:

`pager/#`

## Justering

- `gain`: Start med `auto`.
- `ppm`: Start med `0`, fordi RTL-SDR Blog V4 har TCXO.
- `squelch`: Start med `0`.
