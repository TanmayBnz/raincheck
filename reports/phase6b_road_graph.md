# Phase-6b Critical Roads and Sensor Coverage

_Generated 2026-10-17 13:55 UTC. OpenStreetMap main roads (motorway|trunk|primary|secondary|tertiary and ramps) within 3 km of each city's sensors. Criticality is length-weighted edge betweenness, scaled so the most critical road in each city is 1. "Critical" is the top 10% of road length by that score. A sensor counts as on a road within 40 m._

| city | road km | junctions | road links | sensors (matched) | critical km | critical km with a sensor | all km with a sensor |
|---|---|---|---|---|---|---|---|
| manchester | 357.3 | 1,378 | 2,145 | 147 (144) | 35.9 | 18.5% | 7.4% |
| torino | 1033.7 | 3,641 | 5,688 | 339 (328) | 103.5 | 30.8% | 9.9% |
| essen | 414.9 | 1,384 | 2,197 | 36 (36) | 41.6 | 9.0% | 3.4% |

## manchester: critical roads with no sensor

| road | class | unmonitored critical length (m) | peak criticality |
|---|---|---|---|
| Chester Road | trunk | 1,963 | 0.98 |
| Mancunian Way | motorway | 2,153 | 0.72 |
| Oldham Road | trunk | 2,611 | 0.57 |
| Wharfside Way | trunk | 1,437 | 0.75 |
| Stretford Road | primary | 2,290 | 0.43 |
| Bridgewater Way | trunk | 1,215 | 0.76 |
| Trafford Road | trunk | 1,440 | 0.60 |
| Hyde Road | trunk | 1,771 | 0.40 |

## torino: critical roads with no sensor

| road | class | unmonitored critical length (m) | peak criticality |
|---|---|---|---|
| Strada di Settimo | secondary | 3,471 | 1.00 |
| Via Nizza | tertiary | 2,793 | 0.66 |
| Corso Vercelli | secondary | 2,081 | 0.65 |
| Corso Unione Sovietica | primary | 2,697 | 0.49 |
| Via Bologna | secondary | 1,502 | 0.76 |
| Via Monginevro | tertiary | 2,131 | 0.50 |
| Via Venti Settembre | tertiary | 1,584 | 0.68 |
| Corso Orbassano | primary | 1,676 | 0.57 |

## essen: critical roads with no sensor

| road | class | unmonitored critical length (m) | peak criticality |
|---|---|---|---|
| Bottroper Straße | primary | 4,353 | 0.68 |
| Gelsenkirchener Straße | secondary | 2,382 | 0.67 |
| Altendorfer Straße | primary | 2,600 | 0.56 |
| A 40 | motorway | 2,796 | 0.50 |
| Frohnhauser Straße | secondary | 1,130 | 0.92 |
| Schützenbahn | primary | 900 | 0.85 |
| Essener Straße | secondary | 1,217 | 0.61 |
| Hans-Böckler-Straße | primary | 1,255 | 0.57 |

## Caveats

- Today's OpenStreetMap, against 2016–2017 traffic. Main roads change
  little over that span, but not never.
- One-way restrictions are ignored, so some detours look shorter than
  they are.
- Betweenness measures how many shortest routes use a road, not how
  much traffic it carries. It ranks structural importance; it is not a
  traffic count.
- The box is the sensor extent plus a margin, so roads near its edge
  have fewer routes passing through them than in the full city.
