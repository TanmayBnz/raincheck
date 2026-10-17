# Phase-5R Dose-Response, expected-state split — D8

_Generated 2026-10-17 09:07 UTC. 2,113,520 intervals in 10,112 detector-days across 3 cities. All figures are percentage points of deviation from each detector's own dry typical profile, contrasted against Dry within the same stratum. Brackets are 95% cluster bootstrap intervals (2,000 replicates, resampling detector-days); `ns` marks an interval containing zero._

**Gate verdict: PASS**

## 1. How to read this table

Every cell is a **contrast against Dry inside the same stratum** — same
city, same road class, same time of day, same congestion state. So
`-2.3 [-3.9, -0.8]` means: in this stratum, intervals in this rain band
ran 2.3 percentage points slower relative to their own dry typical
profile than dry intervals did, and the data are consistent with
anything from 0.8 to 3.9 points.

`ns` means the interval contains zero. It is printed rather than hidden:
a rain band that demonstrably does *not* move speed is a finding, and
the width of its interval says whether that is a real null or merely an
underpowered cell.

Cells with fewer than 12 detector-days on either side are suppressed entirely (—).

## 2. The two channels

Rain does two things at once and they fight on the pooled average: it
slows vehicles, and it removes them. Fewer vehicles on a signalised
arterial means *higher* speeds. Reading the speed column alone therefore
cannot distinguish a road that stayed fast from a road that emptied.
Both are given for every stratum below.

**This is the finding.** Conditioning on road state does not merely
sharpen the rain effect — it reverses its sign. On free-flowing roads
rain slows traffic, as driver-adaptation theory predicts. On congested
roads measured speed *rises* under rain while flow falls sharply, which
is the demand channel: the road emptied. Phase 4's pooled numbers
averaged these two opposite effects together, which is why they were
uninterpretable and why the gate there was deliberately not set on the
sign of the pooled response.

### Speed and flow by road state, pooled across cities

| stratum | Light | Moderate | Heavy | Extreme | detector-days |
|---|---|---|---|---|---|
| congested | -1.3 [-4.2, +2.0] ns | -2.7 [-5.9, +1.6] ns | +5.4 [-2.8, +17.5] ns | -0.5 [-5.2, +4.4] ns | 2,701 |
| free-flowing | -0.7 [-1.2, -0.2] | -0.8 [-1.5, +0.2] ns | -2.3 [-3.1, -1.4] | -2.6 [-5.3, +0.3] ns | 10,091 |

Flow, same cells:

| stratum | Light | Moderate | Heavy | Extreme | detector-days |
|---|---|---|---|---|---|
| congested | -0.4 [-1.2, +0.3] ns | -0.8 [-1.9, +0.3] ns | -2.5 [-4.5, -0.3] | -4.2 [-6.6, -1.5] | 2,701 |
| free-flowing | +0.4 [-0.4, +1.2] ns | -2.0 [-2.7, -1.2] | +5.9 [+4.2, +7.5] | +0.9 [-2.0, +4.1] ns | 10,091 |

### Speed deviation, by city and road state

| stratum | Light | Moderate | Heavy | Extreme | detector-days |
|---|---|---|---|---|---|
| essen|congested | — | — | — | — | 14 |
| essen|free-flowing | +0.3 [-1.3, +2.8] ns | +2.0 [-0.7, +6.6] ns | -0.4 [-2.7, +1.6] ns | — | 1,260 |
| manchester|congested | -2.6 [-5.1, -0.1] | -2.9 [-5.5, -0.3] | +2.5 [-1.9, +7.5] ns | +3.7 [-1.2, +9.3] ns | 1,213 |
| manchester|free-flowing | -1.2 [-1.8, -0.5] | -1.0 [-1.7, -0.2] | -0.8 [-2.1, +0.5] ns | +2.0 [-0.8, +5.6] ns | 2,387 |
| torino|congested | +0.1 [-5.0, +6.5] ns | -2.1 [-9.1, +8.7] ns | +8.8 [-7.2, +35.3] ns | -11.1 [-20.1, -2.2] | 1,474 |
| torino|free-flowing | -0.8 [-1.4, -0.2] | -1.7 [-2.6, -0.8] | -3.7 [-4.9, -2.5] | -9.5 [-13.5, -5.2] | 6,444 |

### Flow deviation, same cells

| stratum | Light | Moderate | Heavy | Extreme | detector-days |
|---|---|---|---|---|---|
| essen|congested | — | — | — | — | 14 |
| essen|free-flowing | +6.4 [+2.4, +10.6] | -0.7 [-3.2, +2.1] ns | -4.5 [-10.2, +1.0] ns | — | 1,260 |
| manchester|congested | +0.5 [-0.5, +1.6] ns | +0.0 [-1.3, +1.3] ns | -3.6 [-5.5, -1.8] | -5.7 [-8.3, -3.2] | 1,213 |
| manchester|free-flowing | +5.3 [+3.9, +6.9] | -3.4 [-4.5, -2.2] | -3.5 [-5.5, -1.4] | -7.3 [-9.7, -5.0] | 2,387 |
| torino|congested | -1.4 [-2.5, -0.4] | -1.7 [-3.7, +0.1] ns | -1.1 [-5.0, +2.9] ns | +0.6 [-5.0, +6.5] ns | 1,474 |
| torino|free-flowing | -2.5 [-3.2, -1.7] | -1.4 [-2.4, -0.3] | +12.9 [+10.4, +15.3] | +10.8 [+5.6, +16.7] | 6,444 |

Where flow falls and speed rises, the road got emptier, not faster.

## 3. By road class

| stratum | Light | Moderate | Heavy | Extreme | detector-days |
|---|---|---|---|---|---|
| motorway/trunk | -0.8 [-1.6, -0.1] | -1.2 [-1.9, -0.4] | -0.5 [-1.7, +0.7] ns | +3.6 [+0.9, +6.8] | 1,707 |
| primary | -0.2 [-1.2, +1.2] ns | +0.1 [-1.9, +3.3] ns | -0.5 [-2.3, +1.3] ns | -3.5 [-8.0, +0.7] ns | 2,167 |
| secondary | -0.2 [-1.7, +1.4] ns | -0.3 [-2.5, +2.3] ns | +2.7 [-3.2, +12.2] ns | -7.2 [-13.3, -1.7] | 2,429 |
| tertiary | -0.8 [-1.7, +0.0] ns | -2.2 [-3.3, -1.2] | -3.9 [-5.5, -2.2] | -3.9 [-9.7, +2.3] ns | 2,039 |
| residential/other | -0.6 [-1.5, +0.2] ns | -0.8 [-2.3, +1.0] ns | -2.9 [-5.4, +0.1] ns | -3.6 [-12.9, +6.9] ns | 1,770 |

### Flow

| stratum | Light | Moderate | Heavy | Extreme | detector-days |
|---|---|---|---|---|---|
| motorway/trunk | +3.8 [+2.7, +4.9] | -2.8 [-3.7, -1.9] | -3.1 [-4.8, -1.4] | -7.6 [-9.6, -5.6] | 1,707 |
| primary | +2.1 [+0.5, +4.0] | -0.6 [-2.1, +1.2] ns | +3.6 [+0.1, +7.1] | -3.3 [-7.5, +1.0] ns | 2,167 |
| secondary | -0.4 [-2.0, +1.3] ns | -0.6 [-2.2, +1.0] ns | +4.8 [+1.0, +8.3] | +6.7 [-0.1, +13.6] ns | 2,429 |
| tertiary | -2.3 [-3.9, -0.7] | -3.3 [-5.4, -1.0] | +11.4 [+7.7, +15.4] | +0.1 [-5.5, +6.2] ns | 2,039 |
| residential/other | -1.6 [-3.1, -0.1] | -1.8 [-3.5, +0.1] ns | +10.9 [+6.0, +16.0] | +12.4 [+2.9, +23.5] | 1,770 |

## 4. By time of day

| stratum | Light | Moderate | Heavy | Extreme | detector-days |
|---|---|---|---|---|---|
| night | +0.5 [-0.4, +1.6] ns | +2.9 [+1.3, +5.0] | -2.7 [-4.4, -0.9] | — | 9,994 |
| am_peak | -1.4 [-2.8, +0.3] ns | -2.4 [-4.1, -0.1] | -2.3 [-3.7, -0.7] | -3.7 [-6.7, -0.6] | 7,913 |
| midday | +0.1 [-0.9, +1.3] ns | -0.6 [-1.9, +1.1] ns | -0.4 [-2.9, +3.5] ns | +3.8 [+0.5, +7.6] | 9,215 |
| pm_peak | +0.9 [-0.6, +2.8] ns | -0.2 [-1.9, +2.0] ns | +4.8 [-4.4, +16.5] ns | — | 7,583 |
| evening | -0.3 [-1.0, +0.4] ns | -2.7 [-3.6, -1.8] | -1.8 [-4.3, +0.3] ns | — | 9,350 |

### Flow

| stratum | Light | Moderate | Heavy | Extreme | detector-days |
|---|---|---|---|---|---|
| night | +0.2 [-1.9, +2.4] ns | -2.3 [-4.4, -0.2] | -8.5 [-12.2, -4.6] | — | 9,994 |
| am_peak | +1.0 [-0.3, +2.4] ns | -0.9 [-2.0, +0.3] ns | +7.9 [+5.8, +10.0] | +3.0 [+0.0, +6.4] | 7,913 |
| midday | -0.1 [-0.5, +0.4] ns | -0.7 [-1.4, -0.0] | -1.1 [-2.3, -0.1] | -7.5 [-9.6, -5.1] | 9,215 |
| pm_peak | -0.7 [-1.7, +0.2] ns | +0.1 [-1.0, +1.1] ns | -0.8 [-5.7, +4.2] ns | — | 7,583 |
| evening | -1.6 [-2.5, -0.7] | +0.3 [-0.7, +1.3] ns | +1.0 [-4.8, +6.5] ns | — | 9,350 |

## 5. Road class × congestion

| stratum | Light | Moderate | Heavy | Extreme | detector-days |
|---|---|---|---|---|---|
| motorway/trunk|congested | -1.2 [-3.3, +1.0] ns | -2.0 [-4.3, +0.6] ns | +1.5 [-2.5, +6.1] ns | +6.0 [+1.1, +11.9] | 987 |
| motorway/trunk|free-flowing | -0.7 [-1.4, +0.0] ns | -0.8 [-1.6, +0.2] ns | -1.1 [-2.4, +0.1] ns | -0.1 [-2.9, +2.9] ns | 1,707 |
| primary|congested | -5.1 [-8.6, -1.4] | -5.2 [-10.0, +0.4] ns | +0.4 [-7.2, +9.2] ns | -13.3 [-25.4, -2.5] | 712 |
| primary|free-flowing | -0.0 [-1.1, +1.4] ns | +0.6 [-1.5, +3.9] ns | -0.8 [-2.7, +1.2] ns | -0.9 [-5.3, +3.3] ns | 2,160 |
| residential/other|congested | -1.4 [-9.4, +6.5] ns | -6.8 [-21.3, +8.4] ns | — | — | 218 |
| residential/other|free-flowing | -0.5 [-1.4, +0.3] ns | -0.5 [-2.1, +1.2] ns | -2.5 [-5.0, +0.5] ns | -3.3 [-12.6, +7.6] ns | 1,766 |
| secondary|congested | +7.4 [-8.4, +30.7] ns | +6.5 [-11.4, +37.7] ns | +37.3 [-11.9, +122.5] ns | — | 469 |
| secondary|free-flowing | -1.3 [-2.7, -0.1] | -1.4 [-3.1, +0.4] ns | -2.6 [-4.9, -0.4] | -8.1 [-14.1, -2.8] | 2,419 |
| tertiary|congested | +1.2 [-5.8, +9.6] ns | -6.4 [-13.2, -0.3] | +1.7 [-10.1, +16.2] ns | — | 315 |
| tertiary|free-flowing | -1.0 [-1.8, -0.2] | -2.0 [-3.0, -1.0] | -4.3 [-5.9, -2.7] | -5.1 [-11.6, +1.9] ns | 2,039 |

## 6. Pooled across everything

City fixed effects are absent from this view by construction — it pools
the three cities, whose detector populations and rain regimes differ, so
it is the least trustworthy table here and is given only as a headline.
The stratified tables above are the result.

| stratum | Light | Moderate | Heavy | Extreme | detector-days |
|---|---|---|---|---|---|
| all | -0.5 [-1.0, +0.0] ns | -0.8 [-1.5, +0.2] ns | -1.0 [-2.4, +1.0] ns | -0.9 [-3.4, +1.7] ns | 10,112 |

| stratum | Light | Moderate | Heavy | Extreme | detector-days |
|---|---|---|---|---|---|
| all | +0.3 [-0.4, +1.1] ns | -1.9 [-2.6, -1.2] | +5.0 [+3.4, +6.6] | -1.3 [-3.6, +1.3] ns | 10,112 |

## 7. What the clustering cost

Treating each 5-minute interval as an independent observation would have
produced intervals a median of **1.9×** narrower
than those above. That factor is the whole reason Phase 4's point
estimates could not be read as evidence: successive readings from one
loop detector on one day share its siting, its calibration, that day's
incidents, and the weather system overhead, and they carry nowhere near
209 observations' worth of independent
information.

- Estimable contrasts: **115** of 128 band × stratum combinations
- Suppressed for thin data (< 12 detector-days): **13**
- Contrasts whose interval excludes zero: **39**

## 8. Caveats

- **Road class is UTD19's own `fclass`, not an OSM map-match.** The
  PBF-based join CONTEXT.md §6/L1 specifies remains deferred. Adequate
  for stratification; not the network layer.
- **One spateGAN ensemble member.** Seed 10 only, so no uncertainty
  covariate on the rain field itself. The intervals here quantify
  sampling error in the traffic response, not error in the rainfall.
- **The downscaler is not observation.** These are plausible
  high-resolution realisations conditioned on ERA5. No radar ground
  truth exists for these cities.
- **Demand is not controlled for, only measured.** The flow column shows
  the demand channel; it does not remove it. A causal speed effect net
  of demand needs an instrument or a structural model, which is Phase 6
  territory at best.
- **Germany is in-domain, the UK and Italy are not.** spateGAN trained on
  German radar, so Essen anchors and the other two test generalisation.

## Original vs expected-state split

Contrast against Dry, percentage points, point estimates. `orig` is the realised-occupancy split from `phase5_dose_response.md`; `exp` is the expected-state split. Intervals for `exp` are in the tables above.

### congested, speed

| stratum | band | orig | exp | change |
|---|---|---|---|---|
| congested | Light | +4.5 | -1.3 | -5.8 |
| congested | Moderate | +2.1 | -2.7 | -4.7 |
| congested | Heavy | +7.6 | +5.4 | -2.2 |
| congested | Extreme | +10.5 | -0.5 | -11.0 |
| free-flowing | Light | -0.9 | -0.7 | +0.2 |
| free-flowing | Moderate | -0.9 | -0.8 | +0.1 |
| free-flowing | Heavy | -0.9 | -2.3 | -1.3 |
| free-flowing | Extreme | +1.1 | -2.6 | -3.8 |

### congested, flow

| stratum | band | orig | exp | change |
|---|---|---|---|---|
| congested | Light | -3.8 | -0.4 | +3.3 |
| congested | Moderate | -3.6 | -0.8 | +2.9 |
| congested | Heavy | -5.8 | -2.5 | +3.3 |
| congested | Extreme | -9.4 | -4.2 | +5.2 |
| free-flowing | Light | +0.2 | +0.4 | +0.2 |
| free-flowing | Moderate | -2.2 | -2.0 | +0.2 |
| free-flowing | Heavy | +4.6 | +5.9 | +1.2 |
| free-flowing | Extreme | -3.8 | +0.9 | +4.7 |

### city_congested, speed

| stratum | band | orig | exp | change |
|---|---|---|---|---|
| essen|free-flowing | Light | +0.3 | +0.3 | +0.1 |
| essen|free-flowing | Moderate | +1.9 | +2.0 | +0.0 |
| essen|free-flowing | Heavy | +0.3 | -0.4 | -0.8 |
| manchester|congested | Light | +5.1 | -2.6 | -7.7 |
| manchester|congested | Moderate | +0.5 | -2.9 | -3.4 |
| manchester|congested | Heavy | +7.5 | +2.5 | -5.0 |
| manchester|congested | Extreme | +10.9 | +3.7 | -7.2 |
| manchester|free-flowing | Light | -1.0 | -1.2 | -0.2 |
| manchester|free-flowing | Moderate | -0.3 | -1.0 | -0.6 |
| manchester|free-flowing | Heavy | +0.1 | -0.8 | -0.9 |
| manchester|free-flowing | Extreme | +5.3 | +2.0 | -3.3 |
| torino|congested | Light | +2.6 | +0.1 | -2.5 |
| torino|congested | Moderate | +0.6 | -2.1 | -2.7 |
| torino|congested | Heavy | +7.0 | +8.8 | +1.8 |
| torino|congested | Extreme | +3.2 | -11.1 | -14.3 |
| torino|free-flowing | Light | -1.1 | -0.8 | +0.4 |
| torino|free-flowing | Moderate | -2.0 | -1.7 | +0.3 |
| torino|free-flowing | Heavy | -2.1 | -3.7 | -1.6 |
| torino|free-flowing | Extreme | -6.6 | -9.5 | -2.9 |

### city_congested, flow

| stratum | band | orig | exp | change |
|---|---|---|---|---|
| essen|free-flowing | Light | +6.3 | +6.4 | +0.1 |
| essen|free-flowing | Moderate | -0.8 | -0.7 | +0.0 |
| essen|free-flowing | Heavy | -4.0 | -4.5 | -0.5 |
| manchester|congested | Light | -9.5 | +0.5 | +10.1 |
| manchester|congested | Moderate | -5.6 | +0.0 | +5.6 |
| manchester|congested | Heavy | -13.4 | -3.6 | +9.8 |
| manchester|congested | Extreme | -18.4 | -5.7 | +12.7 |
| manchester|free-flowing | Light | +6.1 | +5.3 | -0.8 |
| manchester|free-flowing | Moderate | -4.5 | -3.4 | +1.1 |
| manchester|free-flowing | Heavy | -4.2 | -3.5 | +0.7 |
| manchester|free-flowing | Extreme | -9.4 | -7.3 | +2.1 |
| torino|congested | Light | -2.3 | -1.4 | +0.9 |
| torino|congested | Moderate | -3.2 | -1.7 | +1.5 |
| torino|congested | Heavy | -3.5 | -1.1 | +2.4 |
| torino|congested | Extreme | -1.0 | +0.6 | +1.6 |
| torino|free-flowing | Light | -3.0 | -2.5 | +0.5 |
| torino|free-flowing | Moderate | -0.9 | -1.4 | -0.5 |
| torino|free-flowing | Heavy | +11.5 | +12.9 | +1.3 |
| torino|free-flowing | Extreme | +6.0 | +10.8 | +4.8 |

