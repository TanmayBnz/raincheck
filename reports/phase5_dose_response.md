# Phase-5 Dose-Response — L3a

_Generated 2026-08-27 09:23 UTC. 2,094,391 intervals in 10,114 detector-days across 3 cities. All figures are percentage points of deviation from each detector's own dry typical profile, contrasted against Dry within the same stratum. Brackets are 95% cluster bootstrap intervals (2,000 replicates, resampling detector-days); `ns` marks an interval containing zero._

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
| congested | +4.5 [+3.3, +5.7] | +2.1 [+0.4, +4.1] | +7.6 [+3.5, +13.7] | +10.5 [+6.3, +14.7] | 7,606 |
| free-flowing | -0.9 [-1.7, -0.2] | -0.9 [-1.8, +0.3] ns | -0.9 [-2.4, +1.2] ns | +1.1 [-1.4, +3.9] ns | 10,091 |

Flow, same cells:

| stratum | Light | Moderate | Heavy | Extreme | detector-days |
|---|---|---|---|---|---|
| congested | -3.8 [-6.1, -1.2] | -3.6 [-5.6, -1.6] | -5.8 [-8.6, -2.8] | -9.4 [-13.8, -4.7] | 7,606 |
| free-flowing | +0.2 [-0.5, +1.0] ns | -2.2 [-2.9, -1.3] | +4.6 [+2.9, +6.3] | -3.8 [-6.6, -0.8] | 10,091 |

### Speed deviation, by city and road state

| stratum | Light | Moderate | Heavy | Extreme | detector-days |
|---|---|---|---|---|---|
| essen|congested | +0.5 [-10.3, +10.2] ns | -3.9 [-13.2, +5.3] ns | — | — | 370 |
| essen|free-flowing | +0.3 [-1.3, +2.9] ns | +1.9 [-0.7, +6.9] ns | +0.3 [-1.2, +1.8] ns | — | 1,260 |
| manchester|congested | +5.1 [+3.4, +6.8] | +0.5 [-1.6, +2.7] ns | +7.5 [+3.7, +11.7] | +10.9 [+6.1, +16.2] | 1,891 |
| manchester|free-flowing | -1.0 [-1.8, -0.2] | -0.3 [-1.0, +0.4] ns | +0.1 [-1.4, +1.5] ns | +5.3 [+2.3, +9.0] | 2,387 |
| torino|congested | +2.6 [+1.0, +4.2] | +0.6 [-1.9, +3.7] ns | +7.0 [+1.2, +16.0] | +3.2 [-2.9, +9.0] ns | 5,345 |
| torino|free-flowing | -1.1 [-2.2, -0.1] | -2.0 [-3.5, -0.3] | -2.1 [-4.5, +1.6] ns | -6.6 [-9.9, -2.8] | 6,444 |

### Flow deviation, same cells

| stratum | Light | Moderate | Heavy | Extreme | detector-days |
|---|---|---|---|---|---|
| essen|congested | +13.8 [-5.1, +36.6] ns | -6.1 [-18.1, +4.9] ns | — | — | 370 |
| essen|free-flowing | +6.3 [+2.2, +10.3] | -0.8 [-3.3, +2.2] ns | -4.0 [-9.8, +1.4] ns | — | 1,260 |
| manchester|congested | -9.5 [-11.7, -7.3] | -5.6 [-8.5, -2.8] | -13.4 [-16.3, -10.5] | -18.4 [-21.6, -15.1] | 1,891 |
| manchester|free-flowing | +6.1 [+4.6, +7.7] | -4.5 [-5.5, -3.4] | -4.2 [-6.1, -2.2] | -9.4 [-11.6, -7.2] | 2,387 |
| torino|congested | -2.3 [-5.7, +1.6] ns | -3.2 [-5.9, -0.3] | -3.5 [-7.5, +0.5] ns | -1.0 [-9.7, +8.5] ns | 5,345 |
| torino|free-flowing | -3.0 [-3.8, -2.3] | -0.9 [-2.1, +0.4] ns | +11.5 [+9.0, +14.1] | +6.0 [+0.6, +12.2] | 6,444 |

Where flow falls and speed rises, the road got emptier, not faster.

## 3. By road class

| stratum | Light | Moderate | Heavy | Extreme | detector-days |
|---|---|---|---|---|---|
| motorway/trunk | -1.1 [-1.8, -0.4] | -1.5 [-2.1, -0.7] | -1.1 [-2.2, +0.2] ns | +3.2 [+0.2, +6.7] | 1,707 |
| primary | -0.2 [-1.3, +1.1] ns | +0.0 [-1.9, +3.2] ns | -0.5 [-2.4, +1.3] ns | -2.7 [-6.6, +1.1] ns | 2,167 |
| secondary | -0.8 [-2.4, +1.0] ns | -0.9 [-3.1, +2.0] ns | +2.5 [-3.2, +12.9] ns | -7.0 [-12.5, -1.6] | 2,429 |
| tertiary | -1.0 [-2.2, +0.1] ns | -2.8 [-4.0, -1.6] | -4.3 [-6.1, -2.6] | -4.5 [-10.8, +1.3] ns | 2,039 |
| residential/other | -0.9 [-1.8, +0.1] ns | -0.7 [-2.3, +1.1] ns | -2.6 [-5.0, +0.2] ns | -2.1 [-10.4, +7.7] ns | 1,770 |

### Flow

| stratum | Light | Moderate | Heavy | Extreme | detector-days |
|---|---|---|---|---|---|
| motorway/trunk | +4.2 [+3.0, +5.3] | -2.8 [-3.7, -2.0] | -2.6 [-4.3, -0.9] | -7.5 [-9.3, -5.6] | 1,707 |
| primary | +2.1 [+0.3, +4.0] | -0.0 [-2.1, +2.1] ns | +3.1 [-0.2, +6.7] ns | -3.6 [-7.7, +1.1] ns | 2,167 |
| secondary | -0.1 [-1.8, +1.6] ns | -0.7 [-2.4, +0.9] ns | +4.5 [+0.9, +8.2] | +6.4 [-0.5, +13.3] ns | 2,429 |
| tertiary | -2.4 [-4.4, -0.3] | -3.8 [-6.1, -1.5] | +10.6 [+6.2, +14.7] | -0.9 [-6.6, +5.1] ns | 2,039 |
| residential/other | -2.1 [-3.7, -0.4] | -1.3 [-3.3, +0.8] ns | +10.7 [+5.9, +16.1] | +13.4 [+3.6, +24.9] | 1,770 |

## 4. By time of day

| stratum | Light | Moderate | Heavy | Extreme | detector-days |
|---|---|---|---|---|---|
| night | +0.4 [-0.5, +1.5] ns | +2.9 [+1.3, +5.0] | -2.7 [-4.5, -0.9] | — | 9,994 |
| am_peak | -1.6 [-3.0, +0.3] ns | -2.4 [-4.0, -0.3] | -1.6 [-3.6, +1.0] ns | -3.8 [-6.8, -0.7] | 7,909 |
| midday | -0.3 [-1.2, +0.7] ns | -1.0 [-2.2, +0.6] ns | -0.6 [-2.9, +3.2] ns | +4.0 [+1.1, +7.6] | 9,216 |
| pm_peak | +1.9 [-0.5, +5.0] ns | -0.4 [-2.3, +1.8] ns | +3.0 [-6.5, +14.6] ns | — | 7,559 |
| evening | -0.7 [-1.4, +0.2] ns | -3.1 [-4.0, -2.1] | -2.5 [-4.9, -0.3] | — | 9,347 |

### Flow

| stratum | Light | Moderate | Heavy | Extreme | detector-days |
|---|---|---|---|---|---|
| night | -0.8 [-2.9, +1.3] ns | -1.5 [-3.8, +0.8] ns | -8.5 [-12.5, -4.3] | — | 9,994 |
| am_peak | +1.6 [+0.1, +3.0] | -0.9 [-2.0, +0.4] ns | +7.9 [+5.8, +10.0] | +3.1 [-0.1, +6.4] ns | 7,909 |
| midday | +0.2 [-0.5, +0.8] ns | -0.4 [-1.4, +0.9] ns | -1.0 [-2.1, +0.1] ns | -7.2 [-9.2, -4.8] | 9,216 |
| pm_peak | +2.1 [-1.6, +8.4] ns | +3.7 [-0.8, +12.0] ns | -0.8 [-5.9, +4.3] ns | — | 7,559 |
| evening | -1.6 [-2.5, -0.7] | +0.3 [-0.7, +1.3] ns | +1.1 [-4.7, +6.9] ns | — | 9,347 |

## 5. Road class × congestion

| stratum | Light | Moderate | Heavy | Extreme | detector-days |
|---|---|---|---|---|---|
| motorway/trunk|congested | +4.2 [+2.6, +5.9] | +0.3 [-1.7, +2.4] ns | +4.0 [+0.3, +8.0] | +8.1 [+2.5, +14.2] | 1,385 |
| motorway/trunk|free-flowing | -0.3 [-1.1, +0.7] ns | -0.1 [-0.8, +0.6] ns | -0.1 [-1.4, +1.4] ns | +5.8 [+2.2, +9.7] | 1,707 |
| primary|congested | +2.9 [+0.6, +5.3] | -2.9 [-6.4, +1.4] ns | +5.2 [-0.5, +11.8] ns | +1.7 [-5.3, +9.3] ns | 1,467 |
| primary|free-flowing | -0.0 [-1.1, +1.4] ns | +0.4 [-1.6, +3.7] ns | -0.2 [-2.1, +1.7] ns | -0.7 [-4.3, +3.1] ns | 2,165 |
| residential/other|congested | -2.0 [-5.1, +1.3] ns | -1.4 [-4.8, +2.2] ns | -1.1 [-6.3, +4.1] ns | -0.5 [-11.9, +10.1] ns | 1,490 |
| residential/other|free-flowing | -1.2 [-2.2, -0.2] | -1.0 [-2.5, +0.9] ns | -2.6 [-5.0, +0.2] ns | +0.7 [-8.5, +11.8] ns | 1,763 |
| secondary|congested | -0.4 [-3.8, +2.7] ns | +3.0 [-2.7, +11.6] ns | +13.4 [-3.0, +41.9] ns | +4.8 [-7.0, +16.6] ns | 1,683 |
| secondary|free-flowing | -1.2 [-3.2, +0.8] ns | -0.8 [-3.6, +2.7] ns | +3.1 [-3.0, +14.5] ns | -5.7 [-10.0, -1.3] | 2,417 |
| tertiary|congested | +0.8 [-2.5, +3.9] ns | -2.6 [-6.3, +0.9] ns | +6.1 [-0.2, +12.4] ns | +10.0 [-2.9, +22.5] ns | 1,581 |
| tertiary|free-flowing | -1.7 [-4.0, +0.1] ns | -3.3 [-5.7, -1.5] | -4.7 [-7.4, -2.4] | -2.8 [-8.2, +2.6] ns | 2,039 |

## 6. Pooled across everything

City fixed effects are absent from this view by construction — it pools
the three cities, whose detector populations and rain regimes differ, so
it is the least trustworthy table here and is given only as a headline.
The stratified tables above are the result.

| stratum | Light | Moderate | Heavy | Extreme | detector-days |
|---|---|---|---|---|---|
| all | -0.8 [-1.3, -0.2] | -1.1 [-1.8, -0.1] | -1.2 [-2.5, +0.9] ns | -0.7 [-3.1, +1.7] ns | 10,112 |

| stratum | Light | Moderate | Heavy | Extreme | detector-days |
|---|---|---|---|---|---|
| all | +0.3 [-0.5, +1.1] ns | -1.9 [-2.7, -1.1] | +4.7 [+3.0, +6.3] | -1.5 [-3.8, +1.1] ns | 10,112 |

## 7. What the clustering cost

Treating each 5-minute interval as an independent observation would have
produced intervals a median of **1.8×** narrower
than those above. That factor is the whole reason Phase 4's point
estimates could not be read as evidence: successive readings from one
loop detector on one day share its siting, its calibration, that day's
incidents, and the weather system overhead, and they carry nowhere near
207 observations' worth of independent
information.

- Estimable contrasts: **121** of 128 band × stratum combinations
- Suppressed for thin data (< 12 detector-days): **7**
- Contrasts whose interval excludes zero: **44**

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

