# `wet_threshold_mm` Sensitivity Sweep -- 2x2 Factorial

_Generated 2026-08-27 15:01 UTC. Four dry-baseline definitions, one frozen estimator._

**Decision: HOLD** -- contrasts move materially but the Light-is-faster sign survives per-detector labelling, so the resolution explanation is not supported and changing the baseline would bury the anomaly.

## 1. What varies, and what does not

Only the definition of the dry baseline changes. `BANDS` in
`build_rain_features.py` stays at the Met Office boundaries that IUTF
uses, so the band an interval is *exposed* to is fine-resolution in
every arm; what moves is the baseline that exposure is measured
*against*. The estimator is imported unchanged from `dose_response.py`
(N_BOOT=2,000, SEED=20260827, MIN_CLUSTERS=12), and all
7 views run against one shared set of replicate draws.

| arm | rain field | resolution | threshold | dry baseline retained | wet-band leak | detector-days | runtime |
|---|---|---|---|---|---|---|---|
| **A0** *(control)* | ERA5 native | city-hour, ~31 km | 0.1 mm | 73.4% | 2.15% | 10,114 | 267s |
| **A2** | spateGAN 2 km | detector, 10 min | 0.1 mm | 74.7% | 0.0% | 10,193 | 1411s |
| **B1** | ERA5 native | city-hour, ~31 km | 0.5 mm | 88.3% | 0.0% | 10,337 | 250s |
| **B3** | spateGAN 2 km | detector, 10 min | 0.5 mm | 79.8% | 0.0% | 10,290 | 820s |

`wet-band leak` is the share of each arm's dry baseline that carries a
rain band anyway. Zero by construction in the corrected arms. For A0 it
measures what production currently admits, and it is a property of the
coarse mask rather than of the threshold: a city-wide hourly average
below 0.1 mm can still sit over a detector that was being rained on.

### Where 0.1 mm comes from, and what the standards actually say

Checked against the published scales rather than assumed:

| scale | lightest | moderate | heavy |
|---|---|---|---|
| UK Met Office (rain, not showers) | slight, **< 0.5** | 0.5-4 | > 4 |
| WMO / MANOBS | light, **< 2.5** | 2.5-7.5 | 7.6-50 |
| this project (`build_rain_features.BANDS`) | Light, **0.1**-0.5 | 0.5-4 | 4-10, >10 |

Two things follow. First, our 0.5 and 4 boundaries are the Met Office
ones, so 0.5 mm is a real meteorological boundary and not an arbitrary
loosening -- the corrected arms sit exactly on the slight/moderate line.

Second, and more importantly: **in both published scales the lightest
category has no lower bound.** 0.1 mm/h is not a rainfall class. It is a
gauge detection floor -- WMO's preferred resolution for professional
instruments, with 0.2 mm the common tipping-bucket increment and the UK
`rain day` threshold. Applying it as a wet/dry rule asks whether a rain
gauge would have registered anything, not whether it was raining enough
to matter for driving.

That is a poor fit for ERA5 specifically, which has a well-documented
*drizzle bias*: it produces precipitation too frequently and too lightly,
overestimating rates below ~1.5 mm/h and inflating wet-hour counts. A
0.1 mm rule applied to a model that drizzles too much will flag hours
that were not meaningfully wet -- which is a second, independent reason
to distrust the Light band, alongside whatever the A0-vs-A2 result is
telling us.

## 2. Harness validity

A0 re-derives production's own mask through `build_rain_mask.label`, the
same function the pipeline calls. Two things are asserted rather than
assumed, and both are fatal on failure:

- the recomputed A0 mask matches the stored `is_dry_clean` row for row;
- A0's re-estimated L3a matches the published Phase 5 figures.

| figure | published Phase 5 | A0 re-run | |
|---|---|---|---|
| detector-days | 10,114 | 10,114 | match |
| estimable contrasts | 121 | 121 | match |
| intervals excluding zero | 44 | 44 | match |
| suppressed (thin) | 7 | 7 | match |
| median design effect | 1.8488x | 1.8488x | match |

## 3. What each arm costs the baseline

`dry baseline retained` is the share of all curated intervals that
survive into L2a. The Phase-3 concern was that 0.1 mm on a 31 km
area-mean removes 22-36% of the data for no measured gain, so the
question is whether a per-detector label buys that back without
readmitting genuinely wet intervals.

| city | A0 dry % | A2 dry % | B1 dry % | B3 dry % |
|---|---|---|---|---|
| essen | 70.5% | 69.2% | 88.1% | 77.8% |
| manchester | 63.9% | 68.3% | 84.5% | 79.8% |
| torino | 77.8% | 78.3% | 89.9% | 80.1% |

Profile cells clearing `min_obs_cell`, which is the coverage that
actually limits the delay metric -- Essen was the weak city at 40.9%:

| city | A0 cells ok | A2 cells ok | B1 cells ok | B3 cells ok |
|---|---|---|---|---|
| essen | 40.9% | 37.1% | 73.8% | 56.7% |
| manchester | 66.4% | 63.5% | 82.0% | 78.8% |
| torino | 81.1% | 79.6% | 90.4% | 82.6% |

Median free-flow speed (km/h) -- does the baseline *level* move, or
only its sample size:

| city | A0 | A2 | B1 | B3 |
|---|---|---|---|---|
| essen | 58.8 | 58.9 | 58.7 | 58.9 |
| manchester | 41.5 | 43.0 | 43.0 | 43.0 |
| torino | 54.0 | 54.0 | 54.0 | 54.0 |

## 4. The Light-is-faster question

Phase 3 found Light rain reading as **faster** than dry (+1.8 km/h
Manchester, +2.8 Torino) and called it confounding rather than physics:
a 31 km area mean flags hours whose drizzle fell nowhere near a
detector, so the label mostly tracks whatever else correlates with
drizzly hours. That explanation makes a falsifiable prediction --
**per-detector labelling should shrink or kill the positive sign.**

Light-vs-Dry contrasts on `typical_speed_deviation`, percentage points:

| view | level | A0 | A2 | B1 | B3 |
|---|---|---|---|---|---|
| city | essen | +0.4 ns | +0.2 ns | -- | -- |
| city | manchester | -1.6 | +1.3 ns | -- | -- |
| city | torino | -0.7 ns | +0.0 ns | -- | -- |
| congested | congested | +4.6 | +9.1 | -- | -- |
| congested | free-flowing | -0.9 | +0.2 ns | -- | -- |
| pooled | all | -0.8 | +0.5 ns | -- | -- |

`ns` marks an interval that includes zero.

- **A0**: 2 of 6 Light contrasts positive.
- **A2**: 6 of 6 Light contrasts positive.
- **B1**: 0 of 0 Light contrasts positive.
- **B3**: 0 of 0 Light contrasts positive.

### Why the 0.5 mm arms report no Light row

The first version of this sweep had a flaw here, and the fix is why the
0.5 mm arms were re-run. Light is 0.1-0.5 mm, so an arm thresholded at
0.5 files Light rain as dry -- and the old arms then compared Light
against a baseline that contained Light rain. The difference shrank
toward zero by construction, and that was mistaken for the anomaly
easing.

`arm_bands()` now enforces that an arm's dry reference is disjoint from
the bands it contrasts: sub-threshold bands are folded into `Dry`, and
baseline intervals must be `Dry` under that folded vocabulary. A 0.5 mm
arm therefore has no Light category to report, which is honest rather
than missing -- at that threshold, light rain *is* the reference.

So the Light question rests on **A0 vs A2**: same 0.1 mm threshold,
resolution the only difference. That is the fair comparison, and it runs
against the hypothesis -- sharpening the label to the detector's own
2 km cell makes drizzle look faster, not less so.

## 5. How far the answers actually move

Each contrast is differenced against A0 and divided by A0's own CI
half-width. That is the number the decision turns on: a 2 pp shift is
enormous against a +-1 pp interval and invisible against a +-15 pp one.

| arm | contrasts compared | median move | p90 | max | sign flips (both significant) |
|---|---|---|---|---|---|
| A2 | 240 | 0.68x | 2.50x | 7.86x | 4 |
| B1 | 178 | 0.34x | 1.18x | 3.59x | 0 |
| B3 | 178 | 0.39x | 1.45x | 2.66x | 0 |

Movement restricted to `road_congested`, the view carrying Phase 5's
headline reversal:

| arm | median move | max | sign flips |
|---|---|---|---|
| A2 | 0.46x | 5.75x | 0 |
| B1 | 0.30x | 1.60x | 0 |
| B3 | 0.35x | 2.04x | 0 |

## 6. Decision

The rule below was fixed in `verdict()` before any arm had run.

- **immaterial** -- typical movement under one CI half-width and no
  significant sign flips. Keep 0.1 mm for continuity.
- **switch** -- per-detector labelling removes the Light-is-faster sign.
- **hold** -- material movement but Light stays positive; something is
  unexplained and changing the baseline would bury it.

**Measured: HOLD.** Contrasts move materially but the light-is-faster sign survives per-detector labelling, so the resolution explanation is not supported and changing the baseline would bury the anomaly.

- largest per-arm median movement: 0.682x the control half-width
- significant sign flips: yes
- positive Light contrasts: 2/6 in the control, A2 6/6

**Gate verdict: PASS**

The gate is on the harness, not the answer: A0 must reproduce
production exactly, every arm must conserve the row count, and every
arm must produce an estimable table. Which way the decision lands is
a finding, not a pass condition.
