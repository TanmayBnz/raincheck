# Phase-3 Baseline QA — L2a

_Generated 2026-08-27 09:28 UTC. Free-flow speed, dry-only typical profiles, and delay metrics for manchester, torino, essen._

**Gate verdict: PASS**

## 1. What the dry filter kept

Rain labels come from native-resolution ERA5 at city-hour granularity
(`lake/era5/curated/rain_hourly`). "Post-rain buffer" is the 2-hour window after rain stops: not raining, but the
surface is still wet, so it is excluded from the baseline too. Intervals
with no rain label at all are excluded rather than assumed dry.

| city | intervals | wet | post-rain buffer | unlabelled | **dry, usable** | rain events touched |
|---|---|---|---|---|---|---|
| manchester | 765,096 | 29.5% | 6.6% | 0.0% | **63.9%** | 28 |
| torino | 1,819,683 | 19.5% | 2.7% | 0.0% | **77.8%** | 10 |
| essen | 291,065 | 24.0% | 5.4% | 0.0% | **70.5%** | 35 |

## 2. Does the dry-only rule actually buy anything here?

CONTEXT.md §L2a calls dry-only *the single most important methodological
decision*: admit rainy intervals and the baseline absorbs the very effect
being measured. That reasoning is sound, but it assumes the wet/dry label
is meaningful. At this rain resolution it largely is not, and the evidence
is worth stating plainly.

First, wet versus dry paired within each `(detector, dow, tbin)` cell — so
detector, weekday and hour are held fixed:

| city | paired cells | dry − wet, mean (km/h) | median | cells where dry is faster |
|---|---|---|---|---|
| manchester | 11,588 | -0.31 | -0.07 | 48.8% |
| torino | 23,702 | +0.71 | +0.65 | 59.2% |
| essen | 3,966 | -0.47 | -0.58 | 39.3% |

Near zero, and the sign is not even consistent across cities. Taken alone
that would suggest either a broken rain join or no rain effect. It is
neither — splitting by intensity band shows why:

| city | dry mean (km/h) | Light | Moderate | Heavy | Extreme | **Moderate+** |
|---|---|---|---|---|---|---|
| manchester | 34.45 | 36.3 (+1.8) | 30.9 (-3.5) | — | — | **30.9 (-3.5)** |
| torino | 44.97 | 47.7 (+2.8) | 41.3 (-3.6) | — | — | **41.3 (-3.6)** |
| essen | 53.54 | 53.1 (-0.4) | 53.5 (-0.0) | 51.0 (-2.6) | — | **53.5 (-0.1)** |

**Light rain reads as *faster* than dry; Moderate+ reads as slower.** Since
Light is ~70% of all wet intervals, pooling them cancels the effect out.

The Light-is-faster result is confounding, not physics. A 31 km cell-hour is
flagged wet when its *area-mean* reaches 0.1 mm — which includes hours that
were mostly dry, and hours whose drizzle fell nowhere near a detector. What
that label mostly tracks is whatever else correlates with drizzly hours
(time of day, season, traffic volume), which is exactly the confounding
CONTEXT.md §9 flags.

Two consequences worth carrying forward:

1. **The dry-only rule is retained**, because over-exclusion costs sample
   size but not validity, and the Moderate+ signal is real and correctly
   signed. But its measured benefit at native ERA5 resolution is ~0 km/h —
   it is currently insurance, not a correction.
2. **`wet_threshold_mm: 0.1` is doing real damage to sample size for no
   measured gain** — it removes 20–30% of every city's data, and the
   removed set is biased toward whatever drizzle correlates with. This
   should be revisited in Phase 4 against the 2 km fields, where "was it
   raining *at this detector*" finally becomes answerable.

## 3. Free-flow speed

Free-flow is the 85th percentile of dry speed **conditioned on occupancy
below critical**, where critical occupancy is read off each detector's own
fundamental diagram (the occupancy bin with the highest median flow).

The last-but-one column is the load-bearing check: free-flow speed must sit
meaningfully **above** the median speed of the same uncongested intervals.
If the two coincide, the percentile is describing an off-peak average rather
than the link's free-flow capability, and the delay metric loses its
physical meaning.

| city | detectors | with free-flow | crit-occ from own FD | median crit occ | median free-flow | range | vs uncongested median | median obs/detector |
|---|---|---|---|---|---|---|---|---|
| manchester | 147 | 100.0% | 60.5% | 0.260 | 41.5 | 24–80 | +6.5 | 2,671 |
| torino | 339 | 99.4% | 82.3% | 0.220 | 54.0 | 18–122 | +8.0 | 4,044 |
| essen | 36 | 100.0% | 16.7% | 0.100 | 58.8 | 34–99 | +6.8 | 5,663 |

## 4. Typical speed profiles

Median dry speed per `(detector, dow, tbin)`, cells needing ≥20 observations. Coverage is lower than the Phase-2
figure by construction — that measured all intervals, this one only dry ones.

| city | resolution | detectors | cells expected | cells built | cells passing min-obs | median obs/cell |
|---|---|---|---|---|---|---|
| manchester | 60 min | 147 | 24,696 | 86.7% | 57.6% | 24 |
| torino | 60 min | 339 | 56,952 | 94.2% | 76.5% | 24 |
| essen | 30 min | 36 | 12,096 | 100.0% | 40.9% | 15 |

## 5. Delay metrics

`free_flow_delay_ratio` = 1 − speed / free-flow speed: congestion against
the link's physical capability, irrespective of cause. Positive means
slower than free-flow, which most intervals should be.

`typical_speed_deviation` = speed / typical speed − 1: anomaly against what
this detector normally does in this hour of this weekday. It is the
prediction target, and it should centre near zero — the recurring commute
pattern is already in the baseline, so what remains is the unusual part.

| city | delay computable | free-flow delay p05/p50/p95 | deviation computable | deviation p05/p50/p95 | intervals above crit occ |
|---|---|---|---|---|---|
| manchester | 98.4% | -0.18 / +0.19 / +0.68 | 63.5% | -0.29 / +0.00 / +0.39 | 17.5% |
| torino | 97.2% | -0.13 / +0.16 / +0.58 | 83.1% | -0.34 / +0.00 / +0.33 | 10.3% |
| essen | 100.0% | -0.08 / +0.09 / +0.30 | 42.3% | -0.15 / +0.00 / +0.17 | 1.3% |

## 6. What this layer does not yet do

- Rain is attributed at **city-hour** granularity from ~31 km ERA5. That is
  adequate for *excluding* contaminated intervals (over-exclusion costs
  sample size, not validity) but not for dose-response, which needs
  intensity right at the detector. Phase 4.
- The two-channel decomposition is not estimated here. The `congested`
  flag it needs is computed and stored per interval; the estimation is
  Phase 5.
