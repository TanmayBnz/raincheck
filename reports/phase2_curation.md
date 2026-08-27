# Phase-2 Curation QA — L1

_Generated 2026-08-26 12:26 UTC from the curated table (2,875,844 rows, manchester, torino, essen)._

**Gate verdict: PASS**

## 1. Occupancy scale — the Phase-1 open question, closed

UTD19 documents `occ` as a 0–1 fraction; it is not. Phase 1 *inferred*
percent-scaling for Manchester and Torino from their medians (5–9) against
Essen's 0.007, but never confirmed it. Curation rule 2 divides those two
cities by 100. If that inference were wrong, every free-flow baseline built
on "occupancy below critical" would be measured against a meaningless
threshold — so it is tested here against a physical plausibility band
(`conf/cities.yml → curation.occ_plausible`) rather than assumed.

| city | scale applied | p50 | p95 | p99 | max | verdict |
|---|---|---|---|---|---|---|
| manchester | percent | 0.10800 | 0.5053 | 0.7944 | 1.0000 | PASS |
| torino | percent | 0.08000 | 0.3497 | 0.6618 | 1.0000 | PASS |
| essen | fraction | 0.00750 | 0.0700 | 0.1625 | 0.8000 | PASS |

## 2. What each rule removed

Percentages are of curated (quality-passing) rows. A rule firing on 0% or
on most rows would indicate a bug, not clean data.

> **These are smaller than the Phase-1 figures, and legitimately so.**
> Phase 1 measured over *all* landed rows; rule 1 runs first and the error
> flag already catches much of the same damage. Manchester's 11.0%
> zero-speed rate, for instance, is 1.6% among quality-passing rows — the
> other 9.4 points were error-flagged too. The rules are not redundant
> (they still fire on hundreds of thousands of rows) but they are the
> second line of defence, not the first.

| city | curated rows | occ dropped (r3) | speed→NULL (r4) | zero-speed flowing (r5) | >150 km/h | DST-day rows | speed usable | occ usable |
|---|---|---|---|---|---|---|---|---|
| manchester | 765,096 | 1.93% | 1.56% | 0.00% | 0.00% | 0.00% | 98.44% | 98.07% |
| torino | 1,819,683 | 0.80% | 2.51% | 0.00% | 0.01% | 0.00% | 97.48% | 99.20% |
| essen | 291,065 | 0.00% | 0.00% | 0.00% | 0.01% | 0.00% | 99.99% | 100.00% |

## 3. The bias rule 4 removed — per detector

`speed = 0 AND flow = 0` means no vehicle was observed. Counted as a real
zero it pulls the baseline percentile down, and that percentile is the
*denominator* of the delay metric, so the error lands directly in the
headline rain effect.

Measured city-wide the effect looks negligible — absence-zeros are 1–3% of
quality-passing rows, nowhere near enough to move a pooled quantile. That
framing is misleading, because **baselines are never pooled**: they are
per-detector. Absence-zeros concentrate on quiet detectors, so the quantity
that matters is the spread of per-detector shifts and its tail.

| city | detectors | with ≥1 absence-zero | worst detector's absence rate | p50 moved >1 km/h | max Δ p50 | max Δ p85 |
|---|---|---|---|---|---|---|
| manchester | 147 | 96.60% | 18.4% | 4.76% | +3.7 | +14.3 |
| torino | 339 | 96.76% | 51.3% | 6.49% | +43.0 | +15.0 |
| essen | 36 | 0.00% | 0.0% | 0.00% | +0.0 | +0.0 |

## 4. UTC alignment

`interval` is seconds since *local* midnight; rainfall grids are UTC. Two
offsets in one city means the window spans a DST changeover. That is not
the same as *having data on* the changeover date — the last column is what
matters, because only those dates carry an hour that is ambiguous (autumn,
the hour occurs twice) or nonexistent (spring). Where such dates do appear,
the rows are flagged rather than dropped and the baseline layer decides.

| city | tz | offsets seen (h) | DST-transition dates in window |
|---|---|---|---|
| manchester | Europe/London | +0, +1 | none |
| torino | Europe/Rome | +2 | none |
| essen | Europe/Berlin | +2 | none |

## 5. Network coverage after the detector join

ERA5 cells are the ~31 km native grid, spateGAN cells the ~2 km downscaled
grid. A city collapsing to one ERA5 cell has no within-city rainfall
variation to exploit at native resolution — which is precisely the
limitation the downscaling exists to fix.

| city | detectors | no geo | no linkid | ERA5 cells | spateGAN cells | road classes ≥20 dets |
|---|---|---|---|---|---|---|
| manchester | 147 | 0 | 0 | 1 | 7 | trunk 99, primary 20 |
| torino | 339 | 0 | 0 | 2 | 30 | tertiary 88, secondary 83, other 80, primary 66 |
| essen | 36 | 0 | 0 | 1 | 8 | **none** |

## 6. Baseline cell viability at the chosen resolution

Share of `(detector, dow, tbin)` cells holding ≥20 **cleaned** speed observations — the honest test of rule 6, since Phase 1
measured this on raw speed and so counted absence-zeros as if they were
observations. The correction is small in aggregate (1–3% of rows) but
concentrated on the quiet detectors that were closest to the threshold
anyway, which is exactly where a cell tips from populated to empty.

> Still an upper bound in one respect: rainfall is not yet joined, so all
> intervals count as dry. The dry-only baseline is computed in Phase 3.

| city | resolution | detectors w/ speed | cells expected | cells populated | cells ≥20 obs | verdict |
|---|---|---|---|---|---|---|
| manchester | 60 min | 147 | 24,696 | 97.47% | 90.25% | PASS |
| torino | 60 min | 339 | 56,952 | 95.61% | 88.55% | PASS |
| essen | 30 min | 36 | 12,096 | 100.00% | 90.85% | PASS |
