# Phase-1 Gate Decision

_Evidence: `phase1_audit.md` (full 134,380,371-row landed Parquet) and
`diagnose_units.py`. Rain criterion still open — see §5._

---

## 1. Headline

Of 39 cities with measurements, **three** survive as usable for a speed-based
rain-delay study: **Manchester**, **Torino** and **Essen**. The NL cohort
(Rotterdam + Groningen) and Bolton fail outright.

CONTEXT.md §8's Tier 1 was Torino, London, Toronto, Manchester. **Only Torino and
Manchester survive** — London and Toronto carry no speed data at all.

---

## 2. What the audit overturned

### 2a. Distinct days are far fewer than the calendar span

The single most consequential correction. Pre-landing reconnaissance used
`max(date) - min(date)` as the window; the true count of **distinct days with
data** is much smaller, because several cities sample scattered days across a
long span.

| city | span (days) | **distinct days** | weekday recurrences |
|---|---|---|---|
| essen | 188 | **35** | 5.0 |
| manchester | 72 | **28** | 4.0 |
| torino | 21 | **21** | 3.0 |
| birmingham | 27 | 12 | 1.7 |
| constance | 7 | 7 | 1.0 |
| bolton | 6 | 6 | 0.86 |
| **rotterdam** | 43 | **6** | 0.86 |
| **groningen** | 16 | **2** | 0.29 |

Rotterdam has **6 days of data spread over 43 calendar days**, not 42 days.
Groningen has 2. Both are unusable for any day-of-week profile, at any
resolution. The whole NL cohort dies here.

### 2b. Essen is the reverse surprise

Planning flagged Essen as likely to fail on ~14% density. That figure was wrong:
it divided by 188 calendar days. Measured against the 35 days it actually
covers, Essen is **94% dense** — and it is by a wide margin the cleanest city in
the dataset:

- 99.7% of rows pass the quality flag (next best: Torino 87.5%)
- **0.00%** zero-speed readings (Manchester 11.0%, Bolton 27.5%)
- **0.00%** out-of-range occupancy (every other candidate is 68–86% out of range)
- 35 days spread across March–September, giving seasonal rain variety
- German, so **in-domain for spateGAN**

### 2c. Occupancy is corrupt or mis-scaled everywhere except Essen

UTD19 documents `occ` as a 0–1 fraction. It is not.

| city | p50 | p99 | max | rows > 1 | rows > 100 | detectors ever > 1 |
|---|---|---|---|---|---|---|
| manchester | 8.25 | 123.1 | 2094 | 80.7% | 1.5% | 170 / 181 |
| torino | 7.35 | **inf** | **inf** | 85.8% | 3.0% | **399 / 399** |
| rotterdam | 5.53 | **inf** | **inf** | 75.5% | 10.8% | 251 / 259 |
| bolton | 8.73 | 73.0 | 1164 | 67.6% | 0.8% | 130 / 160 |
| groningen | 3.88 | 27.9 | **inf** | 70.5% | 0.1% | 53 / 55 |
| **essen** | **0.007** | **0.17** | **0.80** | **0.00%** | **0.00%** | **0 / 36** |

Two distinct defects:

1. **Scale.** Medians of 5–9 with Essen at 0.007 indicate these cities report
   occupancy as a **percentage**, not a fraction. A median of ~8% is physically
   plausible for urban arterials, so the data is real — just differently scaled.
2. **Corruption.** Torino and Rotterdam contain literal `inf` values, and 1.5–10.8%
   of rows exceed 100 on a scale whose ceiling is 100. These are unrecoverable
   and must be dropped, not rescaled.

This is not fatal, but it is load-bearing: **L2a defines free-flow speed as a
high percentile of speed conditioned on occupancy below critical**. That
definition is meaningless until occupancy is normalized per city and the corrupt
tail removed. It also gates the two-channel (speed-reduction vs capacity-reduction)
decomposition, which is one of the project's claimed novel contributions.

### 2d. Zero speeds need nulling, not filtering

| city | speed rows | zero | zero & flow=0 | zero & flow>0 | p01 (excl. zeros) |
|---|---|---|---|---|---|
| manchester | 1,142,893 | 11.0% | 10.6% | 0.4% | 4.0 |
| bolton | 246,842 | 27.5% | 23.4% | 4.1% | 4.0 |
| rotterdam | 351,257 | 22.3% | 11.5% | **10.8%** | 17.6 |
| groningen | 28,378 | 15.8% | 15.7% | 0.1% | 15.7 |
| torino | 2,080,768 | 6.1% | 4.0% | 2.0% | 7.0 |
| **essen** | 291,892 | **0.00%** | 0.00% | 0.00% | 21.2 |

`speed = 0 AND flow = 0` means *no vehicle was observed*, not *traffic was
stopped*. Treating it as a real zero drags any median or 85th-percentile
baseline downward — directly biasing the delay denominator. These must become
NULL in curation.

`speed = 0 AND flow > 0` is different: vehicles counted but zero speed recorded.
At 10.8% of Rotterdam's rows this looks like a sensor fault; at 0.4% for
Manchester it is plausibly genuine standstill.

### 2e. Quality-flag encoding confirmed inconsistent

As suspected, and worse than a two-way split. Three encodings coexist:

- `NULL` / `1` — most cities, including **manchester** (66.9% NULL), **torino** (87.5%)
- `0` / `1` — **essen** (99.7% zero), zurich, london, bremen, bordeaux
- `NULL` only — birmingham, strasbourg (100% NULL; flag never populated)

`WHERE error = 0` would discard **100%** of Manchester and Torino. The
`quality_ok` column (`error IS NULL OR error != '1'`) handles all three.

Bolton is separately disqualified here: only **20.9%** of its rows survive the
flag.

---

## 3. Gate scorecard

| criterion | threshold | manchester | torino | essen | rotterdam | bolton | groningen |
|---|---|---|---|---|---|---|---|
| speed non-null | ≥ 90% | ✅ 100% | ✅ 100% | ✅ 100% | ✅ 100% | ✅ 100% | ✅ 100% |
| occupancy present | ≥ 90% | ✅ 99.6% | ✅ 100% | ✅ 99.9% | ✅ 100% | ✅ 95.9% | ✅ 100% |
| occupancy *usable* | scale known | ⚠️ rescale | ⚠️ rescale+inf | ✅ clean | ⚠️ 10.8% bad | ⚠️ rescale | ⚠️ rescale |
| distinct days (quality-passing) | ≥ 28 | ⚠️ 21 | ⚠️ 21 | ✅ 35 | ❌ 6 | ❌ 4 | ❌ 2 |
| detectors | ≥ 50 | ✅ 181 | ✅ 399 | ❌ 36 | ✅ 259 | ✅ 166 | ✅ 55 |
| profile resolution | ≤ 60 min | ✅ 60 min | ✅ 60 min | ✅ **30 min** | ❌ none | ❌ none | ❌ none |
| quality retention | — | 66.9% | 87.5% | ✅ 99.7% | 71.8% | ❌ 20.9% | 72.5% |
| road classes ≥ 20 dets | ≥ 2 | ✅ trunk 104, primary 33 | ✅ 5 classes | ❌ max 18 | ✅ | ✅ | ❌ |
| rain events (on data days) | ≥ 15 | ✅ 28 | ❌ **10** | ✅ 35 | ❌ 6 | ❌ 2 | ❌ 4 |
| Moderate+ hours | ≥ 3 | ✅ 46 | ✅ 40 | ✅ 60 | ❌ 1 | ❌ 1 | ❌ 2 |

---

## 4. Decision

**Study cities: Manchester + Torino. Validation anchor: Essen.**

- **Manchester** — primary analysis city. 181 detectors, **21 usable days**
  (28 present, 7 entirely error-flagged), hourly baseline, strong road-class
  split (trunk 104 / primary 33), 28 rain events.
- **Torino** — second analysis city. 399 detectors and the best road-class
  spread in the dataset (5 classes ≥ 20 detectors), hourly baseline. Weakest on
  window (21 days, 3 weekday recurrences) and needs `inf` occupancy purged.
- **Essen** — not an analysis city: 36 detectors and no road class reaching 20
  make it unable to support the stratified dose-response. It instead becomes the
  **clean-data and spateGAN in-domain anchor**, at 30-min resolution.

This resolves the CONTEXT.md §8-vs-§9 tension. §9 wanted German cities for
spateGAN in-domain validation; §8 deprioritised them all for short windows.
Essen carries the German validation on genuinely pristine data, while the
traffic analysis rests on Manchester and Torino.

**Dropped:** Rotterdam, Groningen (too few distinct days), Bolton (6 days,
20.9% quality retention), Birmingham and Innsbruck (no occupancy),
Constance (7 days, no link mapping).

Pooling as originally planned no longer applies — the NL cohort is gone and
Bolton cannot join Manchester.

**But pooling returns in a different form, and is now necessary.** After W3, no
single city clears every criterion:

| city | fails on | passes on |
|---|---|---|
| manchester | 21 usable days (3.0 weekday recurrences) | everything else — 28 events, 181 dets, 9 road classes |
| torino | **10 rain events** (< 15) | 399 dets, best road-class spread, 40 Mod+ hrs |
| essen | 36 dets, no road class ≥ 20 | cleanest data, 35 days, 35 events, in-domain |

The workable design is therefore a **pooled dose-response across all three
cities with city fixed effects and a tested city-interaction term**, giving
**73 rain events** in total — comfortably enough for stable coefficients — while
free-flow and typical-speed baselines stay strictly per-detector. Torino's thin
event count stops being disqualifying once its coefficient is estimated jointly
rather than alone.

Roles: **Manchester** primary (best balance), **Torino** secondary (carries
road-class stratification on 399 detectors), **Essen** validation anchor
(spateGAN in-domain, and the only city clean enough to test curation rules
against).

---

## 5. Rain criterion — CLOSED (W3)

Native ~31 km ERA5 hourly total precipitation, area-averaged over each city
bbox. Counted twice: across the calendar window, and **restricted to days that
actually carry quality-passing traffic data**. Only the second is meaningful —
rain on a day with no detector readings contributes nothing.

| city | calendar days | data days | events (calendar) | **events (data days)** | Mod+ hrs | peak wet hrs |
|---|---|---|---|---|---|---|
| manchester | 72 | 21 | 76 | **28** | 46 | 40 |
| torino | 21 | 21 | 10 | **10** | 40 | 28 |
| essen | 188 | 35 | 179 | **35** | 60 | 58 |
| rotterdam | 43 | 6 | 39 | 6 | 1 | 3 |
| groningen | 16 | 2 | 13 | 4 | 2 | 1 |
| bolton | 6 | 4 | 4 | 2 | 1 | 1 |

The calendar-vs-data-day gap is large and would have been badly misleading:
Manchester's 76 calendar events reduce to **28** usable, Essen's 179 to **35**.
Events are also broken at sampling gaps, so rain either side of a 40-day hole is
never merged into one spurious "event".

**Torino fails the event threshold: 10 events against a ≥15 requirement.** Its
40 Moderate+ hours are adequate — the rain simply arrived in few, long bouts
rather than many separate ones. Few independent episodes means wide confidence
intervals on any dose-response coefficient, which is exactly what the threshold
exists to prevent.

Manchester and Essen both clear it comfortably.

### A further correction: Manchester's usable days

7 of Manchester's 28 days are **entirely** `error=1` — every row on those dates
is flagged (2017-09-08, 09-18, 09-20, 09-22, 10-01, 11-13, 11-18). Usable days
are therefore **21, not 28**, giving 3.0 weekday recurrences rather than 4.0 —
the same as Torino. The profile-resolution figures in §2 already applied the
quality filter, so the hourly-baseline conclusion is unaffected.

---

## 6. Required curation rules for Phase 2

Non-negotiable, each derived from a defect above:

1. `quality_ok = error IS NULL OR error != '1'` — never `error = 0`.
2. Per-city occupancy normalization: divide by 100 for manchester/torino/
   rotterdam/bolton/groningen; leave essen as-is. Verify against a physical
   plausibility band before use.
3. Drop `occ` rows that are `inf` or `> 100` (pre-rescale) as unrecoverable.
4. `speed = 0 AND flow = 0` → NULL, before any baseline is computed.
5. Flag and investigate `speed = 0 AND flow > 0` per city (Rotterdam 10.8%
   suggests sensor fault; Manchester 0.4% is plausibly genuine).
6. Baseline resolution: **hourly** for Manchester and Torino, **30-min** for
   Essen — not the 5-min profile CONTEXT.md §L2a specifies.

---

## 7. CONTEXT.md amendments required

- **§4.1** — row count is **134,380,371**, not ~170 million.
- **§4.1** — "at least two of three variables" understates it: **speed is
  present in only 9 of 39 cities**, and usable in 3.
- **§8** — the entire tier list needs rewriting; London, Toronto, Marseille,
  Hamburg, Paris, Melbourne, Taipei and Bordeaux have no speed.
- **§L2a** — the 5-min typical-speed profile is not estimable; re-specify to
  hourly. This does *not* coarsen rain features: 5-min observations still
  compare against an hourly baseline cell, so onset detection survives.
- **§6/L1** — add the occupancy-scale and zero-speed rules above.
