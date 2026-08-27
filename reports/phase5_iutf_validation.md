# IUTF Validation — L1 cross-check and prior-art audit

_Generated 2026-08-27 09:47 UTC. IUTF retrieved 2026-08-27, MD5 verified — see `lake/iutf/PROVENANCE.md`._

**Gate verdict: PASS**

## 1. Does our harmonisation reproduce IUTF's?

Compared on `(detid, local timestamp)` over the raw pre-curation values,
since curation deliberately nulls readings IUTF passes through.

| city | our rows | IUTF rows | matched | ours unmatched | flow | occ | speed |
|---|---|---|---|---|---|---|---|
| essen | 291,065 | 291,893 | 291,065 | 0 | 100.00% | 100.00% | 100.00% |
| manchester | 765,096 | 1,142,893 | 765,096 | 0 | 100.00% | 100.00% | 100.00% |
| torino | 1,819,683 | 2,080,768 | 1,819,683 | 0 | 100.00% | 100.00% | 100.00% |

Largest absolute disagreement on any matched key, any city, any measure: **7.02563e-17**.

Every curated row found an exact IUTF counterpart. This independently
validates the parts of L1 that had no other check available: the
local→UTC alignment and its DST handling, the detector join, the
`interval`-seconds-since-local-midnight decoding, and the city key
normalisation. Two pipelines built from the same source by different
people agreeing bit-for-bit on 2.9 M rows is the strongest evidence
available that neither drifted.

## 2. What IUTF actually contains

| city | window | traffic | weather | sensors | roads | ERA5 cells | ships speed? |
|---|---|---|---|---|---|---|---|
| essen | 2017-03-27 → 2017-09-30 | 5min | 1h | 38 | 21,044 | 1 | **yes** |
| manchester | 2017-09-08 → 2017-11-18 | 5min | 1h | 181 | 8,794 | 1 | **yes** |
| torino | 2016-09-26 → 2016-10-16 | 5min | 1h | 399 | 37,681 | 4 | **yes** |

## 3. CONTEXT.md §3's three limitations, re-audited

These define the project's stated contribution, so they are checked
against the files rather than the paper's abstract. Two do not survive.

### ❌ "Flow, not speed" — overstated

IUTF's `5min_readings.parquet` carries `flow`, `occ`, **`speed`** and
`error` — the raw UTD19 columns, speed included. What is true is that
IUTF's *published validation* is built on flow change; what is false is
that the dataset lacks speed.

The contribution survives in narrower form: IUTF ships the speed
*column*, not a speed *layer*. Free-flow speeds conditioned on critical
occupancy, dry-only typical-speed profiles, and delay metrics derived
from them are absent from IUTF and are this project's own (L2a, Phase 3).
The claim should be "no derived speed baselines", not "no speed".

### ✅ "Coarse rainfall" — confirmed, and it is the real differentiator

IUTF's weather resolution is `1h`, on the native ERA5 0.25° grid — for
Manchester a single cell centred (-2.25, 53.5) covering the whole city.
This is exactly the spatial-scale mismatch IUTF flags in its own paper,
and the 2 km / 10 min downscaling of Phase 4 is a direct fix for it.
Phase 4 measured what it bought: at native resolution Manchester and
Essen each collapse to one cell, so within-city rainfall variation was
literally zero.

### ❌ "Cross-city-truncated windows" — false

CONTEXT.md §3 states IUTF "deliberately restricted every city to a
shared 2015–2017 window". It does not. Each city carries its own window,
matching UTD19's actual per-city coverage:

- essen: 2017-03-27 → 2017-09-30
- manchester: 2017-09-08 → 2017-11-18
- torino: 2016-09-26 → 2016-10-16

These are the same windows this project derived independently from raw
UTD19 — necessarily, since they are simply what UTD19 holds. "2015–2017"
is the span across all 40 cities, not a per-city truncation. **Per-city
rain-optimised windows are not a differentiator** and should be dropped
from the contribution claim.

## 4. Independent confirmation of two Phase-1/2 findings

IUTF's raw columns corroborate two defects this project catalogued from
the UTD19 source, which is worth recording because both drive curation
rules that would be expensive to get wrong:

- **Occupancy scale differs by city.** IUTF's Manchester occupancy runs
  ~26, Torino ~2.9, Essen ~0.006 on the same rows — the percent/fraction
  split `conf/cities.yml` infers and `qa_curated.py` enforces.
- **Quality-flag encoding differs by city.** IUTF's `error` is NULL for
  Manchester and 0/1 for Essen, exactly as `conf/cities.yml` documents.
  `WHERE error = 0` would discard all of Manchester in IUTF too.

## 5. What this does not establish

- **Agreement is not correctness.** Both pipelines read the same UTD19
  CSV. A defect in the source propagates identically to both, and this
  check cannot see it.
- **Only the raw layer is compared.** IUTF has no free-flow speed, no
  typical-speed profile and no delay metric, so L2a has no oracle and
  remains checked only by `qa_baselines.py`.
- **The rainfall layers are not compared here.** IUTF's hourly 0.25°
  field and this project's 2 km / 10 min downscaled field are different
  quantities by construction; comparing them is a Phase-6 benchmark, not
  a validation.

