# Rain-Aware Urban Traffic Delay Prediction

### What Rainfall Resolution Does a Rain–Delay Model Actually Need, and Can Generative Downscaling Supply It?

**Project Synopsis — for mentor review**

---

## 0. Build Status — What the Implementation Actually Found

*This section supersedes any claim below that contradicts it.*

The project has two arms. The **European arm is complete and its result is negative**; that
negative result is what defines the current research question. The **Netherlands arm** is the
active build.

### 0.1 European arm (UTD19 + ERA5) — complete, archived on branch `archive/eu-utd19-arm`

L0–L3 built and run on real data, 48 tests. Results in `reports/`.

| Finding | Detail |
|---|---|
| Corpus size | `utd19_u.csv` holds **134,380,371 rows across 39 cities**, not "~170 M / 40" |
| Speed availability | Only **9 of 39 cities** report speed at all |
| German cities | **Every large German city has zero speed.** Only Essen (36 detectors) and Constance (7 days) carry it |
| Study set | manchester, rotterdam, bolton, groningen, birmingham, essen — sharing 2017-09-08 → 2017-11-18 |
| Curated corpus | 1,395,565 rows, ~7,600 detector-days, **40 distinct calendar days** |
| Concurrency | Cities are **not** simultaneous: no day has more than 3 cities |
| Occupancy | **percent** in manchester/bolton/rotterdam/groningen, a fraction in essen, **absent** in birmingham |
| Error flags | Three-state (0/1/null); outside Essen no city has an explicitly-clean row |
| Rain intensity | ERA5 at 28 km yields max **4.26 mm/h** corpus-wide. `very_heavy` empty, `heavy` one unusable hour |
| Spatial resolution | **Birmingham's entire detector set falls in ONE ERA5 grid cell** |
| Dose-response | light rain **−1.8 pp**, moderate **+1.1 pp**. Partially ordered, and light rain implies *faster* traffic — not physical |
| Rain ablation | temporal +0.9%, cross-city +3.0%, **event-based −2.8%**, **spatial −14.4%**. GBT loses to the historical mean on 2 of 3 splits |

**Conclusion: at raw ERA5 resolution the rain features do not earn their place.** Target noise
(sd = 0.334) dwarfs the effect size (~0.01–0.02).

**The diagnosis matters more than the result.** The failure has three causes, in order of
severity: (1) target noise vastly exceeding effect size, (2) 28 km hourly means destroying
rainfall intensity structure, (3) only 40 independent weather realisations. **None of these is
geographic, and none is a defect of the pipeline.** They are all consequences of rainfall
resolution and corpus length — which is what the current design attacks directly.

### 0.2 Netherlands arm (NDW + KNMI radar) — active

Phase-1 gate **passed**. Full detail in `reports/phase1_nl_audit.md`.

| Finding | Detail |
|---|---|
| Speed availability | **20,518 of 20,519 sites** carry `anyVehicle` speed (vs 9 of 39 UTD19 cities) |
| Resolution | **60 s**, uniformly, on every measurement index |
| Equipment | 20,443 inductive loops, 75 radar, 1 microwave |
| Access | **No account, no API key** for the live feed |
| Quality signal | `numberOfInputValuesUsed` — a continuous sample size, strictly better than a keep/drop flag |
| Occupancy | **Absent entirely.** No `TrafficConcentration` in the feed |
| Road class | OpenLR FRC present on only **1,833 of 20,519 sites (8.9%)** — OSM map-matching still required |
| Feed cohorts | Two: **5,755 sites at ~2 min lag, 14,764 at ~8 min lag**. Consecutive fetches are ~72% duplicate |
| Volume | ~**8.3 M rows/day** measured from the fast cohort alone; **22.7 bytes/row** as Parquet (~107× smaller than raw XML) |
| History | **Blocked.** The open feed is current-minute only; `dexter.ndw.nu/api/export` returns HTTP 401 |

**New binding constraint — informative missingness.** 86% of non-null 1-minute speeds rest on
**fewer than five vehicles**, and thin counts correlate with low flow, which correlates with
rain. So sample size is correlated with the treatment: weighting by sample size is
variance-optimal but **bias-pessimal**, and `speed IS NULL` itself carries information about
rain. This is a bias problem that aggregation alone does not fix, and it is now an **L1 gate
requirement** rather than an analysis-stage afterthought (see §6, L1).

### 0.3 Options evaluated and rejected

- **TomTom Flow Segment Data** — live only, no date parameter, so no route to a historical corpus.
- **TomTom Traffic Stats** — an *aggregation* service: every metric is a mean over
  `dateRange × timeSet`, capped at 20 routes and (on trial) 1 date range per report. Reconstructing
  a time series costs one report per window, and rain is averaged into each aggregate — the same
  defect §3 identifies in Google's `staticDuration`. Trial data is further restricted to a single
  month.
- **Indian open data** — no UTD19 equivalent exists. Delhi OTD and IUDX are real-time only; the
  IIIT-Delhi bus archive is on request; Uber Movement is discontinued; Kaggle "Indian traffic"
  datasets have unverifiable provenance and are almost certainly synthetic. **India is deferred,
  conditional on O5** (see §2) — it is precisely the no-radar case that a positive
  substitutability result would license.

---

## 1. Problem Statement

Rainfall degrades urban road performance, and the literature reports MAPE improvements of roughly
4–5% from adding rainfall to traffic prediction. But per observation the effect is small: in our
own six-city European corpus, speed deviation from a dry baseline has a standard deviation of
**0.334** against a rain effect of order **0.01–0.02**. Learning this relationship is therefore a
signal-recovery problem, and it is decided by how faithfully the rainfall input preserves
intensity structure.

Open-data work in this area — including our own first build — implicitly assumes that freely
available global reanalysis rainfall is adequate to the task. **We tested that assumption and it
is false.** At ERA5's native 28 km hourly resolution, across 1.4 M curated detector-observations,
maximum observed intensity was 4.26 mm/h, the dose-response was not physically ordered, and
adding rain features *reduced* accuracy on the two hardest validation splits. Convective rainfall
is intrinsically a few kilometres wide and a few minutes long; a 28 km hourly mean cannot
represent it. Birmingham's entire detector network sat inside a single grid cell.

**So the open question is not whether rain causes delay.** It is:

> **At what rainfall resolution does the rain–delay relationship become learnable — and can
> generative downscaling manufacture that resolution faithfully enough to substitute for weather
> radar in regions where radar does not exist?**

The second half is what makes this more than a benchmarking exercise. Gauge-adjusted radar exists
for Germany, the Netherlands, the UK, the US and Australia — and for very little of the world that
most needs rain-aware traffic management. If a generative downscaler recovers most of radar's
benefit, that licenses this analysis anywhere ERA5 reaches, which is everywhere. If it does not,
that is a specific and useful limit on a method currently promoted for global use.

**spateGAN-ERA5's own publication validates its rainfall fields against radar. Nobody has tested
whether those fields are good enough to support a downstream physical inference.** That is the gap.

The commercial context is unchanged: Google's *Roads Management Insights* and vendor dashboards
such as Lepton's **TraffiCure** do quantify this, but they are closed — RMI access is restricted
to verified public-sector entities and jurisdiction-locked to territory the customer manages.

---

## 2. Objectives

| # | Objective |
|---|---|
| **O1** | Build a portable ingestion and curation pipeline over a **canonical traffic schema**, so a new country costs one adapter |
| **O2** | Derive per-segment free-flow speed and dry-only typical speed profiles at scale (years, not weeks) |
| **O3** | Construct **three rainfall inputs for the same traffic corpus** at three resolutions: raw ERA5, spateGAN-downscaled, gauge-adjusted radar |
| **O4** | Measure the dose-response at each resolution and identify where it becomes physically ordered and statistically stable |
| **O5** | Quantify how much of radar's benefit generative downscaling recovers — the **substitutability result** |
| **O6** | Measure the cost of **domain shift**: spateGAN in-domain (Germany, its training radar) vs out-of-domain (Netherlands) |
| **O7** | Serve the result on a **live feed**, where forecast rainfall is coarse by necessity and downscaling has no radar alternative |
| **O8** | Expose results through a TraffiCure-style analytics dashboard |

---

## 3. Justification of Data Strategy

*(Anticipating the first question: "why not just use a commercial maps API?")*

Polling Google's Routes API to accumulate a speed history was investigated and **rejected on three
independent grounds**:

1. **Licensing.** Google Maps Platform ToS §3.2.3 prohibits pre-fetching, indexing or storing
   Maps Content, and separately prohibits *creating content based on* it. The narrow caching
   exception requires storage under 30 days and forbids aggregation. Building day-of-week ×
   hour-of-day speed profiles violates all three clauses.
2. **Data shape.** Traffic is returned as a three-level ordinal (`NORMAL`/`SLOW`/`TRAFFIC_JAM`),
   not a numeric speed. Its `staticDuration` field is a *historical-typical* baseline, not a
   free-flow baseline — meaning **rain is already averaged into the denominator**, which
   structurally biases any rain-delay estimate toward zero.
3. **Cost.** Traffic-aware requests bill ~$10 per 1,000 calls. Polling 500 corridors at 5-minute
   cadence is ≈4.3 M calls/month ≈ **$43,000/month**.

The Roads API contains no traffic observations at all — map-matching and posted speed limits only.
TomTom's products were evaluated and rejected for the reasons in §0.3.

**Conclusion:** the analytical corpus is built from openly licensed data. NDW and KNMI are
particularly favourable: NDW's live feed needs no account at all, and KNMI's radar is CC-BY-4.0.
Commercial APIs are reserved strictly for optional live display, where no storage occurs.

---

## 4. Data Sources

### 4.1 NDW — Traffic Layer (Primary)

The Dutch National Road Traffic Data Portal (Nationaal Dataportaal Wegverkeer).

| Property | Value |
|---|---|
| Measurement sites | **20,519** `Point` detector sites (plus 80,709 travel-time sections) |
| Speed + flow | **20,518 sites carry both** at `anyVehicle` aggregation |
| Temporal resolution | **60 s** |
| Format | DATEX II v2.0 — `MeasuredDataPublication` + `MeasurementSiteTablePublication` |
| Geocoding | 100% of sites carry coordinates (WGS84, via the OpenLR `pointExtension`) |
| Quality | `numberOfInputValuesUsed` (continuous sample size), `dataError`, `computationMethod` |
| Licence | Open data; **retention/republication terms still to be confirmed** |
| Access | `https://opendata.ndw.nu/` — **no account, no API key** |

**Critical parse traps** (all handled in `src/raincheck/ndw.py`, all silent if missed):

- **`-1` is a missing-value sentinel** in an otherwise numeric field.
- **`dataError=true` accompanies a legal-looking `vehicleFlowRate` of 0.** Unlike `-1`, zero is a
  valid flow, so only the flag separates "empty road" from "broken loop".
- **The `anyVehicle` measurement index is per-site** — observed at 4, 8 and 16, because it depends
  on lane count × vehicle-length classes. Indices must be resolved from the site table, and a
  site-table version mismatch must be refused rather than mis-parsed.

**History is the open problem.** The open feed publishes only the current minute. Two routes: a
Dexter account (`dexter.ndw.nu/api/export` → HTTP 401 without one), or harvesting the live feed
forward, which `scripts/harvest_ndw.sh` does. Since KNMI's gauge-adjusted radar publishes ~1 month
in arrears, a harvest started now aligns with radar availability in ~5–6 weeks.

### 4.2 UTD19 — Traffic Layer (European arm, archived)

ETH Zürich, 23,541 loop detectors, 134,380,371 rows across 39 cities, 3–5 min resolution.
Retained as the baseline the Netherlands arm is measured against, and as the German in-domain
route via fundamental-diagram speed recovery. Attribution required (see §11).

### 4.3 ERA5 — Coarse Rainfall (rung 1, and spateGAN's input)

ECMWF reanalysis via the Copernicus Climate Data Store.

- **Native resolution:** ~24–31 km, hourly, global, back to 1940.
- **Required by the downscaler:** convective (`cp`) + large-scale (`lsp`) precipitation.
- **Feature engineering:** total precipitation (`tp`), 2 m temperature, 2 m dewpoint, 10 m winds.
- **Access:** CDS API, netCDF. **Already implemented** (`era5.py`, `jobs/fetch_era5.py`). Note the
  CDS zip splits `stepType-instant` from `stepType-accum`; taking only the first member silently
  discards precipitation.

### 4.4 KNMI Gauge-Adjusted Radar — the Referee (rung 3)

The observed ceiling, and the reference against which spateGAN's own fields are scored.

| Dataset | Role | Resolution |
|---|---|---|
| `rad_nl25_rac_mfbs_5min` v2.0 | **Training/ceiling** — climatological, gauge-adjusted | 5 min / 1 km |
| `nl_rdr_data_rtcor_5m` | **Inference-grade** — real-time, unadjusted | 5 min / 1 km |
| `radar_forecast` v2.0 | Nowcast to +2 h | 5 min |

- **Licence:** CC-BY-4.0. **Access:** one KNMI Open Data API key covers all open datasets.
- Note this is **finer than spateGAN's own output** (10 min / 2 km), so comparisons must be made at
  matched resolution.
- The Netherlands is **not** in spateGAN's training domain, which is what makes it a genuine
  out-of-domain test.

### 4.5 DWD RADKLIM-YW — the Domain Control

1 km / 5 min gauge-adjusted German radar, 2001–2018, open, no auth. This **is spateGAN's training
target**, so Germany bounds best-case downscaling skill. Its window overlaps UTD19's 2017 data.

### 4.6 spateGAN-ERA5 — the Hypothesis Under Test (rung 2)

Open-source conditional GAN (`LGlawion/spateGAN_ERA5`), *npj Climate and Atmospheric Science* 2025.

- **Transformation:** ERA5 24 km / 1 h → **2 km / 10 min**.
- **Trained on:** RADKLIM-YW (German gauge-adjusted radar) **only**; validated against US and
  Australian radar.
- **Constraints:** requires `cp` + `lsp`, a **≥672 × 672 km** domain, and a **≥16 h** sequence. The
  Netherlands is ~300 × 400 km, so the ERA5 box necessarily spills into Belgium, Germany and the
  North Sea.
- **Probabilistic:** ensembles by varying seed and slide.

> **Status change.** In the original design spateGAN was an *enhancement*, justified by an
> in-domain argument about German cities. The audit killed that argument (no German UTD19 city
> reports speed), and the European ablation showed raw ERA5 carries no usable signal. spateGAN is
> therefore no longer a nice-to-have but **the hypothesis under test**, with radar as its referee.
> Its authors' own caveat — that German training "may not fully capture the spatial and temporal
> variability of rainband characteristics in other climatic regions" — is exactly what O6 measures.

### 4.7 OpenStreetMap — Network Layer

Road topology, functional class, link geometry and length. **Still required**: NDW's OpenLR
extension supplies FRC for only 8.9% of sites, so map-matching covers the other 91%.

### 4.8 Live Traffic Feed — Demonstration Layer

**Superseded in the best case.** NDW publishes the same measurements as a live DATEX II feed at
1-minute cadence with no account, so the streaming layer can consume a *genuine* live feed and the
Kafka replay harness demotes from production path to test fixture. TomTom Flow Segment Data
remains available as a display-only, never-persisted alternative.

---

## 5. System Architecture

A **Lambda architecture**: a batch layer computing authoritative baselines and models over the
historical corpus, a speed layer consuming the live feed, and a serving layer merging both.

```
┌─────────────────────────────────────────────────────────────────────┐
│  SOURCES                                                             │
│  NDW DATEX II (live + Dexter)   ERA5 netCDF   KNMI radar   OSM PBF   │
│  [UTD19 CSV - archived European arm]                                 │
└────────┬─────────────┬───────────────┬──────────────┬───────────────┘
         │             │               │              │
┌────────▼─────────────▼───────────────▼──────────────▼───────────────┐
│  L0 — INGESTION                                    HDFS · Kafka      │
│  Live-feed harvest → Parquet · CDS pulls · KNMI pulls · PBF parse    │
└────────────────────────────┬────────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────────┐
│  L1 — CURATION                              Spark · Spark SQL        │
│  CANONICAL SCHEMA · dedup on (segment_id, ts_utc) · unit             │
│  normalization · UTC · re-binning · quality gating · map-matching ·  │
│  H3 indexing · Parquet partitioned by date/hour                      │
└──────────────┬──────────────────────────────────┬───────────────────┘
               │                                  │
┌──────────────▼──────────────┐   ┌───────────────▼───────────────────┐
│  L2a — TRAFFIC BASELINE     │   │  L2b — WEATHER, THREE RUNGS       │
│  Free-flow speed            │   │  rung 1  ERA5      28 km / 1 h    │
│  Typical speed profiles     │   │  rung 2  spateGAN   2 km / 10 min │
│  (dry-only)                 │   │  rung 3  radar      1 km / 5 min  │
│  Delay / deviation index    │   │  ONE join path, parameterised     │
└──────────────┬──────────────┘   └───────────────┬───────────────────┘
               └───────────────┬──────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│  L3 — PREDICTION LAYER                      Spark MLlib · GraphX     │
│  (a) dose-response per rung — interpretable elasticities             │
│  (b) GBT + rain ablation per rung → RUNG-TO-RUNG DELTAS are the      │
│      result, not the absolute scores                                 │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│  L4 — SERVING & VISUALIZATION           Cassandra · Dashboard        │
│  Live map · corridor analysis · heatmaps · rain-impact forecast      │
└─────────────────────────────────────────────────────────────────────┘
```

**Portability is asymmetric by design: one global weather path, N thin traffic adapters.** ERA5,
spateGAN, feature engineering, H3 indexing, the model and the serving layer are already
country-agnostic. National radar is an optional per-country *accuracy upgrade*, never a
dependency — otherwise onboarding country N+1 means integrating radar product N+1. In Europe,
Delegated Regulation 2022/670 Art. 6–7 mandates DATEX II across 30+ National Access Points, so one
parser plausibly serves many countries (the *format* is mandated; detector-level speed *content*
is not, and must be checked per country).

---

## 6. Data Flow and Layer Detail

### Layer 0 — Ingestion

NDW arrives two ways: a **live harvest** (`scripts/harvest_ndw.sh`, one fetch per minute, parsed to
Parquet and the XML discarded) and, once credentialed, Dexter export for history. ERA5 is pulled
per domain per month via the CDS API; note the ≥672 × 672 km spateGAN minimum means one fetch
covers a whole region with margin. KNMI radar is pulled per 5-minute file.

**Raw XML is never retained.** At ~72 GB/day uncompressed it exhausts the disk in under four days;
parsed Parquet measures 22.7 bytes/row, a ~107× reduction, which is what makes an open-ended
harvest viable at all.

**On streaming.** The European arm needed a replay harness because UTD19 is an archive. NDW's live
feed removes that need: Spark Streaming consumes the real thing, and replay becomes a test
fixture. Two rain features are genuinely stateful — `hours_since_onset` and
`antecedent_dry_hours` depend on unbounded history, so they need `flatMapGroupsWithState` or a
Cassandra state read rather than plain windowing. Trailing sums are fine with a watermark.

### Layer 1 — Curation

Raw → conformed, onto the **canonical schema**:

```
(source, country, city, segment_id, ts_utc, speed, flow, quality_weight, frc, geometry)
```

This is the decision that makes a new country cheap; retrofitting it later means rewriting L1.

- **Deduplication on `(segment_id, ts_utc)` — mandatory, not optional.** The harvester's dedup
  window is per-process and in-memory, so any restart re-ingests up to 30 minutes of overlap. Two
  concurrent runs were observed producing 29,528 rows for a timestamp whose ceiling is 20,519.
- **Sentinel and flag handling** — `-1` → null; `dataError=true` → null even when the value looks
  legal.
- **Unit normalization** — speeds km/h, flow veh/h. (Occupancy is absent from NDW entirely.)
- **Temporal alignment** — the feed is already UTC, but **sites are not synchronous**: two cohorts
  lag wall clock by ~2 min and ~8 min. Always key on each site's `measurementTimeDefault`, never on
  publication time.
- **Re-binning to ≥5 minutes weighted by `quality_weight`.** Not optional: 86% of non-null
  1-minute speeds rest on fewer than five vehicles.
- **Quality gating with bias diagnostics** — because thin counts correlate with rain, the gate must
  report:
  1. **retention stratified by rain band** (divergence between wet and dry retention is direct
     evidence the bias is live);
  2. the dose-response at **n ≥ 1, 5, 20** as a sensitivity analysis — an estimate that drifts
     monotonically with the threshold is driven by missingness, not rain;
  3. **weighted and unweighted** estimates side by side;
  4. flow retained as a **covariate**, so low volume is controlled rather than confounding.
- **Map matching** — detectors to OSM links for the 91% of sites lacking OpenLR FRC.
- **Spatial indexing** — **H3**, not a hardcoded degree grid, so the same join code serves 0.25°
  ERA5, 1 km radar and 2 km spateGAN.
- **Storage** — Parquet partitioned by `date / hour`, with an explicitly pinned schema. Type
  inference is unsafe here: a quiet detector can be null for a whole hour, which infers a
  null-typed column that Spark then refuses to read alongside typed partitions.

### Layer 2a — Traffic Baseline Layer

**Free-flow speed.** In the European arm this was the 85th percentile of speed *conditioned on
occupancy below the critical occupancy threshold* — conditioning on low density is what makes it a
free-flow rather than an off-peak measurement. **NDW has no occupancy at all**, so this degrades to
a high percentile over **dry, low-flow** intervals. This is a genuine methodological downgrade and
is reported as such, not glossed.

**Typical speed profiles** — median speed per `(segment, weekend-flag, hour-of-day)`, computed over
**dry intervals only**. This remains the single most important methodological decision in the
project: including rainy intervals lets the baseline absorb the very effect being measured, biasing
the estimate toward zero. It is precisely the flaw that makes Google's `staticDuration` unsuitable.
The `MIN_PROFILE_OBS = 30` guard proved essential — before it, single-observation baselines
produced deviations of −63%.

**Delay metrics** — a free-flow delay ratio (congestion irrespective of cause) and a typical-speed
deviation (anomaly against what this hour normally looks like). The second is the target variable.

### Layer 2b — Weather Layer: the Resolution Ladder

**The core experiment. Hold the traffic corpus fixed and vary only the rainfall input.** This is
the controlled comparison the first build could not make, because it had exactly one rainfall
product.

| Rung | Input | Resolution | Status |
|---|---|---|---|
| 0 | rain features ablated | — | baseline |
| 1 | ERA5 `tp` | 28 km / 1 h | **tested — fails** |
| 3 | KNMI gauge-adjusted radar | 5 min / 1 km | the observed ceiling |
| 2 | spateGAN-ERA5 from `cp`+`lsp` | 2 km / 10 min | the hypothesis |

**Run rung 3 before rung 2.** If the effect is not learnable at 1 km / 5 min *observed* rainfall,
there is nothing for spateGAN to recover and rung 2 is meaningless.

Three outcomes, all publishable: rung 3 works and rung 2 recovers most of it (downscaling is a
valid radar substitute — this is what would license the India application); rung 3 works and rung 2
does not (a concrete limit on a method promoted for global use); rung 3 also fails (the
per-observation effect does not exceed traffic's intrinsic variance at any resolution, and a body
of aggregate-level literature needs qualifying).

**Diagnostic coarsening** — radar deliberately degraded, which is *not* an spateGAN input:

- Radar → **28 km / 1 h** vs ERA5 separates "ERA5 is merely smooth" (downscaling is the whole
  problem) from "ERA5 is also biased" (downscaling a biased field will not save it). **This decides
  whether the project's premise holds.**
- Radar → **10 min / 2 km** scores spateGAN at its own native resolution rather than penalising it
  for radar's extra sharpness.
- Verify spateGAN output **conserves the ERA5 hourly aggregate**.

**Rain features** (one code path, parameterised by product):

- **Instantaneous intensity** (mm/h) at the segment's cell.
- **Accumulation windows** — trailing sums capturing standing water and drainage saturation. The
  European arm was forced to 1/3/6 h because raw ERA5 is hourly; radar and spateGAN restore
  sub-hourly windows.
- **Time since rain onset** — the first minutes of rain are disproportionately disruptive.
- **Dry-spell antecedent** — hours since last rain, encoding the road-surface oil-film effect.
- **Ensemble spread** — spateGAN multi-seed members as an explicit uncertainty covariate. Not
  implemented in the European arm; becomes available at rung 2.
- **Intensity banding** — `BAND_EDGES` **must be recalibrated per rung**. Edges tuned to ERA5's
  smoothed drizzle will saturate immediately on radar.

### Layer 3 — Prediction Layer

**(a) Dose-response — for explanation.** GLM or GBT with interactions across `rainfall band × road
class × time-of-day × baseline congestion`, yielding an interpretable elasticity table. Keep the
`delta_vs_none_pct` differencing, which corrects the median-baseline offset. Report CIs and
suppress cells under 30 observations.

**(b) Predictive model — for operation.** GBT in Spark MLlib as the honest baseline, extended to a
spatio-temporal model exploiting network structure. Weight by `quality_weight`
(`GBTRegressor.weightCol`) — **and report unweighted alongside**, per the bias diagnostics in L1.

**Train/serve skew is NOT structurally prevented.** The original design claimed it was, because
training and inference share feature code. That holds for traffic and **fails for rainfall**: the
gauge-adjusted radar used for training publishes per ten days with ~1 month delay, while inference
sees unadjusted real-time radar, then a 2-hour nowcast, then NWP — each with different bias
characteristics. Therefore: **train on the inference-grade product, and reserve adjusted radar
strictly as a validation reference.**

This also reframes downscaling's role. Its real operational job is not improving on ERA5 for
training; it is **the forecast path**, where no radar exists because the future has only coarse
NWP. That is a far better-motivated position than the one the European arm tested.

**Validation design:**

| Split | Tests |
|---|---|
| Temporal holdout | Generalization forward in time |
| Spatial holdout | Generalization to unseen segments (**highest missingness risk**) |
| Event-based | Performance on rain intervals specifically |
| Cross-country | NL (out-of-domain) vs DE (in-domain for spateGAN) |
| Leave-one-city-out | The per-city calibration claim |

**Baselines to beat:** historical mean, naïve persistence, and an identical model with rainfall
features ablated. The rain-ablated comparison isolates the contribution of the entire weather
pipeline, **and is computed at every rung** — the rung-to-rung deltas are the result.

**What genuinely does not transfer.** Rain response depends on drainage, fleet mix, driver
behaviour and road geometry. Cross-city transfer was the second-worst European split, and no
engineering fixes that. The design is a shared architecture with a **per-city calibration layer**,
validated leave-one-city-out. That is the honest scope of the multi-country claim.

### Layer 4 — Serving and Visualization

Precomputed baselines and predictions written to Cassandra keyed by
`(country, segment_id, timestamp_bin)`. The dashboard mirrors commercial platforms: live network
view, congestion heatmaps by time-of-day and corridor, travel-time reliability, anomaly detection,
and a rain-impact forecast panel.

**On "just type a country name".** The weather path (ERA5, IMERG), OSM network, H3 indexing, the
model and the dashboard are genuinely automatic from a country name — a country can light up with
rainfall, network, graph structure and *predicted* delay with zero per-country work. The **traffic
feed cannot be**: feed discovery, credentials and terms acceptance, DATEX II profile semantics, and
licence all require human judgement. The achievable design is a **declarative adapter**
(`conf/countries/<iso>.yaml`) plus a **capability probe** that drafts one from an endpoint for
review. The traffic layer then degrades gracefully — `supported` / `probed, needs review` /
`no known feed` — rather than failing. **Not to be built until a second country exists to
generalise from.**

---

## 7. Mapping to the Big Data Specialization (UC San Diego)

| Course | Concepts | Where applied |
|---|---|---|
| **1. Introduction to Big Data** | Volume/velocity/variety/veracity | Source characterization; veracity via NDW sentinels, `dataError` and sample sizes |
| **2. Big Data Modeling & Management** | Data models, HDFS, streaming vs batch, schema design | L0/L1 — canonical schema, partitioning, pinned Parquet schema, live vs replay |
| **3. Big Data Integration & Processing** | Spark, Spark SQL, ETL, NoSQL | L1/L2 — curation, three-rung weather join, Cassandra serving |
| **4. Machine Learning with Big Data** | MLlib, regression, evaluation | L3 — dose-response, ablations, the resolution ladder |
| **5. Graph Analytics with Big Data** | GraphX/GraphFrames, centrality, traversal | Road network as graph; congestion propagation; structurally critical links |
| **Capstone** | End-to-end integration | The full pipeline |

**Beyond the course foundation:** Kafka, Airflow, H3, PyTorch (spateGAN inference), xarray, Docker,
DATEX II, OpenLR.

---

## 8. Expected Deliverables

1. A reproducible, containerized pipeline from raw sources to served predictions.
2. **An open pipeline plus published coefficients** — *changed from "a published speed-profile
   dataset"*: redistribution rights vary by source and NDW's retention terms are unconfirmed.
3. A quantified rain dose-response table stratified by road class and time of day, **reported at
   each rainfall resolution**.
4. **The substitutability result (O5)**: what fraction of gauge-adjusted radar's benefit generative
   downscaling recovers, plus the in-domain vs out-of-domain gap (O6).
5. A trained delay-prediction model documented against three baselines at every rung.
6. An analytics dashboard demonstrating the operational use case.
7. A written report with reproducibility instructions and full data attribution.

---

## 9. Risks and Mitigations

| Risk | Severity | Mitigation |
|---|---|---|
| **Speed unavailable in chosen cities** | High | **MATERIALISED in the European arm** — 30 of 39 cities have no speed. **Resolved by NDW**: 20,518 of 20,519 sites carry it |
| **Insufficient temporal coverage** | High | **MATERIALISED** — six European cities gave 40 distinct days. NDW gives years, or a forward harvest at ~8.3 M rows/day |
| **Too few rain events for stable estimates** | High | **MATERIALISED — was the binding European constraint.** Addressed by corpus length and by radar restoring the intensity tail |
| **Informative missingness** | **High — new** | Thin counts correlate with low flow, which correlates with rain, so sample size correlates with the treatment. Rain-stratified retention reporting, threshold sensitivity analysis, weighted *and* unweighted estimates, flow as covariate. See §6 L1 |
| **NDW history inaccessible** | High | Dexter export needs an account (401 without). Mitigated by the forward harvest, which also aligns with radar's ~1 month publication lag |
| **Harvester downtime = permanent data loss** | Medium | The open feed has no back-fill. Detached process, restart after reboot, and L1 dedup so restarts are harmless |
| **No occupancy in NDW** | Medium | Free-flow degrades to a dry low-flow percentile; reported explicitly as a downgrade rather than glossed |
| **Licence uncertainty on NDW retention** | Medium | Read terms before publishing; Deliverable #2 already reframed to coefficients rather than corpus |
| **spateGAN out-of-domain error** | Medium | **This is now a measured quantity (O6), not a risk to mitigate**: Germany/RADKLIM is in-domain control, NL/KNMI out-of-domain |
| **No ground truth for downscaled rainfall** | Medium | **Resolved for this arm.** KNMI radar is finer than spateGAN's output, so downscaling error is directly measurable |
| **Train/serve skew on rainfall** | Medium | Train on the inference-grade product; adjusted radar is a validation reference only |
| **Confounding** — rain correlates with season, daylight, temperature | Medium | Control for time-of-day, day-of-week, season; dry-only baseline as counterfactual |
| **Dataset smaller than "big data" implies** | Low | **Retired.** ~8.3 M rows/day measured is ~360× the European arm's curated corpus |

---

## 10. Proposed Phasing

### European arm (UTD19) — complete, archived

| Phase | Focus | Status |
|---|---|---|
| 1 | Feasibility audit; city selection | ✅ **hard gate passed** — `reports/phase1_city_audit.md` |
| 2 | L0/L1 — ingest, curate, map-match, index | ✅ |
| 3 | L2a — free-flow and dry-only profiles | ✅ |
| 4 | L2b — ERA5 acquisition, join, features | ✅ *(raw ERA5 only)* |
| 5 | L3a — dose-response | ✅ |
| 6 | L3b — prediction, validation, ablations | ✅ **negative result — see §0.1** |

### Netherlands arm — active

| Phase | Focus | Status |
|---|---|---|
| 1 | NDW/KNMI feasibility gate; licence check | ✅ traffic gate passed — `reports/phase1_nl_audit.md`. Credentials outstanding |
| 2 | L0 — NDW adapter, live harvest, KNMI + ERA5 fetch | 🔄 adapter and harvester built; KNMI fetcher outstanding |
| 3 | L1 — canonical schema, dedup, re-binning, bias diagnostics | |
| 4 | L2a — baselines without occupancy | |
| 5 | **L2b rung 3 — radar. Establishes whether a ceiling exists** | |
| 6 | **L2b rung 2 — spateGAN. Measures substitutability (O5)** | |
| 7 | L3 — dose-response and ablation at every rung | |
| 8 | Domain shift over Germany with RADKLIM-YW (O6) | |
| 9 | L4 — Cassandra, dashboard, live streaming | |
| 10 | Documentation and reproducibility packaging | |

Phase 1 remains a hard gate in both arms. The single largest risk is building a pipeline before
discovering the data cannot support the question — which is exactly what the European arm's audit
prevented, and what its ablation then revealed at the next level down.

---

## 11. References

- Loder, A., Ambühl, L., Menendez, M. & Axhausen, K.W. (2019). Understanding traffic capacity of
  urban networks. *Scientific Reports*, 9(1), 16283. https://doi.org/10.1038/s41598-019-51539-5
- Glawion, L. et al. (2025). Global spatio-temporal ERA5 precipitation downscaling to km and
  sub-hourly scale using generative AI. *npj Climate and Atmospheric Science*, 8(1), 219.
  https://doi.org/10.1038/s41612-025-01103-y
- spateGAN-ERA5 implementation: https://github.com/LGlawion/spateGAN_ERA5
- NDW open data: https://opendata.ndw.nu · docs: https://docs.ndw.nu
- KNMI Data Platform: https://dataplatform.knmi.nl
- DWD RADKLIM-YW: https://opendata.dwd.de/climate_environment/CDC/
- UTD19 dataset: https://utd19.ethz.ch
- Copernicus Climate Data Store (ERA5): https://cds.climate.copernicus.eu
- DATEX II: https://datex2.eu · EU Delegated Regulation 2022/670 (RTTI), Art. 6–7
- Jia, Y., Wu, J. & Xu, M. (2017). Traffic Flow Prediction with Rainfall Impact Using a Deep
  Learning Method. *Journal of Advanced Transportation*.
- Google Maps Platform Terms of Service §3.2.3 (consulted for data-strategy justification).

---

*Data acknowledgment: this project uses NDW open data (opendata.ndw.nu), KNMI precipitation radar
(CC-BY-4.0) from the KNMI Data Platform, the UTD19 dataset (utd19.ethz.ch) under its academic and
non-commercial terms, and ERA5 reanalysis from the Copernicus Climate Change Service.*
