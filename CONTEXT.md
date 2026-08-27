# Rain-Aware Urban Traffic Delay Prediction

### A Distributed Pipeline Coupling Multi-City Loop-Detector Traffic States with GAN-Downscaled ERA5 Precipitation

**Project Synopsis — for mentor review**

---

## 1. Problem Statement

Rainfall measurably degrades urban road performance, but the degradation is not uniform. It varies with rainfall intensity, the road's functional class, the time of day, the level of congestion already present, and — importantly — whether the rain is the first after a dry spell. City traffic authorities currently have almost no quantitative handle on this. Conventional Intelligent Traffic Management Systems (ITMS) instrument only a small fraction of the network with cameras and roadside sensors, so rain-response is measured, if at all, on a handful of corridors.

The scientific building blocks now exist in the open. Loop-detector traffic archives (UTD19), global reanalysis rainfall (ERA5), and a deep-learning precipitation downscaler (spateGAN-ERA5) are all publicly available. A very recent dataset — IUTF, described below — has even pre-joined UTD19 traffic with ERA5 rainfall for 40 cities. What does not yet exist is an open, reproducible pipeline that turns these ingredients into **decontaminated speed baselines** and a **rain-to-delay prediction layer** at high spatio-temporal resolution.

**The gap this project addresses:** existing open resources quantify rainfall's effect on traffic *flow* against coarse (~31 km, hourly) rainfall, and derive no speed baselines from the speed data they carry. This project instead targets rain-induced *delay* — via free-flow and typical-speed baselines decontaminated of rain — against GAN-downscaled 2 km / 10 min rainfall, with effects reported as cluster-robust intervals rather than point estimates. (An earlier version of this statement also claimed per-city windows as a differentiator; that claim was withdrawn on 2026-08-27 — see §3.)

---

## 2. Objectives

| # | Objective |
|---|---|
| **O1** | Build a distributed ingestion and curation pipeline for multi-city loop-detector traffic data (from raw UTD19, validated against IUTF) |
| **O2** | Derive per-detector **free-flow speed** and **typical speed profiles** by day-of-week × hour-of-day, decontaminated of rainfall effects |
| **O3** | Downscale coarse global reanalysis rainfall to 2 km / 10 min resolution and spatially join it to the traffic network |
| **O4** | Quantify the **dose-response relationship** between rainfall intensity and speed degradation, stratified by road class and time of day |
| **O5** | Train and serve a **prediction layer** that maps forecast rainfall to expected delay |
| **O6** | Expose results through a TraffiCure-style analytics dashboard |

---

## 3. Relationship to Prior Work: The IUTF Dataset

A dataset published while this project was being scoped — **IUTF (Integrated Urban Traffic-Flood)**, *Nature Scientific Data*, accepted November 2025 — is close enough to this project's foundation that it must be addressed head-on. Understanding exactly what IUTF does and does not do is what defines this project's contribution.

**What IUTF is.** An open-access (CC BY 4.0) resource, ~1.61 GB in Apache Parquet and NumPy NPZ formats, that fuses three sources into one aligned package across 40 cities in Europe, North America, and Asia:
- Traffic parameters from ~21,700 UTD19 sensors (raw 5-minute intervals, harmonised to hourly);
- Hourly ERA5 precipitation, spatially attributed to the road network;
- OpenStreetMap topology for over 1 million road segments.

IUTF has already solved parts of what this project's Layer 1 and Layer 2b would otherwise build from scratch: time-zone/DST conversion to UTC, 5-minute-to-hourly aggregation, sensor-to-road-segment matching, and area-weighted rainfall attribution. It also validated that the joined data reveals a **dose-response relationship**, binning rainfall as Light (<0.5), Moderate (0.5–4), Heavy (4–10), and Extreme (>10 mm/hr) per Met Office definitions.

**Where IUTF stops — and this project begins.** Three limitations define the contribution here:

> **Re-audited 2026-08-27 against the downloaded files, not the paper.** Two of the three limitations below were wrong as originally written and have been corrected. See `reports/phase5_iutf_validation.md`. The correction narrows the contribution but does not remove it: limitation 2 is the substantive one and it is intact.

1. **No derived speed layer** (originally, and too strongly, "flow, not speed"). IUTF's `5min_readings.parquet` *does* carry a `speed` column — the raw UTD19 columns are passed through intact. What is true is that IUTF's published *validation* is built on flow change, and that IUTF derives nothing from speed. Free-flow speed conditioned on critical occupancy, dry-only typical-speed profiles, and the delay metrics built on them are absent from IUTF and are this project's own work (L2a). The contribution is the derived baseline layer, not the presence of the column.
2. **Coarse rainfall.** IUTF uses ERA5 at its native ~31 km hourly resolution and explicitly flags the spatial-scale mismatch against point sensors as a limitation. This project's spateGAN downscaling to 2 km / 10 min is a direct fix for the weakness IUTF names in its own paper. **Confirmed against the files** — IUTF's per-city `grid_info.parquet` puts the whole of Manchester in a single 0.25° cell, so within-city rainfall variation there is exactly zero. This is the real differentiator.
3. ~~**Cross-city-truncated windows.**~~ **Withdrawn — this claim was false.** IUTF does *not* restrict cities to a shared window. Each city carries its own, matching UTD19's actual coverage (Manchester 2017-09-08→11-18, Torino 2016-09-26→10-16, Essen 2017-03-27→09-30) — the same windows this project derived independently from raw UTD19, necessarily, since they are simply what UTD19 holds. "2015–2017" is the span across all 40 cities, not a per-city truncation. Per-city windows are not a differentiator and must not be claimed as one.

**How IUTF is used in this project.** As a **validation oracle**, not a substitute for the pipeline and not an accelerator — nothing is imported from it. The core pipeline is built from raw UTD19 + ERA5 so that the curation, downscaling, and baseline logic are genuinely the project's own work; IUTF is then used to (a) cross-check that this project's independent harmonisation reproduces IUTF's aligned output, and (b) serve as a documented prior-art benchmark.

**(a) is done and passed (2026-08-27).** All 2,875,844 curated rows matched an IUTF row exactly on `(detid, local timestamp)`, with 100% agreement on flow, occupancy and speed and a largest absolute disagreement of 7e-17 — float noise. This independently validates the parts of L1 that had no other check: local→UTC alignment and DST handling, the `interval`-seconds-since-local-midnight decoding, the detector join, and the city-key normalisation. It closes the Phase-2 validation checkpoint deferred in §6/L1. Note what it does *not* establish: both pipelines read the same source CSV, so a defect in UTD19 itself propagates identically to both and is invisible to this check.

---

## 4. Data Sources

### 4.1 UTD19 — Traffic Layer (Primary)

Collected by the Institute for Transport Planning and Systems, ETH Zürich, in a research campaign spanning 2017–2019.

| Property | Value |
|---|---|
| Detectors | ~23,500 stationary loop detectors |
| Cities | 40, across Europe, North America, Asia, Australia (39 carry measurements) |
| Data rows | **134,380,371** (counted in the Phase-1 audit; the ~170 M figure quoted elsewhere is not what the raw files contain) |
| Vehicles detected | ~4.9 billion |
| Temporal resolution | 3–5 minute aggregation intervals (1 hour for Paris) |
| Coverage | 3.8 years total, but *per-city windows vary from a single day to several months* |
| Geocoding | All detectors and associated roads in WGS84 |
| Metadata | Link length, number of lanes, road functional classification |
| Quality | Error-flagged, standardized schema |
| Licence | Free for academic / non-commercial use, on sign-up |

**Core variables:** vehicle flow, detector occupancy, and speed. **Critical caveat (see §9):** the dataset guarantees *at least two of the three* fundamental traffic variables per city, so speed is not universally available, and per-city temporal coverage varies enormously. City selection is a design decision, driven by a Phase-1 audit of variable availability and window length — not an afterthought.

> **Phase-1 finding: "at least two of three" badly understates the constraint.** Speed is present in **only 9 of the 39 cities with measurements**, and after applying quality flags, distinct-day counts and rain-event counts, **3 remain usable** (Manchester, Torino, Essen). The audit also found that per-city *distinct days with data* are far fewer than the calendar span suggests — Rotterdam, for example, has 6 days of data spread across 43 calendar days, not 43 days. Two further data defects are documented in §6/L1: occupancy is reported as a percentage (not the documented 0–1 fraction) in every candidate city except Essen, with unrecoverable `inf` values in Torino and Rotterdam; and `speed = 0` is frequently an *absence-of-vehicles* marker rather than a standstill. Full evidence: `reports/phase1_gate.md`.

**Network character — arterials, not just highways.** UTD19 ("Urban Traffic Detectors") was assembled to study Macroscopic Fundamental Diagrams, a concept specific to signalized surface-street networks. The detectors are overwhelmingly inductive loops on arterial intersection approaches, exactly the congested, signalized roads where rain's capacity effect bites — while still carrying enough motorway/arterial road-class labelling to stratify response by road type.

**Attribution requirement:** publications must cite Loder, A., Ambühl, L., Menendez, M. & Axhausen, K.W. (2019), *Understanding traffic capacity of urban networks*, Scientific Reports 9(1) 16283, and acknowledge UTD19 (utd19.ethz.ch).

### 4.2 IUTF — Pre-Harmonised Traffic + Rainfall Layer (Validation / Accelerator)

Integrated Urban Traffic-Flood dataset (Lin et al., *Nature Scientific Data*, 2025). See §3. Used as a validation benchmark for this project's independent harmonisation and as documented prior art. CC BY 4.0; figshare DOI 10.6084/m9.figshare.30022807; processing code on GitHub under GPL-3.0.

### 4.3 ERA5 — Weather Layer (Primary)

ECMWF's global atmospheric reanalysis, distributed free through the Copernicus Climate Data Store.

- **Native resolution:** ~24–31 km grid, hourly, global, extending back to 1940 — comfortably covering the 2017–2019 UTD19 window.
- **Variables required by the downscaler:** convective precipitation and large-scale precipitation.
- **Additional variables for feature engineering:** total precipitation, 2 m temperature, 2 m dewpoint, 10 m wind components (to distinguish rain type and detect freezing conditions).
- **Access:** CDS API, delivered as netCDF.

### 4.4 spateGAN-ERA5 — Downscaling Layer

An open-source conditional GAN (`LGlawion/spateGAN_ERA5`) published in *npj Climate and Atmospheric Science* (2025).

- **Transformation:** ERA5 precipitation from 24 km / 1 hour → **2 km / 10 minutes**.
- **Training target:** RADKLIM-YW, a rain-gauge-adjusted German radar product; validated against US and Australian radar across diverse climate zones.
- **Input constraints:** requires convective + large-scale precipitation, a minimum spatial extent of 672 × 672 km, and a minimum sequence length of 16 hours.
- **Output:** high-resolution fields in UTM projection (2 km, 10 min) and lat/lon (0.018°, 10 min).
- **Probabilistic:** ensembles are generated by varying the seed and slide parameters, giving a native uncertainty estimate.

> **⭐ Downscaling is this project's key differentiator over IUTF.** IUTF's authors name coarse (31 km) rainfall as a limitation. Replacing it with spateGAN's 2 km / 10 min fields directly closes the spatial-scale mismatch between gridded rainfall and point sensors. Note the domain caveat: spateGAN was trained on German radar, so German UTD19 cities (Augsburg, Bremen, Constance, Darmstadt, Essen, Frankfurt, Hamburg, Kassel, Munich, Speyer, Stuttgart, Wolfsburg) sit in-domain and offer the strongest validation, while non-German cities test out-of-domain generalization.

### 4.5 OpenStreetMap — Network Layer

Road network topology, functional road classification, link geometry and length. Used for map-matching detectors to links, deriving graph structure for the analytics layer, and supplying road-class covariates. (IUTF's OSM-derived centreline network can seed or cross-check this step.)

---

## 5. System Architecture

The system follows a **Lambda architecture** — a batch layer computing authoritative baselines and models over the full historical corpus, a speed layer handling streaming updates, and a serving layer merging both.

```
┌─────────────────────────────────────────────────────────────────────┐
│  SOURCES                                                             │
│  UTD19 CSV    ERA5 netCDF    OSM PBF    IUTF Parquet (validation)   │
└────────┬─────────────┬───────────────┬──────────────┬───────────────┘
         │             │               │              │
┌────────▼─────────────▼───────────────▼──────────────▼───────────────┐
│  L0 — INGESTION                                    HDFS · Kafka      │
│  Bulk load · CDS API pulls · PBF parse · replay-to-stream            │
└────────────────────────────┬────────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────────┐
│  L1 — CURATION                              Spark · Spark SQL        │
│  Schema conformance · unit normalization · UTC alignment ·           │
│  5-min re-binning · error-flag filtering · detector→link             │
│  map-matching · H3 spatial indexing · Parquet, partitioned           │
│  [cross-checked against IUTF harmonised output]                      │
└──────────────┬──────────────────────────────────┬───────────────────┘
               │                                  │
┌──────────────▼──────────────┐   ┌───────────────▼───────────────────┐
│  L2a — TRAFFIC BASELINE     │   │  L2b — WEATHER LAYER              │
│  Free-flow speed            │   │  spateGAN-ERA5 downscaling        │
│  Typical speed profiles     │   │  2 km / 10 min fields             │
│  (DoW × HoD, dry-only)      │   │  Reproject · grid→detector join   │
│  Congestion / delay index   │   │  Rain feature engineering         │
└──────────────┬──────────────┘   └───────────────┬───────────────────┘
               └───────────────┬──────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│  L3 — PREDICTION LAYER                      Spark MLlib · GraphX     │
│  (a) Dose-response model — interpretable elasticities                │
│  (b) Predictive model — GBT baseline → spatio-temporal model         │
│  [benchmarked against IUTF's flow-based dose-response]               │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│  L4 — SERVING & VISUALIZATION           Cassandra · Dashboard        │
│  Live map · corridor analysis · congestion heatmaps · alerts         │
└─────────────────────────────────────────────────────────────────────┘
```

**Execution environment — WSL2, not Docker (settled 2026-08-26).** Every Spark
job runs under WSL2 (Ubuntu) against a venv at `~/.venvs/raincheck`, with the
lake on the Windows `D:` volume via `/mnt/d`. This is a deliberate substitution
for the container runtime the deliverable list originally assumed, on two
grounds: Hadoop's permission calls need `winutils.exe` and the Parquet write
fails on native Windows regardless of configuration (see `config.get_spark`),
so a Linux userspace is required either way; and the host `C:` volume has
under 5 GB free, which will not hold a Docker image store. WSL2 supplies the
Linux userspace without one.

The GPU path depends on this too: spateGAN runs in its own Python 3.13 env at
`vendor/spateGAN_ERA5/.venv` with `torch 2.6.0+cu124`, reaching the RTX 3050
through WSL2's CUDA passthrough.

Reproducibility is therefore packaged as a pinned distro + `requirements.txt`
+ documented setup, not as a Dockerfile. If the project later moves to a real
cluster, `config.LAKE_ROOT` becomes an `hdfs://` URI and the containerization
question reopens on its own terms.

---

## 6. Data Flow and Layer Detail

### Layer 0 — Ingestion

UTD19 arrives as bulk CSV (detector metadata, link metadata, measurements) and is landed raw into HDFS. ERA5 is pulled per city-domain per month via the CDS API as netCDF; note the 672 × 672 km minimum extent means a single ERA5 fetch typically covers a whole metropolitan region with margin. OSM extracts are parsed from PBF. The IUTF Parquet files are loaded read-only into a separate validation namespace — never mixed into the primary pipeline, so that the pipeline's output remains independently derived.

**A note on the streaming component.** Because UTD19 is a historical archive, a naïve design would have no streaming layer at all. The solution is a **replay harness**: a Kafka producer reads curated measurements in timestamp order and republishes them at accelerated wall-clock rate onto a topic. Downstream Spark Streaming consumers cannot distinguish this from a live feed. This is a legitimate and commonly used technique, it exercises the full streaming stack, and it makes the eventual substitution of a genuine live feed a configuration change rather than a rewrite.

### Layer 1 — Curation

Raw → conformed. This stage does the unglamorous work that determines whether everything downstream is trustworthy:

- **Schema conformance** across cities with heterogeneous source formats.
- **Unit normalization** — speeds to km/h, flow to veh/h, occupancy to a 0–1 fraction. **Occupancy is not supplied as the documented fraction.** The audit found medians of 5–9 in Manchester, Torino, Rotterdam, Bolton and Groningen against 0.007 in Essen: those five cities report occupancy as a **percentage** and must be divided by 100 per city, with Essen left as-is. Torino and Rotterdam additionally contain literal `inf` values and rows exceeding 100 on a 0–100 scale; these are unrecoverable and are **dropped, not rescaled**. This rule is load-bearing — L2a conditions free-flow speed on occupancy below critical, and the two-channel decomposition depends entirely on occupancy being comparable across cities.
- **Zero-speed handling** — `speed = 0 AND flow = 0` means *no vehicle was observed*, not *traffic was stopped*, and is set to **NULL before any baseline is computed**; left as a real zero it drags medians and 85th percentiles downward and biases the delay denominator directly. It is not rare: 11.0% of Manchester's speed rows and 27.5% of Bolton's. The separate case `speed = 0 AND flow > 0` is flagged and investigated per city — at 10.8% of rows Rotterdam looks like sensor fault, whereas Manchester's 0.4% is plausibly genuine standstill.
- **Temporal alignment** — local time to UTC (essential, since rainfall grids are in UTC, and daylight-saving transitions must be handled explicitly), and re-binning of 3-minute and 5-minute cities onto a common 5-minute grid.
- **Error-flag handling** — UTD19 ships quality flags; these are applied rather than ignored, and the retention rate is logged per city as a data-quality metric. The encoding is **inconsistent across cities** — three coexist: `NULL`/`1` (Manchester, Torino), `0`/`1` (Essen, Zurich, London, Bremen, Bordeaux), and `NULL`-only (Birmingham, Strasbourg, where the flag is never populated). The filter is therefore `quality_ok = error IS NULL OR error != '1'`; the naïve `WHERE error = 0` would discard **100%** of both Manchester and Torino.
- **Map matching** — detectors to OSM links using the supplied WGS84 coordinates, inheriting road class and link length.
- **Spatial indexing** — H3 or geohash cells assigned to every detector, which later becomes the join key against the rainfall grid.
- **Storage** — columnar Parquet, partitioned by `city / year / month / day`, with predicate pushdown for query efficiency.
- **Validation checkpoint** — the conformed output for any IUTF-covered city is cross-checked against IUTF's harmonised readings to confirm the independent harmonisation (time-zone conversion, aggregation, sensor-to-road matching) reproduces a published reference within tolerance.

### Layer 2a — Traffic Baseline Layer

This layer produces the artefact that is genuinely novel relative to IUTF: **speed** baselines, not flow.

**Free-flow speed** is *not* simply the maximum or the night-time average. It is defined properly as a high percentile (typically the 85th) of observed speed **conditioned on occupancy below the critical occupancy threshold** for that detector. Conditioning on low density is what makes it a free-flow measurement rather than an off-peak one, and it is what makes the resulting delay metric physically interpretable.

**Typical speed profiles** are computed as the median speed per `(detector, day-of-week, hour-of-day)` — **hourly for Manchester and Torino, 30-minute for Essen**. The originally specified 5-minute bin is *not estimable in any city*: with 21 usable days, Manchester and Torino offer only 3.0 recurrences of each weekday, and a 5-min × DoW cell would hold a handful of observations per detector. This does **not** coarsen the rain features. Measurements and downscaled rainfall stay at their native 5-/10-minute resolution and are compared against the hourly baseline *cell*, so rain-onset detection and the trailing-accumulation features survive intact — only the denominator is hourly. Crucially, **these profiles are computed over dry intervals only**. This is the single most important methodological decision in the project: if rainy intervals are included in the baseline, the baseline absorbs the very effect the project is trying to measure, and the estimated rain impact is biased toward zero (by a factor of approximately (1 − p), where p is the fraction of rainy intervals).

**Delay metrics** derived from both: a free-flow delay ratio (congestion irrespective of cause) and a typical-speed deviation (anomaly relative to what this hour normally looks like). The second is the target variable for the prediction layer.

**Two-channel decomposition via occupancy.** Rain affects traffic through two channels that behave oppositely by road state: a free-flow *speed reduction* (largest on fast, uncongested roads) and a *capacity reduction* (worsening already-congested roads). The occupancy variable lets these be separated empirically — rain effect on low-occupancy intervals versus high-occupancy intervals — which is a genuinely publishable decomposition and is only possible because UTD19 exposes raw per-interval occupancy.

**Fallback for speed-less cities.** Where a city reports only flow and occupancy, speed is recoverable through the fundamental-diagram relationship — the analysis UTD19 was originally assembled to support. Scoped as a secondary extension, not a dependency.

### Layer 2b — Weather Layer

ERA5 hourly fields are fed through spateGAN-ERA5 to produce 2 km / 10 min precipitation fields, generated as a small ensemble by varying the seed. Output is reprojected and joined to detectors through the spatial index established in L1. This layer is where the project diverges most sharply from IUTF, which stopped at native 31 km ERA5.

Rainfall is then converted into features that reflect how rain actually affects driving, rather than raw intensity alone:

- **Instantaneous intensity** (mm/h) at the detector cell, banded Light / Moderate / Heavy / Extreme to align with IUTF's published thresholds for direct comparison.
- **Accumulation windows** — 10, 30, 60 minute trailing sums, capturing standing water and drainage saturation.
- **Time since rain onset** — driver adaptation means the first minutes of rain are disproportionately disruptive.
- **Dry-spell antecedent** — hours since last rain, encoding the well-documented "first rain after a dry spell" effect from road-surface oil film.
- **Ensemble spread** — the standard deviation across downscaled members, carried forward as an explicit uncertainty covariate.

### Layer 3 — Prediction Layer

Deliberately split into two models serving two different audiences.

**(a) Dose-response model — for explanation.** A generalized linear model or gradient-boosted tree with interaction terms across `rainfall band × road class × time-of-day × baseline congestion`. Its output is an interpretable elasticity table of the form *"heavy rain on an arterial during evening peak costs X% speed."* This is directly comparable to IUTF's published flow-based dose-response, but expressed in speed/delay terms.

**(b) Predictive model — for operation.** Gradient-boosted trees in Spark MLlib as the honest baseline, extended to a spatio-temporal model (graph-based or recurrent) that exploits network structure. Target: speed-reduction ratio relative to the dry typical-speed profile.

**Inference path.** Forecast rainfall → spateGAN downscaling → identical feature pipeline → predicted per-link delay. Because training features and inference features traverse the same code path, train/serve skew is structurally prevented.

**Validation design** — deliberately harder than a random split:

| Split | Tests |
|---|---|
| Temporal holdout | Generalization forward in time |
| Spatial holdout | Generalization to unseen detectors |
| Event-based | Performance on rain intervals specifically, not diluted by dry majority |
| Cross-city transfer | German (in-domain for spateGAN) → non-German (out-of-domain) |

**Baselines to beat:** historical mean, an identical model with rainfall features ablated, naïve persistence, and — where the city overlaps — IUTF's reported flow-response magnitudes as an external reference. The rain-ablated model is the critical comparison: it isolates the contribution of the entire weather pipeline.

### Layer 4 — Serving and Visualization

Precomputed baselines and predictions are written to Cassandra keyed by `(city, link_id, timestamp_bin)` for low-latency lookup. The dashboard mirrors the capability set of commercial platforms: live network view, congestion heatmaps by time-of-day and corridor, corridor travel-time reliability, anomaly and hotspot detection, and a rain-impact forecast panel showing predicted delay against the current baseline.

---

## 7. Expected Deliverables

1. A reproducible distributed pipeline from raw sources to served predictions, validated against the IUTF benchmark. Reproducibility is delivered via a pinned WSL2 (Ubuntu) environment rather than Docker — see §5.
2. A published free-flow and typical-speed profile dataset for the selected cities (the speed layer IUTF does not provide).
3. A quantified rain dose-response table in speed/delay terms, stratified by road class and time of day.
4. A trained delay-prediction model with documented performance against four baselines, including IUTF.
5. An analytics dashboard demonstrating the operational use case.
6. A written report with reproducibility instructions and full data attribution.

---

## 8. City Selection — Settled by the Phase-1 Audit

The shortlist that previously stood here was ranked on window lengths drawn from third-party UTD19 subsets. The Phase-1 audit ran against the full 134 M-row raw corpus and **overturned most of it**. The selection below is the audited result, not a proposal; the scorecard behind it is in `reports/phase1_gate.md`.

**Selected: Manchester (primary) + Torino (secondary), with Essen as validation anchor.**

| city | role | detectors | usable days | rain events | baseline |
|---|---|---|---|---|---|
| **Manchester** | primary analysis | 181 | 21 (28 present, 7 wholly error-flagged) | 28 | hourly |
| **Torino** | secondary analysis — carries road-class stratification (5 classes ≥ 20 detectors) | 399 | 21 | 10 | hourly |
| **Essen** | validation anchor — cleanest data in UTD19, in-domain for spateGAN | 36 | 35 | 35 | 30-min |

**Why the original Tier 1 collapsed.** Of the four Tier-1 cities, only Torino and Manchester carry speed at all. **London and Toronto have none** — and neither do Marseille, Hamburg, Paris, Melbourne or Bordeaux from Tier 2. Speed exists in just 9 of 39 cities, which is the single most consequential audit finding.

**Why Essen was promoted from "deprioritise".** Planning flagged it as ~14% dense; that figure divided by 188 *calendar* days. Against the 35 days it actually covers it is **94% dense**, with 99.7% quality-flag retention, 0.00% zero-speed rows and 0.00% out-of-range occupancy — every other candidate is 68–86% out of range. It is not an analysis city (36 detectors, no road class reaching 20 cannot support the stratified dose-response), but it is the German in-domain anchor for spateGAN and the only city clean enough to test the L1 curation rules against.

**Dropped:** Rotterdam and Groningen (6 and 2 distinct days — the NL cohort is unusable at any resolution), Bolton (6 days, 20.9% quality retention), Birmingham and Innsbruck (no occupancy), Constance (7 days, no link mapping).

**This resolves the old §8-vs-§9 tension.** §9 wanted German cities for spateGAN in-domain validation while §8 deprioritised them all for short windows. Essen now carries the German validation on genuinely pristine data, while the traffic analysis rests on Manchester and Torino.

**Pooling is now necessary, not optional.** After the audit no single city clears every criterion: Manchester is thin on usable days (21, giving 3.0 weekday recurrences), Torino falls short on rain events (10 against a ≥15 requirement), Essen lacks detectors and road-class spread. The workable design is a **pooled dose-response across all three cities with city fixed effects and a tested city-interaction term** — **73 rain events** in total, enough for stable coefficients — while free-flow and typical-speed baselines stay strictly **per-detector**. Torino's thin event count stops being disqualifying once its coefficient is estimated jointly rather than alone.

Ranking logic, unchanged and now vindicated: **speed availability and usable-day count are hard gates; rain exposure only breaks ties among cities that clear both.**

---

## 9. Risks and Mitigations

| Risk | Severity | Mitigation |
|---|---|---|
| **Speed unavailable in chosen cities** — UTD19 guarantees only two of three variables per city | High | City selection driven by an explicit variable-availability audit *before* pipeline build; fundamental-diagram speed recovery as fallback |
| **Insufficient per-city temporal coverage** — coverage ranges from one day to several months | High | Audit coverage per city first; require a minimum window containing an adequate count of distinct rain events; pool multiple cities to increase event count |
| **Too few rain events for stable estimates** | High | Prioritise cities with high rain-day frequency during their actual window; report confidence intervals, not point estimates |
| **spateGAN out-of-domain error** | Medium | Anchor primary analysis on German cities (in-domain for the training radar); validate that downscaled fields conserve the ERA5 hourly aggregate; carry ensemble spread as an explicit uncertainty feature |
| **No ground-truth radar for validation in some cities** | Medium | Frame downscaled rainfall as a *plausible high-resolution realization*, not a measurement; run an ablation against raw ERA5 (as IUTF used) to demonstrate the downscaling earns its place |
| **Confounding** — rainfall correlates with season, daylight and temperature | Medium | Control explicitly for time-of-day, day-of-week and season; use the dry-only baseline as the counterfactual |
| **"Reinventing IUTF"** — mentor asks why not just use IUTF directly | Medium | IUTF is flow-based, coarse-rainfall, and cross-city-truncated; this project is speed-based, downscaled, and per-city-windowed, and uses IUTF as a validation oracle rather than an input |
| **Dataset smaller than "big data" framing implies** — 134 M rows is tens of GB, not TB | Low | Frame honestly as genuinely distributed but modestly sized; the architecture is what scales. Optionally federate PeMS (18,000+ stations, 2001–2019) to reach true multi-terabyte scale |

---

## 10. Proposed Phasing

| Phase | Focus |
|---|---|
| **1. Feasibility audit** ✅ **complete — gate passed** | Obtained UTD19; audited per-city variable availability, temporal coverage, quality-flag encoding, occupancy scale and rain-event counts across all 134,380,371 rows; selected Manchester + Torino + Essen (§8). Decision document: `reports/phase1_gate.md` |
| **2. Foundation** | L0/L1 — ingest, curate, map-match, index; cross-check against IUTF |
| **3. Baselines** | L2a — free-flow and dry-only typical speed profiles |
| **4. Weather** | L2b — ERA5 acquisition, spateGAN downscaling, spatial join, feature engineering |
| **5. Analysis** ✅ **complete — gate passed** | L3(a) — dose-response quantified across band × road class × time-of-day × congestion, with 95% cluster bootstrap intervals over detector-days. Headline: conditioning on road state **reverses** the sign of the rain effect — free-flowing roads slow, congested roads show rising speed with collapsing flow (the demand channel). Report: `reports/phase5_dose_response.md`. IUTF benchmark done (`reports/phase5_iutf_benchmark.md`): reproduction, not transcription — IUTF publishes no per-band magnitudes. L1 cross-check passed exactly (`reports/phase5_iutf_validation.md`). |
| **6. Prediction** | L3(b) — predictive model, validation, ablations |
| **7. Serving** | L4 — Cassandra, dashboard, streaming replay demonstration |
| **8. Documentation** | Report, reproducibility packaging |

Phase 1 is a hard gate. The single largest risk to this project is discovering, after building the pipeline, that the selected cities lack speed data or contain too few rain events within their actual collection window. **That gate has now been run and passed** — and it justified itself: four of the eight originally shortlisted cities turned out to carry no speed data at all, and the NL cohort has too few distinct days to support any day-of-week profile. Building first would have wasted the effort.

---

## 11. References

- Loder, A., Ambühl, L., Menendez, M. & Axhausen, K.W. (2019). Understanding traffic capacity of urban networks. *Scientific Reports*, 9(1), 16283. https://doi.org/10.1038/s41598-019-51539-5
- Lin, X., Lu, Q., Chen, L. et al. (2025). IUTF Dataset: Enabling Cross-Border Resource for Analysing the Impact of Rainfall on Urban Transportation. *Scientific Data*. https://doi.org/10.1038/s41597-025-06336-3 · Data: https://doi.org/10.6084/m9.figshare.30022807 · Code: https://github.com/viviRG2024/IUTDF_processing
- Glawion, L. et al. (2025). Global spatio-temporal ERA5 precipitation downscaling to km and sub-hourly scale using generative AI. *npj Climate and Atmospheric Science*, 8(1), 219. https://doi.org/10.1038/s41612-025-01103-y
- spateGAN-ERA5 implementation: https://github.com/LGlawion/spateGAN_ERA5
- UTD19 dataset: https://utd19.ethz.ch
- Copernicus Climate Data Store (ERA5): https://cds.climate.copernicus.eu
- Jia, Y., Wu, J. & Xu, M. (2017). Traffic Flow Prediction with Rainfall Impact Using a Deep Learning Method. *Journal of Advanced Transportation*.

---

*Data acknowledgment: This project uses the UTD19 dataset (utd19.ethz.ch) under its academic and non-commercial use terms, the IUTF dataset (CC BY 4.0), and ERA5 reanalysis data from the Copernicus Climate Change Service.*