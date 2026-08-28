# Rain-Aware Urban Traffic Delay Prediction

### A Distributed Pipeline Coupling Multi-City Loop-Detector Traffic States with GAN-Downscaled ERA5 Precipitation

**Project Synopsis — for mentor review**

> **Framing, revised 2026-08-28.** This is a **Big Data specialization
> capstone**. It is assessed on the engineering modules it exercises — data
> integration and processing, data modelling and management, distributed
> machine learning (Spark MLlib), and graph processing (GraphX) — not as a
> standalone scientific contribution. The research question exists to give
> those modules something real to work on; the question is not itself the
> deliverable.
>
> Three consequences, applied throughout this document:
>
> 1. **Claims of superiority over prior work are withdrawn.** The IUTF dataset
>    is used as a **validation oracle and documented benchmark**. This project
>    is **complementary to it, not better than it**.
> 2. **Two known weaknesses are stated up front rather than buried** — the
>    downscaled rainfall has never been checked against a rain gauge (§4.4,
>    §9), and the congestion split conditions on a quantity rain itself changes
>    (§6/L2a, §9).
> 3. **§7 maps every deliverable to the module it evidences**, so coverage of
>    the assessed syllabus is legible without reading the whole document.

---

## 1. Problem Statement

Rainfall measurably degrades urban road performance, but the degradation is not uniform. It varies with rainfall intensity, the road's functional class, the time of day, the level of congestion already present, and — importantly — whether the rain is the first after a dry spell. City traffic authorities currently have almost no quantitative handle on this. Conventional Intelligent Traffic Management Systems (ITMS) instrument only a small fraction of the network with cameras and roadside sensors, so rain-response is measured, if at all, on a handful of corridors.

The scientific building blocks now exist in the open. Loop-detector traffic archives (UTD19), global reanalysis rainfall (ERA5), and a deep-learning precipitation downscaler (spateGAN-ERA5) are all publicly available. A very recent dataset — IUTF, described below — has even pre-joined UTD19 traffic with ERA5 rainfall for 40 cities. What does not yet exist is an open, reproducible pipeline that turns these ingredients into **decontaminated speed baselines** and a **rain-to-delay prediction layer** at high spatio-temporal resolution.

**What this project adds:** existing open resources quantify rainfall's effect on traffic *flow* against coarse (~31 km, hourly) rainfall, and derive no speed baselines from the speed data they carry. This project builds the missing pieces — free-flow and typical-speed baselines computed on dry intervals only, GAN-downscaled 2 km / 10 min rainfall joined at the detector, and effects reported as cluster-robust intervals rather than point estimates.

That is an **additive** statement, and it is deliberately no longer a comparative one. Two earlier framings have been withdrawn: the claim that per-city windows were a differentiator (false — see §3), and the claim that this pipeline is superior to IUTF (unsupportable — IUTF is a peer-reviewed, curated, 40-city resource, and this project runs on three cities and has no ground truth for its rainfall). The honest position is that the two are **complementary**: IUTF covers breadth with coarse rainfall and no derived speed layer; this project covers depth on three cities with fine rainfall and a derived speed layer, and uses IUTF to check its own correctness.

---

## 2. Objectives

| # | Objective |
|---|---|
| **O1** | Build a distributed ingestion and curation pipeline for multi-city loop-detector traffic data (from raw UTD19, validated against IUTF) |
| **O2** | Derive per-detector **free-flow speed** and **typical speed profiles** by day-of-week × hour-of-day, decontaminated of rainfall effects |
| **O3** | Downscale coarse global reanalysis rainfall to 2 km / 10 min resolution and spatially join it to the traffic network |
| **O4** | Quantify the **dose-response relationship** between rainfall intensity and speed degradation, stratified by road class and time of day |
| **O5** | Measure what the weather pipeline is worth, via a **rain-ablation experiment** in Spark MLlib — identical models with and without the rainfall features, across four holdout designs *(revised 2026-08-28; previously "train and serve a prediction layer that maps forecast rainfall to expected delay" — the operational framing is dropped, see §6/L3b)* |
| **O6** | Derive **network structure** from the road graph in GraphX — delay propagation to adjacent links, centrality covariates, corridor identification |
| **O7** | Expose results through an analytics dashboard demonstrating the use case |

---

## 3. Relationship to Prior Work: The IUTF Dataset

A dataset published while this project was being scoped — **IUTF (Integrated Urban Traffic-Flood)**, *Nature Scientific Data*, accepted November 2025 — is close enough to this project's foundation that it must be addressed head-on. Understanding exactly what IUTF does and does not do is what defines this project's contribution.

**What IUTF is.** An open-access (CC BY 4.0) resource, ~1.61 GB in Apache Parquet and NumPy NPZ formats, that fuses three sources into one aligned package across 40 cities in Europe, North America, and Asia:
- Traffic parameters from ~21,700 UTD19 sensors (raw 5-minute intervals, harmonised to hourly);
- Hourly ERA5 precipitation, spatially attributed to the road network;
- OpenStreetMap topology for over 1 million road segments.

IUTF has already solved parts of what this project's Layer 1 and Layer 2b would otherwise build from scratch: time-zone/DST conversion to UTC, 5-minute-to-hourly aggregation, sensor-to-road-segment matching, and area-weighted rainfall attribution. It also validated that the joined data reveals a **dose-response relationship**, binning rainfall as Light (<0.5), Moderate (0.5–4), Heavy (4–10), and Extreme (>10 mm/hr) per Met Office definitions.

**Where IUTF stops, and what this project adds on top.** Two gaps define the contribution — stated as gaps IUTF leaves open, not as faults in IUTF:

> **Re-audited 2026-08-27 against the downloaded files, not the paper.** Two of the three limitations below were wrong as originally written and have been corrected. See `reports/phase5_iutf_validation.md`. The correction narrows the contribution but does not remove it: limitation 2 is the substantive one and it is intact.

1. **No derived speed layer** (originally, and too strongly, "flow, not speed"). IUTF's `5min_readings.parquet` *does* carry a `speed` column — the raw UTD19 columns are passed through intact. What is true is that IUTF's published *validation* is built on flow change, and that IUTF derives nothing from speed. Free-flow speed conditioned on critical occupancy, dry-only typical-speed profiles, and the delay metrics built on them are absent from IUTF and are this project's own work (L2a). The contribution is the derived baseline layer, not the presence of the column.
2. **Coarse rainfall.** IUTF uses ERA5 at its native ~31 km hourly resolution and explicitly flags the spatial-scale mismatch against point sensors as a limitation. This project's spateGAN downscaling to 2 km / 10 min is a direct fix for the weakness IUTF names in its own paper. **Confirmed against the files** — IUTF's per-city `grid_info.parquet` puts the whole of Manchester in a single 0.25° cell, so within-city rainfall variation there is exactly zero. This is the substantive addition — though note it is an addition whose *correctness* is itself unverified, since the 2 km fields have no ground truth (§4.4).
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

> **Downscaling is the substantive addition over IUTF — and it is unvalidated.** IUTF's authors name coarse (31 km) rainfall as a limitation of their own resource. Replacing it with spateGAN's 2 km / 10 min fields addresses the spatial-scale mismatch between gridded rainfall and point sensors, and it demonstrably adds spatial information: at 31 km, Manchester and Essen each collapse to a single cell, so within-city rainfall variation is exactly zero, whereas at 2 km detectors within a city disagree on the rainfall band on 67–93% of wet timestamps.
>
> **⚠️ The critical caveat, and it is not a footnote.** These fields have **never been compared against a rain gauge or radar observation**. They are plausible high-resolution realisations conditioned on ERA5, not measurements. Nothing in this project establishes that the 2 km field is *correct* at any given detector — only that it varies where the coarse field could not, and that the variation carries signal (§6/L3, Phase 4). Until gauge records are obtained from the Environment Agency, ARPA Piemonte and DWD, every downscaled-rainfall result must be read as conditional on the downscaler being right. This is the single largest outstanding weakness in the project and is tracked as such in §9 and §7/D7.
>
> Domain caveat on top of that: spateGAN was trained on German radar, so German UTD19 cities (Augsburg, Bremen, Constance, Darmstadt, Essen, Frankfurt, Hamburg, Kassel, Munich, Speyer, Stuttgart, Wolfsburg) sit in-domain, while non-German cities test out-of-domain generalization. Of the three study cities only **Essen** is in-domain; Manchester and Torino are not.

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

> **What of the diagram above is actually built (as of 2026-08-28).** The
> diagram is the *target* architecture and should not be read as a status
> report. Built and gate-passed: **L0 batch ingestion, L1 curation, L2a
> baselines, L2b weather, and L3(a) dose-response.** Not built: **L0's Kafka
> replay harness, L3(b), the GraphX layer, and the whole of L4.** Two
> substitutions are also in force and are deliberate — the lake sits on a
> partitioned local filesystem under `lake/` rather than HDFS, and the runtime
> is WSL2 rather than a container (below). `config.LAKE_ROOT` is the single
> point that becomes an `hdfs://` URI if the project moves to a real cluster.
>
> Two labels inside the diagram are also superseded. L3(b) is no longer
> "GBT baseline → spatio-temporal model" but a **rain-ablation experiment**
> (§6/L3b), and the note "benchmarked against IUTF's flow-based dose-response"
> overstates what is possible, since **IUTF publishes no per-band magnitudes**
> (§3, `reports/phase5_iutf_benchmark.md` §1).

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

**Two-channel decomposition via occupancy.** Rain affects traffic through two channels that behave oppositely by road state: a free-flow *speed reduction* (largest on fast, uncongested roads) and a *capacity reduction* (worsening already-congested roads). The occupancy variable lets these be separated empirically — rain effect on low-occupancy intervals versus high-occupancy intervals — and it is only possible because UTD19 exposes raw per-interval occupancy.

> **⚠️ Known defect in how this split is currently computed (raised 2026-08-27, not yet fixed).** The split uses the occupancy **observed during the rainy interval itself**. But rain changes occupancy — that is the demand channel, and Phase 5 measures it directly: vehicle counts fall 3.8–9.4% under rain on congested roads. Splitting on a quantity the treatment itself moves is not a clean stratification. Intervals can be sorted into the "congested" or "free-flowing" group *because* it rained, so part of the measured contrast between the two groups is selection, not response.
>
> This does not invalidate the finding that the two channels oppose each other — that is visible directly in the paired speed and flow columns, which need no split at all. It does mean the **magnitudes** in the split table cannot be read as clean causal effects.
>
> **The specified fix:** stratify on each detector's **expected** state for that weekday and hour, read from its own dry typical profile, rather than on the state realised during the rainy interval. Expected state is fixed before the rain arrives and so cannot be moved by it. The dry profiles this needs already exist from Phase 3. Both versions should then be reported side by side, since the gap between them measures how much of the original result was selection. Tracked in §7/D8 and §10 Phase 5R.

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

**(b) Predictive model — recast as a rain ablation.** Originally scoped as an operational forecasting service. That framing is dropped: this project has no live feed, no gauge-validated rainfall, and three cities, so a delay-forecasting *product* is not a claim it can support. The model is retained — Spark MLlib is an assessed module and the pipeline needs a modelling layer — but its purpose is now **measurement, not operation**.

The experiment is a **rain ablation**: gradient-boosted trees in Spark MLlib predicting speed deviation against the dry typical profile, trained twice on identical rows, splits and hyperparameters, differing only in whether the rainfall features are present. The contrast between the two isolates the contribution of the entire weather pipeline — acquisition, downscaling, spatial join and feature engineering — in a single number.

This framing is strictly stronger for the project's purposes. It exercises the same MLlib surface (feature assembly, tree ensembles, cross-validation, distributed training) while answering a question the data can actually settle: **did the downscaling earn its cost?** If the ablated model matches the full model, the weather layer bought nothing, and that is a publishable finding rather than a failure. Target remains the speed-reduction ratio relative to the dry typical-speed profile.

**(c) Graph layer — network structure via GraphX.** Assessed module, currently unbuilt (§10 Phase 6b). The raw material is on disk: `links.csv` carries 140,859 link geometries with city codes, and every detector carries a `linkid`. The intended construction is a per-city road graph with links as vertices and shared endpoints as edges, supporting (i) propagation of rain-induced delay to adjacent links, (ii) connected-component and centrality measures used as model covariates, and (iii) corridor identification for the serving layer. Scoping this explicitly matters because no reviewer flagged its absence, which is precisely why it could have been missed.

**Inference path.** Forecast rainfall → spateGAN downscaling → identical feature pipeline → predicted per-link delay. Because training features and inference features traverse the same code path, train/serve skew is structurally prevented.

**Validation design** — deliberately harder than a random split:

| Split | Tests |
|---|---|
| Temporal holdout | Generalization forward in time |
| Spatial holdout | Generalization to unseen detectors |
| Event-based | Performance on rain intervals specifically, not diluted by dry majority |
| Cross-city transfer | German (in-domain for spateGAN) → non-German (out-of-domain) |

**Baselines to beat:** historical mean, an identical model with rainfall features ablated, and naïve persistence. The rain-ablated model is the critical comparison: it isolates the contribution of the entire weather pipeline.

~~and — where the city overlaps — IUTF's reported flow-response magnitudes as an external reference.~~ **Struck 2026-08-28: IUTF publishes no per-band magnitudes**, so it cannot serve as a numeric baseline. It remains a *reproduction* benchmark — its own shipped files run through this project's estimator — rather than a performance one. See `reports/phase5_iutf_benchmark.md` §1 and §7/D5.

### Layer 4 — Serving and Visualization

Precomputed baselines and predictions are written to Cassandra keyed by `(city, link_id, timestamp_bin)` for low-latency lookup. The dashboard mirrors the capability set of commercial platforms: live network view, congestion heatmaps by time-of-day and corridor, corridor travel-time reliability, anomaly and hotspot detection, and a rain-impact forecast panel showing predicted delay against the current baseline.

---

## 7. Expected Deliverables — Mapped to Assessed Modules

Restructured 2026-08-28 so that syllabus coverage is legible without reading the
whole document. **Module** names the specialization component each deliverable
evidences; deliverables that evidence no module are marked as such and justify
themselves on other grounds.

| # | Deliverable | Module evidenced | Status |
|---|---|---|---|
| **D1** | Reproducible distributed pipeline, raw sources → analysis tables, on a pinned WSL2 environment rather than Docker (§5) | **Integration & Processing** | ✅ done |
| **D2** | Partitioned columnar lake with per-city curation rules, quality-flag and unit normalisation, UTC alignment; prior-art data held read-only in a separate namespace | **Modelling & Management** | ✅ done |
| **D3** | Free-flow and dry-only typical-speed profile dataset per detector — the derived speed layer IUTF does not provide | **Integration & Processing** | ✅ done |
| **D4** | Rain dose-response table in speed and flow terms, stratified by band × road class × time of day × road state, with cluster-bootstrap intervals | *(analysis; no single module)* | ✅ done, **pending D8** |
| **D5** | **Rain-ablation experiment** in Spark MLlib — identical model with and without the weather features, across four holdout designs | **Spark MLlib** | ⬜ not started |
| **D6** | Per-city road graph in GraphX — delay propagation to adjacent links, centrality covariates, corridor identification | **GraphX** | ⬜ not started |
| **D7** | **Gauge validation of the downscaled rainfall** against Environment Agency / ARPA Piemonte / DWD records | *(scientific validity)* | ⬜ deferred — see §9 |
| **D8** | **Corrected congestion split** stratifying on expected rather than realised road state, reported beside the original | *(scientific validity)* | ⬜ not started |
| **D9** | Serving layer and analytics dashboard — demonstration of the operational use case, not a production service | **Modelling & Management** | ⬜ not started |
| **D10** | Streaming replay harness — Kafka producer replaying curated readings at accelerated rate | **Integration & Processing** | ⬜ not started |
| **D11** | Final report with reproducibility instructions and full data attribution | — | 🟡 per-phase reports done |

**Two deliverables carry no module and are kept anyway.** D7 and D8 are the two
weaknesses independent review converged on. D8 is cheap and should be done; D7 is
expensive and is honestly deferred, but the project must state what it cannot
claim without it (§4.4, §9).

**Dropped from the original list:** "a trained delay-prediction model with
documented performance against four baselines, **including IUTF**." IUTF publishes
no per-band magnitudes, so it cannot serve as a numeric baseline — see
`reports/phase5_iutf_benchmark.md` §1. It remains a *reproduction* benchmark
(D1/D3) rather than a performance one.

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

### How many storms actually underpin each rainfall band

Added 2026-08-28. The 73-event figure above is counted on the **coarse** city-hour
mask (Manchester 28, Torino 10, Essen 35). Counted on the **2 km detector-level**
fields the picture is more generous, because storms that missed a city's average
still hit individual detectors:

| city | coarse events | **2 km storms** |
|---|---|---|
| essen | 35 | **56** |
| manchester | 28 | **40** |
| torino | 10 | **24** |
| **total** | **73** | **120** |

The number that actually matters is not the total but **how many independent
storms sit behind each band**, since the bootstrap intervals are only as
trustworthy as the count of independent episodes underneath them:

| band | independent city-storms | intervals |
|---|---|---|
| Moderate | 111 | — |
| Heavy | 73 | — |
| **Extreme** | **27** | **2,130** |

**This is stated because it bounds the Extreme-band claims.** Twenty-seven storms
is not many. The confidence intervals reported in Phase 5 already reflect it —
which is why the Extreme cells are wide and several are marked `ns` — but a reader
scanning the effect table sees only the intervals, not the episode count behind
them. The bands are not equally well supported and should not be read as though
they were.

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
| **"Reinventing IUTF"** — mentor asks why not just use IUTF directly | Medium | IUTF derives no speed layer and stops at 31 km rainfall; this project adds both, and uses IUTF as a validation oracle rather than an input. Note the claim of *cross-city-truncated windows* previously stated here was **false and is withdrawn** (§3) — the answer rests on the derived speed layer and the downscaling alone. The pipeline is also itself the assessed deliverable (§7), so building rather than importing is the point |
| **Downscaled rainfall is never validated against observation** | **High** | ⚠️ **Unmitigated.** No radar or gauge ground truth has been obtained for any study city, so the 2 km fields are plausible realisations, not measurements. Partial cover only: the fields conserve the ERA5 hourly aggregate, and the D5 ablation shows whether they carry signal. Neither establishes correctness. Every downscaled result is conditional on the downscaler. Fix is D7 |
| **Congestion split conditions on a quantity rain itself changes** | **High** | ⚠️ **Known defect, fix specified but not applied.** Splitting on occupancy observed during the rainy interval mixes selection into the contrast (§6/L2a). The paired speed-and-flow reporting is unaffected and carries the headline finding; the split *magnitudes* are what is compromised. Fix is D8 — stratify on expected state from the dry profile, report both |
| **Assessed modules left unbuilt** — MLlib and GraphX are graded components | **High** | Both now have explicit deliverables (D5, D6) and phases (6a, 6b) rather than sitting implicit inside "prediction layer". GraphX was at 0% and was flagged by no reviewer, which is exactly how a graded module gets missed |
| **Dataset smaller than "big data" framing implies** — 134 M rows is tens of GB, not TB | Low | Frame honestly as genuinely distributed but modestly sized; the architecture is what scales. Optionally federate PeMS (18,000+ stations, 2001–2019) to reach true multi-terabyte scale |

---

## 10. Proposed Phasing

| Phase | Focus |
|---|---|
| **1. Feasibility audit** ✅ **complete — gate passed** | Obtained UTD19; audited per-city variable availability, temporal coverage, quality-flag encoding, occupancy scale and rain-event counts across all 134,380,371 rows; selected Manchester + Torino + Essen (§8). Decision document: `reports/phase1_gate.md` |
| **2. Foundation** ✅ **complete — gate passed** | L0/L1 — 2,875,844 rows curated across three cities; per-city occupancy rescaling confirmed against a plausibility band rather than assumed; quality-flag, zero-speed and UTC rules applied; detector join complete with zero unmatched. Report: `reports/phase2_curation.md`. **Not built: the Kafka replay harness (D10)** — the batch path is done, the streaming path is not |
| **3. Baselines** ✅ **complete — gate passed** | L2a — free-flow speed for 99.4–100% of detectors, sitting 6.5–8.0 km/h above the uncongested median; dry-only typical profiles; both delay metrics. Report: `reports/phase3_baselines.md`. §2 of that report carries a **falsified explanation, struck through in place** with the disproving evidence beside it |
| **4. Weather** ✅ **complete — gate passed** | L2b — ERA5 acquired, spateGAN downscaling run on the local GPU, reprojected and joined to every detector. Downscaling demonstrably adds spatial information: detectors within a city disagree on band on 67–93% of wet timestamps, against exactly zero variation at 31 km. Report: `reports/phase4_downscaling.md`. **One ensemble member only**, so the uncertainty covariate §L2b specifies does not exist |
| **5. Analysis** ✅ **complete — gate passed** | L3(a) — dose-response quantified across band × road class × time-of-day × congestion, with 95% cluster bootstrap intervals over detector-days. Headline: conditioning on road state **reverses** the sign of the rain effect — free-flowing roads slow, congested roads show rising speed with collapsing flow (the demand channel). Report: `reports/phase5_dose_response.md`. IUTF benchmark done (`reports/phase5_iutf_benchmark.md`): reproduction, not transcription — IUTF publishes no per-band magnitudes. L1 cross-check passed exactly (`reports/phase5_iutf_validation.md`). **⚠️ The road-state split carries a known defect** — it conditions on a quantity rain itself changes (§6/L2a). The paired speed-and-flow finding is unaffected; the split magnitudes are. Fix is Phase 5R. |
| **5R. Analysis correction** | D8 — re-run the road-state split on **expected** state from the dry profile, report beside the original. Cheap; the profiles already exist |
| **6a. Prediction — rain ablation** | D5 — L3(b) recast: identical Spark MLlib model with and without weather features, across the four holdout designs. **Assessed module: Spark MLlib** |
| **6b. Graph layer** | D6 — per-city road graph from `links.csv`, delay propagation, centrality covariates, corridor identification. **Assessed module: GraphX.** Currently 0% |
| **7. Serving** | D9/D10 — L4 storage, dashboard, streaming replay demonstration |
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