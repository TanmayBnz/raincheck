# Rain-Aware Urban Traffic Delay Prediction

### A Distributed Big Data Pipeline Coupling Multi-City Loop-Detector Traffic States with GAN-Downscaled ERA5 Precipitation

**Project Synopsis — for mentor review**

---

## 0. Build Status — What the Implementation Actually Found

*Added after building L0–L3 on real data. This section supersedes any claim below that contradicts it.*

**Phase 1 (feasibility audit) is complete, and it invalidated the study-city plan in §4.3.**

| Finding | Detail |
|---|---|
| Corpus size | `utd19_u.csv` holds **134,380,371 rows across 39 cities**, not "~170 M / 40" |
| Speed availability | Only **9 of 39 cities** report speed at all; within those it is ~100% of rows |
| German cities | **Every large German city has zero speed** — Hamburg, Darmstadt, Bremen, Augsburg, Munich, Stuttgart, Kassel, Speyer, Wolfsburg, Frankfurt. Only Essen (36 detectors) and Constance (7 days) carry speed |
| Study set | manchester, rotterdam, bolton, groningen, birmingham, essen — the six speed-bearing cities sharing 2017-09-08 → 2017-11-18 |
| Curated corpus | 1,395,565 rows, ~7,600 detector-days, **40 distinct calendar days**, 9 road classes |
| Concurrency | Cities are **not** simultaneous: no day has more than 3 cities. Two clusters — NL/DE in September, UK in November |
| Occupancy | Reported as **percent** in manchester/bolton/rotterdam/groningen, as a fraction in essen, and **absent entirely in birmingham** |
| Error flags | Three-state (0/1/null). Outside Essen **no city has any explicitly-clean row**, so the policy is "drop 1, keep 0 and null" |
| Rain intensity | ERA5 at 28 km yields max **4.26 mm/h** across the whole corpus. The `very_heavy` band is empty and `heavy` contains a single hour, which is unusable (weekend, baseline too thin) |
| Dose-response | Differenced against no-rain within (road class × time of day): light rain **−1.8 pp**, moderate **+1.1 pp**. Partially ordered, and light rain implies *faster* traffic — not physical |
| Rain ablation | RMSE gain from rain features: temporal +0.9%, cross-city +3.0%, **event-based −2.8%**, **spatial −14.4%**. The GBT also loses to the historical mean on 2 of 3 splits |

**Conclusion: at raw ERA5 resolution the rain features do not earn their place.** Target noise (sd = 0.334) dwarfs the effect size (~0.01–0.02), so per-observation prediction is hopeless even where the pooled means are ordered. This is the empirical ablation argument *for* spateGAN downscaling, and for a longer corpus — not a result to bury.

**TomTom was evaluated as a substitute and rejected**: Flow Segment Data has no date parameter (live only, so no route to 2017), and Traffic Stats returns date-range aggregates with rain averaged in — the same defect §3 identifies in Google's `staticDuration` — under enterprise licensing. It remains a display-only layer per §4.5.

---

## 1. Problem Statement

Rainfall measurably degrades urban road performance, but the degradation is not uniform. It varies with rainfall intensity, the road's functional class, the time of day, the level of congestion already present, and — importantly — whether the rain is the first after a dry spell. City traffic authorities currently have almost no quantitative handle on this. Conventional Intelligent Traffic Management Systems (ITMS) instrument only a small fraction of the network with cameras and roadside sensors, so rain-response is measured, if at all, on a handful of corridors.

Commercial platforms have begun solving this. Google's *Roads Management Insights* (RMI) streams aggregated probe data from Google Maps into BigQuery and Pub/Sub, and vendor platforms such as Lepton Software's **TraffiCure** wrap it in a city-wide dashboard. But these are closed systems: RMI access is restricted to verified public-sector entities and infrastructure managers, and is jurisdiction-locked to territory the customer officially manages.

**The gap this project addresses:** there is no open, reproducible, end-to-end big-data pipeline that quantifies and predicts rain-induced traffic delay at network scale. This project builds one.

---

## 2. Objectives

| # | Objective |
|---|---|
| **O1** | Build a distributed ingestion and curation pipeline for multi-city loop-detector traffic data |
| **O2** | Derive per-detector **free-flow speed** and **typical speed profiles** by day-of-week × hour-of-day, decontaminated of rainfall effects |
| **O3** | Downscale coarse global reanalysis rainfall to 2 km / 10 min resolution and spatially join it to the traffic network |
| **O4** | Quantify the **dose-response relationship** between rainfall intensity and speed degradation, stratified by road class and time of day |
| **O5** | Train and serve a **prediction layer** that maps forecast rainfall to expected delay |
| **O6** | Expose results through a TraffiCure-style analytics dashboard |

---

## 3. Justification of Data Strategy

*(Anticipating the first question: "why not just use the Google Maps APIs?")*

The obvious approach — polling Google's Routes API on a schedule to accumulate a speed history — was investigated and **rejected on three independent grounds**:

1. **Licensing.** Google Maps Platform Terms of Service §3.2.3 prohibits pre-fetching, indexing, or storing Google Maps Content, and separately prohibits *creating content based on* it. The narrow caching exception requires storage under 30 days and explicitly forbids aggregating the content. Building day-of-week × hour-of-day speed profiles violates all three clauses.
2. **Data shape.** The Routes API returns traffic as a three-level ordinal (`NORMAL` / `SLOW` / `TRAFFIC_JAM`), not a numeric speed. Its `staticDuration` field is a *historical-typical* baseline, not a free-flow baseline — meaning rain is already averaged into the denominator, which structurally contaminates any rain-delay estimate.
3. **Cost.** Traffic-aware requests bill under the Pro SKU at roughly $10 per 1,000 calls. Polling 500 corridors at 5-minute cadence is ≈4.3 M calls/month ≈ **$43,000/month**.

The Roads API was also evaluated and found to contain no traffic observations at all — it provides map-matching (Snap-to-Roads, Nearest-Roads) and posted speed limits, the latter gated behind an Asset Tracking licence.

**Conclusion:** the project uses openly licensed research data for the analytical corpus, and reserves commercial APIs strictly for optional live display, where no storage occurs.

---

## 4. Data Sources

### 4.1 UTD19 — Traffic Layer (Primary)

Collected by the Institute for Transport Planning and Systems, ETH Zürich, in a research campaign spanning 2017–2019.

| Property | Value |
|---|---|
| Detectors | 23,541 stationary loop detectors |
| Cities | 40 documented; **39 present** in `utd19_u.csv` |
| Data rows | ~170 million *(the distributed `utd19_u.csv` contains 134,380,371)* |
| Vehicles detected | ~4.9 billion |
| Temporal resolution | 3–5 minute aggregation intervals |
| Coverage | 3.8 years total |
| Geocoding | All detectors and associated roads in WGS84 |
| Quality | Error-flagged, standardized schema |
| Licence | Free for academic / non-commercial use, on sign-up |

**Core variables:** vehicle flow, detector occupancy, and speed. **Critical caveat (see §9):** not every city carries all three. The dataset guarantees *at least two of the three* fundamental traffic variables per city, so speed is not universally available, and per-city temporal coverage varies widely — from a single day in some cities to several months in others. City selection is therefore a design decision, not an afterthought.

**Attribution requirement:** publications must cite Loder, A., Ambühl, L., Menendez, M. & Axhausen, K.W. (2019), *Understanding traffic capacity of urban networks*, Scientific Reports 9(1) 16283, and acknowledge UTD19 (utd19.ethz.ch).

### 4.2 ERA5 — Weather Layer (Primary)

ECMWF's global atmospheric reanalysis, distributed free through the Copernicus Climate Data Store.

- **Native resolution:** ~24–31 km grid, hourly, global, extending back to 1940 — comfortably covering the 2017–2019 UTD19 window.
- **Variables required by the downscaler:** convective precipitation and large-scale precipitation.
- **Additional variables for feature engineering:** total precipitation, 2 m temperature, 2 m dewpoint, 10 m wind components (to distinguish rain type and detect freezing conditions).
- **Access:** CDS API, delivered as netCDF.

### 4.3 spateGAN-ERA5 — Downscaling Layer

An open-source conditional GAN (`LGlawion/spateGAN_ERA5`) published in *npj Climate and Atmospheric Science* (2025).

- **Transformation:** ERA5 precipitation from 24 km / 1 hour → **2 km / 10 minutes**.
- **Training target:** RADKLIM-YW, a rain-gauge-adjusted German radar product; validated against US and Australian radar across diverse climate zones.
- **Input constraints:** requires convective + large-scale precipitation, a minimum spatial extent of 672 × 672 km, and a minimum sequence length of 16 hours.
- **Output:** high-resolution fields in UTM projection (2 km, 10 min) and lat/lon (0.018°, 10 min).
- **Probabilistic:** ensembles are generated by varying the seed and slide parameters, giving a native uncertainty estimate.

> **⭐ Key design synergy — NOT SUPPORTED BY THE DATA. See §0.** The audit found that none of the twelve German cities reports speed; the argument below only holds for a flow/occupancy-based analysis via fundamental-diagram speed recovery, which is not yet built.
>
> *(original claim retained for the record)* **Key design synergy.** spateGAN-ERA5 was trained on German gauge-adjusted radar. UTD19 contains twelve German cities (Augsburg, Bremen, Constance, Darmstadt, Essen, Frankfurt, Hamburg, Kassel, Munich, Speyer, Stuttgart, Wolfsburg). Restricting the primary study cities to Germany places the downscaler squarely **in-domain**, which is the strongest available defence against the criticism that GAN-generated rainfall is unverifiable. Non-German cities (London, Manchester, Birmingham, Rotterdam, Utrecht, Paris, Toronto) then serve as an out-of-domain transfer test rather than as the primary evidence base.

### 4.4 OpenStreetMap — Network Layer

Road network topology, functional road classification, link geometry and length. Used for map-matching detectors to links, deriving graph structure for the analytics layer, and supplying road-class covariates.

### 4.5 Live Traffic Feed — Demonstration Layer (Optional)

For the dashboard's live view only. TomTom's Flow Segment Data endpoint returns `currentSpeed`, `freeFlowSpeed`, `currentTravelTime`, `freeFlowTravelTime` and `confidence` — exactly the target schema. **Display-only, never persisted**, since TomTom's terms likewise prohibit caching or storing results. This layer is architecturally isolated so the project remains fully functional without it.

---

## 5. System Architecture

The system follows a **Lambda architecture** — a batch layer computing authoritative baselines and models over the full historical corpus, a speed layer handling streaming updates, and a serving layer merging both.

```
┌─────────────────────────────────────────────────────────────────────┐
│  SOURCES                                                             │
│  UTD19 CSV      ERA5 netCDF      OSM PBF      [Live API]            │
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
└──────────────────────────────┬──────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│  L4 — SERVING & VISUALIZATION           Cassandra · Dashboard        │
│  Live map · corridor analysis · congestion heatmaps · alerts         │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 6. Data Flow and Layer Detail

### Layer 0 — Ingestion

UTD19 arrives as bulk CSV (detector metadata, link metadata, measurements) and is landed raw into HDFS. ERA5 is pulled per city-domain per month via the CDS API as netCDF; note the 672 × 672 km minimum extent means a single ERA5 fetch typically covers a whole metropolitan region with margin. OSM extracts are parsed from PBF.

**A note on the streaming component.** Because UTD19 is a historical archive, a naïve design would have no streaming layer at all — which would forfeit a substantial portion of the course syllabus. The solution is a **replay harness**: a Kafka producer reads curated measurements in timestamp order and republishes them at accelerated wall-clock rate onto a topic. Downstream Spark Streaming consumers cannot distinguish this from a live feed. This is a legitimate and commonly used technique, it exercises the full streaming stack, and it makes the eventual substitution of a genuine live feed a configuration change rather than a rewrite.

### Layer 1 — Curation

Raw → conformed. This stage does the unglamorous work that determines whether everything downstream is trustworthy:

- **Schema conformance** across cities with heterogeneous source formats.
- **Unit normalization** — speeds to km/h, flow to veh/h, occupancy to a 0–1 fraction.
- **Temporal alignment** — local time to UTC (essential, since rainfall grids are in UTC), and re-binning of 3-minute and 5-minute cities onto a common 5-minute grid.
- **Error-flag handling** — UTD19 ships quality flags; these are applied rather than ignored, and the retention rate is logged per city as a data-quality metric.
- **Map matching** — detectors to OSM links using the supplied WGS84 coordinates, inheriting road class and link length.
- **Spatial indexing** — H3 or geohash cells assigned to every detector, which later becomes the join key against the rainfall grid.
- **Storage** — columnar Parquet, partitioned by `city / year / month / day`, with predicate pushdown for query efficiency.

### Layer 2a — Traffic Baseline Layer

This layer produces the artefact the commercial platforms sell, computed from open data.

**Free-flow speed** is *not* simply the maximum or the night-time average. It is defined properly as a high percentile (typically the 85th) of observed speed **conditioned on occupancy below the critical occupancy threshold** for that detector. Conditioning on low density is what makes it a free-flow measurement rather than an off-peak one, and it is what makes the resulting delay metric physically interpretable.

**Typical speed profiles** are computed as the median speed per `(detector, weekend-flag, hour-of-day)`. *(Revised from `(detector, day-of-week, 5-min bin)`: with 21–23 days per city that grain leaves ~3 observations per cell, versus 150–250 here. Cells resting on fewer than 30 dry observations yield no deviation at all — before that guard, single-observation baselines produced deviations of −63%.)* Crucially, **these are computed over dry intervals only**. This is the single most important methodological decision in the project: if rainy intervals are included in the baseline, the baseline absorbs the very effect the project is trying to measure, and the estimated rain impact is biased toward zero. This is precisely the flaw that made Google's `staticDuration` unsuitable.

**Delay metrics** derived from both: a free-flow delay ratio (congestion irrespective of cause) and a typical-speed deviation (anomaly relative to what this hour normally looks like). The second is the target variable for the prediction layer.

**Fallback for speed-less cities.** Where a city reports only flow and occupancy, speed is recoverable through the fundamental diagram relationship — which is, not coincidentally, the analysis UTD19 was originally assembled to support. This is scoped as a secondary extension rather than a dependency.

### Layer 2b — Weather Layer

ERA5 hourly fields are fed through spateGAN-ERA5 to produce 2 km / 10 min precipitation fields, generated as a small ensemble by varying the seed. Output is reprojected and joined to detectors through the spatial index established in L1.

Rainfall is then converted into features that reflect how rain actually affects driving, rather than raw intensity alone:

- **Instantaneous intensity** (mm/h) at the detector cell.
- **Accumulation windows** — **1, 3, 6 hour** trailing sums, capturing standing water and drainage saturation. *(Revised from 10/30/60 minutes: raw ERA5 is hourly, so sub-hourly windows are unrecoverable without spateGAN.)*
- **Time since rain onset** — driver adaptation means the first minutes of rain are disproportionately disruptive.
- **Dry-spell antecedent** — hours since last rain, encoding the well-documented "first rain after a dry spell" effect from road-surface oil film.
- **Ensemble spread** — the standard deviation across downscaled members, carried forward as an explicit uncertainty covariate. *(**Not implemented**: it requires spateGAN's multi-seed members. Omitted rather than faked.)*
- **Intensity banding** — categorical none / light / moderate / heavy / very heavy bins for the interpretable model. *(In practice `very_heavy` is empty and `heavy` holds one unusable hour — ERA5's 28 km hourly means smooth away every extreme.)*

### Layer 3 — Prediction Layer

Deliberately split into two models serving two different audiences.

**(a) Dose-response model — for explanation.** A generalized linear model or gradient-boosted tree with interaction terms across `rainfall band × road class × time-of-day × baseline congestion`. Its output is an interpretable elasticity table of the form *"heavy rain on an arterial during evening peak costs X% speed."* This is the artefact a traffic authority would actually act on, and it is directly comparable to published literature, which reports MAPE improvements in the region of 4–5% from adding rainfall to traffic prediction models.

**(b) Predictive model — for operation.** Gradient-boosted trees in Spark MLlib as the honest baseline, extended to a spatio-temporal model (graph-based or recurrent) that exploits network structure. Target: speed-reduction ratio relative to the dry typical-speed profile.

**Inference path.** Forecast rainfall → spateGAN downscaling → identical feature pipeline → predicted per-link delay. Because training features and inference features traverse the same code path, train/serve skew is structurally prevented.

**Validation design** — deliberately harder than a random split:

| Split | Tests |
|---|---|
| Temporal holdout | Generalization forward in time |
| Spatial holdout | Generalization to unseen detectors |
| Event-based | Performance on rain intervals specifically, not diluted by dry majority |
| Cross-city transfer | German (in-domain) → non-German (out-of-domain) |

**Baselines to beat:** historical mean, an identical model with rainfall features ablated, and naïve persistence. The rain-ablated model is the critical comparison — it isolates the contribution of the entire weather pipeline.

### Layer 4 — Serving and Visualization

Precomputed baselines and predictions are written to Cassandra keyed by `(city, link_id, timestamp_bin)` for low-latency lookup. The dashboard mirrors the capability set of commercial platforms: live network view, congestion heatmaps by time-of-day and corridor, corridor travel-time reliability, anomaly and hotspot detection, and a rain-impact forecast panel showing predicted delay against the current baseline.

---

## 7. Mapping to the Big Data Specialization (UC San Diego)

| Course | Concepts | Where applied |
|---|---|---|
| **1. Introduction to Big Data** | Volume/velocity/variety/veracity; data sources | Motivation, source characterization, veracity via UTD19 error flags |
| **2. Big Data Modeling & Management** | Data models, HDFS, streaming vs batch, schema design | L0/L1 — partitioning, Parquet schema, replay harness design |
| **3. Big Data Integration & Processing** | Spark, Spark SQL, ETL, NoSQL stores | L1/L2 — the entire curation and baseline computation; Cassandra serving |
| **4. Machine Learning with Big Data** | MLlib, regression, classification, clustering, evaluation | L3 — dose-response and predictive models, validation design |
| **5. Graph Analytics with Big Data** | GraphX/GraphFrames, centrality, traversal | Road network as graph; congestion propagation between adjacent links; identifying structurally critical links |
| **Capstone** | End-to-end integration | The full pipeline |

**Extending beyond the course foundation:** Kafka (streaming ingest), Airflow (orchestration), H3 (spatial indexing), PyTorch/TensorFlow (spateGAN inference), xarray (netCDF handling), Docker (reproducibility).

---

## 8. Expected Deliverables

1. A reproducible, containerized distributed pipeline from raw sources to served predictions.
2. A published free-flow and typical-speed profile dataset for the selected cities.
3. A quantified rain dose-response table stratified by road class and time of day.
4. A trained delay-prediction model with documented performance against three baselines.
5. An analytics dashboard demonstrating the operational use case.
6. A written report with reproducibility instructions and full data attribution.

---

## 9. Risks and Mitigations

| Risk | Severity | Mitigation |
|---|---|---|
| **Speed unavailable in chosen cities** — UTD19 guarantees only two of three variables per city | High | **MATERIALISED.** 30 of 39 cities have no speed, including every large German city. Mitigation worked as designed: the audit ran before the pipeline build and redirected city selection. Fundamental-diagram recovery is now the route to Hamburg/Luzern and is not yet built |
| **Insufficient per-city temporal coverage** — coverage ranges from one day to several months | High | **MATERIALISED.** The six study cities give 40 distinct days total; Groningen (2 days) yields no usable target at all. Pooling across cities was applied and is still not enough for the four-way stratification |
| **Too few rain events for stable estimates** | High | **MATERIALISED — this is the binding constraint.** Target sd is 0.334 against an effect size of ~0.01–0.02. CIs are reported and cells under 30 observations suppressed, but the rain ablation still fails on 2 of 4 splits |
| **spateGAN out-of-domain error** | Medium | Anchor primary analysis on German cities (in-domain for the training radar); validate that downscaled fields conserve the ERA5 hourly aggregate; carry ensemble spread as an explicit uncertainty feature |
| **No ground-truth radar for validation in some cities** | Medium | Frame downscaled rainfall as a *plausible high-resolution realization*, not a measurement; run an ablation against raw ERA5 to demonstrate the downscaling actually earns its place |
| **Confounding** — rainfall correlates with season, daylight and temperature | Medium | Control explicitly for time-of-day, day-of-week and season; use the dry-only baseline as the counterfactual |
| **Dataset smaller than "big data" framing implies** — 170 M rows is tens of GB, not TB | Low | Frame honestly as genuinely distributed but modestly sized; the architecture is what scales. Optionally federate PeMS (18,000+ stations, 2001–2019) to reach true multi-terabyte scale |

---

## 10. Proposed Phasing

| Phase | Focus |
|---|---|
| **1. Feasibility audit** ✅ | Obtain UTD19; audit per-city variable availability and temporal coverage; select study cities. **Gate passed** — see §0 and `reports/phase1_city_audit.md` |
| **2. Foundation** ✅ | L0/L1 — ingest, curate, map-match, index |
| **3. Baselines** ✅ | L2a — free-flow and dry-only typical speed profiles |
| **4. Weather** ✅ *(raw ERA5; spateGAN outstanding)* | L2b — ERA5 acquisition, spateGAN downscaling, spatial join, feature engineering |
| **5. Analysis** ✅ | L3(a) — dose-response quantification |
| **6. Prediction** ✅ *(negative result — see §0)* | L3(b) — predictive model, validation, ablations |
| **7. Serving** | L4 — Cassandra, dashboard, streaming replay demonstration |
| **8. Documentation** | Report, reproducibility packaging |

Phase 1 is a hard gate. The single largest risk to this project is discovering, after building the pipeline, that the selected cities lack speed data or contain too few rain events.

---

## 11. References

- Loder, A., Ambühl, L., Menendez, M. & Axhausen, K.W. (2019). Understanding traffic capacity of urban networks. *Scientific Reports*, 9(1), 16283. https://doi.org/10.1038/s41598-019-51539-5
- Glawion, L. et al. (2025). Global spatio-temporal ERA5 precipitation downscaling to km and sub-hourly scale using generative AI. *npj Climate and Atmospheric Science*, 8(1), 219. https://doi.org/10.1038/s41612-025-01103-y
- spateGAN-ERA5 implementation: https://github.com/LGlawion/spateGAN_ERA5
- UTD19 dataset: https://utd19.ethz.ch
- Copernicus Climate Data Store (ERA5): https://cds.climate.copernicus.eu
- Jia, Y., Wu, J. & Xu, M. (2017). Traffic Flow Prediction with Rainfall Impact Using a Deep Learning Method. *Journal of Advanced Transportation*.
- Google Maps Platform Terms of Service §3.2.3 and Roads Management Insights usage guidelines (consulted for data-strategy justification).

---

*Data acknowledgment: This project uses the UTD19 dataset (utd19.ethz.ch) under its academic and non-commercial use terms, and ERA5 reanalysis data from the Copernicus Climate Change Service.*