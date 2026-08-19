# RainCheck

**Rain-Aware Urban Traffic Delay Prediction**
*A distributed big-data pipeline coupling multi-city loop-detector traffic states with GAN-downscaled ERA5 precipitation.*

---

## Overview

Rainfall measurably degrades urban road performance, but the degradation is not uniform — it varies with rainfall intensity, road functional class, time of day, existing congestion, and whether the rain is the first after a dry spell. Commercial platforms (Google's Roads Management Insights, Lepton's TraffiCure) have started quantifying this, but they're closed systems, restricted to verified public-sector customers and locked to the territory they manage.

**RainCheck** is an open, reproducible, end-to-end big-data pipeline that quantifies and predicts rain-induced traffic delay at network scale, built entirely on openly licensed data.

## Why Not Just Poll a Maps API?

Polling a commercial routing API for a speed history was investigated and rejected:

- **Licensing** — Google Maps Platform ToS §3.2.3 prohibits pre-fetching, indexing, storing, or aggregating Maps Content, which is exactly what building hour-of-day speed profiles requires.
- **Data shape** — traffic is returned as a three-level ordinal (`NORMAL`/`SLOW`/`TRAFFIC_JAM`), not a numeric speed, and the historical-typical baseline already has rain averaged into it, structurally biasing any rain-delay estimate toward zero.
- **Cost** — traffic-aware requests run ~$10/1,000 calls; polling 500 corridors at 5-minute cadence is ≈$43,000/month.

RainCheck instead builds its analytical corpus from open research data, reserving commercial live feeds strictly for optional, non-persisted display.

## Build Status

L0–L3 are implemented and have been run end to end on real data. **48 tests pass.**

| Layer | Output | Result |
|---|---|---|
| L0 ingest | `hdfs:///raincheck/raw/` | UTD19 6.5 GB + ERA5 (4 months, 2,875,176 grid-hours) |
| L0 audit | `reports/phase1_city_audit.md` | 39 cities, 134,380,371 rows |
| L1 curation | `hdfs:///raincheck/curated/measurements` | 1,395,565 rows, DST-correct UTC, units normalised |
| L2b weather | `reports/l2b_rain_coverage.md` | 100% rain-join coverage |
| L2a baselines | `reports/l2a_baselines.md` | free-flow + dry-only profiles, dry-guard enforced |
| L3a dose-response | `reports/dose_response.md` | light −1.8 pp, moderate +1.1 pp vs no rain |
| L3b model | `reports/model_performance.md` | **rain ablation fails on 2 of 4 splits** |

Two findings changed the project, and both are documented in [`CLAUDE.md` §0](./CLAUDE.md):

1. **No German city in UTD19 reports speed.** The spateGAN in-domain argument below does not survive contact with the data; the study set is six speed-bearing cities across the UK, Netherlands and Germany (Essen only).
2. **At raw ERA5 resolution (28 km, hourly) the rain features do not earn their place.** Maximum observed intensity across the entire corpus is 4.26 mm/h, and target noise dwarfs the effect size. This is the empirical case *for* spateGAN downscaling rather than a reason to abandon the approach.

Not yet built: spateGAN downscaling, Kafka replay, Cassandra serving, the dashboard, GraphX, and fundamental-diagram speed recovery.

## Data Sources

| Source | Role | Notes |
|---|---|---|
| **[UTD19](https://utd19.ethz.ch)** (ETH Zürich) | Traffic layer | 23,541 loop detectors; `utd19_u.csv` holds 134M rows across 39 cities. Only 9 report speed |
| **ERA5** (Copernicus CDS) | Weather layer | Global reanalysis precipitation, ~24–31 km / hourly |
| **[spateGAN-ERA5](https://github.com/LGlawion/spateGAN_ERA5)** | Downscaling | Conditional GAN, ERA5 → 2 km / 10 min precipitation fields |
| **OpenStreetMap** | Network layer | Road topology, functional class, detector-to-link map matching |
| **TomTom Flow Segment Data** (optional) | Live demo | Display-only, never persisted |

> **Superseded.** The intended design synergy was to anchor on Germany, since spateGAN-ERA5 was trained on German gauge-adjusted radar and UTD19 contains twelve German cities. The audit found none of them reports speed, so this holds only for a flow/occupancy analysis via fundamental-diagram speed recovery, which is not yet built. Essen is retained as the single German in-domain check.

## Architecture

A Lambda architecture: a batch layer computes authoritative baselines and models over the full historical corpus, a speed layer replays curated measurements as a simulated live stream, and a serving layer merges both.

```
SOURCES        UTD19 CSV · ERA5 netCDF · OSM PBF · [Live API]
                        │
L0  INGESTION           HDFS · Kafka
                        │
L1  CURATION            Spark · Spark SQL
    schema conformance · unit normalization · UTC alignment ·
    5-min re-binning · error-flag filtering · map matching ·
    H3 spatial indexing · partitioned Parquet
                 │                    │
L2a TRAFFIC BASELINE       L2b WEATHER LAYER
    free-flow speed            spateGAN-ERA5 downscaling
    dry-only typical profiles  grid → detector spatial join
    delay index                 rain feature engineering
                 └────────┬───────────┘
L3  PREDICTION           Spark MLlib · GraphX
    (a) dose-response model — interpretable elasticities
    (b) predictive model — GBT baseline → spatio-temporal model
                        │
L4  SERVING              Cassandra · Dashboard
    live map · congestion heatmaps · corridor reliability ·
    rain-impact forecast panel
```

### Layer highlights

- **Free-flow speed** is the 85th percentile of observed speed conditioned on occupancy below the critical threshold — not simply the max or the night-time average.
- **Typical speed profiles** (median speed per detector × weekend-flag × hour-of-day, coarsened from day-of-week × 5-min bin because 40 days leaves ~3 observations per cell at that grain) are computed over **dry intervals only**. Including rainy intervals would let the baseline absorb the very effect the project measures — the same flaw that makes Google's `staticDuration` unsuitable for this purpose.
- **Rain features** go beyond raw intensity: accumulation windows (1/3/6 h — hourly, since raw ERA5 cannot resolve the 10/30/60 min originally planned), time since rain onset, dry-spell antecedent (the "first rain after a dry spell" effect), and intensity banding. Ensemble spread is *not* implemented: it needs spateGAN's multi-seed members.
- **Two prediction models**: an interpretable dose-response model (GLM / GBT with interactions across rainfall band × road class × time-of-day) for explanation, and a spatio-temporal GBT/graph model for operational forecasting.
- **Validation** goes beyond a random split: temporal holdout, spatial holdout, event-based (rain intervals only), and cross-city transfer (UK → Netherlands/Germany), benchmarked against historical mean, naïve persistence, and a rain-ablated model.

## Tech Stack

Spark & Spark SQL, Spark MLlib, GraphX, Kafka, HDFS, Cassandra, H3, Airflow, PyTorch/TensorFlow (spateGAN inference), xarray, Docker.

## Deliverables

1. Reproducible, containerized pipeline from raw sources to served predictions
2. Published free-flow and typical-speed profile dataset
3. Quantified rain dose-response table by road class and time of day
4. Trained delay-prediction model benchmarked against three baselines
5. Analytics dashboard for the operational use case
6. Written report with reproducibility instructions and data attribution

## Project Phasing

| Phase | Focus |
|---|---|
| 1 | Feasibility audit — per-city variable availability & temporal coverage; **hard gate** on city selection |
| 2 | Foundation — ingest, curate, map-match, index |
| 3 | Baselines — free-flow and dry-only typical speed profiles |
| 4 | Weather — ERA5 acquisition, spateGAN downscaling, spatial join |
| 5 | Analysis — dose-response quantification |
| 6 | Prediction — predictive model, validation, ablations |
| 7 | Serving — Cassandra, dashboard, streaming replay demo |
| 8 | Documentation — report, reproducibility packaging |

Full design details, risk mitigations, and course-mapping are in [`CLAUDE.md`](./CLAUDE.md).

## Data Attribution

This project uses the UTD19 dataset (utd19.ethz.ch) under its academic and non-commercial use terms, and ERA5 reanalysis data from the Copernicus Climate Change Service.

> Loder, A., Ambühl, L., Menendez, M. & Axhausen, K.W. (2019). Understanding traffic capacity of urban networks. *Scientific Reports*, 9(1), 16283.
> Glawion, L. et al. (2025). Global spatio-temporal ERA5 precipitation downscaling to km and sub-hourly scale using generative AI. *npj Climate and Atmospheric Science*, 8(1), 219.

## Reproducing

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
./scripts/test.sh                 # 48 tests
/opt/hadoop/sbin/start-dfs.sh
./scripts/run_audit.sh            # L0  Phase-1 gate
./scripts/run_curate.sh           # L1
./scripts/fetch_era5.sh           # L2b(i)  needs ~/.cdsapirc + accepted ERA5 licence
./scripts/run_rain_features.sh    # L2b(ii)
./scripts/run_baselines.sh        # L2a
./scripts/run_dose_response.sh    # L3a
./scripts/run_models.sh           # L3b
```

Spark comes from `/opt/spark` (4.0.1); the venv deliberately has no `pyspark` so there is only ever one Spark version in play.

## License

TBD.
