# RainCheck

**Rain-Aware Urban Traffic Delay Prediction**
*What rainfall resolution does a rain–delay model actually need, and can generative downscaling supply it?*

---

## Overview

Rainfall degrades urban road performance, but per observation the effect is small — in our own
six-city European corpus, speed deviation from a dry baseline has a standard deviation of **0.334**
against a rain effect of order **0.01–0.02**. Learning that relationship is a signal-recovery
problem, decided by how faithfully the rainfall input preserves intensity structure.

**We built the obvious version of this pipeline and it returned a negative result.** At ERA5's
native 28 km hourly resolution, across 1.4 M curated detector-observations, the rain features made
the model *worse* on the two hardest validation splits. Convective rainfall is a few kilometres
wide and a few minutes long; a 28 km hourly mean cannot represent it. Birmingham's entire detector
network sat inside a single ERA5 grid cell.

So the question RainCheck now answers is not *whether* rain causes delay — that is settled in the
literature — but:

> **At what rainfall resolution does the rain–delay relationship become learnable, and can
> generative downscaling manufacture that resolution faithfully enough to substitute for weather
> radar where radar does not exist?**

That second half matters because gauge-adjusted radar exists for Germany, the Netherlands, the UK,
the US and Australia — and for very little of the world that most needs rain-aware traffic
management. **spateGAN-ERA5's own paper validates its rainfall fields against radar; nobody has
tested whether those fields support a downstream physical inference.** That is the gap.

## The Core Experiment: a Resolution Ladder

Hold the traffic corpus **fixed** and vary only the rainfall input.

| Rung | Rainfall input | Resolution | Status |
|---|---|---|---|
| 0 | features ablated | — | baseline |
| 1 | ERA5 reanalysis | 28 km / 1 h | **tested — fails** |
| 3 | KNMI gauge-adjusted radar | 5 min / 1 km | the observed ceiling |
| 2 | spateGAN-ERA5 | 2 km / 10 min | the hypothesis under test |

**Rung 3 runs before rung 2.** If the effect is not learnable at 1 km / 5 min *observed* rainfall,
there is nothing for spateGAN to recover. Three outcomes, all publishable:

- rung 3 works, rung 2 recovers most of it → **downscaling is a valid radar substitute**
- rung 3 works, rung 2 does not → a concrete limit on a method promoted for global use
- rung 3 also fails → the per-observation effect never exceeds traffic's intrinsic variance, and a
  body of aggregate-level literature needs qualifying

Radar is also finer than spateGAN's own output, so for the first time the downscaling error becomes
**directly measurable** rather than assumed.

## Branches

| Branch | Contents |
|---|---|
| `NL` | **Active.** Netherlands arm — NDW adapter, live-feed harvester, resolution ladder |
| `archive/eu-utd19-arm` | The completed European arm: L0–L3 on UTD19 + ERA5, 48 tests, and the negative result that motivates the pivot. Tagged `eu-arm-final` |

## Build Status

**61 tests pass.** The European arm is complete; the Netherlands arm is in L0.

### European arm (archived)

| Layer | Output | Result |
|---|---|---|
| L0 audit | `reports/phase1_city_audit.md` | 39 cities, 134,380,371 rows; only 9 report speed |
| L1 curation | `hdfs:///raincheck/curated/measurements` | 1,395,565 rows, DST-correct UTC |
| L2b weather | `reports/l2b_rain_coverage.md` | 100% join coverage; **max intensity 4.26 mm/h** |
| L2a baselines | `reports/l2a_baselines.md` | free-flow + dry-only profiles, dry-guard enforced |
| L3a dose-response | `reports/dose_response.md` | light −1.8 pp, moderate +1.1 pp — **not physically ordered** |
| L3b model | `reports/model_performance.md` | **rain ablation fails on 2 of 4 splits** (−2.8% event, −14.4% spatial) |

### Netherlands arm (active)

Phase-1 gate passed — `reports/phase1_nl_audit.md`. NDW removes the constraint that limited the
European arm:

| | UTD19 | NDW |
|---|---|---|
| Speed availability | 9 of 39 cities | **20,518 of 20,519 sites (99.995%)** |
| Resolution | 3–5 min | **60 s** |
| Corpus | 40 distinct days, 1.4 M rows | **~8.3 M rows/day** measured |
| Access | sign-up | **no account, no API key** |

Built: `ndw.py` (DATEX II → canonical schema), `harvest.py` + `jobs/harvest_ndw.py` (live harvest
to Parquet), `scripts/upload_ndw.sh` (stage → HDFS).

Outstanding: KNMI radar fetcher, L1 curation, the three rungs, L4 serving, GraphX, and the German
domain-shift arm.

## Data Sources

| Source | Role | Notes |
|---|---|---|
| **[NDW](https://opendata.ndw.nu)** | Traffic layer (primary) | 20,519 loop sites, 60 s, DATEX II. **Open, no account.** Live feed *and* archive |
| **KNMI radar** (CC-BY-4.0) | Rung 3 — the referee | `rad_nl25_rac_mfbs_5min`, 5 min / 1 km gauge-adjusted |
| **ERA5** (Copernicus CDS) | Rung 1, and spateGAN's input | `cp` + `lsp` required by the downscaler |
| **[spateGAN-ERA5](https://github.com/LGlawion/spateGAN_ERA5)** | Rung 2 — the hypothesis | ERA5 → 2 km / 10 min. Trained on German radar **only** |
| **DWD RADKLIM-YW** | Domain control | *Is* spateGAN's training target — bounds best-case skill |
| **[UTD19](https://utd19.ethz.ch)** | European arm (archived) | 23,541 detectors, 134 M rows |
| **OpenStreetMap** | Network layer | Still needed: NDW's OpenLR FRC covers only 8.9% of sites |

## Three Traps in the NDW Feed

All handled in `src/raincheck/ndw.py`; each corrupts results silently if missed.

- **`-1` is a missing-value sentinel** in an otherwise numeric field.
- **`dataError=true` accompanies a legal-looking `vehicleFlowRate` of 0.** Unlike `-1`, zero is a
  valid flow, so only the flag separates "empty road" from "broken loop".
- **The `anyVehicle` measurement index is per-site** — observed at 4, 8 and 16, since it depends on
  lanes × vehicle-length classes. Indices are resolved from the site table and a version mismatch
  is refused rather than mis-parsed.

The feed is also **not synchronous**: two cohorts stamp measurements ~2 min and ~8 min behind wall
clock, so consecutive fetches are ~72% duplicate and hour partitions keep receiving late writes.

## Two Findings That Changed the Method

**Informative missingness.** 86% of non-null 1-minute speeds rest on fewer than five vehicles, and
thin counts correlate with low flow, which correlates with rain. Sample size is therefore
correlated with the treatment: weighting by it is variance-optimal but **bias-pessimal**, and
`speed IS NULL` itself carries information about rain. Mitigations (rain-stratified retention,
threshold sensitivity analysis, weighted *and* unweighted estimates, flow as covariate) are L1 gate
requirements, not analysis-stage afterthoughts.

**Train/serve skew is not structurally prevented.** Sharing feature code guarantees it for traffic
but not for rainfall: gauge-adjusted radar publishes ~1 month in arrears, while inference sees
unadjusted real-time radar, then a 2-hour nowcast, then NWP. So the pipeline **trains on the
inference-grade product** and reserves adjusted radar as a validation reference. This also reframes
downscaling's real job — the **forecast path**, where no radar can ever exist.

## Architecture

Lambda: a batch layer over the historical corpus, a speed layer on the live feed, a serving layer
merging both. Portability is asymmetric by design — **one global weather path, N thin traffic
adapters**.

```
SOURCES     NDW DATEX II (live + archive) · ERA5 netCDF · KNMI radar · OSM PBF
                        │
L0  INGESTION           HDFS · Kafka        harvest → Parquet, XML discarded
                        │
L1  CURATION            Spark · Spark SQL
    CANONICAL SCHEMA · dedup (segment_id, ts_utc) · UTC · re-binning
    weighted by sample size · bias diagnostics · H3 · map matching
                 │                    │
L2a TRAFFIC BASELINE       L2b WEATHER — THREE RUNGS
    free-flow speed            rung 1  ERA5     28 km / 1 h
    dry-only profiles          rung 2  spateGAN  2 km / 10 min
    deviation index            rung 3  radar     1 km / 5 min
                 └────────┬───────────┘
L3  PREDICTION           Spark MLlib · GraphX
    dose-response + GBT ablation at EVERY rung
    rung-to-rung deltas are the result
                        │
L4  SERVING              Cassandra · Dashboard
```

`src/raincheck/` is one package across both arms: `ndw.py`, `harvest.py`, `era5.py`, `rain.py`,
`baseline.py`, `dose_response.py`, `model.py`, `curate.py`, `audit.py`, `gate.py`.
Roughly 60% of the European arm's code carries over unchanged.

## Tech Stack

Spark & Spark SQL, Spark MLlib, GraphX, Kafka, HDFS, Cassandra, H3, Airflow, PyTorch (spateGAN
inference), xarray, pyarrow, Docker.

## Reproducing

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
./scripts/test.sh                 # 61 tests

# Netherlands arm — needs no credentials at all
./scripts/harvest_ndw.sh --once   # single fetch, smoke test
./scripts/harvest_ndw.sh          # continuous, ~1 fetch/min
./scripts/upload_ndw.sh           # stage → HDFS (closed hours only)

# European arm (archived branch)
/opt/hadoop/sbin/start-dfs.sh
./scripts/run_audit.sh            # L0  Phase-1 gate
./scripts/run_curate.sh           # L1
./scripts/fetch_era5.sh           # L2b  needs ~/.cdsapirc + accepted ERA5 licence
./scripts/run_rain_features.sh
./scripts/run_baselines.sh        # L2a
./scripts/run_dose_response.sh    # L3a
./scripts/run_models.sh           # L3b
```

Spark comes from `/opt/spark` (4.0.1); the venv deliberately has no `pyspark` so there is only ever
one Spark version in play. The harvester is pure Python — a JVM per minute would be pointless
overhead.

## Data Attribution

Uses NDW open data (opendata.ndw.nu), KNMI precipitation radar (CC-BY-4.0) from the KNMI Data
Platform, the UTD19 dataset (utd19.ethz.ch) under its academic and non-commercial use terms, and
ERA5 reanalysis from the Copernicus Climate Change Service.

> Loder, A., Ambühl, L., Menendez, M. & Axhausen, K.W. (2019). Understanding traffic capacity of urban networks. *Scientific Reports*, 9(1), 16283.
> Glawion, L. et al. (2025). Global spatio-temporal ERA5 precipitation downscaling to km and sub-hourly scale using generative AI. *npj Climate and Atmospheric Science*, 8(1), 219.

Full design, risk register and course mapping in [`CLAUDE.md`](./CLAUDE.md).

## License

TBD.
