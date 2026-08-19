# Phase 1 — NDW (Netherlands) feasibility audit

*Probed live, 2026-08-19. Analogue of `phase1_city_audit.md` for the UTD19 arm.*
*Feed snapshot: `publicationTime` 2026-08-19T08:33:42Z; site table version 1727.*

## Verdict

**Gate passed for the traffic layer.** NDW removes the single constraint that shaped and
ultimately limited the European arm: speed availability. It is also open with **no account and
no API key** for the live feed. One item remains blocked (historical export, see §6).

| UTD19 (European arm) | NDW (Netherlands arm) |
|---|---|
| Speed in **9 of 39 cities**; no large German city | Speed at **20,518 of 20,519 sites (99.995%)** |
| 40 distinct calendar days, non-simultaneous cities | Multi-year archive; ~29.5 M site-observations **per day** |
| 3–5 min aggregation | **60 s** aggregation, uniformly |
| Occupancy present but unit-inconsistent across cities | **No occupancy at all** (see §5) |
| Road class via OSM map-matching | Coordinates for 100% of sites; FRC for only 8.9% (see §5) |

## 1. Source catalogue — `https://opendata.ndw.nu/`

Open directory listing, no auth. Relevant files:

| File | Content | Size (gz / raw) |
|---|---|---|
| `trafficspeed.xml.gz` | DATEX II v2.0 `MeasuredDataPublication` — speed + flow, refreshed every minute | 1.1 MB / 50 MB |
| `measurement_current.xml.gz` | DATEX II v2.0 `MeasurementSiteTablePublication` — the site/detector metadata | 11 MB / 371 MB |
| `traveltime.xml.gz` | Section travel times | — |
| `ndw_avg_meetlocaties_shapefile.zip` | Measurement locations as shapefile | — |
| `VILD6.13.A.zip` | Location reference table | — |

`measurement_current.xml.gz` announces `measurementSiteTable version="1727"`, which matches the
`measurementSiteTableReference version="1727"` in `trafficspeed.xml.gz`. **The two files are
join-consistent as published**, and the version must be checked on every ingest — a site-table
bump can silently re-map measurement indices (see §4).

## 2. Site table — 20,519 measurement points

| Property | Finding |
|---|---|
| Total `measurementSiteRecord` | 101,228 |
| of which `Point` (detector sites) | **20,519** |
| of which `ItineraryByIndexedLocations` (travel-time sections) | 80,709 |
| Point sites with coordinates | **20,519 (100%)** |
| Has `anyVehicle` **speed** | **20,518** |
| Has `anyVehicle` **flow** | **20,519** |
| Has **both** | **20,518** |
| Aggregation `period` | **60 s** on every index |

Equipment (`measurementEquipmentTypeUsed`): **20,443 `lus`** (inductive loop), 75 `radar`,
1 `microwave`. Overwhelmingly genuine loop detectors, consistent with the UTD19 lineage.

`computationMethod` is **heterogeneous**: 16,596 `arithmeticAverageOfSamplesInATimePeriod`,
3,923 `harmonicAverageOfSamplesInATimePeriod`. This matters — harmonic mean is the correct
estimator for space-mean speed, arithmetic is biased high under congestion. Carry it as a
covariate and consider it a stratification variable, not a detail to average over.

## 3. Measured values

Per site, per minute, an indexed list of `measuredValue` elements:

```xml
<siteMeasurements>
  <measurementSiteReference id="PZH01_MST_0029-00" version="13"/>
  <measurementTimeDefault>2026-08-19T08:32:00Z</measurementTimeDefault>
  <measuredValue index="7">
    <measuredValue><basicData xsi:type="TrafficSpeed">
      <averageVehicleSpeed numberOfInputValuesUsed="0"><speed>-1</speed></averageVehicleSpeed>
    </basicData></measuredValue>
  </measuredValue>
```

In the snapshot: 97,784 `TrafficFlow` and 97,778 `TrafficSpeed` values across 20,519 sites.

Two fields carry veracity, replacing UTD19's three-state error flag:

- **`-1` is the missing-value sentinel.** It appears as a real number and *must* be nulled before
  any aggregation, or it silently destroys every mean. This is the highest-risk parse bug here.
- **`numberOfInputValuesUsed`** — the sample size behind each value. This is a strictly better
  quality signal than UTD19's flag: it is continuous, so it becomes `quality_weight` and feeds
  `GBTRegressor.weightCol` directly rather than forcing a keep/drop decision.
- `dataError` also appears (14,721 occurrences in the snapshot) and must be honoured.

## 4. Measurement indices are per-site, not fixed

`measurementSpecificCharacteristics index="N"` declares, for each index: `accuracy`, `period`,
`specificLane`, `specificMeasurementValueType` (`trafficFlow` | `trafficSpeed`), and
`specificVehicleCharacteristics` — either vehicle-length bins (1.85–2.4 m, 2.4–5.6, 5.6–11.5,
11.5–12.2, >12.2) or `vehicleType=anyVehicle`.

**The `anyVehicle` index differs per site** — observed at 8, 16, 4 across four sampled sites,
because it depends on lane count × vehicle-class count. So the adapter **must** resolve indices
by joining the site table; hardcoding an index would silently read a lorry-only lane as if it
were the aggregate. Lanes observed: `lane1`–`lane9`, `hardShoulder`,
`allLanesCompleteCarriageway`.

## 5. Two genuine losses relative to UTD19

**No occupancy.** The feed carries `TrafficFlow` and `TrafficSpeed` only — no
`TrafficConcentration`. So `critical_occupancy()` and the occupancy-conditioned free-flow
definition are **inapplicable**, and free-flow degrades to a high speed percentile over dry
intervals. §6 of `CLAUDE.md` explicitly cautions against exactly this, and it must be reported
as a known methodological downgrade rather than glossed. Mitigation: condition on **dry hours**
and low flow instead of low occupancy.

**Road class is mostly absent.** The OpenLR `pointExtension` supplies
`openlrFunctionalRoadClass` and `openlrFormOfWay`, but only on **1,833 of 20,519 sites (8.9%)**.
Distribution where present: FRC3 1,040, FRC4 380, FRC1 142, FRC7 104, FRC0 76, FRC5 48, FRC6 37.
So **OSM map-matching is still required** for the remaining 91%. (An earlier reading that road
class "comes free" was based on the sampled record, which happened to carry the extension.)
`alertCLocation`/TMC references and `measurementSiteName` (e.g. `N207 km 18.768 Re` — road
number, kilometre, direction) provide a second, independent route to road identity.

## 6. Historical access — **blocked, needs a decision**

The open feed is **current-minute only**. History lives in Dexter:

- `https://dexter.ndw.nu/api/export` → **HTTP 401**. The export API exists and requires auth.
- Unrouted paths return the Angular SPA shell (HTTP 200 HTML), so absence of a route cannot be
  inferred from a 200.
- `https://dexter.ndw.nu/api/admin/data-quality/exclusions/` is **public** and returns JSON
  records dated 2020–2021, confirming the archive spans years.

Two routes, not mutually exclusive:

1. **Register for a Dexter account** (`mijn.ndw.nu`) and use the export API. Requires a human to
   accept terms — and the retention/republication licence still needs reading, since it decides
   whether Deliverable #2 can be a corpus or must be coefficients only.
2. **Harvest the live feed forward.** 1-minute cadence, no auth, starts immediately. KNMI's
   gauge-adjusted radar publishes with ~1 month delay anyway, so a harvest started now aligns
   with radar availability in roughly 5–6 weeks. This also builds the streaming layer for real
   rather than by replay.

## 7. Volume — genuinely distributed scale

| Quantity | Value |
|---|---|
| Naive upper bound (every site every minute) | 20,519 × 1,440 = 29.5 M/day |
| **Measured unique observations** | fast cohort 5,755/min = **8.3 M/day**, plus the slow cohort's 14,764 sites at a cadence yet to be measured |
| Raw XML per day | ~72 GB uncompressed (~1.6 GB gzipped) |
| **Parsed Parquet (zstd), measured** | **22.7 bytes/row** |
| Storage for a 60-day corpus | ~**11–17 GB** against 248 GB usable |

Against the European arm's 1,395,565 curated rows, even the measured lower bound is a **~360×
increase**, which retires the §9 risk "dataset smaller than the big-data framing implies".

Raw XML **cannot** be retained: at ~72 GB/day it exhausts the disk in under four days. Parsing to
Parquet on ingest and discarding the XML is a **~107× reduction** (72 GB → ~0.67 GB/day), which is
what makes an open-ended harvest viable at all.

## 8. Adapter run against the real feed

`src/raincheck/ndw.py`, applied to the two files above (site table version 1727):

| Metric | Value |
|---|---|
| Sites parsed from table | 20,519 (exactly the `Point` count) |
| Rows from one minute | 20,519 |
| Speed non-null | 17,390 (**84.8%**) — min 0, max 173, mean **88.6 km/h** |
| Flow non-null | 19,171 (**93.4%**) — min 0, max 3,180 veh/h |
| `quality_weight` | min 0, max 42, **mean 4.7** |
| Sentinel leak check | no `-1` in speed or flow |
| Speed null with `quality_weight` = 0 | 953 |
| Speed null with `quality_weight` > 0 | 2,176 (i.e. `dataError`, not merely absent) |

Two findings here matter more than the parse itself.

**1-minute speeds are extremely thin.** **14,898 of 17,390** non-null speeds (**86%**) rest on
**fewer than five vehicles**. A 1-minute mean over ~5 vehicles is a very noisy estimate, and this
is the same disease that defeated the European arm — target sd 0.334 against an effect of
~0.01–0.02 — reappearing at a different point in the pipeline. The difference is that here it is
*fixable*: re-bin to 5-minute or hourly means **weighted by `quality_weight`**, and average over
20,519 sites rather than the European arm's ~7,600 detector-days. Reporting per-observation
1-minute results without this aggregation would repeat the earlier mistake.

**The feed is not synchronous — and it has two cohorts.** Measuring the lag between fetch time
and each site's `measurementTimeDefault` gives a clean bimodal split:

| Cohort | Sites | Lag behind wall clock |
|---|---|---|
| Fast | **5,755** | ~2 min |
| Slow | **14,764** | ~8 min |

Across five consecutive one-minute harvests the slow cohort **did not advance at all** — every
fetch after the first returned exactly 5,755 new rows and 72% duplicates (14,764 repeats). So the
slow cohort publishes on a cycle longer than a minute; its actual cadence needs a longer
observation to pin down. This revises the volume estimate downward (see §7) and means a
one-minute poll is the right cadence for the fast cohort but oversamples the slow one heavily.

Consequences:

- Never key on publication time; always use each site's `measurementTimeDefault`.
- Consecutive one-minute fetches **overlap**, so a naive harvester double-counts by ~3.6x.
  Deduplicate on `(segment_id, ts_utc)`. The harvester's in-memory window uses a **30-minute**
  horizon, comfortably clear of the slow cohort's 8-minute lag.
- **L1 must deduplicate as well, unconditionally.** The harvester's window is per-process and
  in-memory, so any restart re-ingests the overlap - already observed: two harvester runs in the
  same minute produced 29,528 rows for a single timestamp that should hold at most 20,519. The
  harvester reduces volume; only L1 `dropDuplicates(["segment_id", "ts_utc"])` guarantees
  correctness.
- An hour partition keeps receiving writes for ~8 minutes after the hour rolls over, so
  `upload_ndw.sh` treats a partition as closed only 20 minutes past the hour.
- Max speed 173 km/h exceeds `SPEED_CAP_KMH = 150`, so `curate.clean_speed` will null those —
  existing behaviour, no change needed.

### Informative missingness — a bias risk, not just a variance one

Thin counts are not randomly distributed: low `quality_weight` means low flow, and rain reduces
traffic volume. So sample size is **correlated with the treatment**, which makes this a bias
problem that aggregation alone does not dispose of.

- Any minimum-`n` filter preferentially deletes rainy observations, biasing the effect **toward
  zero** — the direction that manufactures a null result.
- Weighting by `quality_weight` is variance-optimal but **bias-pessimal**: it downweights rainy
  intervals relative to dry ones. The two objectives pull in opposite directions.
- As flow → 0 loop speed becomes undefined and is reported as `-1`, so `speed IS NULL` itself
  carries information about the treatment (2,176 rows show speed null with `quality_weight` > 0).

Required diagnostics, cheap if built into the L1 gate rather than bolted on at analysis time:

1. Aggregate to a **fixed 5-min or hourly bin** so almost no bin is dropped — this converts a
   selection problem into a variance problem, which is the favourable trade.
2. Report **retention stratified by rain band**. Divergence between wet and dry retention is
   direct evidence the bias is live.
3. Treat any minimum-`n` threshold as a **sensitivity analysis** (n>=1, 5, 20), not a single
   choice. An estimate that drifts monotonically with the threshold is driven by missingness.
4. Report **weighted and unweighted** estimates side by side; divergence diagnoses the bias.
5. Carry **flow as a covariate** (93.4% available) so low volume is controlled explicitly.

Risk is low for the pooled dose-response, **high** for per-segment and spatial-holdout estimates
(spatial was already the worst European split at -14.4%) and for night/off-peak strata, which are
thin by construction and are exactly where dry-spell-onset effects are expected.

## 9. Consequences for the build

1. `quality_weight` = `numberOfInputValuesUsed`; null on `-1` and on `dataError`.
2. Resolve `anyVehicle` speed/flow indices **from the site table per site**; never hardcode.
3. Assert `measurementSiteTable` version equality between feed and table on every ingest.
4. Free-flow via dry-hour percentile; document the loss of occupancy conditioning.
5. OSM map-matching retained for road class (91% of sites lack FRC).
6. Parse-to-Parquet on ingest; never archive raw XML.
7. Carry `computationMethod` as a covariate — arithmetic vs harmonic is a real bias difference.
8. Filter the site table to `xsi:type="Point"`; itinerary sections also carry coordinates and
   would otherwise inflate the table five-fold.
9. Re-bin to >=5 minutes weighted by `quality_weight` before any modelling.
10. Deduplicate on `(segment_id, ts_utc)` — publications overlap across minutes.
