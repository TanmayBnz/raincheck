# Phase-5 Benchmark — this pipeline against IUTF

_Generated 2026-08-27 10:28 UTC. Percentage points of deviation from each detector's own dry typical profile, contrasted against Dry within the same stratum. Brackets are 95% cluster bootstrap intervals over detector-days; `ns` marks an interval containing zero._

**Gate verdict: PASS**

## 1. Why this is a reproduction, not a transcription

The plan was to put IUTF's published flow magnitudes beside ours. That
cannot be done: **IUTF publishes no per-band magnitudes.** Its Technical
Validation states only that "increasing rainfall intensity is associated
with more pronounced traffic flow changes"; the numbers live inside box
plots (Figures 8 and 10) and are never given in the text. The paper
reports **no speed-based results at all**.

So IUTF's setup is rebuilt from IUTF's own shipped files and run through
the identical estimator that produced `phase5_dose_response.md`. Three
arms, differing only in the data they are handed:

| arm | traffic | rainfall | measure |
|---|---|---|---|
| **A — IUTF as shipped** | IUTF hourly readings | IUTF ERA5, 0.25° / 1 h | flow |
| **B — this pipeline** | curated 5-min | spateGAN, 2 km / 10 min | flow |
| **C — this pipeline** | curated 5-min | spateGAN, 2 km / 10 min | speed |

Arm A: 4,468 detector-days, 33,175 detector-hours. Arms B/C: 10,114 detector-days, 2,094,391 detector-intervals.

Arm A is restricted to the dates IUTF ships a rainfall file for. IUTF
covers only some dates (Manchester: 28 files spanning 72 days), and
treating the absent ones as dry would feed unlabelled hours into the dry
baseline — the precise error `build_baselines` guards against. Dry and wet
hours in Arm A therefore come from the same days, which also removes
seasonality from the contrast.

## 2. Flow response — A against B

The prior-art comparison proper: the same quantity IUTF measures,
estimated the same way, from the two pipelines.

| stratum | band | A — IUTF as shipped | B — this pipeline |
|---|---|---|---|
| congested | Light | -23.6 [-45.5, -4.8] | -3.8 [-6.1, -1.2] |
| congested | Moderate | +27.1 [-14.3, +88.9] ns | -3.6 [-5.6, -1.6] |
| congested | Heavy | — | -5.8 [-8.6, -2.8] |
| congested | Extreme | — | -9.4 [-13.8, -4.7] |
| free-flowing | Light | -1.9 [-6.2, +2.4] ns | +0.2 [-0.5, +1.0] ns |
| free-flowing | Moderate | -3.6 [-7.6, +0.6] ns | -2.2 [-2.9, -1.3] |
| free-flowing | Heavy | -13.4 [-19.6, -7.1] | +4.6 [+2.9, +6.3] |
| free-flowing | Extreme | — | -3.8 [-6.6, -0.8] |

### Per city

| stratum | band | A — IUTF as shipped | B — this pipeline |
|---|---|---|---|
| essen | Light | -3.2 [-5.0, -1.6] | +6.4 [+2.3, +10.4] |
| essen | Moderate | -8.9 [-11.7, -6.1] | -0.7 [-3.3, +2.2] ns |
| essen | Heavy | -0.3 [-5.8, +5.6] ns | -4.5 [-10.2, +0.9] ns |
| manchester | Light | +26.2 [+15.3, +37.3] | +4.7 [+3.3, +6.0] |
| manchester | Moderate | +11.0 [+1.6, +25.1] | -3.2 [-4.1, -2.2] |
| manchester | Heavy | — | -3.8 [-5.5, -2.0] |
| manchester | Extreme | — | -7.8 [-9.6, -6.0] |
| torino | Light | — | -2.4 [-3.4, -1.6] |
| torino | Moderate | — | -1.2 [-2.4, +0.1] ns |
| torino | Heavy | — | +11.3 [+8.9, +13.8] |
| torino | Extreme | — | +8.7 [+3.9, +13.6] |

## 3. What the speed layer adds — B against C

IUTF has no counterpart for this table. It ships a `speed` column but
derives nothing from it: no free-flow speed, no typical-speed profile, no
delay metric, and no speed result in the paper. Everything below is this
project's own L2a layer, and it is where the headline finding lives — the
sign reversal between free-flowing and congested roads that the flow
channel alone cannot reveal.

| stratum | band | B — flow | C — speed |
|---|---|---|---|
| congested | Light | -3.8 [-6.1, -1.2] | +4.5 [+3.3, +5.7] |
| congested | Moderate | -3.6 [-5.6, -1.6] | +2.1 [+0.4, +4.1] |
| congested | Heavy | -5.8 [-8.6, -2.8] | +7.6 [+3.5, +13.7] |
| congested | Extreme | -9.4 [-13.8, -4.7] | +10.5 [+6.3, +14.7] |
| free-flowing | Light | +0.2 [-0.5, +1.0] ns | -0.9 [-1.7, -0.2] |
| free-flowing | Moderate | -2.2 [-2.9, -1.3] | -0.9 [-1.8, +0.3] ns |
| free-flowing | Heavy | +4.6 [+2.9, +6.3] | -0.9 [-2.4, +1.2] ns |
| free-flowing | Extreme | -3.8 [-6.6, -0.8] | +1.1 [-1.4, +3.9] ns |

## 4. Reading the comparison

- **Coverage.** IUTF-as-shipped can estimate **5 of 8** headline band x road-state cells, against **8 of 8** for this pipeline. At city level IUTF supports 2 of 3 study cities (essen, manchester); this pipeline supports all 3.
- **Torino is the casualty, and the reason is instructive.** Its window is 21 days. Harmonised to hourly, a (detector, hour-of-day) baseline cell can hold at most 21 dry observations, which falls under the min_obs_cell of 20 once wet and buffered hours are removed — so no baseline is estimable and the city drops out entirely. The same 21 days at 5-minute resolution give roughly 250 dry observations per cell. Torino is the largest speed-bearing city in UTD19, and hourly aggregation makes it unanalysable here.
- **Precision.** Median 95% interval width is **12.6 pp** for IUTF-as-shipped against **4.5 pp** for this pipeline — roughly 3x tighter. Arm A's larger point estimates are a symptom of that width, not evidence of a stronger effect: its widest cells carry its biggest numbers.
- **No usable dose-response from the coarse arm.** On congested roads it puts Light at -23.6 pp and Moderate at +27.1 pp — opposite signs, with the Moderate interval spanning zero. That is the qualitative claim IUTF does make ("more rain, more change") failing to survive quantification at its own resolution.
- **The speed layer is not redundant with the flow layer.** Speed and flow move in opposite directions, both significantly, in 4 cells: congested/Light, congested/Moderate, congested/Heavy, congested/Extreme. That is the demand channel made visible — a road that emptied, not a road that got faster. A flow-only analysis cannot tell the two apart, and IUTF is flow-only.

## 5. What this does not isolate

Arms A and B differ in **resolution and curation at once**, so the gap
between them is an end-to-end pipeline difference, not a clean resolution
effect. The clean ablation already exists: `reports/phase4_downscaling.md`
§2 holds the rows fixed and varies only the rain labelling, native
31 km / 1 h against 2 km / 10 min. Read the two together — Phase 4 answers
"does downscaling add information", this answers "is the assembled
pipeline better than the published prior art".

Two caveats carry over from Phase 5: road class is UTD19's own `fclass`
rather than an OSM map-match, and only one spateGAN ensemble member
(seed 10) was run, so these intervals quantify sampling error in the
traffic response and not error in the rainfall field itself.

