# Phase-6a Rain Ablation — Spark MLlib

_Generated 2026-10-17 13:13 UTC. 2,114,968 readings over 75 city-days, 5-fold cross-validation grouped by day: every reading is predicted by models that never saw its day. Gradient-boosted trees, 40 rounds, depth 5, identical for all three models._

Error is RMSE of speed deviation from the detector's normal dry speed, in
percentage points. `zero` is the no-model baseline: predict "normal speed".
Improvement is how much lower one model's error is than another's, with a
95% cluster-bootstrap interval over detector-days (2,000 replicates).
An interval that excludes zero is a real difference.

## All test readings

2,114,968 readings, 10,112 detector-days.

| model | RMSE (pp) |
|---|---|
| zero | 31.30 |
| none | 30.93 |
| coarse | 30.28 |
| fine | 31.24 |

| comparison | error reduction (pp) | 95% interval | % of baseline error |
|---|---|---|---|
| coarse rain vs no rain | 0.65 | [-0.87, 2.20] ns | 2.09% |
| 2 km rain vs no rain | -0.31 | [-0.73, 0.09] ns | -1.00% |
| 2 km rain vs coarse rain | -0.96 | [-2.43, 0.53] ns | -3.15% |

## Wet test readings only (2 km label)

148,406 readings, 6,130 detector-days.

| model | RMSE (pp) |
|---|---|
| zero | 39.75 |
| none | 37.78 |
| coarse | 36.97 |
| fine | 39.09 |

| comparison | error reduction (pp) | 95% interval | % of baseline error |
|---|---|---|---|
| coarse rain vs no rain | 0.81 | [-9.56, 12.14] ns | 2.15% |
| 2 km rain vs no rain | -1.31 | [-2.92, 0.72] ns | -3.46% |
| 2 km rain vs coarse rain | -2.12 | [-15.08, 10.04] ns | -5.73% |

## What the 2 km model leans on

Feature importance (share of total split gain) in the `fine` model.

| feature | importance |
|---|---|
| free_flow_speed | 0.139 |
| typical_speed | 0.137 |
| road_idx | 0.126 |
| hod | 0.118 |
| dry_spell_hours | 0.095 |
| acc_60min_mm | 0.094 |
| length | 0.084 |
| dow | 0.083 |
| city_idx | 0.035 |
| acc_30min_mm | 0.031 |
| precip_mm_h | 0.028 |
| lanes | 0.019 |
| speed_limit | 0.006 |
| minutes_since_onset | 0.004 |
| fine_wet | 0.000 |

## Caveats

- Day-grouped cross-validation only, not the four holdout designs
  CONTEXT.md §6/L3b originally specified (spatial, event, city).
- Hyperparameters are fixed, not tuned; all three models share them, so the
  comparison is fair, but absolute errors are not the best achievable.
- One spateGAN ensemble member; the 2 km rain is a model output, not an
  observation.

