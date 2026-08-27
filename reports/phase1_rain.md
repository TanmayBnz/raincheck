# Phase-1 ERA5 Rain-Event Pre-check

Native-resolution (~31 km) ERA5, area-mean over each city bbox, across
that city's actual UTD19 window. Bands follow Met Office / IUTF thresholds.

| city | hours | wet hrs | wet% | events | peak wet hrs | Light | Moderate | Heavy | Extreme | Mod+ |
|---|---|---|---|---|---|---|---|---|---|---|
| manchester | 1728 | 453 | 26.2 | 76 | 109 | 317 | 135 | 1 | 0 | 136 |
| bolton | 144 | 11 | 7.6 | 4 | 4 | 8 | 3 | 0 | 0 | 3 |
| rotterdam | 1032 | 157 | 15.2 | 39 | 35 | 115 | 41 | 1 | 0 | 42 |
| groningen | 384 | 79 | 20.6 | 13 | 17 | 53 | 26 | 0 | 0 | 26 |
| torino | 504 | 101 | 20.0 | 10 | 28 | 61 | 40 | 0 | 0 | 40 |
| essen | 4512 | 830 | 18.4 | 179 | 237 | 574 | 252 | 4 | 0 | 256 |

## Pooled cohorts

| cohort | cities | events | Mod+ |
|---|---|---|---|
| uk_nw | manchester, bolton | 80 | 139 |
| nl | rotterdam, groningen | 52 | 68 |
| it | torino | 10 | 40 |
| de | essen | 179 | 256 |
