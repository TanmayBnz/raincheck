# L2a baselines

Free-flow speed = p85 of speed at occupancy below critical, per detector.

| city       |   detectors_with_ff |   mean_ff_kmh |   min_ff_kmh |   max_ff_kmh |   mean_obs_per_detector |   mean_ff_share_pct |
|:-----------|--------------------:|--------------:|-------------:|-------------:|------------------------:|--------------------:|
| bolton     |                  45 |         64.03 |        33.86 |        80    |                     929 |                91   |
| essen      |                  36 |         60.02 |        43.58 |        99    |                    4600 |                85.6 |
| groningen  |                  43 |         48.09 |        39.03 |        61.07 |                     424 |                94.9 |
| manchester |                 147 |         46.58 |        25    |        80    |                    4559 |                90.7 |
| rotterdam  |                 212 |         52.97 |        33.01 |        85.59 |                    1063 |                94.6 |

## Dry-only typical speed profiles (detector x weekend x hour)

| city       |   profile_cells |   mean_obs_per_cell |   min_obs |   pct_cells_under_30_obs |
|:-----------|----------------:|--------------------:|----------:|-------------------------:|
| birmingham |            1563 |                48.6 |         1 |                    32.25 |
| bolton     |            1069 |                32.4 |         1 |                    48.83 |
| essen      |            1728 |                46   |         8 |                    19.04 |
| groningen  |             876 |                11.1 |         1 |                   100    |
| manchester |            5969 |                53   |         7 |                    20.2  |
| rotterdam  |            4992 |                27.8 |         1 |                    51.38 |

## Critical occupancy

| city       | fclass         |   critical_occ |   capacity_flow |   critical_bin_obs |   critical_bin_share |
|:-----------|:---------------|---------------:|----------------:|-------------------:|---------------------:|
| bolton     | other          |           0.14 |         347.053 |                 38 |               0.0166 |
| bolton     | primary        |           0.34 |         907.283 |                159 |               0.0128 |
| bolton     | secondary      |           0.26 |         619.335 |                319 |               0.0162 |
| bolton     | trunk          |           0.4  |        1109.62  |                141 |               0.0103 |
| bolton     | trunk_link     |           0.1  |         350.667 |                 27 |               0.0118 |
| essen      | primary        |           0.02 |         585.455 |              13434 |               0.1786 |
| essen      | secondary      |           0.06 |         468.118 |               1845 |               0.0191 |
| essen      | tertiary       |           0.02 |         307.434 |                504 |               0.0235 |
| groningen  | residential    |           0.12 |         448.966 |                 29 |               0.0563 |
| groningen  | secondary      |           0.18 |         622.011 |                276 |               0.0145 |
| groningen  | tertiary       |           0.16 |         604.696 |                 23 |               0.0223 |
| manchester | motorway       |           0.78 |        2162     |                 36 |               0.0114 |
| manchester | other          |           0.22 |         448.979 |                378 |               0.0112 |
| manchester | primary        |           0.26 |         555.673 |               1119 |               0.0107 |
| manchester | secondary      |           0.18 |         414.677 |                248 |               0.0117 |
| manchester | secondary_link |           0.4  |        1166.57  |                267 |               0.0174 |
| manchester | tertiary       |           0.24 |         675.188 |                584 |               0.0115 |
| manchester | trunk          |           0.42 |        1091.46  |               5728 |               0.0111 |
| manchester | trunk_link     |           0.64 |        1061.23  |                141 |               0.0235 |
| rotterdam  | other          |           0.06 |         370.885 |                 71 |               0.0372 |
| rotterdam  | primary        |           0.2  |         702.991 |                527 |               0.0103 |
| rotterdam  | secondary      |           0.22 |         786.101 |               2011 |               0.0128 |
| rotterdam  | tertiary       |           0.22 |         701.88  |                200 |               0.0113 |
| rotterdam  | trunk          |           0.18 |         871.693 |                287 |               0.0121 |
