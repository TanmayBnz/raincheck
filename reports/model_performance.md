# L3(b) delay model performance

Target: `typical_deviation` (speed reduction relative to the dry typical speed).
Features exclude contemporaneous occupancy, flow and speed, so the model reflects the operational case: forecast rain in, expected delay out.

`__rain_gain_pct__` is the RMSE improvement attributable to the rain features - the number that justifies the weather pipeline.

| split                     | model             |       rmse |         mae |      n |
|:--------------------------|:------------------|-----------:|------------:|-------:|
| temporal_holdout          | gbt_with_rain     |   0.382068 |   0.144106  | 352658 |
| temporal_holdout          | gbt_rain_ablated  |   0.385432 |   0.148576  | 352658 |
| temporal_holdout          | historical_mean   |   0.385802 |   0.143257  | 352658 |
| temporal_holdout          | naive_persistence |   0.377247 |   0.130034  | 352614 |
| temporal_holdout          | __rain_gain_pct__ |   0.872965 | nan         | 352658 |
| event_based_wet_only      | gbt_with_rain     |   0.216821 |   0.119009  |  15365 |
| event_based_wet_only      | gbt_rain_ablated  |   0.210899 |   0.110393  |  15365 |
| event_based_wet_only      | __rain_gain_pct__ |  -2.80768  | nan         |  15365 |
| spatial_holdout           | gbt_with_rain     |   0.419321 |   0.15743   | 264453 |
| spatial_holdout           | gbt_rain_ablated  |   0.366388 |   0.156879  | 264453 |
| spatial_holdout           | historical_mean   |   0.289183 |   0.145244  | 264453 |
| spatial_holdout           | naive_persistence |   0.273273 |   0.13461   | 264331 |
| spatial_holdout           | __rain_gain_pct__ | -14.4475   | nan         | 264453 |
| cross_city_uk_to_mainland | gbt_with_rain     |   0.16702  |   0.117276  | 306799 |
| cross_city_uk_to_mainland | gbt_rain_ablated  |   0.172142 |   0.12052   | 306799 |
| cross_city_uk_to_mainland | historical_mean   |   0.137278 |   0.0864528 | 306799 |
| cross_city_uk_to_mainland | naive_persistence |   0.147613 |   0.0884847 | 306568 |
| cross_city_uk_to_mainland | __rain_gain_pct__ |   2.97547  | nan         | 306799 |
