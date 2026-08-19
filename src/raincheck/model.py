"""L3(b): predictive model and the rain ablation.

The ablation is the point of this module. An identical model is trained with and
without the rain features, and the difference is the only honest measure of what
the entire weather pipeline (ERA5 acquisition, spell features, spatial join)
actually buys.

Feature choice follows the operational use case in CLAUDE.md 6: forecast rainfall
in, expected delay out. So contemporaneous occupancy, flow and speed are excluded
- at inference time you do not have them, and including them would let the model
interpolate the target while drowning the rain signal it exists to test.
"""

from pyspark.sql import DataFrame, Window, functions as F

TARGET = "typical_deviation"

# Known at forecast time: clock, road geometry, and the detector's own baselines.
BASE_FEATURES = (
    "hour",
    "is_weekend_num",
    "fclass_idx",
    "city_idx",
    "lanes",
    "limit",
    "length",
    "free_flow_speed",
    "typical_speed",
)

RAIN_FEATURES = (
    "rain_mm_h",
    "rain_1h",
    "rain_3h",
    "rain_6h",
    "rain_band_idx",
    "hours_since_onset_f",
    "antecedent_dry_hours_f",
)

# Cities grouped by ERA5 domain, for the cross-city transfer test.
UK_CITIES = ("manchester", "bolton", "birmingham")
MAINLAND_CITIES = ("rotterdam", "groningen", "essen")

TEMPORAL_HOLDOUT_DAYS = 5
SPATIAL_TEST_PCT = 25


def feature_columns(include_rain: bool) -> list[str]:
    return list(BASE_FEATURES) + (list(RAIN_FEATURES) if include_rain else [])


def add_splits(
    df: DataFrame,
    temporal_holdout_days: int = TEMPORAL_HOLDOUT_DAYS,
    spatial_test_pct: int = SPATIAL_TEST_PCT,
) -> DataFrame:
    """Flag the temporal, spatial and cross-city holdouts.

    Deliberately harder than a random split: a random split would let the model
    see the same detector on the same day in both train and test, and would
    flatter it enormously.
    """
    per_city = Window.partitionBy("city")
    cutoff = F.date_sub(F.max("day").over(per_city), temporal_holdout_days - 1)

    return (
        df.withColumn("temporal_test", F.col("day") >= cutoff)
        # Hash the detector id so every row of a detector falls on one side.
        .withColumn(
            "spatial_test", (F.abs(F.hash("detid")) % 100) < spatial_test_pct
        )
        .withColumn("mainland_test", F.col("city").isin(list(MAINLAND_CITIES)))
    )
