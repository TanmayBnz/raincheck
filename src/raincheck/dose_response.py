"""L3(a): the interpretable dose-response layer.

Produces the artefact a traffic authority would act on: "heavy rain on an
arterial during evening peak costs X% speed", stratified by rainfall band, road
class and time of day, with an interval rather than a bare point estimate.
"""

from pyspark.sql import Column, DataFrame, Window, functions as F

from raincheck.rain import BAND_NONE

# Ordered so the CASE chain reads as ascending hour boundaries.
TIME_OF_DAY = (
    (6, "night"),
    (10, "am_peak"),
    (16, "midday"),
    (20, "pm_peak"),
)
TIME_OF_DAY_LAST = "evening"

STRATA = ("rain_band", "fclass", "tod")

# Below this many observations a cell is suppressed. The study window is 40
# calendar days, so the four-way stratification in CLAUDE.md 6 leaves some
# strata nearly empty, and a mean over a handful of rows is not an elasticity.
MIN_CELL_N = 30

# Normal approximation; adequate at n >= 30, which MIN_CELL_N enforces.
Z_95 = 1.959964


def _bucket(hour: Column) -> Column:
    bucket = F.lit(TIME_OF_DAY_LAST)
    for edge, name in reversed(TIME_OF_DAY):
        bucket = F.when(hour < edge, F.lit(name)).otherwise(bucket)
    return bucket


def with_time_of_day(df: DataFrame, hour_column: str = "hour") -> DataFrame:
    return df.withColumn("tod", _bucket(F.col(hour_column)))


def dose_response_table(df: DataFrame, min_n: int = MIN_CELL_N) -> DataFrame:
    """Mean speed deviation per (rain band x road class x time of day), with CIs."""
    scoped = with_time_of_day(df).filter(F.col("typical_deviation").isNotNull())
    agg = scoped.groupBy(*STRATA).agg(
        F.count(F.lit(1)).alias("n"),
        F.avg("typical_deviation").alias("mean_deviation"),
        F.stddev_samp("typical_deviation").alias("sd"),
    )
    margin = Z_95 * F.col("sd") / F.sqrt(F.col("n"))
    kept = (
        agg.filter(F.col("n") >= min_n)
        .withColumn("ci_low", F.col("mean_deviation") - margin)
        .withColumn("ci_high", F.col("mean_deviation") + margin)
        .withColumn("pct_speed_loss", F.round(100.0 * F.col("mean_deviation"), 2))
    )

    # The dry band is the reference. Comparing bands only within a stratum keeps
    # road class and time of day fixed, so the difference is not confounded by
    # rain falling at different hours than dry weather.
    stratum = Window.partitionBy("fclass", "tod")
    dry_reference = F.max(
        F.when(F.col("rain_band") == BAND_NONE, F.col("mean_deviation"))
    ).over(stratum)
    return (
        kept.withColumn("dry_reference", dry_reference)
        .withColumn(
            "delta_vs_none_pct",
            F.round(100.0 * (F.col("mean_deviation") - F.col("dry_reference")), 3),
        )
        .orderBy(*STRATA)
    )
