"""Phase-1 feasibility audit over the raw UTD19 measurements table.

The project's design gates all downstream work on this audit: a city is only
usable if it actually reports speed and covers enough distinct days.
"""

from pyspark.sql import Column, DataFrame, Window, functions as F

MEASURED_VARIABLES = ("flow", "occ", "speed")


def _pct(numerator: Column) -> Column:
    return F.round(100.0 * numerator / F.count(F.lit(1)), 4)


def _pct_present(column: str) -> Column:
    """Percent of rows in the group where `column` is not null."""
    return _pct(F.count(F.col(column)))


def _pct_where(condition: Column) -> Column:
    return _pct(F.sum(F.when(condition, 1).otherwise(0)))


def audit_by_city(df: DataFrame) -> DataFrame:
    """One row per city summarising what that city's measurements contain."""
    span_days = F.datediff(F.max("day"), F.min("day")) + 1
    error = F.col("error")
    return df.groupBy("city").agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("detid").alias("n_detectors"),
        *[_pct_present(v).alias(f"{v}_pct") for v in MEASURED_VARIABLES],
        F.countDistinct("day").alias("n_days"),
        F.min("day").alias("first_day"),
        F.max("day").alias("last_day"),
        span_days.alias("span_days"),
        # Calendar completeness: a city can span two years yet hold ten days.
        F.round(100.0 * F.countDistinct("day") / span_days, 4).alias("day_density_pct"),
        # Three-state, so retention is a choice the curation layer must make
        # explicitly rather than inherit from a null-collapsing default.
        _pct_where(error == 0).alias("error_ok_pct"),
        _pct_where(error == 1).alias("error_flagged_pct"),
        _pct_where(error.isNull()).alias("error_unassessed_pct"),
        # Ranges expose per-city unit differences that L1 has to normalise:
        # occupancy as fraction vs percent, speed as km/h vs m/s.
        F.min("occ").alias("occ_min"),
        F.max("occ").alias("occ_max"),
        F.min("speed").alias("speed_min"),
        F.max("speed").alias("speed_max"),
        F.round(F.avg("speed"), 4).alias("speed_avg"),
    )


def interval_resolution_by_city(df: DataFrame) -> DataFrame:
    """Aggregation bin width in seconds, per city.

    Taken as the smallest gap between consecutive time-of-day slots. Counting
    slots per day would misreport any city whose coverage includes partial days.
    """
    slots = df.select("city", "interval").distinct()
    previous = F.lag("interval").over(
        Window.partitionBy("city").orderBy("interval")
    )
    gaps = slots.withColumn("gap", F.col("interval") - previous)
    return gaps.groupBy("city").agg(F.min("gap").cast("int").alias("resolution_sec"))
