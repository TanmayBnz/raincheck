"""L2a/L4 for NDW travel-time sections: speed, free-flow, and delay indices.

Travel time carries the monitoring layer better than loop speed does, for reasons
measured in ``reports/phase1_nl_audit.md``:

* it is a **space-mean** speed over the section, so it sees a queue that a point
  detector 300 m upstream does not;
* 90.5% of sections are **floating car data** - an independent sensor, not a
  re-aggregation of the loops;
* 95.9% of sections carry a valid value against 84.8% of loop speeds, and support
  is near-uniform rather than concentrated near zero;
* section length is **published** (``lengthAffected``), so the conversion to km/h
  is exact rather than estimated.

What it does not supply is a trustworthy free-flow reference. NDW publishes a
``staticReferenceValue`` per section, but a third of observations are *faster*
than it, so it is not a free-flow ceiling. Free-flow is therefore derived here as
a high percentile of dry-interval section speed, and the published reference is
carried alongside so the two can be compared rather than conflated.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, functions as F

SECONDS_PER_HOUR = 3600.0
METRES_PER_KM = 1000.0


def _speed(length_m: Column, duration_s: Column) -> Column:
    """Space-mean speed in km/h, or null for a non-positive duration.

    Travel time reuses the ``-1.0`` sentinel alongside ``dataError=true``, so a
    naive division produces a negative speed; a zero duration produces infinity.
    Both are nulled rather than clamped, for the same reason ``clean_speed``
    nulls: a clamp manufactures observations at the boundary, and the boundary is
    exactly the upper tail the free-flow percentile reads from.
    """
    return F.when(
        (duration_s > 0) & (length_m > 0),
        (length_m / METRES_PER_KM) / (duration_s / SECONDS_PER_HOUR),
    )


def to_speed_kmh(df: DataFrame) -> DataFrame:
    """Add ``speed`` and ``reference_speed`` in km/h from durations and length."""
    length = F.col("length_m")
    return (
        df.withColumn("speed", _speed(length, F.col("duration_s")))
        .withColumn("reference_speed", _speed(length, F.col("reference_duration_s")))
    )


# A high percentile, not the maximum: the maximum of a section's speeds is a
# single anomalous traversal. CLAUDE.md 6 asks for p85 conditioned on occupancy
# below critical, but NDW publishes no occupancy at all, so the density
# conditioning is unavailable and this is a documented downgrade.
FREE_FLOW_PERCENTILE = 0.95
MIN_FREE_FLOW_OBS = 30


def free_flow_speed(df: DataFrame, percentile: float = FREE_FLOW_PERCENTILE,
                    min_obs: int = MIN_FREE_FLOW_OBS) -> DataFrame:
    """Per-section free-flow speed: a high percentile of dry-interval speed.

    Dry-only because a baseline computed over wet intervals absorbs the very
    effect the project measures. The guard matters for the same reason it did in
    the European arm, where single-observation baselines produced deviations of
    -63% before ``MIN_PROFILE_OBS`` was added.

    ``reference_speed`` - NDW's own published ``staticReferenceValue`` converted
    to km/h - is carried alongside rather than merged in. A third of live sections
    are *faster* than their own reference, so it is not a free-flow ceiling; the
    two are reported together precisely so that discrepancy stays visible.
    """
    dry = df.filter(F.col("is_dry_baseline") & F.col("speed").isNotNull())
    aggregated = dry.groupBy("section_id").agg(
        F.round(F.expr(f"percentile(speed, {percentile})"), 4).alias("_free_flow"),
        F.round(F.expr("percentile(reference_speed, 0.5)"), 4).alias("reference_speed"),
        F.count(F.lit(1)).alias("n_dry_obs"),
    )
    return aggregated.withColumn(
        "free_flow_speed",
        F.when(F.col("n_dry_obs") >= min_obs, F.col("_free_flow")),
    ).drop("_free_flow")


TIMEZONE = "Europe/Amsterdam"

# Spark's dayofweek is 1=Sunday .. 7=Saturday.
_WEEKEND_DOW = (1, 7)

PROFILE_KEYS = ("section_id", "is_weekend", "hour")

MIN_PROFILE_OBS = 30


def with_local_time(df: DataFrame, timezone: str = TIMEZONE) -> DataFrame:
    """Add ``ts_local`` and the local-time keys the typical profile groups by.

    NDW stamps everything UTC, but day-of-week x hour-of-day is a *local* concept
    and the Netherlands observes DST. 22:30 UTC on a summer Saturday is 00:30 on
    Sunday in Amsterdam, so keying on UTC would get both the hour and the weekend
    flag wrong.
    """
    local = F.from_utc_timestamp(F.col("ts_utc"), timezone)
    return (
        df.withColumn("ts_local", local)
        .withColumn("is_weekend", F.dayofweek(local).isin(list(_WEEKEND_DOW)))
        .withColumn("hour", F.hour(local))
    )


def typical_profile(df: DataFrame, min_obs: int = MIN_PROFILE_OBS) -> DataFrame:
    """Median dry-interval speed per section, weekend flag and local hour.

    Dry-only is the single most important methodological decision in the project:
    a baseline that includes rainy intervals absorbs the effect being measured and
    biases it toward zero. It is exactly the flaw that makes Google's
    ``staticDuration`` unusable for this purpose.
    """
    dry = df.filter(F.col("is_dry_baseline") & F.col("speed").isNotNull())
    return (
        dry.groupBy(*PROFILE_KEYS)
        .agg(
            F.round(F.expr("percentile(speed, 0.5)"), 4).alias("typical_speed"),
            F.count(F.lit(1)).alias("n_obs"),
        )
        .filter(F.col("n_obs") >= min_obs)
    )


def monitoring_view(df: DataFrame, freeflow: DataFrame,
                    profile: DataFrame) -> DataFrame:
    """Attach both delay definitions to every section observation.

    ``ff_delay_ratio`` is congestion irrespective of cause - the number a live map
    colours by. ``typical_deviation`` is the anomaly against what this hour
    normally looks like, and is the target variable for L3. They answer different
    questions and are deliberately not collapsed into one index.

    Left joins throughout: a section whose profile rests on too few observations
    keeps its row and withholds the anomaly, rather than being dropped or given a
    fabricated baseline.
    """
    joined = (
        df.join(freeflow, on="section_id", how="left")
        .join(profile, on=list(PROFILE_KEYS), how="left")
    )
    return (
        joined.withColumn(
            "ff_delay_ratio",
            F.when(F.col("free_flow_speed") > 0,
                   1 - F.col("speed") / F.col("free_flow_speed")),
        ).withColumn(
            "typical_deviation",
            F.when(F.col("typical_speed") > 0,
                   1 - F.col("speed") / F.col("typical_speed")),
        )
    )
