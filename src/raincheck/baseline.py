"""L2a: free-flow speed and dry-only typical speed profiles.

This layer produces what the commercial platforms sell, computed from open data.
Two definitions carry the weight:

* Free-flow speed is the 85th percentile of speed **conditioned on occupancy
  below critical**. Conditioning on low density is what makes it a free-flow
  measurement rather than merely an off-peak one.
* Typical profiles are computed over **dry intervals only**. Including wet
  intervals lets the baseline absorb the very effect the project measures, which
  is the defect CLAUDE.md 3 identifies in Google's `staticDuration`.
"""

from pyspark.sql import DataFrame, Window, functions as F

# Occupancy bin width for locating the peak of the flow-occupancy curve.
OCC_BIN = 0.02

# Bins thinner than this are dropped before taking the maximum, so a single
# freak observation cannot masquerade as capacity.
MIN_BIN_OBS = 20

# A bin must also hold this share of its stratum's observations. An absolute
# count alone is not enough: on Manchester's trunk roads a bin at occ=0.98 held
# 431 observations - comfortably over any absolute floor, yet only ~0.1% of the
# stratum - and won the argmax with a physically impossible "capacity". Requiring
# a share confines the estimate to a well-populated part of the curve.
MIN_BIN_SHARE = 0.01

FREE_FLOW_PERCENTILE = 0.85

# A typical-speed cell built on fewer dry observations than this is not treated
# as a norm, and no deviation is computed against it. Without this the thinnest
# cells dominate the extremes of the dose-response table.
MIN_PROFILE_OBS = 30


def critical_occupancy(
    df: DataFrame,
    bin_width: float = OCC_BIN,
    min_obs: int = MIN_BIN_OBS,
    min_share: float = MIN_BIN_SHARE,
) -> DataFrame:
    """Occupancy at which flow peaks, per (city, road class).

    Pooled at road-class level rather than per detector on purpose: a single
    detector contributes on the order of a thousand observations, which is too
    few to locate the maximum of a noisy curve reliably.
    """
    stratum = Window.partitionBy("city", "fclass")
    binned = (
        df.filter(F.col("occ").isNotNull() & F.col("flow").isNotNull())
        .withColumn("occ_bin", F.round(F.floor(F.col("occ") / bin_width) * bin_width, 4))
        .groupBy("city", "fclass", "occ_bin")
        .agg(F.avg("flow").alias("mean_flow"), F.count(F.lit(1)).alias("n"))
        .withColumn("bin_share", F.col("n") / F.sum("n").over(stratum))
        .filter((F.col("n") >= min_obs) & (F.col("bin_share") >= min_share))
    )
    ranked = Window.partitionBy("city", "fclass").orderBy(F.desc("mean_flow"))
    return (
        binned.withColumn("rank", F.row_number().over(ranked))
        .filter(F.col("rank") == 1)
        .select(
            "city",
            "fclass",
            F.col("occ_bin").alias("critical_occ"),
            F.col("mean_flow").alias("capacity_flow"),
            F.col("n").alias("critical_bin_obs"),
            F.round(F.col("bin_share"), 4).alias("critical_bin_share"),
        )
    )


def free_flow_speed(
    df: DataFrame, critical: DataFrame, percentile: float = FREE_FLOW_PERCENTILE
) -> DataFrame:
    """Per-detector free-flow speed: high percentile of speed at low density.

    Detectors whose city reports no occupancy drop out rather than falling back
    to an unconditioned percentile. A second, weaker definition of the project's
    central metric would not be comparable with the first.
    """
    usable = df.join(critical, on=["city", "fclass"], how="inner").filter(
        F.col("occ").isNotNull() & F.col("speed").isNotNull()
    )
    totals = usable.groupBy("city", "detid").agg(
        F.count(F.lit(1)).alias("detector_obs")
    )
    scoped = usable.filter(F.col("occ") < F.col("critical_occ"))
    return (
        scoped.groupBy("city", "detid")
        .agg(
            F.round(F.expr(f"percentile(speed, {percentile})"), 4).alias("free_flow_speed"),
            F.count(F.lit(1)).alias("free_flow_obs"),
        )
        .join(totals, on=["city", "detid"], how="left")
        # A share near 1.0 means the conditioning did nothing and the result is
        # an off-peak percentile masquerading as free-flow speed.
        .withColumn("free_flow_share", F.col("free_flow_obs") / F.col("detector_obs"))
    )


# Spark's dayofweek is 1=Sunday .. 7=Saturday.
_WEEKEND_DOW = (1, 7)

PROFILE_KEYS = ("city", "detid", "is_weekend", "hour")


def with_profile_keys(df: DataFrame) -> DataFrame:
    """Add the local-time keys the typical profile is grouped by.

    Coarsened from CLAUDE.md 6's (detector, day-of-week, 5-min bin): with 21-23
    days per city that grain leaves roughly three observations per cell, while
    weekday/weekend x hour leaves 150-250. Hourly also matches ERA5's native
    resolution, so no rain detail is lost by the coarsening.
    """
    return df.withColumn(
        "is_weekend", F.dayofweek("ts_local").isin(list(_WEEKEND_DOW))
    ).withColumn("hour", F.hour("ts_local"))


def typical_profile(df: DataFrame) -> DataFrame:
    """Median dry-weather speed per detector, weekend flag and hour of day."""
    dry = df.filter(F.col("is_dry_baseline") & F.col("speed").isNotNull())
    return (
        with_profile_keys(dry)
        .groupBy(*PROFILE_KEYS)
        .agg(
            F.round(F.expr("percentile(speed, 0.5)"), 4).alias("typical_speed"),
            F.count(F.lit(1)).alias("n_obs"),
        )
    )


def delay_metrics(
    df: DataFrame,
    freeflow: DataFrame,
    profile: DataFrame,
    min_profile_obs: int = MIN_PROFILE_OBS,
) -> DataFrame:
    """Attach both delay definitions to every measurement.

    `ff_delay_ratio` is congestion irrespective of cause; `typical_deviation` is
    the anomaly against what this hour normally looks like, and is the target
    variable for L3.
    """
    keyed = with_profile_keys(df)
    joined = keyed.join(freeflow, on=["city", "detid"], how="left").join(
        profile, on=list(PROFILE_KEYS), how="left"
    )
    return joined.withColumn(
        "ff_delay_ratio",
        F.when(
            F.col("free_flow_speed") > 0, 1 - F.col("speed") / F.col("free_flow_speed")
        ),
    ).withColumn(
        "typical_deviation",
        F.when(
            (F.col("typical_speed") > 0) & (F.col("n_obs") >= min_profile_obs),
            1 - F.col("speed") / F.col("typical_speed"),
        ),
    )
