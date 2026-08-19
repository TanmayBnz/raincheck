"""L1 curation: raw UTD19 measurements to a conformed, unit-normalised table.

Each transform is a standalone DataFrame->DataFrame step so it can be tested
against a handful of rows rather than only observed at 134M-row scale.
"""

from pyspark.sql import Column, DataFrame, functions as F

from raincheck.cities import OCC_SCALES, SPEED_CAP_KMH, TIMEZONES


def apply_error_policy(df: DataFrame) -> DataFrame:
    """Drop rows UTD19 flagged as faulty, keep clean and unassessed rows.

    The flag is three-state (0 clean / 1 flagged / null unassessed). Filtering to
    error == 0 would be the obvious reading and would also discard everything:
    outside Essen, none of the study cities has any explicitly-clean row.
    """
    return df.filter(~F.coalesce(F.col("error"), F.lit(0)).eqNullSafe(F.lit(1)))


def _city_lookup(mapping: dict[str, float], default: float) -> Column:
    """Build a CASE expression mapping the `city` column through `mapping`."""
    expr = F.lit(default)
    for city, value in mapping.items():
        expr = F.when(F.col("city") == city, F.lit(value)).otherwise(expr)
    return expr


def normalize_occupancy(df: DataFrame, scales: dict[str, float] | None = None) -> DataFrame:
    """Put `occ` on a 0-1 fraction and null anything that cannot be one.

    Two distinct problems share this column. Four of the six study cities report
    occupancy as a percentage, which is a unit difference. All four additionally
    carry values that no unit can explain - Manchester reaches 2094, Rotterdam
    and Groningen contain literal Infinity - which is corruption. Rescaling
    without range-checking would turn 2094% into a plausible-looking 20.94.
    """
    scales = OCC_SCALES if scales is None else scales
    scaled = F.col("occ") / _city_lookup(scales, default=1.0)
    return df.withColumn(
        "occ",
        F.when(scaled.between(0.0, 1.0) & ~F.isnan(scaled), scaled).otherwise(None),
    )


def clean_speed(df: DataFrame, cap_kmh: float = SPEED_CAP_KMH) -> DataFrame:
    """Null non-positive and implausibly high speeds.

    Nulled rather than clipped: clipping would manufacture observations sitting
    exactly at the cap, which is precisely the upper tail the p85 free-flow
    percentile reads from.
    """
    speed = F.col("speed")
    return df.withColumn(
        "speed",
        F.when((speed > 0) & (speed <= cap_kmh) & ~F.isnan(speed), speed).otherwise(None),
    )


def add_timestamps(df: DataFrame, timezones: dict[str, str] | None = None) -> DataFrame:
    """Add `ts_local` (naive wall clock) and `ts_utc` (true instant).

    UTD19 stores `day` plus `interval` seconds after *local* midnight - confirmed
    empirically, since pooled weekday flow peaks at 17:00 and troughs at 03:00 on
    this axis. Both representations are kept because they answer different
    questions: day-of-week x hour-of-day profiles are a local-time concept, while
    the ERA5 rainfall grid is indexed in UTC.
    """
    timezones = TIMEZONES if timezones is None else timezones
    wall_clock = F.timestamp_seconds(
        F.unix_timestamp(F.col("day").cast("timestamp")) + F.col("interval")
    )
    tz = _city_lookup_str(timezones, default="UTC")
    return df.withColumn("ts_local", wall_clock).withColumn(
        # to_utc_timestamp reads the wall clock as local in `tz`, so it applies
        # the correct offset either side of the 2017-10-29 DST change.
        "ts_utc",
        F.to_utc_timestamp(wall_clock, tz),
    )


def _city_lookup_str(mapping: dict[str, str], default: str) -> Column:
    expr = F.lit(default)
    for city, value in mapping.items():
        expr = F.when(F.col("city") == city, F.lit(value)).otherwise(expr)
    return expr


DETECTOR_ATTRIBUTES = ("fclass", "road", "limit", "lanes", "linkid", "length", "pos", "long", "lat")


def join_detectors(measurements: DataFrame, detectors: DataFrame) -> DataFrame:
    """Attach detector attributes (road class, link id, position) to measurements.

    Left join on purpose. The Phase-1 audit found the measurement and metadata
    files disagree on which detectors exist, so an inner join would quietly drop
    real observations rather than surfacing the mismatch as null road class.

    Note this is a join, not map matching: detectors_public.csv already carries
    `linkid` and `fclass`. OSM is needed later for link geometry and the graph
    layer, not to establish detector-to-link correspondence.
    """
    meta = detectors.withColumnRenamed("citycode", "city").select(
        "city", "detid", *DETECTOR_ATTRIBUTES
    )
    return measurements.join(meta, on=["city", "detid"], how="left")


def curation_quality(scoped: DataFrame, curated: DataFrame) -> DataFrame:
    """Per-city record of what curation kept and what it nulled.

    CLAUDE.md 6 asks for retention to be logged per city as a data-quality
    metric rather than applied silently. Null rates are reported against the
    surviving rows, so `retention_pct` and `occ_null_pct` answer two separate
    questions: how much was dropped, and how much of what remains is unusable.
    """
    rows_in = scoped.groupBy("city").agg(F.count(F.lit(1)).alias("rows_in"))
    out = curated.groupBy("city").agg(
        F.count(F.lit(1)).alias("rows_out"),
        F.round(100.0 * F.sum(F.when(F.col("occ").isNull(), 1).otherwise(0)) / F.count(F.lit(1)), 4).alias("occ_null_pct"),
        F.round(100.0 * F.sum(F.when(F.col("speed").isNull(), 1).otherwise(0)) / F.count(F.lit(1)), 4).alias("speed_null_pct"),
    )
    return (
        rows_in.join(out, on="city", how="left")
        .withColumn("rows_out", F.coalesce(F.col("rows_out"), F.lit(0)))
        .withColumn(
            "retention_pct",
            F.round(100.0 * F.col("rows_out") / F.col("rows_in"), 4),
        )
    )
