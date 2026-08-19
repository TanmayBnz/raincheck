"""L2b, second half: rainfall features joined to detectors.

Two conventions drive this module and both are easy to get wrong silently:

* ERA5 hourly `tp` is the accumulation over the hour **ending** at its timestamp.
* UTD19's `interval` is the **start** of a 5-minute aggregation bin.

Together they mean a bin stamped 14:00 (covering 14:00-14:05) belongs to the
ERA5 record stamped 15:00. An off-by-one here does not raise - it just weakens
the measured rain effect, which is indistinguishable from "rain matters less
than we thought".
"""

from pyspark.sql import Column, DataFrame, Window, functions as F

# ERA5 single-level grid spacing in degrees.
GRID_DEG = 0.25

# ERA5 puts tiny non-zero precipitation almost everywhere. Treating any tp > 0 as
# rain would mark nearly every hour wet and leave no dry baseline to compare
# against, so "wet" needs a floor.
WET_THRESHOLD_MM_H = 0.1

# Intensity bands for the interpretable dose-response model. **Per rung**: edges
# tuned to one rainfall product do not transfer to another.
#
# BAND_EDGES was calibrated against raw ERA5, whose 28 km hourly means capped the
# whole European corpus at 4.26 mm/h - so `heavy` held one unusable hour and
# `very_heavy` was empty. A single 5-minute radar frame reaches 65.6 mm/h with
# p99.9 at 14.2, so under these edges everything from 10 mm/h upward would
# collapse into one band. RADAR_BAND_EDGES adds the tier that separation needs.
BAND_EDGES = ((1.0, "light"), (4.0, "moderate"), (10.0, "heavy"))
RADAR_BAND_EDGES = (
    (1.0, "light"), (4.0, "moderate"), (10.0, "heavy"), (30.0, "very_heavy"))
BAND_TOP = "very_heavy"
BAND_NONE = "none"


def assign_grid_cell(df: DataFrame, resolution: float = GRID_DEG) -> DataFrame:
    """Snap detector coordinates onto the ERA5 grid.

    At ~28 km spacing each study city occupies one or two cells, so this join is
    coarse by construction. That is a real limitation of using raw ERA5, and it
    is precisely what spateGAN's 2 km fields would fix.
    """
    snap = lambda col: F.round(F.round(F.col(col) / resolution) * resolution, 4)
    return df.withColumn("grid_lat", snap("lat")).withColumn("grid_lon", snap("long"))


def add_rain_hour(df: DataFrame) -> DataFrame:
    """Key each measurement to the ERA5 hour whose accumulation window contains it."""
    return df.withColumn(
        "rain_hour", F.date_trunc("hour", F.col("ts_utc")) + F.expr("INTERVAL 1 HOUR")
    )


def _band(intensity: Column, edges=BAND_EDGES, top: str = BAND_TOP) -> Column:
    band = F.lit(top)
    for edge, name in reversed(edges):
        band = F.when(intensity < edge, F.lit(name)).otherwise(band)
    banded = F.when(intensity < WET_THRESHOLD_MM_H, F.lit(BAND_NONE)).otherwise(band)
    # A null intensity makes every comparison above null, so without this guard
    # the row falls through to the *heaviest* band. On the first radar join that
    # placed 20,621 bins with no rainfall data into "extreme" - 8.73% of the
    # corpus and more than heavy and moderate combined. Missing stays missing.
    return F.when(intensity.isNull(), F.lit(None).cast("string")).otherwise(banded)


def with_rain_band(df: DataFrame, column: str = "rain_mm_h",
                   edges=BAND_EDGES, top: str = BAND_TOP) -> DataFrame:
    """Assign an intensity band. Pass ``edges``/``top`` per rung - see BAND_EDGES."""
    return df.withColumn("rain_band", _band(F.col(column), edges, top))


# Hours of lookback used to decide whether the road surface is dry. Rain stops
# before puddles do, so "not raining right now" is not the same as "dry".
DRY_BASELINE_LOOKBACK_H = 6


def rain_history(era5: DataFrame) -> DataFrame:
    """Derive intensity, accumulation and spell features per ERA5 grid cell.

    Ranges are expressed in seconds rather than row counts so a gap in the hourly
    series cannot quietly turn a 3-hour window into a 3-row one.
    """
    cell = ["grid_lat", "grid_lon"]
    df = era5.withColumn("ts_epoch", F.unix_timestamp("ts_utc"))

    ordered = Window.partitionBy(*cell).orderBy("ts_epoch")
    trailing = lambda hours: ordered.rangeBetween(-(hours - 1) * 3600, 0)

    df = (
        df.withColumn("rain_mm_h", F.col("tp_mm"))
        .withColumn("rain_1h", F.col("tp_mm"))
        .withColumn("rain_3h", F.round(F.sum("tp_mm").over(trailing(3)), 6))
        .withColumn("rain_6h", F.round(F.sum("tp_mm").over(trailing(6)), 6))
        .withColumn("is_wet", F.col("tp_mm") >= WET_THRESHOLD_MM_H)
    )

    # A wet spell starts on the first wet hour after any dry hour. Cumulative
    # onsets give every spell a stable id to group by.
    previously_wet = F.lag("is_wet").over(ordered)
    onset = F.col("is_wet") & (previously_wet.isNull() | ~previously_wet)
    df = df.withColumn("is_onset", onset).withColumn(
        "spell_id",
        F.sum(F.col("is_onset").cast("int")).over(
            ordered.rowsBetween(Window.unboundedPreceding, 0)
        ),
    )

    spell = Window.partitionBy(*cell, "spell_id")
    df = df.withColumn(
        "hours_since_onset",
        F.when(
            F.col("is_wet"),
            ((F.col("ts_epoch") - F.min("ts_epoch").over(spell)) / 3600).cast("int"),
        ),
    )

    # Length of the dry gap immediately before this spell, measured at the onset
    # row and then carried across the spell. Null for a spell with no earlier
    # rain on record - unknown, not zero.
    last_wet_before = F.max(F.when(F.col("is_wet"), F.col("ts_epoch"))).over(
        ordered.rowsBetween(Window.unboundedPreceding, -1)
    )
    at_onset = F.when(
        F.col("is_onset"),
        ((F.col("ts_epoch") - last_wet_before) / 3600 - 1).cast("int"),
    )
    df = df.withColumn("_antecedent_at_onset", at_onset).withColumn(
        "antecedent_dry_hours",
        F.when(F.col("is_wet"), F.max("_antecedent_at_onset").over(spell)),
    )

    df = df.withColumn(
        "is_dry_baseline",
        F.sum("tp_mm").over(trailing(DRY_BASELINE_LOOKBACK_H)) < WET_THRESHOLD_MM_H,
    )

    return with_rain_band(df).drop("ts_epoch", "is_onset", "_antecedent_at_onset")


RAIN_FEATURE_COLUMNS = (
    "rain_mm_h",
    "rain_1h",
    "rain_3h",
    "rain_6h",
    "rain_band",
    "is_wet",
    "is_dry_baseline",
    "hours_since_onset",
    "antecedent_dry_hours",
)


def join_rain(measurements: DataFrame, rain: DataFrame) -> DataFrame:
    """Attach rain features to each measurement by grid cell and containing hour.

    Left join: a measurement with no matching ERA5 hour keeps its row with null
    rain, so coverage gaps show up as a countable number rather than as silently
    vanished traffic observations.
    """
    keyed = add_rain_hour(assign_grid_cell(measurements))
    lookup = rain.select(
        "grid_lat",
        "grid_lon",
        F.col("ts_utc").alias("rain_hour"),
        *RAIN_FEATURE_COLUMNS,
    )
    return keyed.join(lookup, on=["grid_lat", "grid_lon", "rain_hour"], how="left")
