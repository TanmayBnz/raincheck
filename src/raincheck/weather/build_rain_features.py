"""Phase 4 / L2b -- rain features, and the join that finally puts rain on traffic.

Raw intensity is a poor predictor of how rain affects driving. CONTEXT.md §L2b
specifies features that encode the mechanisms instead, and all of them are
derived here from the 2 km / 10 min detector series:

  precip_mm_h        instantaneous intensity at the detector's own cell
  band               Light / Moderate / Heavy / Extreme (Met Office = IUTF)
  acc_10/30/60       trailing accumulation -- standing water, drainage load
  minutes_since_onset  driver adaptation: the first minutes of rain are
                     disproportionately disruptive, and this is only knowable
                     at 10-minute resolution. It is the single feature that
                     most justifies the downscaling.
  dry_spell_hours    hours since rain last fell -- the "first rain after a dry
                     spell" oil-film effect

Output:
  lake/era5/curated/rain_features   -- per detector per 10 min
  lake/analysis/measurements_rain   -- the analysis table: every curated traffic
                                       interval with its baselines, delay
                                       metrics and rain features attached

Run:  python -m raincheck.weather.build_rain_features
"""

from __future__ import annotations

import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

from raincheck import config
from raincheck.weather.extract_detector_rain import DETECTOR_RAIN

RAIN_FEATURES = config.LAKE_ROOT / "era5" / "curated" / "rain_features"
ANALYSIS = config.LAKE_ROOT / "analysis" / "measurements_rain"

STEP_MIN = 10
WET_MM_H = 0.1
# Same bands as era5_precheck and build_rain_mask, so every rain figure in the
# project -- event counts, baseline exclusions, dose-response bins -- is spoken
# in one vocabulary.
BANDS = [("Light", 0.1, 0.5), ("Moderate", 0.5, 4.0), ("Heavy", 4.0, 10.0),
         ("Extreme", 10.0, float("inf"))]
# A gap of this many dry steps ends a rain event (2 hours, matching the hourly
# EVENT_GAP_HOURS used at native resolution).
EVENT_GAP_STEPS = 12


def features(rain):
    """Derive the mechanism features per detector, ordered in time."""
    w = Window.partitionBy("city", "detid").orderBy("ts_utc")

    is_wet = F.col("precip_mm_h") >= F.lit(WET_MM_H)
    band = F.lit("Dry")
    for name, lo, hi in reversed(BANDS):
        band = F.when(
            (F.col("precip_mm_h") >= F.lit(lo)) & (F.col("precip_mm_h") < F.lit(hi)),
            F.lit(name),
        ).otherwise(band)

    # mm/h over a 10-minute step contributes mm/h * (10/60) millimetres.
    step_mm = F.col("precip_mm_h") * F.lit(STEP_MIN / 60.0)

    df = rain.withColumn("is_wet", is_wet).withColumn("band", band).withColumn("step_mm", step_mm)

    for minutes in (10, 30, 60):
        n = minutes // STEP_MIN
        df = df.withColumn(
            f"acc_{minutes}min_mm",
            F.sum("step_mm").over(w.rowsBetween(-(n - 1), 0)),
        )

    # Steps since the last wet step. Row-index arithmetic is valid because the
    # series is a complete, gap-free 10-minute grid within each downscaled run.
    df = (
        df.withColumn("rn", F.row_number().over(w))
        .withColumn(
            "last_wet_rn",
            F.last(F.when(F.col("is_wet"), F.col("rn")), ignorenulls=True).over(
                w.rowsBetween(Window.unboundedPreceding, 0)
            ),
        )
        .withColumn(
            "steps_since_wet",
            F.when(F.col("is_wet"), F.lit(0)).otherwise(F.col("rn") - F.col("last_wet_rn")),
        )
    )

    # Onset: a wet step preceded by a long enough dry gap starts a new event.
    prev_gap = F.lag("steps_since_wet", 1).over(w)
    onset = F.col("is_wet") & (prev_gap.isNull() | (prev_gap >= F.lit(EVENT_GAP_STEPS)))
    df = (
        df.withColumn("is_onset", onset)
        .withColumn(
            "event_id",
            F.when(
                F.col("is_wet"),
                F.sum(onset.cast("int")).over(w.rowsBetween(Window.unboundedPreceding, 0)),
            ),
        )
        .withColumn(
            "onset_rn",
            F.last(F.when(onset, F.col("rn")), ignorenulls=True).over(
                w.rowsBetween(Window.unboundedPreceding, 0)
            ),
        )
        .withColumn(
            "minutes_since_onset",
            F.when(F.col("is_wet"), (F.col("rn") - F.col("onset_rn")) * F.lit(STEP_MIN)),
        )
        .withColumn(
            "dry_spell_hours",
            F.when(~F.col("is_wet"), F.col("steps_since_wet") * F.lit(STEP_MIN / 60.0)),
        )
    )

    return df.drop("rn", "last_wet_rn", "onset_rn", "steps_since_wet", "step_mm")


def main() -> int:
    spark = config.get_spark("build_rain_features")
    spark.sparkContext.setLogLevel("WARN")

    rain = spark.read.parquet(config.spark_path(DETECTOR_RAIN))
    feat = features(rain)
    feat.write.mode("overwrite").partitionBy("city").parquet(config.spark_path(RAIN_FEATURES))
    written = spark.read.parquet(config.spark_path(RAIN_FEATURES))
    print(f"rain features: {written.count():,} detector-timesteps")

    # ---- join onto traffic ----------------------------------------------
    m = spark.read.parquet(config.spark_path(config.MEASUREMENTS_DELAY))

    # measurements_delay already carries rain columns -- the COARSE ones, from
    # the city-hour ~31 km mask that Phase 3 used to define "dry". They are not
    # dropped: holding both labels side by side is the cleanest demonstration of
    # what the downscaling actually bought, and `is_dry_clean` in particular is
    # the provenance of which intervals the baselines were built from.
    #
    # They are renamed instead, so the plain names belong to the 2 km / 10 min
    # fields that supersede them.
    for col in ("precip_mm", "band", "is_wet", "event_id", "hours_since_rain"):
        if col in m.columns:
            m = m.withColumnRenamed(col, f"era5_{col}")

    # Measurements are 5-minute (or 3-minute) instants; the downscaled fields
    # are stamped every 10 minutes. The stamp labels the interval BEGINNING at
    # that time, so a reading is matched to the 10-minute bin containing it --
    # a floor, not a round. Rounding would attribute a 14:09 reading to the
    # 14:10 field, i.e. to rain that had not fallen yet.
    keyed = m.withColumn(
        "ts_10min",
        F.timestamp_seconds((F.unix_timestamp("ts_utc") / 600).cast("long") * 600),
    )

    joined = keyed.join(
        written.select(
            "city", "detid",
            F.col("ts_utc").alias("ts_10min"),
            "precip_mm_h", "band", "is_wet", "is_onset", "event_id",
            "acc_10min_mm", "acc_30min_mm", "acc_60min_mm",
            "minutes_since_onset", "dry_spell_hours",
        ),
        ["city", "detid", "ts_10min"],
        "left",
    )

    n_in, n_out = m.count(), joined.count()
    if n_out != n_in:
        print(f"FAIL: join changed row count by {n_out - n_in:+,} (duplicate rain keys?)")
        spark.stop()
        return 1

    joined.write.mode("overwrite").partitionBy("city", "year", "month").parquet(
        config.spark_path(ANALYSIS)
    )
    out = spark.read.parquet(config.spark_path(ANALYSIS))

    matched = out.filter(F.col("precip_mm_h").isNotNull()).count()
    print(f"analysis table: {out.count():,} rows, {100.0 * matched / n_in:.1f}% with rain features")

    out.groupBy("city").agg(
        F.count(F.lit(1)).alias("rows"),
        F.round(100.0 * F.avg(F.col("precip_mm_h").isNotNull().cast("int")), 1).alias("rain_pct"),
        F.round(100.0 * F.avg(F.col("is_wet").cast("int")), 1).alias("wet_pct"),
        F.round(F.max("precip_mm_h"), 1).alias("max_mm_h"),
        F.countDistinct("event_id").alias("events"),
    ).orderBy("city").show(truncate=False)

    if matched == 0:
        print("FAIL: no measurement matched a rain field -- check the 10-minute key")
        spark.stop()
        return 1

    print(f"wrote {ANALYSIS}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
