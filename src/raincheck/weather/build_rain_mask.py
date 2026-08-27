"""Phase 3 -- hourly city-level rain labels from the cached ERA5 files.

The L2a baseline is dry-only, so before any baseline can be computed each
traffic interval must be labelled wet or dry. That needs rainfall, which
nominally belongs to Phase 4 -- but only the *downscaling* does. Marking an
hour wet or dry needs nothing finer than the native ~31 km reanalysis, which
is already on disk from the Phase-1 pre-check.

So this deliberately builds the coarse mask and stops there:

  - City-level area-mean, not per-detector. Manchester and Essen resolve to a
    single ERA5 cell anyway (confirmed in reports/phase2_curation.md §5), so a
    per-detector join at this resolution would be fake precision -- every
    detector in the city would receive an identical value dressed up as a
    spatial join. Torino has two cells and is treated the same way for
    consistency; the real spatial attribution arrives with the 2 km fields.
  - Exclusion only. A coarse mask is well suited to *removing* contaminated
    intervals from a baseline, because over-exclusion costs sample size but
    does not bias the result. It is NOT suitable for dose-response, where
    intensity has to be right at the detector. That is Phase 4's job.

Run:  python -m raincheck.weather.build_rain_mask
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from raincheck import config

# Met Office / IUTF bands in mm/h, kept identical to era5_precheck.py so event
# counts and baseline exclusions cannot drift apart.
BANDS = [
    ("Light", 0.1, 0.5),
    ("Moderate", 0.5, 4.0),
    ("Heavy", 4.0, 10.0),
    ("Extreme", 10.0, float("inf")),
]
EVENT_GAP_HOURS = 2

SCHEMA = StructType(
    [
        StructField("city", StringType(), False),
        StructField("rain_ts", TimestampType(), False),
        StructField("precip_mm", DoubleType(), False),
    ]
)


def read_city_series(city: str, path) -> list[tuple]:
    """Extract the hourly area-mean precipitation series from one netCDF file.

    ERA5 `tp` accumulates metres over the hour ENDING at the stamped valid_time,
    so the value at 15:00 covers 14:00-15:00. That convention is preserved here
    and honoured at join time -- see rain_ts_for() below. Getting it wrong
    shifts every rain label by an hour, which for onset effects is the
    difference between measuring driver adaptation and measuring nothing.
    """
    import xarray as xr

    ds = xr.open_dataset(path)
    var = "tp" if "tp" in ds else list(ds.data_vars)[0]
    da = ds[var]

    tname = next((d for d in ("valid_time", "time") if d in da.dims), None)
    if tname is None:
        raise ValueError(f"no time dimension in {path.name}; dims={da.dims}")

    space_dims = [d for d in da.dims if d != tname]
    series = (da.mean(dim=space_dims) if space_dims else da) * 1000.0  # m -> mm

    times = series[tname].values
    mm = series.values
    out = []
    for t, v in zip(times, mm):
        # numpy datetime64 -> naive UTC datetime. Built as tz-aware then
        # stripped: the Spark session timezone is pinned to UTC, so a naive
        # value round-trips literally instead of picking up a machine offset.
        secs = int(t.astype("datetime64[s]").astype("int64"))
        out.append((city, datetime.fromtimestamp(secs, timezone.utc).replace(tzinfo=None), float(v)))
    return out


def rain_ts_for(ts_col: str):
    """Map a measurement instant to the ERA5 stamp whose accumulation covers it.

    A reading at 14:23 UTC falls inside the accumulation window (14:00, 15:00],
    which ERA5 stamps 15:00. So the key is the hour floor plus one hour, not
    the hour floor.
    """
    return F.date_trunc("hour", F.col(ts_col)) + F.expr("INTERVAL 1 HOUR")


def label(df, wet_threshold: float, dry_buffer: int):
    """Add wet/dry, band, event id, and dry-spell antecedent per city."""
    is_wet = F.col("precip_mm") >= F.lit(wet_threshold)

    band = F.lit("Dry")
    for name, lo, hi in reversed(BANDS):
        band = F.when(
            (F.col("precip_mm") >= F.lit(lo)) & (F.col("precip_mm") < F.lit(hi)), F.lit(name)
        ).otherwise(band)

    w = Window.partitionBy("city").orderBy("rain_ts")
    labelled = df.withColumn("is_wet", is_wet).withColumn("band", band)

    # Hours since the last wet hour. Rows are contiguous hourly ERA5 stamps, so
    # a row counter difference is a genuine hour count -- but only within one
    # city, hence the partition.
    labelled = (
        labelled.withColumn("rn", F.row_number().over(w))
        .withColumn(
            "last_wet_rn",
            F.last(F.when(F.col("is_wet"), F.col("rn")), ignorenulls=True).over(
                w.rowsBetween(Window.unboundedPreceding, 0)
            ),
        )
        .withColumn(
            "hours_since_rain",
            F.when(F.col("is_wet"), F.lit(0)).otherwise(F.col("rn") - F.col("last_wet_rn")),
        )
    )

    # A new event starts on a wet hour preceded by >= EVENT_GAP_HOURS dry hours.
    # Same rule as the Phase-1 count, so the two remain comparable.
    prev_gap = F.lag("hours_since_rain", 1).over(w)
    starts = F.col("is_wet") & (prev_gap.isNull() | (prev_gap >= F.lit(EVENT_GAP_HOURS)))
    labelled = labelled.withColumn(
        "event_id",
        F.when(
            F.col("is_wet"),
            F.sum(starts.cast("int")).over(w.rowsBetween(Window.unboundedPreceding, 0)),
        ),
    )

    # THE column the baseline layer filters on. Not simply "not raining": the
    # surface stays wet and drivers stay cautious after rain stops, so an
    # unbuffered test would readmit the recovery period into the baseline.
    labelled = labelled.withColumn(
        "is_dry_clean",
        (~F.col("is_wet")) & (F.col("hours_since_rain") >= F.lit(dry_buffer)),
    )

    return labelled.drop("rn", "last_wet_rn")


def main() -> int:
    spark = config.get_spark("build_rain_mask")
    spark.sparkContext.setLogLevel("WARN")
    conf = config.load_cities_conf()
    bconf = conf["baseline"]
    cities = list(conf["study"].keys())

    rows = []
    for city in cities:
        path = config.ERA5_RAW / f"{city}.nc"
        if not path.exists():
            print(f"FAIL: {path} missing -- run raincheck.weather.era5_precheck first")
            spark.stop()
            return 1
        series = read_city_series(city, path)
        print(f"  {city}: {len(series):,} hourly stamps from {path.name}")
        rows.extend(series)

    df = spark.createDataFrame(rows, schema=SCHEMA)
    labelled = label(df, float(bconf["wet_threshold_mm"]), int(bconf["dry_buffer_hours"]))

    out = config.spark_path(config.RAIN_HOURLY)
    labelled.write.mode("overwrite").partitionBy("city").parquet(out)

    written = spark.read.parquet(out)
    summary = (
        written.groupBy("city")
        .agg(
            F.count(F.lit(1)).alias("hours"),
            F.sum(F.col("is_wet").cast("int")).alias("wet"),
            F.sum(F.col("is_dry_clean").cast("int")).alias("dry_clean"),
            F.countDistinct("event_id").alias("events"),
            F.max("hours_since_rain").alias("longest_dry_spell_h"),
        )
        .orderBy("city")
    )
    print()
    summary.show(truncate=False)

    total = written.count()
    print(f"wrote {out} ({total:,} city-hours)")

    # The buffer must actually cost something; if dry_clean equals not-wet, the
    # window function silently did nothing.
    check = written.agg(
        F.sum((~F.col("is_wet")).cast("int")).alias("not_wet"),
        F.sum(F.col("is_dry_clean").cast("int")).alias("dry_clean"),
    ).collect()[0]
    if check["not_wet"] == check["dry_clean"]:
        print("FAIL: dry buffer excluded nothing -- the post-rain window is not being applied")
        spark.stop()
        return 1
    print(
        f"dry buffer excluded {check['not_wet'] - check['dry_clean']:,} "
        f"post-rain hours that a naive 'not raining' test would have kept"
    )

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
