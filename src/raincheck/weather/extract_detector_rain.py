"""Phase 4 / L2b -- sample the downscaled fields at detector locations.

The downscaler emits 2 km / 10 min precipitation over a ~440 km radius patch:
about 41 MB per day per city, and almost all of it is sea, farmland and other
cities. What the pipeline needs is a small table -- one precipitation value per
detector per 10 minutes.

Sampling is **nearest-neighbour on the model's own grid**, not on the
`hires_cell` id computed during L1 curation. That id was a rounded 0.018 deg
lattice built before the model had ever run; the real output grid is offset
from it (52.33, 52.34, 52.36 ... are not multiples of 0.018). Joining on the
synthetic id would silently mis-assign roughly half the detectors by one cell.
`hires_cell` is kept in the curated table as a coarse grouping key, but the
rainfall join uses the actual coordinates.

Output: lake/era5/curated/detector_rain, partitioned by city.

Run:  python -m raincheck.weather.extract_detector_rain
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from raincheck import config

DOWNSCALED = config.LAKE_ROOT / "era5" / "downscaled"
DETECTOR_RAIN = config.LAKE_ROOT / "era5" / "curated" / "detector_rain"

# The vendored model writes precipitation as float64 over a 144x247x432 grid.
# Only the detector cells are retained, so the working set collapses by ~99%.


def detector_coords(city: str) -> list[tuple[str, float, float]]:
    """(detid, lat, lon) for every detector with geometry in one city."""
    import duckdb

    path = (config.CURATED_MEASUREMENTS / f"city={city}" / "**" / "*.parquet").as_posix()
    rows = duckdb.sql(
        f"""
        SELECT DISTINCT detid, lat, lon
        FROM read_parquet('{path}')
        WHERE lat IS NOT NULL AND lon IS NOT NULL
        ORDER BY detid
        """
    ).fetchall()
    return [(r[0], float(r[1]), float(r[2])) for r in rows]


def extract_file(path: Path, dets: list[tuple[str, float, float]]):
    """Pull each detector's 10-minute series out of one downscaled file."""
    import numpy as np
    import xarray as xr

    ds = xr.open_dataset(path)
    da = ds["precipitation"]

    lats = np.array([d[1] for d in dets])
    lons = np.array([d[2] for d in dets])

    # One vectorised nearest-neighbour selection for all detectors at once.
    # Looping .sel() per detector would re-index the array hundreds of times.
    sel = da.sel(
        lat=xr.DataArray(lats, dims="det"),
        lon=xr.DataArray(lons, dims="det"),
        method="nearest",
    )

    times = sel["time"].values
    values = sel.values  # (time, det)

    # Report how far each detector was moved to reach a grid cell. At 2 km
    # resolution anything beyond ~1.5 km means the detector sits outside the
    # patch and is being silently snapped to its edge.
    grid_lat = sel["lat"].values
    grid_lon = sel["lon"].values
    dlat_km = (grid_lat - lats) * 111.32
    dlon_km = (grid_lon - lons) * 111.32 * np.cos(np.radians(lats))
    offsets = np.sqrt(dlat_km**2 + dlon_km**2)

    # Spark's TimestampType rejects numpy.datetime64 outright, so convert once
    # here rather than per detector. Built tz-aware from epoch seconds then
    # stripped: the session timezone is pinned to UTC, so a naive value
    # round-trips literally instead of picking up a machine offset.
    stamps = [
        datetime.fromtimestamp(
            int(t.astype("datetime64[s]").astype("int64")), timezone.utc
        ).replace(tzinfo=None)
        for t in times
    ]

    rows = []
    for j, (detid, _, _) in enumerate(dets):
        for i, stamp in enumerate(stamps):
            rows.append((detid, stamp, float(values[i, j])))
    return rows, times, offsets


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--city", help="single city (default: all study cities)")
    args = parser.parse_args()

    import numpy as np
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    spark = config.get_spark("extract_detector_rain")
    spark.sparkContext.setLogLevel("WARN")
    conf = config.load_cities_conf()
    cities = [args.city] if args.city else list(conf["study"].keys())

    schema = StructType(
        [
            StructField("detid", StringType(), False),
            StructField("ts_utc", TimestampType(), False),
            # The model emits precipitation in mm/h at 10-minute resolution.
            StructField("precip_mm_h", DoubleType(), False),
        ]
    )

    any_written = False
    for city in cities:
        files = sorted((DOWNSCALED / city).glob("spateGAN_ERA5_latlon_*.nc"))
        if not files:
            print(f"  {city}: no downscaled files -- run raincheck.weather.run_downscaling")
            continue

        dets = detector_coords(city)
        print(f"  {city}: {len(dets)} detectors x {len(files)} files")

        all_rows, max_offset, n_times = [], 0.0, 0
        for path in files:
            rows, times, offsets = extract_file(path, dets)
            all_rows.extend(rows)
            max_offset = max(max_offset, float(np.max(offsets)))
            n_times += len(times)
            print(f"    {path.name[:60]}: {len(times)} steps")

        if max_offset > 1.5:
            # 2 km cells: a detector should never be more than ~1.4 km from a
            # cell centre unless it is outside the patch entirely.
            print(f"    FAIL: detector snapped {max_offset:.1f} km to reach the grid")
            spark.stop()
            return 1
        print(f"    max detector-to-cell distance: {max_offset:.2f} km")

        df = spark.createDataFrame(all_rows, schema=schema).withColumn("city", F.lit(city))

        # Chunks overlap by a day, and that overlap is load-bearing: the model
        # emits NaN for roughly the first four hours of every run while it has
        # no preceding context to condition on. The overlapping neighbour holds
        # a real value for exactly those timesteps.
        #
        # So deduplication must PREFER the non-NaN reading. dropDuplicates keeps
        # an arbitrary row, which silently kept the warm-up NaNs and left ~4.5%
        # of every detector's series missing, concentrated on chunk boundaries.
        df = (
            df.groupBy("city", "detid", "ts_utc")
            .agg(
                F.first(
                    F.when(~F.isnan("precip_mm_h"), F.col("precip_mm_h")),
                    ignorenulls=True,
                ).alias("precip_mm_h")
            )
            # Whatever is still missing is a genuine edge of the whole series.
            # Carried as NULL, not NaN: NaN silently compares false against
            # every threshold and would masquerade as "dry".
            .select("city", "detid", "ts_utc", "precip_mm_h")
        )

        (
            df.write.mode("overwrite" if city == cities[0] else "append")
            .partitionBy("city")
            .parquet(config.spark_path(DETECTOR_RAIN))
        )
        any_written = True
        print(f"    wrote {len(all_rows):,} detector-timesteps ({n_times} unique steps)")

    if not any_written:
        print("FAIL: nothing extracted")
        spark.stop()
        return 1

    out = spark.read.parquet(config.spark_path(DETECTOR_RAIN))
    summary = out.groupBy("city").agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("detid").alias("dets"),
        F.min("ts_utc").alias("from"),
        F.max("ts_utc").alias("to"),
        F.round(F.max("precip_mm_h"), 2).alias("max_mm_h"),
        F.round(100.0 * F.avg((F.col("precip_mm_h") >= 0.1).cast("int")), 1).alias("wet_pct"),
        # Residual gaps after the overlap repair -- the true start/end edges of
        # each city's series, which fall in the padding days.
        F.round(100.0 * F.avg(F.col("precip_mm_h").isNull().cast("int")), 2).alias("null_pct"),
    )
    summary.show(truncate=False)

    # The residual gaps are structural, not a defect: the model emits NaN for
    # its first few hours, and while chunk overlap repairs interior boundaries,
    # the OUTER edge of each contiguous group has no neighbour to borrow from.
    # Manchester has three groups (its window is broken by data gaps), so six
    # such edges -- about 24 of 648 hours, i.e. the 3.7% seen above.
    #
    # That is harmless as long as the edges fall in the padding days, which is
    # exactly what PAD_DAYS exists for. So the gate is not "how much of the
    # series is missing" -- that number is expected to be non-zero -- but "does
    # any missing value land on a day that carries traffic data".
    from raincheck.weather.run_downscaling import data_days

    bad = 0
    for city in cities:
        days = {d.isoformat() for d in data_days(city)}
        if not days:
            continue
        n = (
            out.filter((F.col("city") == city) & F.col("precip_mm_h").isNull())
            .filter(F.date_format("ts_utc", "yyyy-MM-dd").isin(list(days)))
            .count()
        )
        print(f"  {city}: {n:,} missing values on days that carry traffic data")
        bad += n
    if bad:
        print(f"FAIL: {bad:,} rain gaps fall on traffic data days -- widen PAD_DAYS")
        spark.stop()
        return 1
    print("PASS: every gap falls in padding, not on a traffic data day")

    print(f"wrote {out.count():,} rows to {DETECTOR_RAIN}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
