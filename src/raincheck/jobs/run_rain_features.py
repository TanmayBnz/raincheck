"""L2b job: derive rain features and attach them to curated measurements.

Run via scripts/run_rain_features.sh, after fetch_era5.sh.
"""

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from raincheck import paths
from raincheck.rain import WET_THRESHOLD_MM_H, join_rain, rain_history
from raincheck.session import build_session


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="reports")
    parser.add_argument("--master", default=None)
    args = parser.parse_args()

    spark = build_session("raincheck-l2b-rain", master=args.master)

    measurements = spark.read.parquet(paths.CURATED_MEASUREMENTS)
    era5 = spark.read.parquet(f"{paths.CURATED_ERA5}/era5_long.parquet")
    print(f"ERA5_ROWS {era5.count()}")
    print(f"ERA5_CELLS {era5.select('grid_lat', 'grid_lon').distinct().count()}")

    rain = rain_history(era5)
    joined = join_rain(measurements, rain).cache()

    total = joined.count()
    matched = joined.filter(F.col("rain_mm_h").isNotNull()).count()
    print(f"JOIN_COVERAGE {matched}/{total} = {100.0 * matched / total:.4f}%")

    joined.write.mode("overwrite").partitionBy("city").parquet(paths.RAIN_FEATURES)

    split = joined.groupBy("city").agg(
        F.count(F.lit(1)).alias("rows"),
        F.round(100.0 * F.avg(F.col("rain_mm_h").isNotNull().cast("int")), 3).alias("rain_matched_pct"),
        F.round(100.0 * F.avg(F.col("is_wet").cast("int")), 3).alias("wet_pct"),
        F.round(100.0 * F.avg(F.col("is_dry_baseline").cast("int")), 3).alias("dry_baseline_pct"),
        F.round(F.max("rain_mm_h"), 3).alias("max_rain_mm_h"),
        F.countDistinct("grid_lat", "grid_lon").alias("era5_cells"),
    ).orderBy("city")

    bands = joined.groupBy("rain_band").agg(F.count(F.lit(1)).alias("rows")).orderBy("rain_band")

    table = split.toPandas()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    table.to_csv(out_dir / "l2b_rain_coverage.csv", index=False)
    (out_dir / "l2b_rain_coverage.md").write_text(
        f"# L2b rain coverage (wet threshold {WET_THRESHOLD_MM_H} mm/h)\n\n"
        + table.to_markdown(index=False)
        + "\n\n## Intensity bands\n\n"
        + bands.toPandas().to_markdown(index=False)
        + "\n"
    )
    print(table.to_string(index=False))
    print(bands.toPandas().to_string(index=False))
    print(f"RAIN_FEATURES {paths.RAIN_FEATURES}")
    spark.stop()


if __name__ == "__main__":
    main()
