"""L3(a) job: assemble the training table and quantify the dose-response.

Run via scripts/run_dose_response.sh, after run_baselines.sh.
"""

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from raincheck import paths
from raincheck.baseline import delay_metrics
from raincheck.dose_response import MIN_CELL_N, dose_response_table, with_time_of_day
from raincheck.session import build_session

BAND_ORDER = ("none", "light", "moderate", "heavy", "very_heavy")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="reports")
    parser.add_argument("--master", default=None)
    args = parser.parse_args()

    spark = build_session("raincheck-l3a-dose-response", master=args.master)

    rain = spark.read.parquet(paths.RAIN_FEATURES)
    freeflow = spark.read.parquet(paths.BASELINE_FREEFLOW)
    profile = spark.read.parquet(paths.BASELINE_TYPICAL)

    training = with_time_of_day(delay_metrics(rain, freeflow, profile)).cache()
    training.write.mode("overwrite").partitionBy("city").parquet(paths.TRAINING)
    print(f"TRAINING_ROWS {training.count()} -> {paths.TRAINING}")

    usable = training.filter(F.col("typical_deviation").isNotNull())
    withheld = training.count() - usable.count()
    print(f"WITH_TARGET {usable.count()}  WITHHELD_THIN_BASELINE {withheld}")

    print("=== target availability per city ===")
    training.groupBy("city").agg(
        F.count(F.lit(1)).alias("rows"),
        F.round(100.0 * F.avg(F.col("typical_deviation").isNotNull().cast("int")), 2).alias(
            "with_target_pct"
        ),
    ).orderBy("city").show(10, False)

    # Headline: deviation by rain band, pooled across road class and time of day.
    headline = (
        usable.groupBy("rain_band")
        .agg(
            F.count(F.lit(1)).alias("n"),
            F.round(100.0 * F.avg("typical_deviation"), 3).alias("pct_speed_loss"),
            F.round(F.avg("rain_mm_h"), 3).alias("mean_mm_h"),
        )
        .orderBy(F.expr(f"array_position(array{BAND_ORDER}, rain_band)"))
    )
    print("=== headline: speed loss by rain band ===")
    headline.show(10, False)

    onset = (
        usable.filter(F.col("is_wet"))
        .groupBy("hours_since_onset")
        .agg(
            F.count(F.lit(1)).alias("n"),
            F.round(100.0 * F.avg("typical_deviation"), 3).alias("pct_speed_loss"),
        )
        .orderBy("hours_since_onset")
    )
    print("=== driver adaptation: speed loss by hours since rain onset ===")
    onset.show(12, False)

    table = dose_response_table(usable).toPandas()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    table.to_csv(out_dir / "dose_response.csv", index=False)
    (out_dir / "dose_response.md").write_text(
        "# Rain dose-response\n\n"
        "`pct_speed_loss` is the mean percentage reduction in speed relative to the "
        "dry-weather typical speed for that detector, weekend flag and hour. "
        f"Cells with fewer than {MIN_CELL_N} observations are suppressed.\n\n"
        "## Headline by rain band\n\n"
        + headline.toPandas().to_markdown(index=False)
        + "\n\n## Speed loss by hours since rain onset\n\n"
        + onset.toPandas().to_markdown(index=False)
        + "\n\n## Stratified by rain band x road class x time of day\n\n"
        + table.to_markdown(index=False)
        + "\n"
    )
    print(f"CELLS_REPORTED {len(table)}")
    spark.stop()


if __name__ == "__main__":
    main()
