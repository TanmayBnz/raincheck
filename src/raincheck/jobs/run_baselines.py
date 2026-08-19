"""L2a job: free-flow speed and dry-only typical speed profiles.

Reads the rain-joined table, because the typical profile is dry-only and so
depends on `is_dry_baseline` from L2b. Run via scripts/run_baselines.sh.
"""

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from raincheck import paths
from raincheck.baseline import (
    FREE_FLOW_PERCENTILE,
    critical_occupancy,
    free_flow_speed,
    typical_profile,
)
from raincheck.session import build_session

# Cells thinner than this are reported as such: with 21-23 days per city the
# median of a nearly-empty cell is noise dressed as a baseline.
THIN_CELL_OBS = 30


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="reports")
    parser.add_argument("--master", default=None)
    args = parser.parse_args()

    spark = build_session("raincheck-l2a-baselines", master=args.master)
    df = spark.read.parquet(paths.RAIN_FEATURES).cache()

    critical = critical_occupancy(df).cache()
    freeflow = free_flow_speed(df, critical, FREE_FLOW_PERCENTILE).cache()
    profile = typical_profile(df).cache()

    # Guard the central methodological claim rather than trusting it: the
    # observations behind the profile must be exactly the dry ones.
    dry_rows = df.filter(F.col("is_dry_baseline") & F.col("speed").isNotNull()).count()
    profile_rows = profile.agg(F.sum("n_obs")).collect()[0][0] or 0
    if dry_rows != profile_rows:
        raise SystemExit(
            f"dry-only guard failed: profile used {profile_rows} rows, "
            f"{dry_rows} dry rows available"
        )
    print(f"DRY_ONLY_GUARD_OK {profile_rows} dry observations built the profile")

    freeflow.write.mode("overwrite").parquet(paths.BASELINE_FREEFLOW)
    profile.write.mode("overwrite").parquet(paths.BASELINE_TYPICAL)

    print("=== critical occupancy per city x road class ===")
    critical.orderBy("city", "fclass").show(60, False)

    ff_summary = freeflow.groupBy("city").agg(
        F.count(F.lit(1)).alias("detectors_with_ff"),
        F.round(F.avg("free_flow_speed"), 2).alias("mean_ff_kmh"),
        F.round(F.min("free_flow_speed"), 2).alias("min_ff_kmh"),
        F.round(F.max("free_flow_speed"), 2).alias("max_ff_kmh"),
        F.round(F.avg("free_flow_obs"), 0).alias("mean_obs_per_detector"),
        # Near 100% means the occupancy conditioning is inert for that city.
        F.round(100.0 * F.avg("free_flow_share"), 1).alias("mean_ff_share_pct"),
    ).orderBy("city")

    prof_summary = profile.groupBy("city").agg(
        F.count(F.lit(1)).alias("profile_cells"),
        F.round(F.avg("n_obs"), 1).alias("mean_obs_per_cell"),
        F.min("n_obs").alias("min_obs"),
        F.round(100.0 * F.avg((F.col("n_obs") < THIN_CELL_OBS).cast("int")), 2).alias(
            f"pct_cells_under_{THIN_CELL_OBS}_obs"
        ),
    ).orderBy("city")

    ff_table, prof_table = ff_summary.toPandas(), prof_summary.toPandas()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ff_table.to_csv(out_dir / "l2a_freeflow.csv", index=False)
    prof_table.to_csv(out_dir / "l2a_typical_profile.csv", index=False)
    (out_dir / "l2a_baselines.md").write_text(
        "# L2a baselines\n\n"
        f"Free-flow speed = p{int(FREE_FLOW_PERCENTILE * 100)} of speed at occupancy below "
        "critical, per detector.\n\n"
        + ff_table.to_markdown(index=False)
        + "\n\n## Dry-only typical speed profiles (detector x weekend x hour)\n\n"
        + prof_table.to_markdown(index=False)
        + "\n\n## Critical occupancy\n\n"
        + critical.orderBy("city", "fclass").toPandas().to_markdown(index=False)
        + "\n"
    )
    print(ff_table.to_string(index=False))
    print(prof_table.to_string(index=False))
    spark.stop()


if __name__ == "__main__":
    main()
