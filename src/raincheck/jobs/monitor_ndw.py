"""L2a/L4: build the section monitoring layer from harvested travel times.

    ./scripts/run_monitor_ndw.sh

Travel time + published section length -> space-mean speed -> free-flow and
dry-only typical profiles -> the two delay indices a live map is coloured by.

Profiles need at least ``MIN_PROFILE_OBS`` dry observations per
(section, weekend, hour) cell, so this reports its own coverage rather than
emitting baselines it cannot support. With a 1-minute feed a cell fills at 60
observations per hour of matching wall-clock time, so a usable profile needs
roughly a day of harvest per hour-of-day cell.
"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from raincheck import paths
from raincheck.curate import clean_speed
from raincheck.monitor import (
    MIN_PROFILE_OBS,
    free_flow_speed,
    monitoring_view,
    to_speed_kmh,
    typical_profile,
    with_local_time,
)
from raincheck.session import build_session

CURATED = paths.LOCAL_STAGE.parent / "curated"
STAGE = paths.LOCAL_STAGE / "ndw"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--travel-times", default=str(STAGE / "traveltime"))
    parser.add_argument("--sections", default=str(CURATED / "ndw_sections"))
    parser.add_argument("--output", default=str(CURATED / "ndw_monitor"))
    parser.add_argument("--min-obs", type=int, default=MIN_PROFILE_OBS)
    parser.add_argument("--exclude-loop-derived", action="store_true",
                        help="drop the 9.1%% of sections computed from the same "
                             "loops as the speed feed")
    args = parser.parse_args(argv)

    spark = build_session("raincheck-monitor-ndw")
    sections = spark.read.parquet(args.sections)
    if args.exclude_loop_derived:
        sections = sections.filter(~F.col("is_loop_derived"))

    observations = (
        spark.read.parquet(args.travel_times)
        .join(sections.select("section_id", "length_m", "equipment",
                              "is_loop_derived"),
              on="section_id", how="inner")
    )
    speeds = clean_speed(to_speed_kmh(observations))
    # No rainfall joined yet, so every interval counts as dry. Once L2b runs for
    # sections this becomes the real rain-derived flag - and until then the
    # baselines below are NOT dry-conditioned, which is stated rather than hidden.
    speeds = with_local_time(speeds.withColumn("is_dry_baseline", F.lit(True))).cache()

    rows = speeds.count()
    with_speed = speeds.filter(F.col("speed").isNotNull()).count()
    print("\n=== L4 section monitoring layer ===")
    print(f"  observations        {rows:>10,}")
    print(f"  with a speed        {with_speed:>10,}  ({100 * with_speed / rows:.1f}%)")
    stats = speeds.select(
        F.round(F.expr("percentile(speed, 0.05)"), 1).alias("p5"),
        F.round(F.expr("percentile(speed, 0.5)"), 1).alias("p50"),
        F.round(F.expr("percentile(speed, 0.95)"), 1).alias("p95"),
    ).first()
    print(f"  speed km/h          p5={stats.p5}  p50={stats.p50}  p95={stats.p95}")

    freeflow = free_flow_speed(speeds, min_obs=args.min_obs).cache()
    profile = typical_profile(speeds, min_obs=args.min_obs).cache()
    supported = freeflow.filter(F.col("free_flow_speed").isNotNull()).count()
    print(f"\n  sections with free-flow   {supported:>8,} of {freeflow.count():,}"
          f"   (needs {args.min_obs} dry observations)")
    print(f"  profile cells             {profile.count():>8,}"
          f"   (section x weekend x hour)")

    if supported == 0 or profile.count() == 0:
        print("\n  Not enough harvested history yet for baselines. The wiring is"
              "\n  exercised end to end; the layer becomes meaningful once the"
              "\n  harvester has covered each hour-of-day cell.")

    view = monitoring_view(speeds, freeflow.select("section_id", "free_flow_speed"),
                           profile)
    delayed = view.filter(F.col("ff_delay_ratio") > 0.2).count()
    print(f"  sections >20% below free-flow  {delayed:>8,}")

    (view.withColumn("date", F.to_date("ts_utc"))
     .write.mode("overwrite").partitionBy("date").parquet(args.output))
    print(f"\n  written to {args.output}")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
