"""Tests for the section monitoring layer: travel time -> speed -> delay indices.

Section length is published (`lengthAffected`), so the conversion is exact rather
than estimated - which is why travel time can carry the monitoring layer at all.
"""
from pyspark.sql import functions as F

from raincheck.monitor import (
    free_flow_speed,
    monitoring_view,
    to_speed_kmh,
    typical_profile,
    with_local_time,
)

_COLUMNS = ("section_id", "ts", "duration_s", "reference_duration_s", "length_m")


def sections(spark, rows):
    return (
        spark.createDataFrame(list(rows), list(_COLUMNS))
        .withColumn("ts_utc", F.col("ts").cast("timestamp"))
        .drop("ts")
    )


def test_travel_time_and_published_length_convert_to_space_mean_speed(spark):
    # 445 m (the median NDW section) in 20 s is 80.1 km/h: length_m * 3.6 / s.
    # This is a space-mean speed over the whole section, not a point sample,
    # which is the point - a loop at a free-flowing spot misses a queue 300 m on.
    df = sections(spark, [
        ("A", "2026-08-19 09:00:00", 20.0, 16.0, 445.0),
    ])

    row = to_speed_kmh(df).collect()[0]

    assert round(row.speed, 1) == 80.1
    assert round(row.reference_speed, 1) == 100.1


def test_the_minus_one_sentinel_and_zero_durations_do_not_become_speeds(spark):
    # Travel time reuses the -1.0 sentinel with dataError=true, the fifth
    # instance of this pattern in the project. Dividing by it yields a negative
    # speed; dividing by zero yields infinity. Both must be null.
    df = sections(spark, [
        ("A", "2026-08-19 09:00:00", -1.0, -1.0, 445.0),
        ("B", "2026-08-19 09:00:00", 0.0, 16.0, 445.0),
        ("C", "2026-08-19 09:00:00", 20.0, 0.0, 445.0),
    ])

    rows = {r.section_id: r for r in to_speed_kmh(df).collect()}

    assert rows["A"].speed is None and rows["A"].reference_speed is None
    assert rows["B"].speed is None
    assert rows["C"].reference_speed is None
    assert round(rows["C"].speed, 1) == 80.1


def observed(spark, rows):
    """(section_id, speed, reference_speed, is_dry_baseline) observations."""
    return spark.createDataFrame(
        list(rows), "section_id string, speed double, reference_speed double, "
                    "is_dry_baseline boolean")


def test_free_flow_reads_the_upper_tail_of_dry_observations_only(spark):
    # The wet values here are unphysically *fast* on purpose. Wet traffic is
    # normally slower, which would barely move a p95, so a fixture like that
    # cannot detect a missing dry filter. These values can: if the filter is
    # dropped, the percentile jumps to ~200.
    rows = [("A", 90.0, 100.0, True), ("A", 95.0, 100.0, True),
            ("A", 100.0, 100.0, True), ("A", 200.0, 100.0, False)]

    result = free_flow_speed(spark.createDataFrame(
        rows, "section_id string, speed double, reference_speed double, "
              "is_dry_baseline boolean"), percentile=0.5, min_obs=3).collect()

    assert len(result) == 1
    assert result[0].free_flow_speed == 95.0
    assert result[0].n_dry_obs == 3
    # The published reference is carried, never merged into the derived estimate:
    # a third of live sections are faster than their own reference.
    assert result[0].reference_speed == 100.0


def test_a_section_with_too_few_dry_observations_yields_no_free_flow(spark):
    # The European arm's hard-won lesson: before a minimum-observation guard,
    # single-observation baselines produced deviations of -63%.
    rows = [("A", 90.0, 100.0, True), ("A", 95.0, 100.0, True)]

    result = free_flow_speed(observed(spark, rows), percentile=0.5,
                             min_obs=30).collect()

    assert result == [] or result[0].free_flow_speed is None


def test_typical_profile_is_keyed_on_local_hour_not_utc(spark):
    # NDW stamps everything UTC, but a day-of-week x hour-of-day profile is a
    # local-time concept: 22:30 UTC on a summer Saturday is 00:30 Sunday in
    # Amsterdam (CEST, UTC+2). A UTC-keyed profile files it under hour 22 and
    # Saturday - wrong hour AND wrong weekend flag. Both are asserted here
    # because in winter (CET, UTC+1) only the hour would move.
    df = (spark.createDataFrame(
        [("A", "2026-08-22 22:30:00", 90.0, True),
         ("A", "2026-08-22 22:40:00", 70.0, True),
         ("A", "2026-08-22 22:50:00", 200.0, False)],
        "section_id string, ts string, speed double, is_dry_baseline boolean")
        .withColumn("ts_utc", F.col("ts").cast("timestamp")).drop("ts"))

    rows = typical_profile(with_local_time(df), min_obs=2).collect()

    assert len(rows) == 1
    row = rows[0]
    assert row.hour == 0                 # 22:30 UTC -> 00:30 CEST
    assert row.is_weekend is True        # Saturday 22:30 UTC -> Sunday local
    assert row.typical_speed == 80.0     # median of the two dry values only
    assert row.n_obs == 2


def test_monitoring_view_reports_both_delay_definitions(spark):
    # Two different questions, deliberately kept separate (CLAUDE.md 6):
    # ff_delay_ratio is congestion irrespective of cause; typical_deviation is
    # the anomaly against what this hour normally looks like, and is the target
    # for L3. Section B has free-flow but no typical profile, so it must report
    # congestion and withhold the anomaly rather than inventing one.
    observations = (spark.createDataFrame(
        [("A", "2026-08-19 09:00:00", 60.0),
         ("B", "2026-08-19 09:00:00", 60.0)],
        "section_id string, ts string, speed double")
        .withColumn("ts_utc", F.col("ts").cast("timestamp")).drop("ts"))
    freeflow = spark.createDataFrame(
        [("A", 100.0), ("B", 120.0)],
        "section_id string, free_flow_speed double")
    profile = spark.createDataFrame(
        [("A", False, 11, 80.0)],
        "section_id string, is_weekend boolean, hour int, typical_speed double")

    rows = {r.section_id: r for r in monitoring_view(
        with_local_time(observations), freeflow, profile).collect()}

    assert rows["A"].ff_delay_ratio == 0.4        # 1 - 60/100
    assert rows["A"].typical_deviation == 0.25    # 1 - 60/80
    assert rows["B"].ff_delay_ratio == 0.5        # 1 - 60/120
    assert rows["B"].typical_deviation is None
