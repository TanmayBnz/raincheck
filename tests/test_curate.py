import datetime as dt

from pyspark.sql import functions as F

from raincheck.curate import (
    add_timestamps,
    apply_error_policy,
    clean_speed,
    curation_quality,
    join_detectors,
    normalize_occupancy,
)

RAW_COLS = ["day", "interval", "detid", "flow", "occ", "error", "city", "speed"]


def _raw(spark, rows):
    return spark.createDataFrame(rows, RAW_COLS)


def test_error_policy_drops_flagged_rows_but_keeps_unassessed_ones(spark):
    # None of these cities except Essen has any explicitly-clean rows, so
    # keeping only error == 0 would discard the entire corpus.
    df = _raw(
        spark,
        [
            (dt.date(2017, 9, 8), 0, "d1", 1.0, 0.1, 0, "manchester", 40.0),
            (dt.date(2017, 9, 8), 300, "d2", 1.0, 0.1, 1, "manchester", 40.0),
            (dt.date(2017, 9, 8), 600, "d3", 1.0, 0.1, None, "manchester", 40.0),
        ],
    )

    kept = {r["detid"] for r in apply_error_policy(df).collect()}

    assert kept == {"d1", "d3"}


def test_occupancy_is_rescaled_per_city_then_range_checked(spark):
    # Manchester reports percent, Essen reports a fraction, and both carry
    # values no rescaling can rescue (2094%, Infinity).
    df = _raw(
        spark,
        [
            (dt.date(2017, 9, 8), 0, "m1", 1.0, 8.25, 0, "manchester", 40.0),
            (dt.date(2017, 9, 8), 300, "m2", 1.0, 2094.0, 0, "manchester", 40.0),
            (dt.date(2017, 9, 8), 0, "r1", 1.0, float("inf"), 0, "rotterdam", 40.0),
            (dt.date(2017, 9, 8), 300, "r2", 1.0, -5.0, 0, "rotterdam", 40.0),
            (dt.date(2017, 9, 8), 0, "e1", 1.0, 0.0075, 0, "essen", 40.0),
        ],
    )

    result = {r["detid"]: r["occ"] for r in normalize_occupancy(df).collect()}

    assert result["m1"] == 0.0825          # 8.25% -> fraction
    assert result["m2"] is None            # 20.94 is not an occupancy
    assert result["r1"] is None            # Infinity
    assert result["r2"] is None            # negative
    assert result["e1"] == 0.0075          # already a fraction, unscaled


def test_implausible_speeds_are_nulled_not_clipped(spark):
    # Clipping 243 km/h to the cap would invent an observation at the cap and
    # drag the p85 free-flow percentile upward; nulling removes it honestly.
    df = _raw(
        spark,
        [
            (dt.date(2017, 9, 8), 0, "a", 1.0, 0.1, 0, "essen", 40.0),
            (dt.date(2017, 9, 8), 0, "b", 1.0, 0.1, 0, "essen", 243.0),
            (dt.date(2017, 9, 8), 0, "c", 1.0, 0.1, 0, "essen", 0.0),
            (dt.date(2017, 9, 8), 0, "d", 1.0, 0.1, 0, "essen", -1.0),
        ],
    )

    result = {r["detid"]: r["speed"] for r in clean_speed(df).collect()}

    assert result["a"] == 40.0
    assert result["b"] is None
    assert result["c"] is None
    assert result["d"] is None


def test_local_to_utc_conversion_respects_the_dst_transition(spark):
    # The study window (2017-09-08 to 2017-11-18) straddles 2017-10-29, when the
    # UK and EU left summer time. A fixed offset would shift half the corpus by
    # an hour against the rainfall grid, and the delay signal would just look
    # weaker rather than obviously broken.
    df = _raw(
        spark,
        [
            # 12:00 local, British Summer Time (UTC+1)
            (dt.date(2017, 10, 28), 43200, "m1", 1.0, 0.1, 0, "manchester", 40.0),
            # 12:00 local, back on GMT (UTC+0)
            (dt.date(2017, 10, 30), 43200, "m2", 1.0, 0.1, 0, "manchester", 40.0),
            # 12:00 local, Central European Summer Time (UTC+2)
            (dt.date(2017, 10, 28), 43200, "e1", 1.0, 0.1, 0, "essen", 40.0),
        ],
    )

    stamped = add_timestamps(df).select(
        "detid",
        F.date_format("ts_local", "yyyy-MM-dd HH:mm").alias("local"),
        F.date_format("ts_utc", "yyyy-MM-dd HH:mm").alias("utc"),
    )
    result = {r["detid"]: (r["local"], r["utc"]) for r in stamped.collect()}

    assert result["m1"] == ("2017-10-28 12:00", "2017-10-28 11:00")
    assert result["m2"] == ("2017-10-30 12:00", "2017-10-30 12:00")
    assert result["e1"] == ("2017-10-28 12:00", "2017-10-28 10:00")


DETECTOR_COLS = ["detid", "length", "pos", "fclass", "road", "limit",
                 "citycode", "lanes", "linkid", "long", "lat"]


def test_detector_join_adds_road_class_without_dropping_unmatched_measurements(spark):
    # The audit found the two files disagree on detector counts (Augsburg 713 vs
    # 717), so an inner join would silently delete measurements.
    measurements = _raw(
        spark,
        [
            (dt.date(2017, 9, 8), 0, "m1", 1.0, 0.1, 0, "manchester", 40.0),
            (dt.date(2017, 9, 8), 0, "ghost", 1.0, 0.1, 0, "manchester", 40.0),
        ],
    )
    detectors = spark.createDataFrame(
        [("m1", 0.2, 0.01, "secondary", "Oxford Road", 30, "manchester", 2, 77, -2.24, 53.47)],
        DETECTOR_COLS,
    )

    result = {r["detid"]: r for r in join_detectors(measurements, detectors).collect()}

    assert result["m1"]["fclass"] == "secondary"
    assert result["m1"]["linkid"] == 77
    assert result["m1"]["lat"] == 53.47
    assert result["ghost"]["fclass"] is None
    assert len(result) == 2


def test_quality_report_shows_retention_and_what_each_filter_cost(spark):
    scoped = _raw(
        spark,
        [
            (dt.date(2017, 9, 8), 0, "m1", 1.0, 8.25, 0, "manchester", 40.0),
            (dt.date(2017, 9, 8), 300, "m2", 1.0, 2094.0, 0, "manchester", 243.0),
            (dt.date(2017, 9, 8), 600, "m3", 1.0, 8.25, 1, "manchester", 40.0),
            (dt.date(2017, 9, 8), 0, "e1", 1.0, 0.0075, 0, "essen", 40.0),
        ],
    )
    curated = clean_speed(normalize_occupancy(apply_error_policy(scoped)))

    report = {r["city"]: r for r in curation_quality(scoped, curated).collect()}

    # Manchester: 3 rows in, m3 dropped by the error flag -> 2 out.
    assert report["manchester"]["rows_in"] == 3
    assert report["manchester"]["rows_out"] == 2
    assert report["manchester"]["retention_pct"] == 66.6667
    # Of the 2 survivors, m2's occupancy and speed were both nulled.
    assert report["manchester"]["occ_null_pct"] == 50.0
    assert report["manchester"]["speed_null_pct"] == 50.0
    assert report["essen"]["retention_pct"] == 100.0
    assert report["essen"]["occ_null_pct"] == 0.0
