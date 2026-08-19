import datetime as dt

from raincheck.audit import audit_by_city, interval_resolution_by_city

RAW_COLS = ["day", "interval", "detid", "flow", "occ", "error", "city", "speed"]


def _raw(spark, rows):
    return spark.createDataFrame(rows, RAW_COLS)


def test_counts_rows_and_detectors_per_city(spark):
    df = _raw(
        spark,
        [
            (dt.date(2017, 5, 6), 0, "d1", 12.0, 0.1, 0, "augsburg", 40.0),
            (dt.date(2017, 5, 6), 300, "d1", 15.0, 0.2, 0, "augsburg", 38.0),
            (dt.date(2017, 5, 6), 0, "d2", 9.0, 0.1, 0, "augsburg", 44.0),
            (dt.date(2017, 5, 6), 0, "z1", 20.0, 0.3, 0, "zurich", 30.0),
        ],
    )

    result = {r["city"]: r for r in audit_by_city(df).collect()}

    assert result["augsburg"]["rows"] == 3
    assert result["augsburg"]["n_detectors"] == 2
    assert result["zurich"]["rows"] == 1
    assert result["zurich"]["n_detectors"] == 1


def test_reports_fraction_of_rows_carrying_each_variable(spark):
    df = _raw(
        spark,
        [
            # 4 rows: all have flow, 2 have occ, 1 has speed
            (dt.date(2017, 5, 6), 0, "d1", 12.0, 0.1, 0, "augsburg", 40.0),
            (dt.date(2017, 5, 6), 300, "d1", 15.0, 0.2, 0, "augsburg", None),
            (dt.date(2017, 5, 6), 600, "d1", 9.0, None, 0, "augsburg", None),
            (dt.date(2017, 5, 6), 900, "d1", 7.0, None, 0, "augsburg", None),
        ],
    )

    row = audit_by_city(df).collect()[0]

    assert row["flow_pct"] == 100.0
    assert row["occ_pct"] == 50.0
    assert row["speed_pct"] == 25.0


def test_measures_coverage_as_distinct_days_against_calendar_span(spark):
    # 3 distinct days spanning a 5-day calendar window: 2017-05-08/09 are missing.
    df = _raw(
        spark,
        [
            (dt.date(2017, 5, 6), 0, "d1", 1.0, 0.1, 0, "augsburg", 40.0),
            (dt.date(2017, 5, 7), 0, "d1", 1.0, 0.1, 0, "augsburg", 40.0),
            (dt.date(2017, 5, 10), 0, "d1", 1.0, 0.1, 0, "augsburg", 40.0),
        ],
    )

    row = audit_by_city(df).collect()[0]

    assert row["n_days"] == 3
    assert row["first_day"] == dt.date(2017, 5, 6)
    assert row["last_day"] == dt.date(2017, 5, 10)
    assert row["span_days"] == 5
    assert row["day_density_pct"] == 60.0


def test_splits_error_flag_into_ok_faulty_and_unassessed(spark):
    # UTD19's error column is three-state: 0 = clean, 1 = flagged, null = never
    # assessed. Collapsing null into either bucket would misstate retention.
    df = _raw(
        spark,
        [
            (dt.date(2017, 5, 6), 0, "d1", 1.0, 0.1, 0, "augsburg", 40.0),
            (dt.date(2017, 5, 6), 300, "d1", 1.0, 0.1, 0, "augsburg", 40.0),
            (dt.date(2017, 5, 6), 600, "d1", 1.0, 0.1, 1, "augsburg", 40.0),
            (dt.date(2017, 5, 6), 900, "d1", 1.0, 0.1, None, "augsburg", 40.0),
        ],
    )

    row = audit_by_city(df).collect()[0]

    assert row["error_ok_pct"] == 50.0
    assert row["error_flagged_pct"] == 25.0
    assert row["error_unassessed_pct"] == 25.0


def test_derives_interval_resolution_from_smallest_gap_between_slots(spark):
    # Augsburg bins at 5 min, Zurich at 3 min. Counting slots would be wrong for
    # partial days, so resolution is the smallest gap between distinct slots.
    df = _raw(
        spark,
        [
            (dt.date(2017, 5, 6), 0, "d1", 1.0, 0.1, 0, "augsburg", 40.0),
            (dt.date(2017, 5, 6), 300, "d1", 1.0, 0.1, 0, "augsburg", 40.0),
            (dt.date(2017, 5, 6), 600, "d1", 1.0, 0.1, 0, "augsburg", 40.0),
            (dt.date(2017, 5, 6), 0, "z1", 1.0, 0.1, 0, "zurich", 40.0),
            (dt.date(2017, 5, 6), 180, "z1", 1.0, 0.1, 0, "zurich", 40.0),
            (dt.date(2017, 5, 6), 360, "z1", 1.0, 0.1, 0, "zurich", 40.0),
        ],
    )

    result = {r["city"]: r["resolution_sec"] for r in interval_resolution_by_city(df).collect()}

    assert result["augsburg"] == 300
    assert result["zurich"] == 180


def test_reports_value_ranges_so_unit_differences_are_visible(spark):
    # Zurich here reports occupancy as a percentage and speed in m/s, Augsburg as
    # a fraction and km/h. L1 cannot normalise units it cannot see.
    df = _raw(
        spark,
        [
            (dt.date(2017, 5, 6), 0, "d1", 1.0, 0.10, 0, "augsburg", 40.0),
            (dt.date(2017, 5, 6), 300, "d1", 1.0, 0.30, 0, "augsburg", 60.0),
            (dt.date(2017, 5, 6), 0, "z1", 1.0, 12.0, 0, "zurich", 8.0),
            (dt.date(2017, 5, 6), 300, "z1", 1.0, 44.0, 0, "zurich", 12.0),
        ],
    )

    result = {r["city"]: r for r in audit_by_city(df).collect()}

    assert result["augsburg"]["occ_max"] == 0.30
    assert result["augsburg"]["speed_min"] == 40.0
    assert result["augsburg"]["speed_max"] == 60.0
    assert result["zurich"]["occ_max"] == 44.0
    assert result["zurich"]["speed_max"] == 12.0
