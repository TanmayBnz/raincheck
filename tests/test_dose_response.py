from raincheck.dose_response import (
    MIN_CELL_N,
    dose_response_table,
    with_time_of_day,
)


def test_hours_map_to_named_time_of_day_buckets(spark):
    df = spark.createDataFrame([(3,), (8,), (12,), (17,), (21,)], ["hour"])

    buckets = [r["tod"] for r in with_time_of_day(df).orderBy("hour").collect()]

    assert buckets == ["night", "am_peak", "midday", "pm_peak", "evening"]


def _cells(spark, spec):
    """spec: list of (rain_band, fclass, hour, deviation, repeats)"""
    rows = [
        (band, fclass, hour, dev)
        for band, fclass, hour, dev, repeats in spec
        for dev in dev * repeats
    ]
    return spark.createDataFrame(rows, ["rain_band", "fclass", "hour", "typical_deviation"])


def test_dose_response_reports_mean_with_a_confidence_interval_and_count(spark):
    # Heavier rain should show a larger positive deviation from the dry norm.
    df = _cells(
        spark,
        [
            ("none", "trunk", 17, [-0.01, 0.0, 0.01], 40),
            ("heavy", "trunk", 17, [0.18, 0.20, 0.22], 40),
        ],
    )

    result = {r["rain_band"]: r for r in dose_response_table(df).collect()}

    assert result["heavy"]["n"] == 120
    assert abs(result["heavy"]["mean_deviation"] - 0.20) < 1e-6
    assert abs(result["none"]["mean_deviation"] - 0.0) < 1e-6
    # A real interval, and one that excludes the dry case.
    assert result["heavy"]["ci_low"] < 0.20 < result["heavy"]["ci_high"]
    assert result["heavy"]["ci_low"] > result["none"]["ci_high"]


def test_thin_cells_are_suppressed_rather_than_reported_as_noise(spark):
    # With only 40 calendar days some strata are near-empty; a mean over three
    # observations is not an elasticity.
    df = _cells(
        spark,
        [
            ("none", "trunk", 17, [0.0, 0.01, -0.01], 40),
            ("very_heavy", "residential", 3, [0.9], 2),
        ],
    )

    bands = {r["rain_band"] for r in dose_response_table(df).collect()}

    assert "none" in bands
    assert "very_heavy" not in bands
    assert MIN_CELL_N > 2


def test_each_band_is_differenced_against_no_rain_within_its_stratum(spark):
    # The dry `none` band should sit at zero but does not: typical_speed is a
    # median while the deviation is averaged, which leaves a per-cell offset.
    # Differencing against `none` inside the stratum cancels that offset, so the
    # reported number is "cost of rain relative to no rain" rather than an
    # artefact of how the baseline was summarised.
    df = _cells(
        spark,
        [
            ("none", "trunk", 17, [0.01, 0.02, 0.03], 40),
            ("heavy", "trunk", 17, [0.09, 0.10, 0.11], 40),
            ("light", "secondary", 17, [0.05], 40),   # stratum has no dry band
        ],
    )

    result = {
        (r["rain_band"], r["fclass"]): r for r in dose_response_table(df).collect()
    }

    assert abs(result[("heavy", "trunk")]["delta_vs_none_pct"] - 8.0) < 1e-6
    assert abs(result[("none", "trunk")]["delta_vs_none_pct"]) < 1e-9
    # No dry reference in this stratum, so no honest difference can be formed.
    assert result[("light", "secondary")]["delta_vs_none_pct"] is None
