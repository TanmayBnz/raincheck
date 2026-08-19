from pyspark.sql import functions as F

from raincheck.rain import (
    RADAR_BAND_EDGES,
    add_rain_hour,
    assign_grid_cell,
    join_rain,
    rain_history,
    with_rain_band,
)


def test_detectors_snap_to_the_nearest_quarter_degree_era5_cell(spark):
    # ERA5 single levels is a regular 0.25 degree grid, so the nearest cell is
    # arithmetic - no geospatial library needed at this resolution.
    df = spark.createDataFrame(
        [("manchester", 53.492, -2.276), ("essen", 51.448, 7.040), ("rotterdam", 51.869, 4.420)],
        ["city", "lat", "long"],
    )

    result = {r["city"]: (r["grid_lat"], r["grid_lon"]) for r in assign_grid_cell(df).collect()}

    assert result["manchester"] == (53.5, -2.25)
    assert result["essen"] == (51.5, 7.0)
    assert result["rotterdam"] == (51.75, 4.5)


def test_rain_hour_maps_a_five_minute_bin_to_the_hour_that_contains_it(spark):
    # ERA5 tp at time t accumulates over the hour ENDING at t, and UTD19's
    # `interval` is the START of a 5-minute bin. So a bin starting 14:00 covers
    # 14:00-14:05, which lies inside the hour ending 15:00.
    # Built from strings, not Python datetimes: createDataFrame reads a naive
    # datetime in the JVM's system timezone, which on this machine is +05:30 and
    # would silently shift every timestamp.
    df = spark.createDataFrame(
        [
            ("a", "2017-09-08 14:00:00"),
            ("b", "2017-09-08 14:35:00"),
            ("c", "2017-09-08 14:55:00"),
            ("d", "2017-09-08 15:00:00"),
        ],
        ["detid", "ts_utc"],
    ).withColumn("ts_utc", F.col("ts_utc").cast("timestamp"))

    stamped = add_rain_hour(df).select(
        "detid", F.date_format("rain_hour", "HH:mm").alias("hour")
    )
    result = {r["detid"]: r["hour"] for r in stamped.collect()}

    assert result["a"] == "15:00"
    assert result["b"] == "15:00"
    assert result["c"] == "15:00"
    assert result["d"] == "16:00"


def test_rain_band_thresholds(spark):
    df = spark.createDataFrame(
        [(0.0,), (0.05,), (0.4,), (2.0,), (6.0,), (25.0,)], ["rain_mm_h"]
    )

    bands = [r["rain_band"] for r in with_rain_band(df).orderBy("rain_mm_h").collect()]

    assert bands == ["none", "none", "light", "moderate", "heavy", "very_heavy"]


def _hourly(spark, intensities):
    """One ERA5 grid cell, consecutive hours from 2017-09-08T00:00Z."""
    rows = [
        (53.5, -2.25, f"2017-09-08 {h:02d}:00:00", mm)
        for h, mm in enumerate(intensities)
    ]
    return spark.createDataFrame(
        rows, ["grid_lat", "grid_lon", "ts_utc", "tp_mm"]
    ).withColumn("ts_utc", F.col("ts_utc").cast("timestamp"))


# hour:      0    1    2    3    4    5    6    7    8    9   10   11
SERIES = [0.0, 0.0, 0.0, 0.5, 2.0, 0.0, 0.0, 0.0, 0.0, 3.0, 1.0, 0.0]


def _by_hour(df):
    rows = df.select("*", F.hour("ts_utc").alias("h")).collect()
    return {r["h"]: r for r in rows}


def test_trailing_accumulations_sum_the_preceding_hours(spark):
    result = _by_hour(rain_history(_hourly(spark, SERIES)))

    assert result[4]["rain_3h"] == 2.5    # hours 2,3,4 -> 0 + 0.5 + 2.0
    assert result[5]["rain_3h"] == 2.5    # hours 3,4,5 -> 0.5 + 2.0 + 0
    assert result[9]["rain_6h"] == 5.0    # hours 4..9 -> 2.0 + 3.0


def test_wet_spells_are_identified_and_timed_from_their_onset(spark):
    result = _by_hour(rain_history(_hourly(spark, SERIES)))

    assert [h for h, r in sorted(result.items()) if r["is_wet"]] == [3, 4, 9, 10]
    assert result[3]["hours_since_onset"] == 0     # first hour of spell one
    assert result[4]["hours_since_onset"] == 1
    assert result[9]["hours_since_onset"] == 0     # first hour of spell two
    assert result[10]["hours_since_onset"] == 1
    assert result[5]["hours_since_onset"] is None  # undefined while dry


def test_antecedent_dry_spell_is_constant_across_a_wet_spell(spark):
    # This is the "first rain after a dry spell" covariate: the oil-film effect
    # depends on how long the road was dry BEFORE this rain began, so the value
    # must persist through the spell rather than reset each hour.
    result = _by_hour(rain_history(_hourly(spark, SERIES)))

    # Spell two starts at 09; the last wet hour was 04, so 05-08 were dry.
    assert result[9]["antecedent_dry_hours"] == 4
    assert result[10]["antecedent_dry_hours"] == 4
    # Spell one has no earlier rain on record, so its antecedent is unknown,
    # not zero. This is why the ERA5 pull starts two weeks before the traffic.
    assert result[3]["antecedent_dry_hours"] is None


def test_dry_baseline_excludes_hours_that_are_merely_between_showers(spark):
    # Hour 5 is not raining, but it rained 2.5 mm in the previous two hours, so
    # the road is still wet. Including it in the "dry" baseline would let the
    # baseline absorb part of the rain effect.
    result = _by_hour(rain_history(_hourly(spark, SERIES)))

    assert result[2]["is_dry_baseline"] is True
    assert result[5]["is_wet"] is False
    assert result[5]["is_dry_baseline"] is False
    assert result[11]["is_dry_baseline"] is False


def test_measurements_join_to_the_era5_hour_containing_their_bin(spark):
    measurements = spark.createDataFrame(
        [
            ("m1", "manchester", 53.492, -2.276, "2017-09-08 09:35:00"),
            ("m2", "manchester", 53.492, -2.276, "2017-09-08 23:35:00"),  # past fixture end
        ],
        ["detid", "city", "lat", "long", "ts_utc"],
    ).withColumn("ts_utc", F.col("ts_utc").cast("timestamp"))

    rain = rain_history(_hourly(spark, SERIES))

    result = {r["detid"]: r for r in join_rain(measurements, rain).collect()}

    # 09:35 sits in the hour ending 10:00, where SERIES has 1.0 mm.
    assert result["m1"]["rain_mm_h"] == 1.0
    assert result["m1"]["rain_6h"] == 4.0     # hours 5..10 -> 3.0 + 1.0
    # Left join: a measurement outside the ERA5 record keeps its row.
    assert result["m2"]["rain_mm_h"] is None
    assert len(result) == 2


def test_band_edges_are_per_rung_and_radar_needs_an_extra_tier(spark):
    # ERA5's edges (1/4/10 mm/h) top out at "very_heavy" above 10, which was
    # empty across the whole European corpus. Radar reaches 65.6 mm/h in a single
    # frame, so everything from 10 upward would collapse into one band. The
    # radar edges add a 30 mm/h tier, and the top band is named per rung.
    df = spark.createDataFrame(
        [(0.05,), (2.0,), (6.0,), (20.0,), (45.0,)], ["rain_mm_h"])

    banded = with_rain_band(df, edges=RADAR_BAND_EDGES, top="extreme")

    assert [r.rain_band for r in banded.collect()] == [
        "none", "moderate", "heavy", "very_heavy", "extreme"]


def test_default_band_edges_are_unchanged_for_the_era5_rung(spark):
    df = spark.createDataFrame([(0.05,), (2.0,), (6.0,), (20.0,)], ["rain_mm_h"])

    assert [r.rain_band for r in with_rain_band(df).collect()] == [
        "none", "moderate", "heavy", "very_heavy"]


def test_unmatched_rainfall_is_not_silently_classified_as_the_heaviest_band(spark):
    # _band walks the edges with when(intensity < edge); a null intensity makes
    # every comparison null, so the row falls through to the top band. On the
    # first real radar join that put 20,621 bins with NO rainfall data into
    # "extreme" - 8.73% of the corpus, more than heavy and moderate combined,
    # and physically impossible. A missing measurement must stay missing.
    df = spark.createDataFrame(
        [(None,), (0.05,), (45.0,)], "rain_mm_h double")

    bands = [r.rain_band for r in
             with_rain_band(df, edges=RADAR_BAND_EDGES, top="extreme").collect()]

    assert bands == [None, "none", "extreme"]
