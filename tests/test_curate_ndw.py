"""Tests for L1 curation of the NDW canonical schema.

Timestamps are built from strings and cast, not from naive Python datetimes:
createDataFrame reads a naive datetime in the JVM's system zone, which silently
shifts it. See scripts/test.sh, which pins TZ=UTC for the same reason.
"""
from pyspark.sql import functions as F

from raincheck.curate_ndw import (
    dedupe,
    rebin,
    retention_by_rain_band,
    threshold_sensitivity,
)

_COLUMNS = ("segment_id", "ts", "speed", "flow", "quality_weight")


def observations(spark, rows):
    """Build a canonical-schema DataFrame from (segment, ts, speed, flow, w)."""
    return (
        spark.createDataFrame(list(rows), list(_COLUMNS))
        .withColumn("ts_utc", F.col("ts").cast("timestamp"))
        .drop("ts")
    )


def test_dedupe_keeps_the_observation_backed_by_more_vehicles(spark):
    # A harvester restart re-ingests the overlap, and the two copies need not be
    # identical: a later publication can revise a value once more samples have
    # arrived. dropDuplicates would keep an arbitrary row, so the 4-vehicle
    # estimate could survive over the 20-vehicle one.
    df = observations(spark, [
        ("A", "2026-08-19 09:00:00", 88.0, 780.0, 4.0),
        ("A", "2026-08-19 09:00:00", 91.0, 800.0, 20.0),
        ("B", "2026-08-19 09:00:00", 70.0, 500.0, 6.0),
    ])

    result = dedupe(df).orderBy("segment_id").collect()

    assert len(result) == 2
    assert (result[0].segment_id, result[0].speed, result[0].quality_weight) == ("A", 91.0, 20.0)
    assert result[1].segment_id == "B"


def test_rebin_weights_speed_by_sample_size_and_ignores_null_speeds(spark):
    # Three 1-minute observations in the 09:00 bin. The plain mean of the two
    # non-null speeds is 75.0; weighted by vehicles it is
    # (60*1 + 90*9) / (1+9) = 87.0. Asserting 87.0 is what distinguishes a
    # weighted implementation from an unweighted one - and the distinction
    # matters because 86% of 1-minute speeds rest on fewer than five vehicles.
    df = observations(spark, [
        ("A", "2026-08-19 09:00:00", 60.0, 300.0, 1.0),
        ("A", "2026-08-19 09:01:00", 90.0, 900.0, 9.0),
        ("A", "2026-08-19 09:04:00", None, 600.0, 5.0),   # flow only
        ("A", "2026-08-19 09:05:00", 50.0, 200.0, 2.0),   # next bin
    ])

    result = rebin(df, minutes=5).orderBy("ts_utc").collect()

    assert len(result) == 2
    first = result[0]
    assert first.speed == 87.0
    assert first.n_obs == 3
    assert first.quality_weight == 15.0        # every vehicle in the bin
    assert first.speed_weight == 10.0          # only those behind a speed value
    assert result[1].speed == 50.0


def rain_observations(spark, rows):
    """(segment, rain_band, speed, quality_weight) rows for the bias diagnostic."""
    return spark.createDataFrame(
        list(rows), ["segment_id", "rain_band", "speed", "quality_weight"])


def test_retention_by_rain_band_exposes_the_missingness_bias(spark):
    # The whole point of this report. Rain reduces traffic volume, so wet
    # observations are the thin ones - and any minimum-sample-size filter
    # therefore deletes rain preferentially, biasing the effect toward zero.
    # Here a threshold of 5 vehicles keeps every dry row and no wet row, which is
    # exactly the pattern that must be visible rather than silently applied.
    df = rain_observations(spark, [
        ("A", "none", 90.0, 20.0),
        ("B", "none", 92.0, 20.0),
        ("C", "moderate", 70.0, 3.0),
        ("D", "moderate", None, 3.0),
    ])

    report = {r.rain_band: r for r in
              retention_by_rain_band(df, min_weight=5.0).collect()}

    dry, wet = report["none"], report["moderate"]
    assert (dry.n_obs, wet.n_obs) == (2, 2)
    assert dry.speed_null_pct == 0.0
    assert wet.speed_null_pct == 50.0
    assert dry.mean_quality_weight == 20.0
    assert wet.mean_quality_weight == 3.0
    assert dry.retained_pct == 100.0
    assert wet.retained_pct == 0.0


def test_threshold_sensitivity_reveals_an_estimate_that_drifts(spark):
    # The thin wet observation is also the slow one, which is what informative
    # missingness looks like. At a threshold of 1 the wet mean is 72.5 against a
    # dry 91.0 - an 18.5 km/h effect. At a threshold of 5 the slow thin row is
    # gone and the same effect reads as 6.0. An estimate that moves this much
    # with the threshold is being driven by missingness, not by rain, and the
    # table has to make that visible.
    df = rain_observations(spark, [
        ("A", "none", 90.0, 20.0),
        ("B", "none", 92.0, 20.0),
        ("C", "moderate", 60.0, 3.0),
        ("D", "moderate", 85.0, 20.0),
    ])

    rows = {(r.min_weight, r.rain_band): r
            for r in threshold_sensitivity(df, thresholds=(1.0, 5.0)).collect()}

    assert rows[(1.0, "none")].mean_speed == 91.0
    assert rows[(1.0, "moderate")].mean_speed == 72.5
    assert rows[(5.0, "none")].mean_speed == 91.0
    assert rows[(5.0, "moderate")].mean_speed == 85.0
    assert rows[(5.0, "moderate")].n_obs == 1


def test_a_speed_without_a_sample_size_is_still_a_speed(spark):
    # numberOfInputValuesUsed is absent on 59% of live rows: 313,791 of 630,250
    # harvested rows carry a valid speed with a null weight. A weighted mean of
    # speed*weight drops those terms, so a bin whose speeds all lack a weight
    # yields sum(null)/sum(null) = null - which inflated speed-null from 14% at
    # 1-minute grain to 70% after re-binning. An unknown sample size must count
    # as one observation, not as zero.
    df = observations(spark, [
        ("A", "2026-08-19 09:00:00", 80.0, 300.0, None),
        ("A", "2026-08-19 09:01:00", 90.0, 400.0, None),
        ("B", "2026-08-19 09:00:00", 60.0, 300.0, None),
        ("B", "2026-08-19 09:01:00", 90.0, 400.0, 9.0),
    ])

    result = {r.segment_id: r for r in rebin(df, minutes=5).collect()}

    # A: both weights unknown, so an unweighted mean of the two speeds.
    assert result["A"].speed == 85.0
    # B: 60 counts once, 90 counts nine times -> (60 + 810) / 10.
    assert result["B"].speed == 87.0
