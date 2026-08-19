from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from raincheck.baseline import (
    critical_occupancy,
    delay_metrics,
    free_flow_speed,
    typical_profile,
)

# Explicit: Birmingham's occupancy is null in every row, and Spark cannot infer a
# column type from nulls alone.
_OBS_SCHEMA = StructType(
    [
        StructField("city", StringType()),
        StructField("fclass", StringType()),
        StructField("detid", StringType()),
        StructField("occ", DoubleType()),
        StructField("flow", DoubleType()),
        StructField("speed", DoubleType()),
        StructField("ts_local", StringType()),
        StructField("is_dry_baseline", BooleanType()),
    ]
)


def _obs(spark, rows):
    return spark.createDataFrame(rows, _OBS_SCHEMA).withColumn(
        "ts_local", F.col("ts_local").cast("timestamp")
    )


def test_critical_occupancy_is_where_flow_peaks(spark):
    # A fundamental diagram: flow rises with occupancy up to capacity, then falls
    # as the road jams. Critical occupancy is the turning point, and it is what
    # separates free-flow observations from congested ones.
    curve = [(0.03, 200.0), (0.09, 500.0), (0.15, 900.0), (0.21, 700.0), (0.31, 300.0)]
    rows = [
        ("manchester", "trunk", f"d{i}", occ, flow, 50.0, "2017-09-08 08:00:00", True)
        for i, (occ, flow) in enumerate(curve)
        for _ in range(20)  # enough observations per bin to clear the noise floor
    ]

    result = {
        (r["city"], r["fclass"]): r["critical_occ"]
        for r in critical_occupancy(_obs(spark, rows)).collect()
    }

    assert result[("manchester", "trunk")] == 0.14   # 0.02-wide bin holding 0.15


def test_free_flow_speed_reads_only_the_uncongested_observations(spark):
    # The congested speeds (10, 12 km/h at high occupancy) must not enter the
    # percentile, or "free flow" degrades into "average over all conditions".
    free = [(0.02, 40.0), (0.02, 45.0), (0.03, 50.0), (0.03, 55.0), (0.04, 60.0)]
    congested = [(0.40, 10.0), (0.45, 12.0)]
    rows = [
        ("manchester", "trunk", "d1", occ, 500.0, speed, "2017-09-08 08:00:00", True)
        for occ, speed in free + congested
    ]
    critical = spark.createDataFrame(
        [("manchester", "trunk", 0.14)], ["city", "fclass", "critical_occ"]
    )

    result = free_flow_speed(_obs(spark, rows), critical).collect()

    assert len(result) == 1
    # Exact p85 of [40,45,50,55,60] interpolates to 57.0.
    assert result[0]["free_flow_speed"] == 57.0
    assert result[0]["free_flow_obs"] == 5


def test_detectors_without_occupancy_get_no_free_flow_speed(spark):
    # Birmingham ships no occupancy, so the conditioning variable does not exist
    # and no honest free-flow speed can be computed for it.
    rows = [
        ("birmingham", "trunk", "b1", None, 500.0, 48.0, "2017-09-08 08:00:00", True),
        ("birmingham", "trunk", "b1", None, 500.0, 52.0, "2017-09-08 08:05:00", True),
    ]
    critical = spark.createDataFrame(
        [("birmingham", "trunk", 0.14)], ["city", "fclass", "critical_occ"]
    )

    assert free_flow_speed(_obs(spark, rows), critical).collect() == []


def test_typical_profile_excludes_wet_intervals(spark):
    # THE methodological guard. If the 10 km/h wet observation leaks into the
    # baseline, the baseline absorbs the rain effect and every downstream
    # estimate is biased toward zero - the exact flaw CLAUDE.md 3 pins on
    # Google's staticDuration.
    rows = [
        ("manchester", "trunk", "d1", 0.05, 500.0, 40.0, "2017-09-08 08:00:00", True),
        ("manchester", "trunk", "d1", 0.05, 500.0, 50.0, "2017-09-08 08:05:00", True),
        ("manchester", "trunk", "d1", 0.05, 500.0, 60.0, "2017-09-08 08:10:00", True),
        ("manchester", "trunk", "d1", 0.05, 500.0, 10.0, "2017-09-08 08:15:00", False),
    ]

    result = typical_profile(_obs(spark, rows)).collect()

    assert len(result) == 1
    assert result[0]["typical_speed"] == 50.0   # median of 40/50/60, not of all four
    assert result[0]["n_obs"] == 3


def test_typical_profile_separates_weekend_from_weekday_using_local_time(spark):
    # 2017-09-08 is a Friday, 2017-09-09 a Saturday. Day-of-week and hour are
    # local-time concepts, so the profile must key off ts_local, not ts_utc.
    rows = [
        ("manchester", "trunk", "d1", 0.05, 500.0, 40.0, "2017-09-08 08:00:00", True),
        ("manchester", "trunk", "d1", 0.05, 500.0, 70.0, "2017-09-09 08:00:00", True),
    ]

    result = {r["is_weekend"]: r["typical_speed"] for r in typical_profile(_obs(spark, rows)).collect()}

    assert result[False] == 40.0
    assert result[True] == 70.0


def test_delay_metrics_express_both_definitions_of_delay(spark):
    rows = [("manchester", "trunk", "d1", 0.05, 500.0, 30.0, "2017-09-08 08:00:00", True)]
    freeflow = spark.createDataFrame(
        [("manchester", "d1", 60.0)], ["city", "detid", "free_flow_speed"]
    )
    profile = spark.createDataFrame(
        [("manchester", "d1", False, 8, 40.0, 500)],
        ["city", "detid", "is_weekend", "hour", "typical_speed", "n_obs"],
    )

    row = delay_metrics(_obs(spark, rows), freeflow, profile).collect()[0]

    assert row["ff_delay_ratio"] == 0.5        # 30 km/h against 60 free-flow
    assert row["typical_deviation"] == 0.25    # 30 km/h against a 40 norm


def test_capacity_is_not_read_off_a_sparse_high_occupancy_tail_bin(spark):
    # Regression: on real Manchester data the argmax of mean flow landed at
    # occ=0.98 on a bin holding ~0.1% of observations. A loop at 98% occupancy is
    # jammed, so that "capacity" then let essentially every observation through
    # the free-flow filter, turning free-flow speed into p85 of all speeds.
    bulk = [
        ("manchester", "trunk", f"b{i}", occ, flow, 50.0, "2017-09-08 08:00:00", True)
        for occ, flow, n in [(0.05, 600.0, 2000), (0.13, 900.0, 2000), (0.25, 700.0, 2000)]
        for i in range(n)
    ]
    # A tail bin with the highest mean flow of all. 30 rows clears any absolute
    # observation floor, but is only 0.5% of the stratum - the same shape as the
    # real Manchester failure, where the bad bin held 431 rows.
    tail = [
        ("manchester", "trunk", f"t{i}", 0.97, 1500.0, 50.0, "2017-09-08 08:00:00", True)
        for i in range(30)
    ]

    result = critical_occupancy(_obs(spark, bulk + tail)).collect()

    assert len(result) == 1
    assert result[0]["critical_occ"] == 0.12   # the well-populated peak, not 0.96


def test_free_flow_reports_what_share_of_observations_it_kept(spark):
    # If this share approaches 100% the occupancy conditioning is not filtering
    # anything and "free-flow speed" has quietly become p85 of all speeds. The
    # number needs to be visible in the output, not inferred later.
    free = [(0.02, 40.0), (0.02, 45.0), (0.03, 50.0), (0.03, 55.0), (0.04, 60.0)]
    congested = [(0.40, 10.0), (0.45, 12.0)]
    rows = [
        ("manchester", "trunk", "d1", occ, 500.0, speed, "2017-09-08 08:00:00", True)
        for occ, speed in free + congested
    ]
    critical = spark.createDataFrame(
        [("manchester", "trunk", 0.14)], ["city", "fclass", "critical_occ"]
    )

    row = free_flow_speed(_obs(spark, rows), critical).collect()[0]

    assert row["free_flow_obs"] == 5
    assert row["detector_obs"] == 7
    assert abs(row["free_flow_share"] - 5 / 7) < 1e-6


def test_deviation_is_withheld_when_its_baseline_rests_on_too_few_observations(spark):
    # A typical_speed built from one dry observation is not a norm. Left
    # unguarded these cells produced deviations like -63%, i.e. "traffic ran 63%
    # faster than typical", which is an artefact of the baseline, not a finding.
    rows = [("manchester", "trunk", "d1", 0.05, 500.0, 30.0, "2017-09-08 08:00:00", True)]
    freeflow = spark.createDataFrame(
        [("manchester", "d1", 60.0)], ["city", "detid", "free_flow_speed"]
    )
    thin = spark.createDataFrame(
        [("manchester", "d1", False, 8, 40.0, 2)],
        ["city", "detid", "is_weekend", "hour", "typical_speed", "n_obs"],
    )
    thick = spark.createDataFrame(
        [("manchester", "d1", False, 8, 40.0, 500)],
        ["city", "detid", "is_weekend", "hour", "typical_speed", "n_obs"],
    )

    assert delay_metrics(_obs(spark, rows), freeflow, thin).collect()[0]["typical_deviation"] is None
    assert delay_metrics(_obs(spark, rows), freeflow, thick).collect()[0]["typical_deviation"] == 0.25
    # The free-flow metric is unaffected; it has its own observation count.
    assert delay_metrics(_obs(spark, rows), freeflow, thin).collect()[0]["ff_delay_ratio"] == 0.5
