"""D8: expected road state must ignore the occupancy observed in the rainy interval."""
from pyspark.sql import SparkSession

from raincheck.analysis.dose_response import with_expected_state


def test_expected_state_ignores_realised_occupancy():
    spark = SparkSession.builder.master("local[1]").appName("t").getOrCreate()
    # Same detector-slot, rain pushes realised occupancy over the line (0.3 >= 0.2),
    # but the dry profile says this slot is normally quiet (0.1 < 0.2).
    m = spark.createDataFrame(
        [("a", 1, 1, 7, 0.30, 0.2, True), ("a", 1, 2, 7, 0.30, 0.2, True)],
        "city string, detid long, dow int, tbin int, occ double, occ_crit double, congested boolean",
    )
    prof = spark.createDataFrame(
        [("a", 1, 1, 7, 0.10, True), ("a", 1, 2, 7, 0.10, False)],
        "city string, detid long, dow int, tbin int, typical_occ double, cell_ok boolean",
    )
    out = {(r.dow): r.congested for r in with_expected_state(m, prof).collect()}
    assert out[1] is False   # realised said True; expected must say False
    assert out[2] is None    # profile cell failed its floor -> unknowable, not guessed
