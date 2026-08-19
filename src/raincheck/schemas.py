"""Explicit schemas for the raw UTD19 files.

Read explicitly rather than with inferSchema: inference costs an extra pass over
6.5 GB, and it silently picked `string` for columns that are entirely empty in
the first city it encountered, which would have made `speed` untypeable.
"""

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# utd19_u.csv. Note the file is CRLF-terminated; Spark's CSV reader strips the
# carriage return from the trailing `speed` field, but a hand-rolled split would
# not, so never parse this file outside Spark without stripping \r.
RAW_MEASUREMENTS = StructType(
    [
        StructField("day", DateType()),
        StructField("interval", IntegerType()),  # seconds after local midnight
        StructField("detid", StringType()),
        StructField("flow", DoubleType()),  # veh/h
        StructField("occ", DoubleType()),
        StructField("error", IntegerType()),  # 0 clean, 1 flagged, null unassessed
        StructField("city", StringType()),
        StructField("speed", DoubleType()),
    ]
)
