"""SparkSession construction for RainCheck jobs."""

import os
from pathlib import Path

from pyspark.sql import SparkSession

# /tmp on this machine is tmpfs backed by RAM, so Spark's default spark.local.dir
# would send every shuffle spill to memory and OOM the box on a wide shuffle.
DEFAULT_LOCAL_DIR = Path.home() / "spark-tmp"


def build_session(app_name: str, master: str | None = None) -> SparkSession:
    local_dir = Path(os.environ.get("RAINCHECK_SPARK_LOCAL_DIR", DEFAULT_LOCAL_DIR))
    local_dir.mkdir(parents=True, exist_ok=True)

    session = (
        SparkSession.builder.appName(app_name)
        .master(master or os.environ.get("RAINCHECK_MASTER", "local[*]"))
        .config("spark.local.dir", str(local_dir))
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    return session
