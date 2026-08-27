"""Shared paths, config loading, and Spark session construction."""

from __future__ import annotations

import os
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# The three source CSVs sit at the project root as delivered by UTD19. They are
# treated as read-only; nothing in the pipeline writes here.
RAW_MEASUREMENTS = PROJECT_ROOT / "utd19_u.csv"
RAW_DETECTORS = PROJECT_ROOT / "detectors_public.csv"
RAW_LINKS = PROJECT_ROOT / "links.csv"

CONF_DIR = PROJECT_ROOT / "conf"
REPORTS_DIR = PROJECT_ROOT / "reports"

# HDFS-shaped layout on the local filesystem. Moving to real HDFS in a later
# phase is then a change of LAKE_ROOT to an hdfs:// URI, not a rewrite.
LAKE_ROOT = PROJECT_ROOT / "lake"
LANDED_MEASUREMENTS = LAKE_ROOT / "utd19" / "landed" / "measurements"
LANDED_DETECTORS = LAKE_ROOT / "utd19" / "landed" / "detectors"

# L1 output: study cities only, quality-filtered, units normalized, UTC-aligned,
# detector metadata joined, indexed onto the rainfall grids.
CURATED_MEASUREMENTS = LAKE_ROOT / "utd19" / "curated" / "measurements"

# L2b (native-resolution slice): hourly city-level rain labels. Enough to mark
# intervals dry for the L2a baseline; the per-detector spatial join against
# downscaled fields is Phase 4.
ERA5_RAW = LAKE_ROOT / "era5" / "raw"
RAIN_HOURLY = LAKE_ROOT / "era5" / "curated" / "rain_hourly"

# L2a outputs.
BASELINE_FREEFLOW = LAKE_ROOT / "utd19" / "baselines" / "freeflow"
BASELINE_PROFILE = LAKE_ROOT / "utd19" / "baselines" / "profile"
MEASUREMENTS_DELAY = LAKE_ROOT / "utd19" / "curated" / "measurements_delay"

# Expected raw row count, established by a full scan of utd19_u.csv.
# The landing job asserts against this: any drift means rows were dropped.
EXPECTED_MEASUREMENT_ROWS = 134_380_371
EXPECTED_DETECTOR_ROWS = 23_626


def load_env() -> dict[str, str]:
    """Read the project-root .env into a dict (no external dependency).

    Holds CDS credentials so the ERA5 pre-check needs no ~/.cdsapirc. The file
    is gitignored; .env.example documents the expected keys.
    """
    env: dict[str, str] = {}
    path = PROJECT_ROOT / ".env"
    if not path.exists():
        return env
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        # Tolerate quoted values; a pasted token often arrives wrapped.
        env[key.strip()] = value.strip().strip("'\"")
    return env


def load_cities_conf() -> dict:
    """Load conf/cities.yml."""
    with open(CONF_DIR / "cities.yml", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def spark_path(path: Path) -> str:
    """Spark wants forward slashes; Windows drive letters otherwise confuse the URI parser."""
    return path.resolve().as_posix()


def get_spark(app_name: str, driver_memory: str | None = None, shuffle_partitions: int = 64):
    """Build a local-mode SparkSession.

    Intended to run under WSL2 (Ubuntu), venv at ~/.venvs/raincheck, lake
    reached via /mnt/d -- see CONTEXT.md §5. On native Windows the Parquet
    write fails regardless of configuration, because Hadoop's permission calls
    need winutils.exe.

    driver_memory must be applied before the JVM launches, which is why it goes
    through PYSPARK_SUBMIT_ARGS rather than SparkSession.builder.config().
    In local mode the driver is also the executor, so this is the memory knob
    that actually matters.
    """
    mem = driver_memory or os.environ.get("SPARK_DRIVER_MEMORY", "5g")
    os.environ.setdefault(
        "PYSPARK_SUBMIT_ARGS",
        f"--driver-memory {mem} pyspark-shell",
    )

    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        # Treat all naive timestamps literally. Local->UTC conversion is a later
        # phase and needs a per-city timezone table; doing it implicitly here
        # would shift every reading by an unknown offset.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Local mode has no shuffle service; keep spill on the big D: volume.
        .config("spark.local.dir", spark_path(PROJECT_ROOT / ".spark-tmp"))
        .getOrCreate()
    )
