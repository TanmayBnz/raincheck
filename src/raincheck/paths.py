"""HDFS locations for the RainCheck data lake.

Fully qualified on purpose: HADOOP_CONF_DIR is unset on this machine, so Spark
does not pick up fs.defaultFS implicitly and a bare /path would resolve to the
local filesystem.
"""

import os
import pathlib

HDFS = os.environ.get("RAINCHECK_HDFS", "hdfs://localhost:9000")
ROOT = f"{HDFS}/raincheck"

# Local staging for the NDW harvester. It writes with pyarrow, which would need
# libhdfs to address hdfs:// directly, so partitions land here and are uploaded
# separately. Kept outside the repo, and off "/" which is nearly full.
LOCAL_STAGE = pathlib.Path(
    os.environ.get("RAINCHECK_STAGE", "/home/tanbnz/raincheck-data/stage"))
LOCAL_STAGE_NDW = LOCAL_STAGE / "ndw" / "measurements"

RAW_NDW = f"{ROOT}/nl/raw/ndw/measurements"
RAW_NDW_SITES = f"{ROOT}/nl/raw/ndw/sites"
CURATED_NDW = f"{ROOT}/nl/curated/measurements"

RAW_MEASUREMENTS = f"{ROOT}/raw/utd19/measurements/utd19_u.csv"
RAW_DETECTORS = f"{ROOT}/raw/utd19/detectors/detectors_public.csv"
RAW_LINKS = f"{ROOT}/raw/utd19/links/links.csv"

AUDIT = f"{ROOT}/audit"

CURATED_MEASUREMENTS = f"{ROOT}/curated/measurements"
CURATED_QUALITY = f"{ROOT}/curated/quality"

RAW_ERA5 = f"{ROOT}/raw/era5"
CURATED_ERA5 = f"{ROOT}/curated/era5"
RAIN_FEATURES = f"{ROOT}/weather/rain_features"
BASELINE_FREEFLOW = f"{ROOT}/baselines/freeflow"
BASELINE_TYPICAL = f"{ROOT}/baselines/typical_profile"
TRAINING = f"{ROOT}/features/training"
