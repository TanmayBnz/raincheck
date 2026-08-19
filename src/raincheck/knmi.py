"""KNMI Data Platform radar acquisition.

Supplies rungs 2 and 3 of the resolution ladder (see CLAUDE.md §6, Layer 2b):

* ``ADJUSTED_5MIN`` - climatological **gauge-adjusted** radar, 5 min / 1 km. The
  observed ceiling, and the reference against which spateGAN's own fields are
  scored. Published as **19 annual zips covering 2008-2026, 24.8 GB total**, so
  the whole archive is 19 requests.
* ``REALTIME_TAR`` - the **inference-grade** unadjusted product, as one ~26 MB
  tar per day, retained back to 2019-10-27.
* ``REALTIME_5MIN`` - the same product as individual 5-minute HDF5 files
  (~187 KB, ~2 minute latency). Use it for live serving, not for bulk history.

Which one to train on is not a preference. The adjusted archive publishes weeks
in arrears, while inference sees the unadjusted real-time product, so training on
the adjusted fields would bake in a bias the live system never sees. Train on
REALTIME, and keep ADJUSTED strictly as a validation reference.

One API key covers every open dataset. The registered tier allows 1000
requests/hour, which is ample here precisely because both bulk products are
pre-aggregated - no bulk key is required.
"""
from __future__ import annotations

import datetime as dt
import os
import pathlib
import re
from dataclasses import dataclass

import numpy as np

API_ROOT = "https://api.dataplatform.knmi.nl/open-data/v1"

KEY_FILE = pathlib.Path(os.environ.get(
    "RAINCHECK_KNMI_KEY_FILE", pathlib.Path.home() / ".knmi_api_key"))


@dataclass(frozen=True)
class Dataset:
    """A KNMI Data Platform dataset and the granularity of its files."""

    name: str
    version: str
    granularity: str

    def files_url(self) -> str:
        return f"{API_ROOT}/datasets/{self.name}/versions/{self.version}/files"

    def download_url(self, filename: str) -> str:
        """Endpoint returning a temporary signed URL for one file."""
        return f"{self.files_url()}/{filename}/url"


ADJUSTED_5MIN = Dataset("rad_nl25_rac_mfbs_5min", "2.0", "annual")
REALTIME_TAR = Dataset("nl_rdr_data_rtcor_5m_tar", "1.0", "daily")
REALTIME_5MIN = Dataset("nl_rdr_data_rtcor_5m", "1.0", "5min")
NOWCAST = Dataset("radar_forecast", "2.0", "5min")

# Both bulk products embed their coverage interval as two compact timestamps.
# Parse that interval; never infer coverage from the year or day in the name:
#
#   - annual archives are offset five minutes from the year boundary, so
#     20071231T235500_20081231T235500 covers 2008, not 2007;
#   - daily tars run 08:05 -> 08:00 next day, so one calendar day needs two;
#   - the stamp format changes partway through the archive. Tars before ~2019
#     are 20181219080000 (14 digits, no separator) running 08:00 -> 07:55;
#     later ones are 20260817T080500. Both forms must parse, or the older half
#     of the usable history raises.
_INTERVAL = re.compile(r"(\d{8}T?\d{6})_(\d{8}T?\d{6})")


def _stamp(value: str) -> dt.datetime:
    return dt.datetime.strptime(value.replace("T", ""), "%Y%m%d%H%M%S")


def coverage_of(filename: str) -> tuple[dt.datetime, dt.datetime]:
    """The (start, end) interval a bulk radar file covers, in UTC."""
    match = _INTERVAL.search(filename)
    if match is None:
        raise ValueError(f"no coverage interval in filename: {filename!r}")
    return _stamp(match.group(1)), _stamp(match.group(2))


def select_files(filenames: list[str], start: dt.datetime,
                 end: dt.datetime) -> list[str]:
    """Those files whose coverage overlaps [start, end)."""
    selected = []
    for name in filenames:
        covers_from, covers_to = coverage_of(name)
        if covers_from < end and covers_to > start:
            selected.append(name)
    return selected


def api_key() -> str:
    """The KNMI API key, from the environment or ~/.knmi_api_key.

    Read from outside the repository on purpose - the key must never be
    committed, mirroring how the CDS credentials live in ~/.cdsapirc.
    """
    if key := os.environ.get("KNMI_API_KEY"):
        return key.strip()
    if KEY_FILE.exists():
        return KEY_FILE.read_text().strip()
    raise RuntimeError(
        f"no KNMI API key: set KNMI_API_KEY or write it to {KEY_FILE}. "
        f"Register at https://developer.dataplatform.knmi.nl/open-data-api"
    )


MAX_KEYS = 1000
"""Server-side cap on files per listing response."""


def list_files(dataset: Dataset, fetch) -> list[str]:
    """Every filename in ``dataset``, following pagination to exhaustion.

    ``fetch`` takes a URL and returns parsed JSON, so the traversal is testable
    without network access. Following ``nextPageToken`` is not optional: the
    daily-tar archive exceeds the 1000-file cap, and a single-page read returns
    only the oldest files - which looks like "no recent radar exists" rather
    than like an error.
    """
    names: list[str] = []
    url = f"{dataset.files_url()}?maxKeys={MAX_KEYS}"

    while True:
        page = fetch(url)
        names.extend(entry["filename"] for entry in page.get("files", []))
        if not page.get("isTruncated"):
            return names
        token = page.get("nextPageToken")
        if not token:
            return names
        url = f"{dataset.files_url()}?maxKeys={MAX_KEYS}&nextPageToken={token}"


# From the file's own image1/calibration attributes: GEO = 0.010000*PV + 0.000000
CALIBRATION_GAIN = 0.01
MISSING_DATA = 65534
OUT_OF_IMAGE = 65535
ACCUMULATION_MINUTES = 5

GRID_ROWS, GRID_COLUMNS = 765, 700
PROJ4 = ("+proj=stere +lat_0=90 +lon_0=0 +lat_ts=60 "
         "+a=6378137 +b=6356752 +x_0=0 +y_0=0 +units=km")


def decode_precipitation(raw):
    """Raw uint16 radar counts to millimetres, with sentinels as NaN.

    ``65534`` (missing) and ``65535`` (outside the radar domain) live in the same
    numeric field as real values. At a gain of 0.01 they decode to 655.34 and
    655.35 mm in five minutes, and the out-of-image value alone covers ~12% of
    the grid, so they must be masked before any arithmetic.
    """
    values = np.asarray(raw)
    millimetres = values.astype("float64") * CALIBRATION_GAIN
    return np.where(
        (values == MISSING_DATA) | (values == OUT_OF_IMAGE), np.nan, millimetres)


def to_mm_per_hour(millimetres, minutes: int = ACCUMULATION_MINUTES):
    """Convert an accumulation over ``minutes`` to an intensity in mm/h.

    Files carry PRECIP_[MM] accumulated over the interval, while every rain
    feature and every entry in BAND_EDGES is defined in mm/h.
    """
    return np.asarray(millimetres, dtype="float64") * (60.0 / minutes)


# From the file's own geographic attributes. Pixel (0, 0) is the upper-left
# corner, at projection coordinates (COLUMN_OFFSET, -ROW_OFFSET) in km.
COLUMN_OFFSET = 0.0
ROW_OFFSET = 3650.0


def _transformer():
    """Lazily built WGS84 -> radar-projection transformer (pyproj caches CRSs)."""
    global _TRANSFORMER
    try:
        return _TRANSFORMER
    except NameError:
        from pyproj import Transformer

        _TRANSFORMER = Transformer.from_crs("EPSG:4326", PROJ4, always_xy=True)
        return _TRANSFORMER


def radar_cells(lats, lons):
    """Vectorised (row, col) for WGS84 coordinates; -1 where outside the grid.

    Detectors are projected onto the radar grid rather than the grid being
    unprojected to lat/lon: there are 20,519 detectors against 535,500 pixels per
    frame, so this direction is ~26x less work and involves no resampling error.

    Verified against each file's own ``geo_product_corners``, which map to the
    grid corners exactly under this convention.
    """
    x, y = _transformer().transform(np.asarray(lons), np.asarray(lats))
    row = np.floor(-(ROW_OFFSET + np.asarray(y))).astype("int64")
    col = np.floor(np.asarray(x) - COLUMN_OFFSET).astype("int64")

    outside = (row < 0) | (row >= GRID_ROWS) | (col < 0) | (col >= GRID_COLUMNS)
    return np.where(outside, -1, row), np.where(outside, -1, col)


def radar_cell(lat: float, lon: float) -> tuple[int, int] | None:
    """(row, col) of one WGS84 point, or None if it falls outside the grid."""
    rows, cols = radar_cells([lat], [lon])
    if rows[0] < 0:
        return None
    return int(rows[0]), int(cols[0])
