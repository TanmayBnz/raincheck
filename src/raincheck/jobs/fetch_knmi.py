"""Sample KNMI radar rainfall at every NDW detector, for a time window.

    ./scripts/fetch_knmi.sh --start 2026-08-19T08:50 --end 2026-08-19T10:00

Projects the detectors onto the radar grid and reads only those pixels, rather
than unprojecting the grid: 20,519 detectors against 535,500 pixels per frame is
~26x less work and introduces no resampling error.

Uses the **real-time (unadjusted)** product by default. That is deliberate and not
a convenience: the gauge-adjusted archive publishes weeks in arrears, so the live
system will only ever see the unadjusted field. Training on the adjusted product
would bake in a bias inference never encounters. The adjusted archive is pulled
separately, as a validation reference.
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import pathlib
import urllib.request

import h5py
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from raincheck import paths
from raincheck.knmi import (
    ACCUMULATION_MINUTES,
    REALTIME_5MIN,
    api_key,
    decode_precipitation,
    radar_cells,
    to_mm_per_hour,
)

SCHEMA = pa.schema([
    ("segment_id", pa.string()),
    ("ts_utc", pa.timestamp("us")),
    ("rain_mm_h", pa.float64()),
])

# RAD_NL25_RAC_RT_202608190925.h5
FILENAME_TEMPLATE = "RAD_NL25_RAC_RT_%Y%m%d%H%M.h5"


def _request(url: str, key: str):
    return urllib.request.Request(url, headers={"Authorization": key})


def _json(url: str, key: str):
    return json.load(urllib.request.urlopen(_request(url, key), timeout=120))


def frame_stamps(start: dt.datetime, end: dt.datetime) -> list[dt.datetime]:
    """The frame end-stamps covering [start, end).

    Files are labelled by the **end** of their accumulation window, so the frame
    stamped 09:25 covers 09:20-09:25 and belongs to the 5-minute bin starting
    09:20. Getting this backwards shifts every rainfall value one bin.
    """
    step = dt.timedelta(minutes=ACCUMULATION_MINUTES)
    first = start + step
    count = int((end - start) // step)
    return [first + i * step for i in range(count)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--segments", type=pathlib.Path,
                        default=paths.LOCAL_STAGE.parent / "curated" / "ndw_segments")
    parser.add_argument("--output", type=pathlib.Path,
                        default=paths.LOCAL_STAGE.parent / "curated" / "ndw_rain")
    parser.add_argument("--start", required=True, help="UTC, e.g. 2026-08-19T08:50")
    parser.add_argument("--end", required=True)
    args = parser.parse_args(argv)

    start = dt.datetime.fromisoformat(args.start)
    end = dt.datetime.fromisoformat(args.end)
    key = api_key()

    segments = pq.read_table(args.segments)
    ids = np.array(segments.column("segment_id").to_pylist())
    rows, cols = radar_cells(segments.column("lat").to_numpy(),
                             segments.column("lon").to_numpy())
    inside = rows >= 0
    print(f"{len(ids):,} segments; {inside.sum():,} inside the radar grid "
          f"({100 * inside.mean():.1f}%)")
    ids, rows, cols = ids[inside], rows[inside], cols[inside]

    records: list[dict] = []
    missing = 0
    for stamp in frame_stamps(start, end):
        name = stamp.strftime(FILENAME_TEMPLATE)
        try:
            signed = _json(REALTIME_5MIN.download_url(name), key)["temporaryDownloadUrl"]
            payload = urllib.request.urlopen(signed, timeout=180).read()
        except Exception as error:                       # a frame can be absent
            missing += 1
            print(f"  {name}: unavailable ({type(error).__name__})")
            continue

        with h5py.File(io.BytesIO(payload), "r") as handle:
            grid = to_mm_per_hour(decode_precipitation(handle["image1/image_data"][:]))

        # The frame stamped `stamp` covers the bin starting one interval earlier.
        bin_start = stamp - dt.timedelta(minutes=ACCUMULATION_MINUTES)
        values = grid[rows, cols]
        records.extend(
            {"segment_id": sid, "ts_utc": bin_start, "rain_mm_h": None if np.isnan(v)
             else float(v)}
            for sid, v in zip(ids, values))
        wet = np.nansum(values > 0.1)
        print(f"  {name}: max {np.nanmax(grid):5.1f} mm/h, "
              f"{wet:,} of {len(values):,} detectors wet")

    args.output.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(records, schema=SCHEMA),
                   args.output / "rain.parquet", compression="zstd")
    print(f"\n{len(records):,} detector-frames written to {args.output}"
          f"  ({missing} frames unavailable)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
