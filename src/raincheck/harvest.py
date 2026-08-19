"""Harvest the NDW live feed into Parquet.

The open feed at https://opendata.ndw.nu/ publishes only the current minute, so
history has to be accumulated. Two properties of the feed drive this module:

* Publications **overlap** - sites are stamped with their own
  ``measurementTimeDefault``, not the publication time, so the same
  ``(segment_id, ts_utc)`` observation appears in consecutive fetches.
* Raw XML is ~72 GB/day uncompressed, so it is parsed and discarded, never
  archived.
"""
from __future__ import annotations

import datetime as dt
import pathlib
import uuid

import pyarrow as pa
import pyarrow.parquet as pq


class SeenWindow:
    """Remembers recently seen ``(segment_id, ts_utc)`` keys to drop duplicates."""

    def __init__(self, horizon: dt.timedelta):
        self._horizon = horizon
        self._seen: set[tuple[str, dt.datetime]] = set()

    def filter_new(self, rows: list[dict]) -> list[dict]:
        """Return only those rows not already seen, recording them as seen."""
        fresh = []
        for row in rows:
            key = (row["segment_id"], row["ts_utc"])
            if key in self._seen:
                continue
            self._seen.add(key)
            fresh.append(row)
        self._evict()
        return fresh

    def _evict(self) -> None:
        """Drop keys older than the horizon, measured from the latest event time.

        Event time rather than wall clock, so replaying an archive behaves
        exactly like consuming the live feed.
        """
        if not self._seen:
            return
        cutoff = max(ts for _, ts in self._seen) - self._horizon
        self._seen = {(sid, ts) for sid, ts in self._seen if ts >= cutoff}

    def __len__(self) -> int:
        return len(self._seen)


def partition_of(row: dict) -> str:
    """Hive-style partition path for a row, keyed on its UTC event time."""
    ts = row["ts_utc"]
    return f"date={ts:%Y-%m-%d}/hour={ts:%H}"


NDW_SCHEMA = pa.schema([
    ("source", pa.string()),
    ("country", pa.string()),
    ("segment_id", pa.string()),
    # Microseconds, not nanoseconds: nanosecond timestamps are rejected on write
    # by the Parquet version this project reads with.
    ("ts_utc", pa.timestamp("us")),
    ("speed", pa.float64()),
    ("flow", pa.float64()),
    ("quality_weight", pa.float64()),
    ("frc", pa.string()),
    ("lat", pa.float64()),
    ("lon", pa.float64()),
    ("computation_method", pa.string()),
    ("equipment", pa.string()),
])
"""The canonical schema, pinned explicitly.

Type inference is not safe here: ~15% of speeds are null in any given minute and
a quiet detector can be null for a whole hour, which infers a null-typed column
that Spark then refuses to read alongside typed partitions.
"""


def write_rows(rows: list[dict], root: pathlib.Path,
               schema: pa.Schema | None = NDW_SCHEMA) -> list[pathlib.Path]:
    """Write rows as Parquet under ``root``, one file per hour partition.

    Each call writes a new uniquely-named file rather than rewriting a
    partition, so a long-running harvest only ever appends. Raw XML is never
    retained: at ~72 GB/day uncompressed it would exhaust the disk in under
    four days.
    """
    if not rows:
        return []

    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(partition_of(row), []).append(row)

    written = []
    stamp = f"{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    for partition, batch in sorted(grouped.items()):
        directory = root / partition
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"part-{stamp}.parquet"
        table = pa.Table.from_pylist(batch, schema=schema)
        pq.write_table(table, path, compression="zstd")
        written.append(path)
    return written
