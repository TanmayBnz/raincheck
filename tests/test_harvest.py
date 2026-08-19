"""Tests for the NDW live-feed harvester.

The feed is not synchronous: a single publication carried 14,764 sites stamped
08:31 and 5,755 stamped 08:32 (reports/phase1_nl_audit.md). Consecutive
one-minute fetches therefore overlap, and a naive harvester double-counts.
"""
import datetime as dt
import tempfile

import pyarrow as pa
import pathlib

from raincheck.harvest import SeenWindow, partition_of, write_rows


ROOT = pathlib.Path(tempfile.mkdtemp()) / "ndw"


def row(segment_id: str, minute: int, speed: float = 90.0) -> dict:
    return {
        "segment_id": segment_id,
        "ts_utc": dt.datetime(2026, 8, 19, 8, minute),
        "speed": speed,
    }


def test_overlapping_publications_yield_each_observation_once():
    # Site A reported 08:31 in both fetches; site B only appeared in the second.
    window = SeenWindow(horizon=dt.timedelta(minutes=10))

    first = window.filter_new([row("A", 31), row("B", 31)])
    second = window.filter_new([row("A", 31), row("B", 31), row("C", 32)])

    assert [r["segment_id"] for r in first] == ["A", "B"]
    assert [r["segment_id"] for r in second] == ["C"]


def test_keys_older_than_the_horizon_are_evicted():
    # At 29.5 M observations/day an unbounded dedup set exhausts memory within
    # hours. Eviction tracks event time, not wall clock, so a replayed archive
    # behaves identically to the live feed. Duplicates cannot span more than a
    # few minutes, so a short horizon loses nothing.
    window = SeenWindow(horizon=dt.timedelta(minutes=10))
    window.filter_new([row("A", 31), row("B", 31)])

    window.filter_new([row("C", 55)])           # advances event time past 08:41

    assert len(window) == 1
    assert [r["segment_id"] for r in window.filter_new([row("A", 31)])] == ["A"]


def test_rows_partition_by_utc_date_and_hour():
    # ~29.5 M rows/day makes daily files unwieldy; hourly gives ~1.2 M rows each.
    # Hive-style key=value so Spark reads date and hour back as columns.
    assert partition_of(row("A", 31)) == "date=2026-08-19/hour=08"


def test_rows_are_written_into_their_own_hour_partitions():
    import pyarrow.parquet as pq

    rows = [row("A", 31, speed=90.0),
            {"segment_id": "B", "ts_utc": dt.datetime(2026, 8, 19, 9, 5),
             "speed": 70.0}]

    written = write_rows(rows, ROOT)

    assert sorted(p.relative_to(ROOT).parent.as_posix() for p in written) == [
        "date=2026-08-19/hour=08", "date=2026-08-19/hour=09",
    ]
    table = pq.read_table(ROOT)
    assert sorted(table.column("segment_id").to_pylist()) == ["A", "B"]


def test_repeated_writes_append_rather_than_clobber():
    import pyarrow.parquet as pq
    root = pathlib.Path(tempfile.mkdtemp()) / "append"

    write_rows([row("A", 31)], root)
    write_rows([row("B", 31)], root)

    partition = root / "date=2026-08-19/hour=08"
    assert len(list(partition.glob("*.parquet"))) == 2
    assert sorted(pq.read_table(root).column("segment_id").to_pylist()) == ["A", "B"]


def test_an_all_null_speed_batch_still_writes_a_float_column():
    # 15% of speeds are null per minute, and a whole hour partition can be null
    # for a quiet detector. Type inference would emit a null-typed column, and
    # Spark then refuses to read the partition alongside typed ones.
    import pyarrow.parquet as pq
    root = pathlib.Path(tempfile.mkdtemp()) / "nulls"

    write_rows([row("A", 31, speed=None), row("B", 31, speed=None)], root)

    assert pq.read_table(root).schema.field("speed").type == pa.float64()
