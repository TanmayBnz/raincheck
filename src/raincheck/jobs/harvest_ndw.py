"""Harvest the NDW live feed into local Parquet.

The open feed publishes only the current minute, so history must be accumulated.
This runs as a long-lived process, one fetch per minute:

    ./scripts/harvest_ndw.sh                  # run until stopped
    ./scripts/harvest_ndw.sh --once           # single fetch, for smoke testing

Writes locally rather than to HDFS because pyarrow would need libhdfs to write
hdfs:// directly; a separate step uploads the staged partitions. Raw XML is
parsed and discarded - at ~72 GB/day uncompressed, retaining it would exhaust
the disk in under four days.
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import io
import logging
import pathlib
import signal
import time
import urllib.request

from raincheck import paths
from raincheck.harvest import TRAVEL_TIME_SCHEMA, SeenWindow, write_rows
from raincheck.ndw import SiteTable, parse_measurements, parse_site_table
from raincheck.travel_time import parse_travel_times

TRAFFICSPEED_URL = "https://opendata.ndw.nu/trafficspeed.xml.gz"
TRAVELTIME_URL = "https://opendata.ndw.nu/traveltime.xml.gz"
SITE_TABLE_URL = "https://opendata.ndw.nu/measurement_current.xml.gz"

FETCH_TIMEOUT_S = 120

# Measured lag between fetch time and measurementTimeDefault: the feed carries
# two cohorts, 5,755 sites at ~2 minutes and 14,764 at ~8 minutes. The horizon
# must comfortably exceed the slower cohort or duplicates slip past the dedup,
# and 30 minutes of keys is only ~600k entries.
DEDUP_HORIZON = dt.timedelta(minutes=30)

log = logging.getLogger("harvest_ndw")

_stopping = False


def _stop(signum, _frame) -> None:
    global _stopping
    _stopping = True
    log.info("signal %s received; finishing the current fetch then stopping", signum)


def _fetch(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=FETCH_TIMEOUT_S) as response:
        return response.read()


def _load_site_table() -> SiteTable:
    """Fetch and parse the measurement site table (~371 MB uncompressed)."""
    started = time.monotonic()
    with gzip.open(io.BytesIO(_fetch(SITE_TABLE_URL)), "rb") as handle:
        table = parse_site_table(handle)
    log.info("site table version %s: %d sites in %.1fs",
             table.version, len(table.sites), time.monotonic() - started)
    return table


def _parse(payload: bytes, table: SiteTable) -> list[dict]:
    with gzip.open(io.BytesIO(payload), "rb") as handle:
        return parse_measurements(handle, table)


def harvest_travel_time(window: SeenWindow, stage: pathlib.Path) -> int:
    """Fetch and persist one minute of section travel times.

    Harvested alongside the loop speeds because it is a largely independent
    sensor - 90.5% of sections are floating car data - and because it is the
    only NDW series carrying a published reference value. Keyed on
    ``section_id``, so it needs its own dedup window.
    """
    payload = _fetch(TRAVELTIME_URL)
    with gzip.open(io.BytesIO(payload), "rb") as handle:
        rows = parse_travel_times(handle)
    fresh = window.filter_new(rows)
    write_rows(fresh, stage, schema=TRAVEL_TIME_SCHEMA)
    return len(fresh)


def harvest_once(table: SiteTable, window: SeenWindow,
                 stage: pathlib.Path) -> tuple[SiteTable, int]:
    """One fetch-parse-write cycle. Returns the (possibly reloaded) site table.

    A site table version bump makes ``parse_measurements`` raise, because
    measurement indices are table-specific. That is recoverable: reload the
    table and re-parse the same payload.
    """
    payload = _fetch(TRAFFICSPEED_URL)
    try:
        rows = _parse(payload, table)
    except ValueError as mismatch:
        log.warning("%s - reloading site table", mismatch)
        table = _load_site_table()
        rows = _parse(payload, table)

    fresh = window.filter_new(rows)
    written = write_rows(fresh, stage)
    log.info("fetched %d rows, %d new (%.0f%% dup), %d files, dedup window %d",
             len(rows), len(fresh),
             100 * (1 - len(fresh) / len(rows)) if rows else 0,
             len(written), len(window))
    return table, len(fresh)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", type=pathlib.Path, default=paths.LOCAL_STAGE_NDW,
                        help="local directory to write Parquet partitions into")
    parser.add_argument("--interval", type=float, default=60.0,
                        help="seconds between fetches (the feed updates each minute)")
    parser.add_argument("--once", action="store_true", help="single fetch, then exit")
    parser.add_argument("--no-travel-time", action="store_true",
                        help="harvest loop speeds only")
    parser.add_argument("--max-fetches", type=int, default=0,
                        help="stop after this many fetches (0 = unlimited)")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    args.stage.mkdir(parents=True, exist_ok=True)
    log.info("staging to %s", args.stage)

    table = _load_site_table()
    window = SeenWindow(horizon=DEDUP_HORIZON)
    travel_window = SeenWindow(horizon=DEDUP_HORIZON, key="section_id")
    travel_stage = args.stage.parent / "traveltime"
    limit = 1 if args.once else args.max_fetches
    fetches = total = 0

    while not _stopping:
        started = time.monotonic()
        try:
            table, new_rows = harvest_once(table, window, args.stage)
            total += new_rows
            if not args.no_travel_time:
                fresh = harvest_travel_time(travel_window, travel_stage)
                log.info("travel time: %d new sections", fresh)
        except Exception:                        # keep the harvest alive
            log.exception("fetch failed; retrying next interval")
        fetches += 1

        if limit and fetches >= limit:
            break
        elapsed = time.monotonic() - started
        if (remaining := args.interval - elapsed) > 0 and not _stopping:
            time.sleep(remaining)

    log.info("stopped after %d fetches, %d rows written", fetches, total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
