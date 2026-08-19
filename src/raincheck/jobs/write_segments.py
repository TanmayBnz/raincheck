"""Persist the NDW measurement site table as the segments dimension.

    ./scripts/write_segments.sh

The harvester keeps only measurements; site metadata lives in a separate feed and
changes slowly (version 1727 at time of writing). Coordinates are needed to join
rainfall, and road class for the dose-response stratification - so the table is
materialised once rather than re-parsed per job. 371 MB of XML collapses to a few
hundred KB of Parquet.
"""
from __future__ import annotations

import argparse
import gzip
import io
import pathlib
import urllib.request

import pyarrow as pa
import pyarrow.parquet as pq

from raincheck import paths
from raincheck.ndw import parse_site_table
from raincheck.travel_time import parse_sections

SITE_TABLE_URL = "https://opendata.ndw.nu/measurement_current.xml.gz"

SECTION_SCHEMA = pa.schema([
    ("section_id", pa.string()),
    ("length_m", pa.float64()),
    ("equipment", pa.string()),
    ("lat", pa.float64()),
    ("lon", pa.float64()),
    ("n_links", pa.int64()),
    ("is_loop_derived", pa.bool_()),
])

SCHEMA = pa.schema([
    ("segment_id", pa.string()),
    ("lat", pa.float64()),
    ("lon", pa.float64()),
    ("name", pa.string()),
    ("equipment", pa.string()),
    ("computation_method", pa.string()),
    ("frc", pa.string()),
    ("site_table_version", pa.string()),
])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=pathlib.Path,
                        default=paths.LOCAL_STAGE.parent / "curated" / "ndw_segments")
    args = parser.parse_args(argv)

    payload = urllib.request.urlopen(SITE_TABLE_URL, timeout=180).read()
    with gzip.open(io.BytesIO(payload), "rb") as handle:
        table = parse_site_table(handle)
    with gzip.open(io.BytesIO(payload), "rb") as handle:
        sections = parse_sections(handle)

    rows = [{
        "segment_id": site.site_id,
        "lat": site.lat,
        "lon": site.lon,
        "name": site.name,
        "equipment": site.equipment,
        "computation_method": site.computation_method,
        "frc": site.frc,
        "site_table_version": table.version,
    } for site in table.sites.values()]

    args.output.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA),
                   args.output / "segments.parquet", compression="zstd")

    section_rows = [{
        "section_id": s.section_id,
        "length_m": s.length_m,
        "equipment": s.equipment,
        "lat": s.lat,
        "lon": s.lon,
        "n_links": s.n_links,
        "is_loop_derived": s.is_loop_derived,
    } for s in sections.values()]
    section_out = args.output.parent / "ndw_sections"
    section_out.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(section_rows, schema=SECTION_SCHEMA),
                   section_out / "sections.parquet", compression="zstd")
    loop_derived = sum(1 for r in section_rows if r["is_loop_derived"])
    print(f"travel-time sections: {len(section_rows):,}"
          f"  ({loop_derived:,} loop-derived, not independent of the speed feed)")
    print(f"  written to {section_out}")

    with_frc = sum(1 for r in rows if r["frc"])
    print(f"site table version {table.version}: {len(rows):,} segments")
    print(f"  with OpenLR road class: {with_frc:,} ({100 * with_frc / len(rows):.1f}%)"
          f" - the rest need OSM map-matching")
    print(f"  written to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
