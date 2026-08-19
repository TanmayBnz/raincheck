"""Download ERA5 hourly fields and land them in HDFS as raw netCDF + long Parquet.

Not a Spark job: the download is a handful of HTTP requests and the resulting
cube is ~76 MB, so pandas/xarray handle it directly. Run via scripts/fetch_era5.sh.
"""

import argparse
import datetime as dt
import subprocess
import zipfile
from pathlib import Path

import xarray as xr

from raincheck import paths
from raincheck.era5 import DATASET, build_monthly_requests, era5_to_long

# Two weeks before the first traffic day (2017-09-08). The dry-spell antecedent
# feature counts hours since the last rain, which is undefined at the very start
# of the record - without a lead-in, every detector looks like it has been dry
# forever on day one.
LEAD_IN_START = dt.date(2017, 8, 25)
WINDOW_END = dt.date(2017, 11, 19)


def _hdfs(*args: str) -> None:
    subprocess.run(["hdfs", "dfs", *args], check=True)


def _nc_members(path: Path) -> list[Path]:
    """All netCDF members of a CDS download, extracted if it is a zip.

    The CDS returns a zip holding TWO netCDFs - `stepType-instant` with t2m/d2m/
    u10/v10 and `stepType-accum` with tp/cp/lsp. Taking only the first silently
    discards every precipitation variable, so every member must be kept.
    """
    with path.open("rb") as handle:
        if handle.read(2) != b"PK":
            return [path]
    extracted = []
    with zipfile.ZipFile(path) as archive:
        members = [n for n in archive.namelist() if n.endswith(".nc")]
        if not members:
            raise SystemExit(f"{path} is a zip with no .nc inside: {archive.namelist()}")
        for member in members:
            stem = Path(member).stem
            target = path.with_name(f"{path.stem}__{stem}.nc")
            with archive.open(member) as src, target.open("wb") as dst:
                dst.write(src.read())
            extracted.append(target)
    return extracted


def _open_month(paths: list[Path]) -> xr.Dataset:
    """Merge one month's netCDF streams into a single dataset."""
    # compat/join stated explicitly: the two streams carry disjoint variables on
    # identical coordinates, so an override is safe and a coordinate mismatch
    # should fail loudly rather than align silently.
    return xr.merge(
        [xr.open_dataset(str(p)) for p in paths], compat="override", join="exact"
    )


def download(stage: Path) -> list[list[Path]]:
    import cdsapi

    client = cdsapi.Client()
    files = []
    for request in build_monthly_requests(LEAD_IN_START, WINDOW_END):
        name = f"era5_{request['year']}_{request['month']}.nc"
        target = stage / name
        if target.exists() and target.stat().st_size > 0:
            print(f"SKIP {name} (already downloaded)")
        else:
            print(f"FETCH {name} days={request['day'][0]}..{request['day'][-1]}")
            client.retrieve(DATASET, request, str(target))
        files.append(_nc_members(target))
    return files


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", default="data/era5", help="local staging directory")
    parser.add_argument("--skip-download", action="store_true")
    args = parser.parse_args()

    stage = Path(args.stage)
    stage.mkdir(parents=True, exist_ok=True)

    if args.skip_download:
        downloads = sorted(p for p in stage.glob("era5_????_??.nc"))
        months = [_nc_members(p) for p in downloads]
    else:
        months = download(stage)

    # open_dataset + merge + concat rather than open_mfdataset: the latter needs
    # dask, a heavy dependency for a cube totalling ~76 MB.
    parts = [_open_month(m) for m in months]
    time_coord = "valid_time" if "valid_time" in parts[0].coords else "time"
    cube = xr.concat(parts, dim=time_coord).sortby(time_coord)
    print(f"CUBE vars={sorted(cube.data_vars)}")
    n_hours = cube.sizes[time_coord]
    print(f"CUBE dims={dict(cube.sizes)}")
    print(f"CUBE time {cube[time_coord].values[0]} .. {cube[time_coord].values[-1]}")

    expected = (WINDOW_END - LEAD_IN_START).days * 24 + 24
    if n_hours != expected:
        print(f"WARNING hour count {n_hours} != expected {expected}")

    long = era5_to_long(cube)
    parquet = stage / "era5_long.parquet"
    long.to_parquet(parquet, index=False)
    print(f"LONG_ROWS {len(long)}  -> {parquet}")
    print(long.describe().to_string())

    _hdfs("-mkdir", "-p", paths.RAW_ERA5, paths.CURATED_ERA5)
    for month in months:
        for path in month:
            _hdfs("-put", "-f", str(path), f"{paths.RAW_ERA5}/")
    _hdfs("-put", "-f", str(parquet), f"{paths.CURATED_ERA5}/")
    print(f"HDFS_RAW {paths.RAW_ERA5}")
    print(f"HDFS_CURATED {paths.CURATED_ERA5}")


if __name__ == "__main__":
    main()
