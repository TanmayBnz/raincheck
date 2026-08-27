"""Phase 4 / L2b -- drive spateGAN-ERA5 over the study windows.

The downscaler is vendored at vendor/spateGAN_ERA5 with its own Python 3.13 +
torch 2.6 environment, because torch has no wheels for the 3.14 the rest of the
project runs on. It is therefore invoked as a subprocess, not imported.

**Only days that carry traffic data are downscaled.** This is the difference
between a 90-minute job and a 7-hour one. Essen's window spans 188 calendar
days but holds traffic on 35 of them; downscaling the other 153 would produce
rainfall fields that nothing will ever join against. Manchester is 28 of 72.

Contiguous data days are grouped into single runs -- partly for efficiency
(the model loads once per run) and partly because the model needs a >= 16 hour
sequence, so isolated days must be padded anyway. Each run is padded by a day
either side: traffic timestamps are local and rainfall is UTC, so a local date
reaches an hour or two outside its own UTC day.

Runs are cached by output filename; re-running skips completed work.

Run:  python -m raincheck.weather.run_downscaling [--city essen] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from raincheck import config

VENDOR = config.PROJECT_ROOT / "vendor" / "spateGAN_ERA5"
VENDOR_PY = VENDOR / ".venv" / "bin" / "python"
DOWNSCALED = config.LAKE_ROOT / "era5" / "downscaled"

# Pad each run by a day either side to cover the local/UTC offset at the edges.
PAD_DAYS = 1

# Maximum days per invocation. The downscaler holds its whole output in memory
# before writing: one day is 144 timesteps on a 144x247 float64 grid (~41 MB),
# so a 23-day contiguous run needs ~1 GB for the result alone, plus model
# intermediates and the GPU copy. This WSL VM has 7 GB, and an over-long run
# does not raise -- the VM is OOM-killed and the parent process dies silently
# with no traceback. Chunking is what makes the job survivable, not faster.
MAX_RUN_DAYS = 6
# Chunks overlap so the trailing-accumulation features (acc_10/30/60min) and
# onset detection have continuous input across a boundary. Duplicate timesteps
# are dropped during extraction.
CHUNK_OVERLAP_DAYS = 1
# The model requires >= 16 contiguous hours; with padding every run clears this
# comfortably, but assert rather than assume.
MIN_HOURS = 16


def cuda_available() -> bool:
    """Ask the vendored environment, not this one.

    The downscaler runs under its own Python 3.13 + torch install; whether the
    interpreter running THIS module can see a GPU says nothing about it.
    """
    probe = subprocess.run(
        [str(VENDOR_PY), "-c", "import torch; print(int(torch.cuda.is_available()))"],
        capture_output=True, text=True, cwd=VENDOR,
    )
    return probe.returncode == 0 and probe.stdout.strip().endswith("1")


def data_days(city: str) -> list[date]:
    """Distinct local dates carrying curated traffic data for one city."""
    import duckdb

    path = (config.CURATED_MEASUREMENTS / f"city={city}" / "**" / "*.parquet").as_posix()
    rows = duckdb.sql(
        f"SELECT DISTINCT date FROM read_parquet('{path}') ORDER BY date"
    ).fetchall()
    return [r[0] for r in rows]


def contiguous_runs(days: list[date], pad: int = PAD_DAYS) -> list[tuple[date, date]]:
    """Group consecutive dates, then pad. Overlapping padded runs are merged."""
    if not days:
        return []
    groups: list[list[date]] = [[days[0]]]
    for d in days[1:]:
        if (d - groups[-1][-1]).days == 1:
            groups[-1].append(d)
        else:
            groups.append([d])

    padded = [(g[0] - timedelta(days=pad), g[-1] + timedelta(days=pad)) for g in groups]

    merged: list[tuple[date, date]] = []
    for start, end in padded:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))

    return [c for run in merged for c in chunk_run(*run)]


def chunk_run(start: date, end: date) -> list[tuple[date, date]]:
    """Split an over-long span into overlapping chunks the VM can hold."""
    if (end - start).days + 1 <= MAX_RUN_DAYS:
        return [(start, end)]
    chunks, cur = [], start
    while cur <= end:
        stop = min(cur + timedelta(days=MAX_RUN_DAYS - 1), end)
        chunks.append((cur, stop))
        if stop >= end:
            break
        # Step forward by the chunk length less the overlap, so consecutive
        # chunks share CHUNK_OVERLAP_DAYS of context.
        cur = stop + timedelta(days=1 - CHUNK_OVERLAP_DAYS)
    return chunks


def run_one(
    city: str, centre: dict, start: date, end: date, out_dir: Path, seed: int, device: str
) -> bool:
    """Invoke the vendored downscaler for one contiguous span."""
    hours = ((end - start).days + 1) * 24
    if hours < MIN_HOURS:
        print(f"    {start}..{end}: only {hours}h < {MIN_HOURS}h minimum -- skipped")
        return False

    # The vendor names outputs by centre and date range, so an existing file for
    # this span means the work is already done.
    stamp = f"{start:%Y%m%d}_{end:%Y%m%d}"
    existing = list(out_dir.glob(f"spateGAN_ERA5_latlon_*_{stamp}_e*.nc"))
    if existing:
        print(f"    {start}..{end}: cached ({existing[0].stat().st_size / 1e6:.0f} MB)")
        return True

    cmd = [
        str(VENDOR_PY), "main.py",
        "--input", str(config.LAKE_ROOT / "era5" / "domain" / f"{city}_cp_lsp.nc"),
        "--center-lat", str(centre["centre_lat"]),
        "--center-lon", str(centre["centre_lon"]),
        "--start-date", start.isoformat(),
        "--end-date", end.isoformat(),
        "--device", device,
        "--seed", str(seed),
        "--output-latlon", str(out_dir) + "/",
        "--quiet",
    ]
    t0 = time.time()
    proc = subprocess.run(cmd, cwd=VENDOR, capture_output=True, text=True)
    dt = time.time() - t0
    if proc.returncode != 0:
        print(f"    {start}..{end}: FAILED after {dt:.0f}s (exit {proc.returncode})")
        # Both streams can be empty -- when the WSL VM itself dies mid-run the
        # child is killed without writing anything. Indexing splitlines()[-1]
        # here would then raise IndexError and mask the real failure.
        tail = ((proc.stderr or "") + (proc.stdout or "")).strip().splitlines()
        print("      " + (tail[-1][:200] if tail else "no output from the downscaler"))
        return False
    print(f"    {start}..{end}: {hours}h in {dt / 60:.1f} min")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--city", help="single city (default: all study cities)")
    parser.add_argument(
        "--dry-run", action="store_true", help="show the runs and cost estimate, do nothing"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=10,
        help="generator seed. spateGAN is probabilistic; varying this is how the "
             "uncertainty ensemble is produced (CONTEXT.md §4.4).",
    )
    parser.add_argument(
        "--device",
        default="cuda",
        choices=["cuda", "cpu"],
        help="cuda by default. The vendored inference code falls back to CPU on its "
             "own if CUDA is unavailable, which is silent and ~15x slower, so the "
             "device is verified up front instead.",
    )
    args = parser.parse_args()

    if args.device == "cuda" and not args.dry_run and not cuda_available():
        print(
            "FAIL: --device cuda requested but the vendored environment reports no CUDA.\n"
            "  The downscaler would fall back to CPU silently and take ~15x longer.\n"
            "  Install the CUDA torch build, or pass --device cpu deliberately."
        )
        return 1

    if not VENDOR_PY.exists():
        print(f"FAIL: vendored downscaler not installed at {VENDOR_PY}")
        return 1

    domains_path = config.REPORTS_DIR / "phase4_domains.json"
    if not domains_path.exists():
        print(f"FAIL: {domains_path} missing -- run raincheck.weather.era5_fetch_domain")
        return 1
    domains = json.loads(domains_path.read_text(encoding="utf-8"))

    conf = config.load_cities_conf()
    cities = [args.city] if args.city else list(conf["study"].keys())

    plan, total_hours = {}, 0
    for city in cities:
        if city not in domains:
            print(f"  {city}: no domain -- run era5_fetch_domain first")
            continue
        days = data_days(city)
        runs = contiguous_runs(days)
        hours = sum(((e - s).days + 1) * 24 for s, e in runs)
        plan[city] = runs
        total_hours += hours
        print(
            f"{city}: {len(days)} data days -> {len(runs)} runs, {hours:,}h "
            f"(window spans {(max(days) - min(days)).days + 1} calendar days)"
        )

    # Measured on this machine: 22 s per 8-hour stride window on CPU.
    per_window_s = 22.0 if args.device == "cpu" else 1.5
    est_min = total_hours / 8 * per_window_s / 60
    print(f"\ntotal {total_hours:,} ERA5 hours, ~{est_min:.0f} min estimated on {args.device}")

    if args.dry_run:
        for city, runs in plan.items():
            for s, e in runs:
                print(f"  {city}: {s} .. {e}")
        return 0

    ok = True
    for city, runs in plan.items():
        out_dir = DOWNSCALED / city
        out_dir.mkdir(parents=True, exist_ok=True)
        print(f"\n{city}: {len(runs)} runs -> {out_dir}")
        for s, e in runs:
            if not run_one(city, domains[city], s, e, out_dir, args.seed, args.device):
                ok = False

    print("\nPASS" if ok else "\nFAIL: some runs did not complete")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
