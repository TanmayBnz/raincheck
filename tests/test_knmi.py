"""Tests for the KNMI radar acquisition layer.

Filenames verified against the live KNMI Data Platform, 2026-08-19. Two naming
traps are load-bearing and are what these tests pin down:

* Annual adjusted archives are offset five minutes from the year boundary -
  ``20071231T235500_20081231T235500`` covers **2008**, not 2007.
* Daily real-time tars run 08:05 to 08:00 next day, so covering one calendar day
  needs **two** tars.
"""
import datetime as dt

import numpy as np
import pytest

from raincheck.knmi import (
    ADJUSTED_5MIN,
    REALTIME_TAR,
    coverage_of,
    decode_precipitation,
    list_files,
    select_files,
    to_mm_per_hour,
)

ADJUSTED_2008 = "RADNL_CLIM____MFBSNL25_05m_20071231T235500_20081231T235500_0002.zip"
ADJUSTED_2026 = "RADNL_CLIM____MFBSNL25_05m_20251231T235500_20261231T235500_0002.zip"
TAR_17_AUG = "RAD25_OPER_R___TARRRT__L2__20260817T080500_20260818T080000_0001.tar"
TAR_18_AUG = "RAD25_OPER_R___TARRRT__L2__20260818T080500_20260819T080000_0001.tar"


def test_annual_archive_coverage_is_offset_from_the_year_boundary():
    # A filename-year match would assign this file to 2007 and fetch the wrong
    # year for every request at an archive boundary.
    start, end = coverage_of(ADJUSTED_2008)

    assert start == dt.datetime(2007, 12, 31, 23, 55)
    assert end == dt.datetime(2008, 12, 31, 23, 55)


def test_daily_tar_coverage_runs_0805_to_0800_next_day():
    start, end = coverage_of(TAR_17_AUG)

    assert start == dt.datetime(2026, 8, 17, 8, 5)
    assert end == dt.datetime(2026, 8, 18, 8, 0)


def test_covering_one_calendar_day_needs_two_daily_tars():
    # 18 Aug 00:00-24:00 UTC spans the 08:00 tar boundary, so a single tar can
    # never cover a calendar day. Selecting one would silently lose 8 or 16 hours.
    selected = select_files(
        [TAR_17_AUG, TAR_18_AUG],
        dt.datetime(2026, 8, 18), dt.datetime(2026, 8, 19),
    )

    assert selected == [TAR_17_AUG, TAR_18_AUG]


def test_selection_excludes_archives_that_do_not_overlap():
    selected = select_files(
        [ADJUSTED_2008, ADJUSTED_2026],
        dt.datetime(2026, 7, 1), dt.datetime(2026, 8, 1),
    )

    assert selected == [ADJUSTED_2026]


def test_datasets_expose_their_api_coordinates():
    assert (ADJUSTED_5MIN.name, ADJUSTED_5MIN.version) == (
        "rad_nl25_rac_mfbs_5min", "2.0")
    assert (REALTIME_TAR.name, REALTIME_TAR.version) == (
        "nl_rdr_data_rtcor_5m_tar", "1.0")


def test_older_tars_use_a_compact_timestamp_without_the_T_separator():
    # Real filenames change format partway through the archive: tars before
    # ~2019 are 20181219080000_20181220075500 (14 digits, no T) and run
    # 08:00 -> 07:55, while later ones are 20260817T080500 and run 08:05 -> 08:00.
    # A parser accepting only the newer form raises on the older half of the
    # archive, which is most of the usable history.
    start, end = coverage_of(
        "RAD25_OPER_R___TARRRT__L2__20181219080000_20181220075500_0001.tar")

    assert start == dt.datetime(2018, 12, 19, 8, 0)
    assert end == dt.datetime(2018, 12, 20, 7, 55)


def test_listing_follows_pagination_until_the_archive_is_exhausted():
    # maxKeys is capped at 1000 server-side, so the daily-tar archive (2018 to
    # present, ~2800 files) is truncated on the first page. Ignoring
    # nextPageToken silently yields only the oldest 1000 files - which reads as
    # "no radar exists for 2026" rather than as an error.
    pages = [
        {"files": [{"filename": "a"}, {"filename": "b"}],
         "isTruncated": True, "nextPageToken": "TOKEN1"},
        {"files": [{"filename": "c"}], "isTruncated": False},
    ]
    seen_urls = []

    def fetch(url):
        seen_urls.append(url)
        return pages[len(seen_urls) - 1]

    names = list_files(REALTIME_TAR, fetch)

    assert names == ["a", "b", "c"]
    assert len(seen_urls) == 2
    assert "nextPageToken=TOKEN1" in seen_urls[1]


def test_sentinels_become_nan_rather_than_655_millimetres():
    # KNMI encodes missing as 65534 and out-of-radar-domain as 65535 in a uint16
    # field whose calibration is GEO = 0.01 * PV. Applied naively that is 655.34
    # and 655.35 mm of rain in five minutes, and 65535 covers 65,263 of the
    # 535,500 pixels in a real file - the domain corners. This is the same class
    # of bug as NDW's -1 speed sentinel.
    raw = np.array([[0, 100, 65534], [250, 65535, 1]], dtype=np.uint16)

    mm = decode_precipitation(raw)

    assert mm[0, 0] == 0.0
    assert mm[0, 1] == pytest.approx(1.0)       # 100 * 0.01
    assert mm[1, 0] == pytest.approx(2.5)       # 250 * 0.01
    assert np.isnan(mm[0, 2]) and np.isnan(mm[1, 1])
    assert np.nanmax(mm) < 10.0


def test_five_minute_accumulation_converts_to_an_intensity_rate():
    # Files carry PRECIP_[MM] accumulated over five minutes, not mm/h. The rain
    # features and BAND_EDGES are all defined in mm/h, so 2.5 mm in 5 minutes is
    # 30 mm/h - a factor of 12 that would otherwise silently flatten every band.
    mm = np.array([[2.5, np.nan]])

    rate = to_mm_per_hour(mm, minutes=5)

    assert rate[0, 0] == pytest.approx(30.0)
    assert np.isnan(rate[0, 1])
