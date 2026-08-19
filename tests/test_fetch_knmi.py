"""Tests for radar frame selection."""
import datetime as dt

from raincheck.jobs.fetch_knmi import frame_stamps


def test_frames_are_labelled_by_the_end_of_their_accumulation_window():
    # A file stamped 09:25 covers 09:20-09:25, so the frames for the window
    # 08:50-09:00 are 08:55 and 09:00 - not 08:50 and 08:55. Getting this
    # backwards shifts every rainfall value one bin, which silently decorrelates
    # rain from traffic and would read as "rain has no effect".
    stamps = frame_stamps(dt.datetime(2026, 8, 19, 8, 50),
                          dt.datetime(2026, 8, 19, 9, 0))

    assert stamps == [dt.datetime(2026, 8, 19, 8, 55),
                      dt.datetime(2026, 8, 19, 9, 0)]
