from HSTB.kluster.utc_helpers import *


def test_julian_day_time_to_utctimestamp():
    tstmp = julian_day_time_to_utctimestamp(2021, 100, 14, 44, 21)
    assert tstmp == 1618065861.0


def test_calendar_day_time_to_utctimestamp():
    tstmp = calendar_day_time_to_utctimestamp(2021, 4, 10, 14, 44, 21)
    assert tstmp == 1618065861.0