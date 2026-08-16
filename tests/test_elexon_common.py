"""
Test the settlement period math shared by both Elexon extractors.

This is the one rule the task was explicit about getting right: normal
days have 48 settlement periods, the spring clock-change day has 46, and
the autumn clock-change day has 50. The count must never be hardcoded.
"""
import datetime

from lakehouse.extractors.elexon_common import expected_settlement_period_count


def test_expected_settlement_period_count_normal_day():
    # An ordinary day, no clock change nearby.
    assert expected_settlement_period_count(datetime.date(2026, 8, 8)) == 48


def test_expected_settlement_period_count_spring_clock_change():
    # Clocks go forward, the day loses an hour, 46 periods.
    assert expected_settlement_period_count(datetime.date(2026, 3, 29)) == 46


def test_expected_settlement_period_count_autumn_clock_change():
    # Clocks go back, the day gains an hour, 50 periods.
    assert expected_settlement_period_count(datetime.date(2025, 10, 26)) == 50


def test_expected_settlement_period_count_new_year():
    # A day picked at random with no clock change, should still be 48.
    assert expected_settlement_period_count(datetime.date(2026, 1, 1)) == 48
