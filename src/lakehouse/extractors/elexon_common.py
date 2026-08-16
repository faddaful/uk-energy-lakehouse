"""
Shared helpers for the Elexon Insights extractors.

System prices and generation by fuel type are two different endpoints with
two different response shapes, so they stay as two separate extractor
modules, one per data source, the same as carbon_intensity.py. But both hit
the same API base, both need the same settlement period math, and both need
to be a good citizen of the same server. Those three things are genuinely
shared, so they live here instead of being copied into both files, where
the settlement period rule in particular could quietly drift out of sync
between the two copies.
"""
import datetime
import logging
import time
from typing import cast
from zoneinfo import ZoneInfo

import pandas as pd
import requests

# Use own logger to identify logging messages by module name.
logger = logging.getLogger(__name__)

BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1"

# Identify this as a personal, non-commercial project so Elexon can see who
# is calling if they ever need to. No API key is required for this API.
USER_AGENT = "uk-energy-lakehouse/0.1 (personal learning project; contact: olayanjubiodun24@gmail.com)"

# A small fixed pause before every request, on top of the retry backoff
# below. This is a free public API with no published rate limit, so this
# is a courtesy delay rather than a response to any documented limit.
REQUEST_DELAY_SECONDS = 0.5

MAX_ATTEMPTS = 4
BACKOFF_BASE_SECONDS = 2
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def expected_settlement_period_count(date: datetime.date) -> int:
    """
    The number of settlement periods a UK settlement date has.

    A settlement period is 30 minutes of clock time in the Europe/London
    zone, not UTC. Most days have 48. The day the clocks go forward
    (spring) loses an hour and has 46. The day the clocks go back (autumn)
    gains an hour and has 50.

    This is computed from the timezone database rather than a hardcoded
    list of clock-change dates, so it does not need maintaining every year
    and does not depend on me remembering which Sunday it is.

    Args:
        date (datetime.date): The settlement date to check.
    """
    tz = ZoneInfo("Europe/London")
    start_local = datetime.datetime(date.year, date.month, date.day, tzinfo=tz)
    next_date = date + datetime.timedelta(days=1)
    end_local = datetime.datetime(next_date.year, next_date.month, next_date.day, tzinfo=tz)

    start_utc = start_local.astimezone(datetime.UTC)
    end_utc = end_local.astimezone(datetime.UTC)
    duration_hours = (end_utc - start_utc).total_seconds() / 3600

    return round(duration_hours * 2)


def parse_api_timestamp(value: str | None) -> pd.Timestamp:
    """
    Parse an Elexon timestamp string into a timezone-aware UTC Timestamp.

    This is deliberate per-field parsing rather than a blind pd.to_datetime()
    call across a whole DataFrame. The API returns Zulu-suffixed ISO 8601
    strings (for example "2026-08-07T23:00:00Z"), and my own loaded_at value
    is generated separately with a different format again. If both ended up
    stored as raw strings, a later blind cast could misparse one of them, or
    silently mix formats in the same parquet column. Parsing explicitly here
    and always normalising to a UTC-aware Timestamp means every timestamp
    column in bronze has one consistent type regardless of what string shape
    it started as.

    Args:
        value (str | None): The raw timestamp string from the API response.
    """
    if value is None:
        # pandas-stubs types pd.NaT as NaTType, not Timestamp, even though
        # it's the null value every other Timestamp in this column shares.
        return cast(pd.Timestamp, pd.NaT)
    return pd.to_datetime(value, utc=True, format="ISO8601")


def get_with_retry(path: str, params: dict | None = None) -> dict:
    """
    GET an Elexon Insights endpoint, with rate limiting and retry.

    Sleeps briefly before every request. Retries a small number of times
    with exponential backoff on 429 (rate limited) or 5xx (server error)
    responses. Any other failure, or exhausting the retries, raises rather
    than returning empty or partial data silently, so a bad fetch fails
    loudly instead of landing a bronze file that looks fine but is not.

    Args:
        path (str): The endpoint path, relative to BASE_URL, for example
            "/balancing/settlement/system-prices/2026-08-08".
        params (dict | None): Query string parameters, if any.
    """
    url = f"{BASE_URL}{path}"
    headers = {"User-Agent": USER_AGENT}

    for attempt in range(1, MAX_ATTEMPTS + 1):
        time.sleep(REQUEST_DELAY_SECONDS)
        response = requests.get(url, params=params, headers=headers, timeout=30)

        if response.status_code == 200:
            return response.json()

        if response.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_ATTEMPTS:
            backoff = BACKOFF_BASE_SECONDS ** attempt
            logger.warning(
                f"Request to {url} failed with {response.status_code}, "
                f"retrying in {backoff}s (attempt {attempt}/{MAX_ATTEMPTS})"
            )
            time.sleep(backoff)
            continue

        logger.error(f"Request to {url} failed: {response.status_code} - {response.text}")
        response.raise_for_status()

    # Unreachable in practice: the loop above either returns or raises.
    raise RuntimeError(f"Exhausted retries fetching {url}")
