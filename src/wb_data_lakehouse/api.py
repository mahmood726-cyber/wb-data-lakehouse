"""World Bank API v2 client with pagination and retry logic."""
from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from wb_data_lakehouse.config import BASE_URL, DEFAULT_PER_PAGE, DEFAULT_TIMEOUT_SECONDS, USER_AGENT


def build_session(user_agent: str = USER_AGENT) -> requests.Session:
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_indicator(
    indicator_code: str,
    per_page: int = DEFAULT_PER_PAGE,
    session: requests.Session | None = None,
) -> tuple[list[dict], bool]:
    """Fetch all country-year observations for one indicator. Handles pagination.

    Returns (records, is_complete). is_complete is False if pagination was
    truncated due to errors on later pages.
    """
    if session is None:
        session = build_session()

    url = f"{BASE_URL}/country/all/indicator/{indicator_code}"
    params = {"format": "json", "per_page": per_page, "page": 1}

    all_records: list[dict] = []
    is_complete = True
    while True:
        try:
            resp = session.get(url, params=params, timeout=DEFAULT_TIMEOUT_SECONDS)
            if resp.status_code >= 400:
                if all_records:
                    is_complete = False
                    break
                resp.raise_for_status()
            data = resp.json()
        except requests.RequestException:
            if all_records:
                is_complete = False
                break
            raise

        if not isinstance(data, list) or len(data) < 2 or data[1] is None:
            break

        header = data[0]
        records = data[1]
        all_records.extend(records)

        if header["page"] >= header["pages"]:
            break
        params["page"] += 1

    return all_records, is_complete


def fetch_indicator_metadata(
    indicator_code: str,
    session: requests.Session | None = None,
) -> dict:
    """Fetch metadata (name, source, topic) for one indicator."""
    if session is None:
        session = build_session()

    url = f"{BASE_URL}/indicator/{indicator_code}"
    params = {"format": "json"}
    resp = session.get(url, params=params, timeout=DEFAULT_TIMEOUT_SECONDS)
    resp.raise_for_status()
    data = resp.json()

    if isinstance(data, list) and len(data) >= 2 and data[1]:
        return data[1][0]
    return {}


def flatten_observations(records: list[dict]) -> list[dict]:
    """Flatten nested WB API response records to flat dicts.

    Skips records where value is None (no data).
    """
    flat: list[dict] = []
    for rec in records:
        if rec.get("value") is None:
            continue
        flat.append({
            "indicator_id": rec["indicator"]["id"],
            "indicator_name": rec["indicator"]["value"],
            "country_id": rec["country"]["id"],
            "country_name": rec["country"]["value"],
            "countryiso3code": rec["countryiso3code"],
            "date": rec["date"],
            "value": rec["value"],
            "unit": rec.get("unit", ""),
            "obs_status": rec.get("obs_status", ""),
            "decimal": rec.get("decimal", 0),
        })
    return flat
