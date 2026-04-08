"""Tests for World Bank API client (uses mocked responses)."""
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from wb_data_lakehouse.api import (
    build_session,
    fetch_indicator,
    fetch_indicator_metadata,
    flatten_observations,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _mock_response(fixture_name: str) -> MagicMock:
    data = json.loads((FIXTURES / fixture_name).read_text(encoding="utf-8"))
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = data
    resp.raise_for_status.return_value = None
    return resp


def test_build_session():
    session = build_session()
    assert "User-Agent" in session.headers


def test_fetch_indicator_returns_records_and_completeness():
    mock_resp = _mock_response("wb_api_indicator_response.json")
    session = MagicMock()
    session.get.return_value = mock_resp
    records, is_complete = fetch_indicator("SP.DYN.LE00.IN", session=session)
    assert len(records) == 5
    assert records[0]["countryiso3code"] == "PAK"
    assert is_complete is True


def test_fetch_indicator_metadata():
    mock_resp = _mock_response("wb_api_metadata_response.json")
    session = MagicMock()
    session.get.return_value = mock_resp
    meta = fetch_indicator_metadata("SP.DYN.LE00.IN", session=session)
    assert meta["id"] == "SP.DYN.LE00.IN"
    assert "Life expectancy" in meta["name"]


def test_flatten_observations():
    data = json.loads((FIXTURES / "wb_api_indicator_response.json").read_text(encoding="utf-8"))
    records = data[1]
    flat = flatten_observations(records)
    assert len(flat) == 5
    assert flat[0]["indicator_id"] == "SP.DYN.LE00.IN"
    assert flat[0]["indicator_name"] == "Life expectancy at birth, total (years)"
    assert flat[0]["country_id"] == "PK"
    assert flat[0]["country_name"] == "Pakistan"
    assert flat[0]["countryiso3code"] == "PAK"
    assert flat[0]["date"] == "2022"
    assert flat[0]["value"] == 66.094


def test_flatten_observations_skips_null_value():
    records = [
        {
            "indicator": {"id": "X", "value": "Test"},
            "country": {"id": "XX", "value": "Nowhere"},
            "countryiso3code": "XXX",
            "date": "2020",
            "value": None,
            "unit": "",
            "obs_status": "",
            "decimal": 0,
        }
    ]
    flat = flatten_observations(records)
    assert len(flat) == 0
