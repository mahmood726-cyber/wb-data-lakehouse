"""Tests for the fetch engine (mocked API calls)."""
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from wb_data_lakehouse.sources import fetch_domain

FIXTURES = Path(__file__).parent / "fixtures"


def _make_mock_session():
    """Build a mock session that returns fixture data for any indicator."""
    data = json.loads((FIXTURES / "wb_api_indicator_response.json").read_text(encoding="utf-8"))
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = data
    resp.raise_for_status.return_value = None
    session = MagicMock()
    session.get.return_value = resp
    return session


def test_fetch_domain_creates_raw_json(tmp_path):
    raw_dir = tmp_path / "raw"
    manifest_dir = tmp_path / "manifests"
    raw_dir.mkdir()
    manifest_dir.mkdir()

    with patch("wb_data_lakehouse.sources.build_session", return_value=_make_mock_session()):
        with patch("wb_data_lakehouse.sources.load_registry") as mock_reg:
            from wb_data_lakehouse.registry import RegistryDomain
            mock_reg.return_value = {
                "wdi": RegistryDomain(
                    name="wdi", description="test", source_id=2,
                    indicators=["SP.DYN.LE00.IN"],
                )
            }
            result = fetch_domain("wdi", raw_dir=raw_dir, manifest_dir=manifest_dir)

    assert result["status"] == "fetched"
    assert result["fetched_count"] == 1
    raw_file = raw_dir / "wdi" / "SP.DYN.LE00.IN.json"
    assert raw_file.exists()


def test_fetch_domain_skip_existing(tmp_path):
    raw_dir = tmp_path / "raw"
    manifest_dir = tmp_path / "manifests"
    raw_dir.mkdir()
    manifest_dir.mkdir()
    indicator_dir = raw_dir / "wdi"
    indicator_dir.mkdir(parents=True)
    (indicator_dir / "SP.DYN.LE00.IN.json").write_text("[]", encoding="utf-8")

    with patch("wb_data_lakehouse.sources.build_session", return_value=_make_mock_session()):
        with patch("wb_data_lakehouse.sources.load_registry") as mock_reg:
            from wb_data_lakehouse.registry import RegistryDomain
            mock_reg.return_value = {
                "wdi": RegistryDomain(
                    name="wdi", description="test", source_id=2,
                    indicators=["SP.DYN.LE00.IN"],
                )
            }
            result = fetch_domain("wdi", raw_dir=raw_dir, manifest_dir=manifest_dir, skip_existing=True)

    assert result["cached_count"] == 1
    assert result["fetched_count"] == 0


def test_fetch_domain_unknown_returns_error(tmp_path):
    raw_dir = tmp_path / "raw"
    manifest_dir = tmp_path / "manifests"
    raw_dir.mkdir()
    manifest_dir.mkdir()

    with patch("wb_data_lakehouse.sources.load_registry", return_value={}):
        result = fetch_domain("nonexistent", raw_dir=raw_dir, manifest_dir=manifest_dir)

    assert result["status"] == "error"
