"""Tests for promote modules — raw JSON -> bronze CSV -> silver Parquet."""
import shutil
from pathlib import Path

import pandas as pd

from wb_data_lakehouse.promote.wdi import promote_wdi

FIXTURES = Path(__file__).parent / "fixtures"


def _setup_raw(tmp_path: Path, domain: str = "wdi") -> Path:
    """Copy fixture JSON files to raw/{domain}/."""
    raw = tmp_path / "raw" / domain
    raw.mkdir(parents=True)
    for fixture in ("SP.DYN.LE00.IN.json", "SP.POP.TOTL.json"):
        src = FIXTURES / fixture
        if src.exists():
            shutil.copy(src, raw / fixture)
    return raw


def test_promote_creates_bronze_csv(tmp_path):
    _setup_raw(tmp_path)
    promote_wdi(
        tmp_path / "raw" / "wdi",
        tmp_path / "bronze",
        tmp_path / "silver",
    )
    bronze_dir = tmp_path / "bronze" / "wdi"
    csv_files = list(bronze_dir.glob("*.csv"))
    assert len(csv_files) >= 1
    df = pd.read_csv(csv_files[0])
    assert "_source_indicator" in df.columns
    assert "_download_timestamp" in df.columns


def test_promote_creates_native_parquet(tmp_path):
    _setup_raw(tmp_path)
    promote_wdi(
        tmp_path / "raw" / "wdi",
        tmp_path / "bronze",
        tmp_path / "silver",
    )
    native_dir = tmp_path / "silver" / "wdi" / "native"
    pq_files = list(native_dir.glob("*.parquet"))
    assert len(pq_files) >= 1
    df = pd.read_parquet(pq_files[0])
    assert "indicator_id" in df.columns
    assert "value" in df.columns
    assert len(df) > 0


def test_promote_creates_harmonized_parquet(tmp_path):
    _setup_raw(tmp_path)
    promote_wdi(
        tmp_path / "raw" / "wdi",
        tmp_path / "bronze",
        tmp_path / "silver",
    )
    harm_dir = tmp_path / "silver" / "wdi" / "harmonized"
    pq_files = list(harm_dir.glob("*.parquet"))
    assert len(pq_files) >= 1
    df = pd.read_parquet(pq_files[0])
    assert set(df.columns) == {
        "iso3c", "year", "indicator_code", "indicator_name",
        "value", "lower", "upper", "sex", "age_group",
    }
    pak = df[df["iso3c"] == "PAK"]
    assert len(pak) > 0
    assert df["indicator_code"].str.startswith("wb_").all()


def test_promote_rejects_bad_json(tmp_path):
    raw = tmp_path / "raw" / "wdi"
    raw.mkdir(parents=True)
    (raw / "BAD.INDICATOR.json").write_text('[{"foo": 1}]', encoding="utf-8")
    results = promote_wdi(raw, tmp_path / "bronze", tmp_path / "silver")
    rejected = [r for r in results if r.get("status") == "rejected"]
    assert len(rejected) == 1


def test_promote_returns_status_per_indicator(tmp_path):
    _setup_raw(tmp_path)
    results = promote_wdi(
        tmp_path / "raw" / "wdi",
        tmp_path / "bronze",
        tmp_path / "silver",
    )
    promoted = [r for r in results if r.get("status") == "promoted"]
    assert len(promoted) == 2
