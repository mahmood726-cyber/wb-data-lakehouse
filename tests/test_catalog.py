"""Tests for catalog builder and search."""
import shutil
from pathlib import Path

import pandas as pd

from wb_data_lakehouse.catalog import build_catalog, search_catalog
from wb_data_lakehouse.promote.wdi import promote_wdi

FIXTURES = Path(__file__).parent / "fixtures"


def _build_silver(tmp_path):
    """Promote fixture data to silver and return silver_dir."""
    raw = tmp_path / "raw" / "wdi"
    raw.mkdir(parents=True)
    for f in ("SP.DYN.LE00.IN.json", "SP.POP.TOTL.json"):
        shutil.copy(FIXTURES / f, raw / f)
    promote_wdi(raw, tmp_path / "bronze", tmp_path / "silver")
    return tmp_path / "silver"


def test_build_catalog(tmp_path):
    silver = _build_silver(tmp_path)
    catalog = build_catalog(silver)
    assert len(catalog) > 0
    assert "domain" in catalog.columns
    assert "tier" in catalog.columns
    assert catalog["domain"].iloc[0] == "wdi"


def test_catalog_save_to_parquet(tmp_path):
    silver = _build_silver(tmp_path)
    out = tmp_path / "catalog.parquet"
    catalog = build_catalog(silver, output_path=out)
    assert out.exists()
    loaded = pd.read_parquet(out)
    assert len(loaded) == len(catalog)


def test_search_by_keyword(tmp_path):
    silver = _build_silver(tmp_path)
    catalog = build_catalog(silver)
    hits = search_catalog(catalog, keyword="SP.DYN")
    assert len(hits) > 0


def test_search_by_domain(tmp_path):
    silver = _build_silver(tmp_path)
    catalog = build_catalog(silver)
    hits = search_catalog(catalog, domain="wdi")
    assert len(hits) == len(catalog)


def test_search_by_tier(tmp_path):
    silver = _build_silver(tmp_path)
    catalog = build_catalog(silver)
    native = search_catalog(catalog, tier="native")
    harmonized = search_catalog(catalog, tier="harmonized")
    assert len(native) > 0
    assert len(harmonized) > 0
    assert len(native) + len(harmonized) == len(catalog)
