"""Promote raw Wealth Accounts JSON to bronze + silver."""
from __future__ import annotations
from pathlib import Path
from wb_data_lakehouse.promote.wdi import _promote_domain

def promote_wealth(raw_dir: Path, bronze_dir: Path, silver_dir: Path, skip_existing: bool = False) -> list[dict]:
    return _promote_domain("wealth", raw_dir, bronze_dir, silver_dir, skip_existing)
