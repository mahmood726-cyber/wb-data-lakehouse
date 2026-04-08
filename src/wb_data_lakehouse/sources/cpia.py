"""Country Policy and Institutional Assessment fetcher."""
from __future__ import annotations
from pathlib import Path
from wb_data_lakehouse.sources import fetch_domain

def fetch_cpia(raw_dir: Path | None = None, skip_existing: bool = False) -> dict:
    return fetch_domain("cpia", raw_dir=raw_dir, skip_existing=skip_existing)
