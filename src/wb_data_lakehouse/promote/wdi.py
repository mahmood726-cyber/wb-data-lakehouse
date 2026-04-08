"""Promote raw WDI JSON to bronze CSV + silver Parquet (native + harmonized)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from wb_data_lakehouse.normalize import (
    WB_REQUIRED_COLUMNS,
    add_provenance,
    coerce_types,
    harmonize_wb,
    validate_columns,
)
from wb_data_lakehouse.storage import read_json, write_dataframe


def promote_wdi(
    raw_dir: Path,
    bronze_dir: Path,
    silver_dir: Path,
    skip_existing: bool = False,
) -> list[dict]:
    return _promote_domain("wdi", raw_dir, bronze_dir, silver_dir, skip_existing)


def _promote_domain(
    domain: str,
    raw_dir: Path,
    bronze_dir: Path,
    silver_dir: Path,
    skip_existing: bool = False,
) -> list[dict]:
    """Promote all raw JSON files for a domain to bronze + silver."""
    native_dir = silver_dir / domain / "native"
    harmonized_dir = silver_dir / domain / "harmonized"
    bronze_out = bronze_dir / domain
    native_dir.mkdir(parents=True, exist_ok=True)
    harmonized_dir.mkdir(parents=True, exist_ok=True)
    bronze_out.mkdir(parents=True, exist_ok=True)

    json_files = sorted(raw_dir.glob("*.json"))
    if not json_files:
        return [{"domain": domain, "status": "no_json_found"}]

    results: list[dict] = []

    for json_path in json_files:
        indicator_code = json_path.stem

        if skip_existing and (native_dir / f"{indicator_code}.parquet").exists():
            results.append({"indicator": indicator_code, "status": "skipped"})
            continue

        try:
            records = read_json(json_path)
        except Exception as exc:
            results.append({"indicator": indicator_code, "status": "error", "reason": str(exc)})
            continue

        if not records:
            results.append({"indicator": indicator_code, "status": "empty"})
            continue

        df = pd.DataFrame(records)
        missing = validate_columns(df)
        if missing:
            results.append({"indicator": indicator_code, "status": "rejected", "missing_columns": missing})
            continue

        df = coerce_types(df)

        # Bronze: add provenance and write CSV
        bronze_df = add_provenance(df, indicator_code)
        write_dataframe(bronze_df, bronze_out / f"{indicator_code}.csv")

        # Silver native: write per-indicator Parquet
        write_dataframe(df, native_dir / f"{indicator_code}.parquet")

        # Silver harmonized
        harmonized = harmonize_wb(df)
        write_dataframe(harmonized, harmonized_dir / f"{indicator_code}.parquet")

        results.append({
            "indicator": indicator_code,
            "status": "promoted",
            "rows": len(df),
        })

    return results
