"""World Bank normalization: column validation, type coercion, harmonization."""
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

WB_REQUIRED_COLUMNS = {
    "indicator_id",
    "indicator_name",
    "country_id",
    "country_name",
    "countryiso3code",
    "date",
    "value",
}


def validate_columns(df: pd.DataFrame) -> list[str]:
    """Return list of missing required columns."""
    return sorted(WB_REQUIRED_COLUMNS - set(df.columns))


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce date -> Int64, value -> float64."""
    out = df.copy()
    out["date"] = pd.to_numeric(out["date"], errors="coerce").astype("Int64")
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    return out


def add_provenance(df: pd.DataFrame, indicator_code: str) -> pd.DataFrame:
    """Add provenance columns for bronze tier."""
    out = df.copy()
    out["_source_indicator"] = indicator_code
    out["_download_timestamp"] = datetime.now(timezone.utc).isoformat()
    return out


def harmonize_wb(df: pd.DataFrame) -> pd.DataFrame:
    """Convert WB-native DataFrame to cross-lakehouse harmonized schema.

    WB already provides countryiso3code — no crosswalk needed.
    indicator_code is prefixed with 'wb_' to avoid collisions with IHME/WHO.
    lower/upper are null (WB indicators have no confidence intervals).
    sex/age_group are empty strings (WB does not disaggregate most indicators).
    """
    return pd.DataFrame({
        "iso3c": df["countryiso3code"],
        "year": pd.to_numeric(df["date"], errors="coerce").astype("Int64"),
        "indicator_code": "wb_" + df["indicator_id"].astype(str),
        "indicator_name": df["indicator_name"],
        "value": pd.to_numeric(df["value"], errors="coerce"),
        "lower": pd.array([pd.NA] * len(df), dtype="Float64"),
        "upper": pd.array([pd.NA] * len(df), dtype="Float64"),
        "sex": "",
        "age_group": "",
    })
