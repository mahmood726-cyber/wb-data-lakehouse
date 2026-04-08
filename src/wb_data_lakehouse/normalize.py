"""World Bank normalization: column validation, type coercion, harmonization."""
from __future__ import annotations

import sys
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

# WB aggregate region codes that are NOT real ISO 3166-1 alpha-3 countries.
# These appear in API responses alongside real country data.
WB_AGGREGATE_CODES = frozenset({
    "AFE", "AFW", "ARB", "CEB", "CSS", "EAP", "EAR", "EAS", "ECA", "ECS",
    "EMU", "FCS", "HIC", "HPC", "IBD", "IBT", "IDA", "IDB", "IDX", "INX",
    "LAC", "LCN", "LDC", "LIC", "LMC", "LMY", "LTE", "MEA", "MIC", "MNA",
    "NAC", "OED", "OSS", "PRE", "PSS", "PST", "SAS", "SSA", "SSF", "SST",
    "TEA", "TEC", "TLA", "TMN", "TSA", "TSS", "UMC", "WLD",
})


def validate_columns(df: pd.DataFrame) -> list[str]:
    """Return list of missing required columns."""
    return sorted(WB_REQUIRED_COLUMNS - set(df.columns))


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce date -> Int64, value -> float64.

    Warns to stderr if any date values are non-numeric (sub-annual like 2020Q1).
    """
    out = df.copy()
    date_numeric = pd.to_numeric(out["date"], errors="coerce")
    bad_dates = date_numeric.isna() & out["date"].notna() & (out["date"] != "")
    if bad_dates.any():
        samples = out.loc[bad_dates, "date"].unique()[:5]
        print(f"WARNING: {bad_dates.sum()} non-numeric dates dropped: {list(samples)}", file=sys.stderr)
    out["date"] = date_numeric.astype("Int64")
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    return out


def add_provenance(df: pd.DataFrame, indicator_code: str, truncated: bool = False) -> pd.DataFrame:
    """Add provenance columns for bronze tier."""
    out = df.copy()
    out["_source_indicator"] = indicator_code
    out["_download_timestamp"] = datetime.now(timezone.utc).isoformat()
    out["_truncated"] = truncated
    return out


def harmonize_wb(df: pd.DataFrame) -> pd.DataFrame:
    """Convert WB-native DataFrame to cross-lakehouse harmonized schema.

    Filters out aggregate regions (WLD, AFE, etc.) and rows with empty iso3c.
    Assumes input has already been through coerce_types().
    indicator_code is prefixed with 'wb_' to avoid collisions with IHME/WHO.
    """
    # Filter: keep only real countries (non-empty iso3c, not an aggregate)
    mask = (
        df["countryiso3code"].notna()
        & (df["countryiso3code"] != "")
        & ~df["countryiso3code"].isin(WB_AGGREGATE_CODES)
    )
    filtered = df.loc[mask]

    return pd.DataFrame({
        "iso3c": filtered["countryiso3code"].values,
        "year": filtered["date"].values,
        "indicator_code": "wb_" + filtered["indicator_id"].astype(str),
        "indicator_name": filtered["indicator_name"].values,
        "value": filtered["value"].values,
        "lower": pd.array([pd.NA] * len(filtered), dtype="Float64"),
        "upper": pd.array([pd.NA] * len(filtered), dtype="Float64"),
        "sex": "",
        "age_group": "",
    })
