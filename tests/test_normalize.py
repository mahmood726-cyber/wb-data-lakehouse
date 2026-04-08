"""Tests for World Bank normalization module."""
from pathlib import Path

import pandas as pd

from wb_data_lakehouse.normalize import (
    WB_REQUIRED_COLUMNS,
    add_provenance,
    coerce_types,
    harmonize_wb,
    validate_columns,
)

FIXTURES = Path(__file__).parent / "fixtures"


def test_validate_columns_pass():
    df = pd.read_csv(FIXTURES / "wdi_sample.csv")
    missing = validate_columns(df)
    assert missing == [], f"Missing columns: {missing}"


def test_validate_columns_fail():
    df = pd.DataFrame({"foo": [1], "bar": [2]})
    missing = validate_columns(df)
    assert len(missing) > 0


def test_coerce_types_date_is_int():
    df = pd.read_csv(FIXTURES / "wdi_sample.csv")
    coerced = coerce_types(df)
    assert coerced["date"].dtype.name == "Int64"


def test_coerce_types_value_is_float():
    df = pd.read_csv(FIXTURES / "wdi_sample.csv")
    coerced = coerce_types(df)
    assert coerced["value"].dtype == "float64"


def test_add_provenance():
    df = pd.read_csv(FIXTURES / "wdi_sample.csv")
    with_prov = add_provenance(df, "SP.DYN.LE00.IN")
    assert "_source_indicator" in with_prov.columns
    assert "_download_timestamp" in with_prov.columns
    assert with_prov["_source_indicator"].iloc[0] == "SP.DYN.LE00.IN"


def test_harmonize_wb():
    df = pd.read_csv(FIXTURES / "wdi_sample.csv")
    harmonized = harmonize_wb(df)
    assert set(harmonized.columns) == {
        "iso3c", "year", "indicator_code", "indicator_name",
        "value", "lower", "upper", "sex", "age_group",
    }
    assert harmonized["iso3c"].iloc[0] == "PAK"
    assert harmonized["indicator_code"].iloc[0].startswith("wb_")
    assert harmonized["lower"].isna().all()
    assert harmonized["upper"].isna().all()


def test_harmonize_wb_indicator_code_prefix():
    df = pd.read_csv(FIXTURES / "wdi_sample.csv")
    harmonized = harmonize_wb(df)
    for code in harmonized["indicator_code"]:
        assert code.startswith("wb_"), f"Missing wb_ prefix: {code}"
