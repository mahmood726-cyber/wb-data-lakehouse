"""Tests for storage I/O utilities."""
import json

import pandas as pd

from wb_data_lakehouse.storage import (
    read_json,
    utc_stamp,
    write_dataframe,
    write_json,
    write_manifest,
)


def test_utc_stamp_format():
    stamp = utc_stamp()
    assert len(stamp) == 16
    assert stamp.endswith("Z")
    assert "T" in stamp


def test_write_and_read_json(tmp_path):
    data = {"key": "value", "count": 42}
    path = tmp_path / "test.json"
    write_json(data, path)
    loaded = read_json(path)
    assert loaded == data


def test_write_dataframe_csv(tmp_path):
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    path = tmp_path / "test.csv"
    write_dataframe(df, path)
    assert path.exists()
    loaded = pd.read_csv(path)
    assert len(loaded) == 2


def test_write_dataframe_parquet(tmp_path):
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    path = tmp_path / "test.parquet"
    write_dataframe(df, path)
    assert path.exists()
    loaded = pd.read_parquet(path)
    assert len(loaded) == 2


def test_write_manifest(tmp_path):
    summary = {"domain": "wdi", "status": "fetched"}
    result = write_manifest("fetch_wdi", summary, tmp_path)
    assert "manifest_path" in result
    loaded = read_json(tmp_path / result["manifest_path"].split("/")[-1].split("\\")[-1])
    assert loaded["domain"] == "wdi"
