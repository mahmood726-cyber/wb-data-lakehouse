# World Bank Data Lakehouse Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an API-driven Python pipeline that fetches curated World Bank indicators, normalizes them through a bronze/silver medallion architecture, and produces harmonized Parquet files compatible with the IHME and WHO lakehouses.

**Architecture:** Registry-driven (YAML) with 6 domains (wdi, hnp, poverty, governance, education, gender). Each domain lists curated indicator codes. The API client fetches paginated JSON from `api.worldbank.org/v2/`, promotes through raw→bronze→silver with dual silver schema (native + harmonized). CLI entry point: `wb-data`.

**Tech Stack:** Python 3.11+, pandas, pyarrow, requests, pyyaml, pytest

**Spec:** `docs/superpowers/specs/2026-04-08-wb-data-lakehouse-design.md`

---

## File Structure

| File | Responsibility |
|------|---------------|
| `src/wb_data_lakehouse/__init__.py` | Package marker + docstring |
| `src/wb_data_lakehouse/config.py` | Paths, DOMAINS tuple, constants, `ensure_project_directories()` |
| `src/wb_data_lakehouse/storage.py` | I/O: JSON, CSV, Parquet, manifests, `utc_stamp()` |
| `src/wb_data_lakehouse/api.py` | World Bank API v2 client: paginated fetch, metadata, retries |
| `src/wb_data_lakehouse/registry.py` | YAML registry loader/validator for indicator codes per domain |
| `src/wb_data_lakehouse/normalize.py` | Column validation, type coercion, provenance, `harmonize_wb()` |
| `src/wb_data_lakehouse/catalog.py` | `build_catalog()`, `search_catalog()` over silver Parquet |
| `src/wb_data_lakehouse/cli.py` | CLI: fetch, promote, catalog, search, registry-list, status |
| `src/wb_data_lakehouse/sources/__init__.py` | `fetch_domain()` shared engine using API client |
| `src/wb_data_lakehouse/sources/{domain}.py` | Thin wrappers: `fetch_wdi()`, `fetch_hnp()`, etc. |
| `src/wb_data_lakehouse/promote/__init__.py` | Package marker |
| `src/wb_data_lakehouse/promote/{domain}.py` | Per-domain raw→bronze→silver promotion |
| `registry/sources.yaml` | Curated indicator codes per domain |
| `pyproject.toml` | Build config, dependencies, `wb-data` entry point |
| `tests/conftest.py` | Shared fixtures: `tmp_data`, `fixtures_dir` |
| `tests/fixtures/*.json` | Sample API responses |
| `tests/fixtures/*.csv` | Sample bronze CSVs |
| `tests/test_*.py` | Unit tests per module |

---

### Task 1: Project Scaffold + pyproject.toml

**Files:**
- Create: `pyproject.toml`
- Create: `src/wb_data_lakehouse/__init__.py`
- Create: `src/wb_data_lakehouse/config.py`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`

- [ ] **Step 1: Create pyproject.toml**

```toml
[build-system]
requires = ["setuptools>=69", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "wb-data-lakehouse"
version = "0.1.0"
description = "API-driven pipeline for World Bank open data."
readme = "README.md"
requires-python = ">=3.11"
dependencies = [
  "pandas>=2.2",
  "pyarrow>=15.0",
  "requests>=2.31",
  "pyyaml>=6.0",
]

[project.optional-dependencies]
dev = ["pytest>=8.0"]

[project.scripts]
wb-data = "wb_data_lakehouse.cli:main"

[tool.setuptools.packages.find]
where = ["src"]
```

- [ ] **Step 2: Create package init**

```python
# src/wb_data_lakehouse/__init__.py
"""World Bank Data Lakehouse — API-driven World Bank data pipeline."""
```

- [ ] **Step 3: Create config.py**

```python
# src/wb_data_lakehouse/config.py
"""Project paths and constants."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
REFERENCE_DIR = DATA_DIR / "reference"
MANIFEST_DIR = ROOT / "manifests"
REGISTRY_DIR = ROOT / "registry"

BASE_URL = "https://api.worldbank.org/v2"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_PER_PAGE = 1000
USER_AGENT = "wb-data-lakehouse/0.1"

DOMAINS = (
    "wdi",
    "hnp",
    "poverty",
    "governance",
    "education",
    "gender",
)


def ensure_project_directories() -> None:
    for domain in DOMAINS:
        (RAW_DIR / domain).mkdir(parents=True, exist_ok=True)
        (BRONZE_DIR / domain).mkdir(parents=True, exist_ok=True)
        (SILVER_DIR / domain / "native").mkdir(parents=True, exist_ok=True)
        (SILVER_DIR / domain / "harmonized").mkdir(parents=True, exist_ok=True)
    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
```

- [ ] **Step 4: Create tests/conftest.py**

```python
# tests/conftest.py
"""Shared pytest fixtures."""
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def tmp_data(tmp_path):
    """Create a temporary data directory mirroring the project layout."""
    for sub in ("raw", "bronze", "silver", "reference", "manifests"):
        (tmp_path / sub).mkdir()
    return tmp_path


@pytest.fixture
def fixtures_dir():
    return FIXTURES_DIR
```

Create empty `tests/__init__.py`.

- [ ] **Step 5: Create fixtures directory**

```bash
mkdir -p tests/fixtures
```

- [ ] **Step 6: Install in dev mode and verify**

```bash
cd C:\Projects\wb-data-lakehouse
pip install -e ".[dev]"
python -c "import wb_data_lakehouse; print('OK')"
```

Expected: `OK`

- [ ] **Step 7: Commit**

```bash
git init
git add pyproject.toml src/ tests/
git commit -m "feat: project scaffold with config, pyproject.toml, test fixtures"
```

---

### Task 2: Storage Module

**Files:**
- Create: `src/wb_data_lakehouse/storage.py`
- Create: `tests/test_storage.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_storage.py
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
    assert len(stamp) == 16  # "20260408T120000Z"
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
    loaded = read_json(tmp_path / result["manifest_path"].split("/")[-1])
    assert loaded["domain"] == "wdi"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd C:\Projects\wb-data-lakehouse
python -m pytest tests/test_storage.py -v
```

Expected: FAIL (ImportError)

- [ ] **Step 3: Implement storage.py**

```python
# src/wb_data_lakehouse/storage.py
"""I/O utilities: JSON, CSV, Parquet, manifest writing."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(payload: object, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return destination


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def write_dataframe(frame: pd.DataFrame, destination: Path, index: bool = False) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.suffix == ".parquet":
        frame.to_parquet(destination, index=index)
    elif destination.suffix == ".csv":
        frame.to_csv(destination, index=index)
    else:
        raise ValueError(f"Unsupported format: {destination.suffix}")
    return destination


def write_manifest(prefix: str, summary: dict, manifest_dir: Path) -> dict:
    stamp = utc_stamp()
    path = manifest_dir / f"{prefix}_{stamp}.json"
    write_json(summary, path)
    summary["manifest_path"] = str(path)
    return summary
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_storage.py -v
```

Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add src/wb_data_lakehouse/storage.py tests/test_storage.py
git commit -m "feat: storage module — JSON, CSV, Parquet, manifests"
```

---

### Task 3: API Client

**Files:**
- Create: `src/wb_data_lakehouse/api.py`
- Create: `tests/test_api.py`
- Create: `tests/fixtures/wb_api_indicator_response.json`
- Create: `tests/fixtures/wb_api_metadata_response.json`

- [ ] **Step 1: Create test fixtures — API response samples**

```json
// tests/fixtures/wb_api_indicator_response.json
[
  {
    "page": 1,
    "pages": 1,
    "per_page": 1000,
    "total": 5,
    "sourceid": "2",
    "lastupdated": "2024-12-19"
  },
  [
    {
      "indicator": {"id": "SP.DYN.LE00.IN", "value": "Life expectancy at birth, total (years)"},
      "country": {"id": "PK", "value": "Pakistan"},
      "countryiso3code": "PAK",
      "date": "2022",
      "value": 66.094,
      "unit": "",
      "obs_status": "",
      "decimal": 0
    },
    {
      "indicator": {"id": "SP.DYN.LE00.IN", "value": "Life expectancy at birth, total (years)"},
      "country": {"id": "PK", "value": "Pakistan"},
      "countryiso3code": "PAK",
      "date": "2021",
      "value": 65.867,
      "unit": "",
      "obs_status": "",
      "decimal": 0
    },
    {
      "indicator": {"id": "SP.DYN.LE00.IN", "value": "Life expectancy at birth, total (years)"},
      "country": {"id": "GB", "value": "United Kingdom"},
      "countryiso3code": "GBR",
      "date": "2022",
      "value": 80.693,
      "unit": "",
      "obs_status": "",
      "decimal": 0
    },
    {
      "indicator": {"id": "SP.DYN.LE00.IN", "value": "Life expectancy at birth, total (years)"},
      "country": {"id": "US", "value": "United States"},
      "countryiso3code": "USA",
      "date": "2022",
      "value": 77.504,
      "unit": "",
      "obs_status": "",
      "decimal": 0
    },
    {
      "indicator": {"id": "SP.DYN.LE00.IN", "value": "Life expectancy at birth, total (years)"},
      "country": {"id": "CN", "value": "China"},
      "countryiso3code": "CHN",
      "date": "2022",
      "value": 78.213,
      "unit": "",
      "obs_status": "",
      "decimal": 0
    }
  ]
]
```

```json
// tests/fixtures/wb_api_metadata_response.json
[
  {"page": 1, "pages": 1, "per_page": 50, "total": 1},
  [
    {
      "id": "SP.DYN.LE00.IN",
      "name": "Life expectancy at birth, total (years)",
      "unit": "",
      "source": {"id": "2", "value": "World Development Indicators"},
      "sourceNote": "Life expectancy at birth indicates the number of years a newborn infant would live.",
      "sourceOrganization": "UN Population Division",
      "topics": [{"id": "8", "value": "Health"}]
    }
  ]
]
```

- [ ] **Step 2: Write failing tests**

```python
# tests/test_api.py
"""Tests for World Bank API client (uses mocked responses)."""
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from wb_data_lakehouse.api import (
    build_session,
    fetch_indicator,
    fetch_indicator_metadata,
    flatten_observations,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _mock_response(fixture_name: str) -> MagicMock:
    data = json.loads((FIXTURES / fixture_name).read_text(encoding="utf-8"))
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = data
    resp.raise_for_status.return_value = None
    return resp


def test_build_session():
    session = build_session()
    assert "User-Agent" in session.headers


def test_fetch_indicator_returns_records():
    mock_resp = _mock_response("wb_api_indicator_response.json")
    with patch("wb_data_lakehouse.api.build_session") as mock_build:
        session = MagicMock()
        session.get.return_value = mock_resp
        mock_build.return_value = session
        records = fetch_indicator("SP.DYN.LE00.IN", session=session)
    assert len(records) == 5
    assert records[0]["countryiso3code"] == "PAK"


def test_fetch_indicator_metadata():
    mock_resp = _mock_response("wb_api_metadata_response.json")
    with patch("wb_data_lakehouse.api.build_session") as mock_build:
        session = MagicMock()
        session.get.return_value = mock_resp
        mock_build.return_value = session
        meta = fetch_indicator_metadata("SP.DYN.LE00.IN", session=session)
    assert meta["id"] == "SP.DYN.LE00.IN"
    assert "Life expectancy" in meta["name"]


def test_flatten_observations():
    data = json.loads((FIXTURES / "wb_api_indicator_response.json").read_text(encoding="utf-8"))
    records = data[1]
    flat = flatten_observations(records)
    assert len(flat) == 5
    assert flat[0]["indicator_id"] == "SP.DYN.LE00.IN"
    assert flat[0]["indicator_name"] == "Life expectancy at birth, total (years)"
    assert flat[0]["country_id"] == "PK"
    assert flat[0]["country_name"] == "Pakistan"
    assert flat[0]["countryiso3code"] == "PAK"
    assert flat[0]["date"] == "2022"
    assert flat[0]["value"] == 66.094


def test_flatten_observations_skips_null_value():
    records = [
        {
            "indicator": {"id": "X", "value": "Test"},
            "country": {"id": "XX", "value": "Nowhere"},
            "countryiso3code": "XXX",
            "date": "2020",
            "value": None,
            "unit": "",
            "obs_status": "",
            "decimal": 0,
        }
    ]
    flat = flatten_observations(records)
    assert len(flat) == 0
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
python -m pytest tests/test_api.py -v
```

Expected: FAIL (ImportError)

- [ ] **Step 4: Implement api.py**

```python
# src/wb_data_lakehouse/api.py
"""World Bank API v2 client with pagination and retry logic."""
from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from wb_data_lakehouse.config import BASE_URL, DEFAULT_PER_PAGE, DEFAULT_TIMEOUT_SECONDS, USER_AGENT


def build_session(user_agent: str = USER_AGENT) -> requests.Session:
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_indicator(
    indicator_code: str,
    per_page: int = DEFAULT_PER_PAGE,
    session: requests.Session | None = None,
) -> list[dict]:
    """Fetch all country-year observations for one indicator. Handles pagination."""
    if session is None:
        session = build_session()

    url = f"{BASE_URL}/country/all/indicator/{indicator_code}"
    params = {"format": "json", "per_page": per_page, "page": 1}

    all_records: list[dict] = []
    while True:
        resp = session.get(url, params=params, timeout=DEFAULT_TIMEOUT_SECONDS)
        resp.raise_for_status()
        data = resp.json()

        if not isinstance(data, list) or len(data) < 2 or data[1] is None:
            break

        header = data[0]
        records = data[1]
        all_records.extend(records)

        if header["page"] >= header["pages"]:
            break
        params["page"] += 1

    return all_records


def fetch_indicator_metadata(
    indicator_code: str,
    session: requests.Session | None = None,
) -> dict:
    """Fetch metadata (name, source, topic) for one indicator."""
    if session is None:
        session = build_session()

    url = f"{BASE_URL}/indicator/{indicator_code}"
    params = {"format": "json"}
    resp = session.get(url, params=params, timeout=DEFAULT_TIMEOUT_SECONDS)
    resp.raise_for_status()
    data = resp.json()

    if isinstance(data, list) and len(data) >= 2 and data[1]:
        return data[1][0]
    return {}


def flatten_observations(records: list[dict]) -> list[dict]:
    """Flatten nested WB API response records to flat dicts.

    Skips records where value is None (no data).
    """
    flat: list[dict] = []
    for rec in records:
        if rec.get("value") is None:
            continue
        flat.append({
            "indicator_id": rec["indicator"]["id"],
            "indicator_name": rec["indicator"]["value"],
            "country_id": rec["country"]["id"],
            "country_name": rec["country"]["value"],
            "countryiso3code": rec["countryiso3code"],
            "date": rec["date"],
            "value": rec["value"],
            "unit": rec.get("unit", ""),
            "obs_status": rec.get("obs_status", ""),
            "decimal": rec.get("decimal", 0),
        })
    return flat
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
python -m pytest tests/test_api.py -v
```

Expected: 5 passed

- [ ] **Step 6: Commit**

```bash
git add src/wb_data_lakehouse/api.py tests/test_api.py tests/fixtures/wb_api_*.json
git commit -m "feat: World Bank API v2 client with pagination and flatten"
```

---

### Task 4: Registry Module

**Files:**
- Create: `registry/sources.yaml`
- Create: `src/wb_data_lakehouse/registry.py`
- Create: `tests/test_registry.py`

- [ ] **Step 1: Create registry/sources.yaml**

```yaml
# World Bank Data Lakehouse — Indicator Registry
# Each domain lists curated indicator codes fetched from api.worldbank.org/v2.
# source_id: World Bank source identifier.
# topic_id: optional World Bank topic filter.

wdi:
  description: "World Development Indicators — flagship dataset"
  source_id: 2
  indicators:
    - SP.DYN.LE00.IN       # Life expectancy at birth
    - SH.XPD.CHEX.GD.ZS   # Current health expenditure (% GDP)
    - SH.XPD.CHEX.PC.CD    # Current health expenditure per capita
    - NY.GDP.PCAP.CD       # GDP per capita (current US$)
    - NY.GDP.MKTP.CD       # GDP (current US$)
    - SP.POP.TOTL          # Population, total
    - SP.POP.GROW          # Population growth (annual %)
    - SP.DYN.CBRT.IN       # Birth rate, crude
    - SP.DYN.CDRT.IN       # Death rate, crude
    - SP.URB.TOTL.IN.ZS    # Urban population (% of total)
    - EN.ATM.CO2E.PC       # CO2 emissions (metric tons per capita)
    - EG.ELC.ACCS.ZS       # Access to electricity (%)
    - IT.NET.USER.ZS       # Individuals using the Internet (%)
    - SL.UEM.TOTL.ZS       # Unemployment, total (% of labor force)
    - FP.CPI.TOTL.ZG       # Inflation, consumer prices (annual %)
    - BX.KLT.DINV.WD.GD.ZS # Foreign direct investment (% GDP)
    - NE.TRD.GNFS.ZS       # Trade (% GDP)
    - GC.DOD.TOTL.GD.ZS    # Central government debt (% GDP)
    - SM.POP.NETM           # Net migration
    - SH.H2O.BASW.ZS       # People using basic drinking water (%)

hnp:
  description: "Health, Nutrition and Population Statistics"
  source_id: 37
  indicators:
    - SH.STA.MMRT          # Maternal mortality ratio (per 100,000)
    - SH.DYN.NMRT          # Neonatal mortality rate
    - SH.DYN.MORT          # Under-5 mortality rate
    - SP.DYN.IMRT.IN       # Infant mortality rate
    - SH.MED.PHYS.ZS       # Physicians (per 1,000 people)
    - SH.MED.BEDS.ZS       # Hospital beds (per 1,000 people)
    - SH.MED.NUMW.P3       # Nurses and midwives (per 1,000)
    - SH.IMM.MEAS          # Immunization, measles (% children 12-23m)
    - SH.IMM.IDPT          # Immunization, DPT (% children 12-23m)
    - SH.STA.BRTC.ZS       # Births attended by skilled staff (%)
    - SH.PRV.SMOK.MA       # Smoking prevalence, males (%)
    - SH.PRV.SMOK.FE       # Smoking prevalence, females (%)
    - SH.STA.OWGH.ZS       # Prevalence of overweight (% adults)
    - SH.STA.DIAB.ZS       # Diabetes prevalence (% ages 20-79)
    - SH.TBS.INCD          # Tuberculosis incidence (per 100,000)
    - SH.DYN.AIDS.ZS       # HIV prevalence (% ages 15-49)
    - SH.STA.WASH.P5       # People with basic handwashing facilities (%)
    - SH.XPD.OOPC.CH.ZS    # Out-of-pocket expenditure (% health)
    - SH.UHC.SRVS.CV.XD    # UHC service coverage index
    - SH.STA.SUIC.P5       # Suicide mortality rate (per 100,000)

poverty:
  description: "Poverty and Inequality"
  source_id: 2
  topic_id: 11
  indicators:
    - SI.POV.DDAY          # Poverty headcount ratio ($2.15/day)
    - SI.POV.LMIC          # Poverty headcount ($3.65/day)
    - SI.POV.UMIC          # Poverty headcount ($6.85/day)
    - SI.POV.NAHC          # Poverty headcount (national)
    - SI.POV.GINI          # Gini index
    - SI.DST.FRST.10       # Income share held by lowest 10%
    - SI.DST.10TH.10       # Income share held by highest 10%
    - SI.SPR.PCAP.ZG       # Survey mean consumption/income growth
    - SI.POV.GAPS          # Poverty gap ($2.15/day)
    - NY.GNP.PCAP.CD       # GNI per capita, Atlas method

governance:
  description: "Worldwide Governance Indicators"
  source_id: 3
  indicators:
    - VA.EST               # Voice and Accountability
    - PS.EST               # Political Stability
    - GE.EST               # Government Effectiveness
    - RQ.EST               # Regulatory Quality
    - RL.EST               # Rule of Law
    - CC.EST               # Control of Corruption
    - VA.PER.RNK           # Voice and Accountability (percentile rank)
    - PS.PER.RNK           # Political Stability (percentile rank)
    - GE.PER.RNK           # Government Effectiveness (percentile rank)
    - CC.PER.RNK           # Control of Corruption (percentile rank)

education:
  description: "Education Statistics"
  source_id: 2
  topic_id: 4
  indicators:
    - SE.ADT.LITR.ZS       # Adult literacy rate (%)
    - SE.ADT.LITR.FE.ZS    # Adult literacy rate, female (%)
    - SE.PRM.ENRR           # Primary school enrollment (% gross)
    - SE.SEC.ENRR           # Secondary school enrollment (% gross)
    - SE.TER.ENRR           # Tertiary school enrollment (% gross)
    - SE.XPD.TOTL.GD.ZS    # Government education expenditure (% GDP)
    - SE.PRM.CMPT.ZS       # Primary completion rate (%)
    - SE.SEC.CMPT.LO.ZS    # Lower secondary completion rate (%)
    - SE.PRM.TENR           # Adjusted net enrollment, primary (%)
    - SE.COM.DURS           # Compulsory education duration (years)
    - UIS.FOSEP.56.F600     # STEM graduates (% of total)
    - SE.ADT.1524.LT.ZS    # Youth literacy rate (15-24)

gender:
  description: "Gender Statistics"
  source_id: 14
  indicators:
    - SG.GEN.PARL.ZS       # Women in parliament (%)
    - SL.TLF.CACT.FE.ZS    # Female labor force participation (%)
    - SL.TLF.CACT.MA.ZS    # Male labor force participation (%)
    - SE.ENR.PRIM.FM.ZS    # School enrollment, primary (gender parity index)
    - SE.ENR.SECO.FM.ZS    # School enrollment, secondary (gender parity)
    - SH.STA.MMRT          # Maternal mortality ratio
    - SP.ADO.TFRT          # Adolescent fertility rate
    - SG.VAW.1549.ZS       # Women experiencing violence (%)
    - SG.LAW.NODC.HR       # Women can get a job same way as men (1=yes)
    - SL.EMP.MPYR.FE.ZS    # Employers, female (% female employment)
    - SP.DYN.LE00.FE.IN    # Life expectancy at birth, female
    - SP.DYN.LE00.MA.IN    # Life expectancy at birth, male
```

- [ ] **Step 2: Write failing tests**

```python
# tests/test_registry.py
"""Tests for registry loading and validation."""
from pathlib import Path

import yaml

from wb_data_lakehouse.registry import load_registry, validate_registry

REGISTRY_PATH = Path(__file__).resolve().parents[1] / "registry" / "sources.yaml"


def test_registry_loads():
    registry = load_registry(REGISTRY_PATH)
    assert len(registry) == 6


def test_registry_validates_clean():
    registry = load_registry(REGISTRY_PATH)
    errors = validate_registry(registry)
    assert errors == [], f"Registry validation errors: {errors}"


def test_registry_all_domains_present():
    registry = load_registry(REGISTRY_PATH)
    for domain in ("wdi", "hnp", "poverty", "governance", "education", "gender"):
        assert domain in registry, f"Missing domain: {domain}"
        assert len(registry[domain].indicators) > 0, f"{domain} has no indicators"


def test_registry_no_duplicate_indicators():
    registry = load_registry(REGISTRY_PATH)
    for domain_name, domain in registry.items():
        seen = set()
        for code in domain.indicators:
            key = f"{domain_name}/{code}"
            assert key not in seen, f"Duplicate: {key}"
            seen.add(key)


def test_validate_catches_missing_domain(tmp_path):
    minimal = {
        "wdi": {
            "description": "test",
            "source_id": 2,
            "indicators": ["SP.POP.TOTL"],
        }
    }
    path = tmp_path / "sources.yaml"
    path.write_text(yaml.dump(minimal), encoding="utf-8")
    registry = load_registry(path)
    errors = validate_registry(registry)
    assert any("Missing domain" in e for e in errors)
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
python -m pytest tests/test_registry.py -v
```

Expected: FAIL (ImportError)

- [ ] **Step 4: Implement registry.py**

```python
# src/wb_data_lakehouse/registry.py
"""YAML registry loader and validator for World Bank indicator codes."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from wb_data_lakehouse.config import DOMAINS, REGISTRY_DIR


@dataclass
class RegistryDomain:
    name: str
    description: str
    source_id: int
    topic_id: int | None = None
    indicators: list[str] = field(default_factory=list)


def load_registry(path: Path | None = None) -> dict[str, RegistryDomain]:
    """Load sources.yaml and return {domain_name: RegistryDomain}."""
    if path is None:
        path = REGISTRY_DIR / "sources.yaml"
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    registry: dict[str, RegistryDomain] = {}
    for domain_name, domain_data in raw.items():
        registry[domain_name] = RegistryDomain(
            name=domain_name,
            description=domain_data.get("description", ""),
            source_id=domain_data.get("source_id", 0),
            topic_id=domain_data.get("topic_id"),
            indicators=domain_data.get("indicators", []),
        )
    return registry


def validate_registry(registry: dict[str, RegistryDomain]) -> list[str]:
    """Check registry for issues. Returns list of error strings (empty = valid)."""
    errors: list[str] = []

    for domain_name in DOMAINS:
        if domain_name not in registry:
            errors.append(f"Missing domain: {domain_name}")

    for domain_name, domain in registry.items():
        if not domain.indicators:
            errors.append(f"{domain_name}: no indicators registered")
        if not domain.source_id:
            errors.append(f"{domain_name}: missing source_id")

    return errors
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
python -m pytest tests/test_registry.py -v
```

Expected: 5 passed

- [ ] **Step 6: Commit**

```bash
git add registry/sources.yaml src/wb_data_lakehouse/registry.py tests/test_registry.py
git commit -m "feat: indicator registry with 6 domains, ~145 curated indicators"
```

---

### Task 5: Normalize Module

**Files:**
- Create: `src/wb_data_lakehouse/normalize.py`
- Create: `tests/test_normalize.py`
- Create: `tests/fixtures/wdi_sample.csv`

- [ ] **Step 1: Create test fixture — wdi_sample.csv**

```csv
indicator_id,indicator_name,country_id,country_name,countryiso3code,date,value,unit,obs_status,decimal
SP.DYN.LE00.IN,Life expectancy at birth total (years),PK,Pakistan,PAK,2022,66.094,,, 0
SP.DYN.LE00.IN,Life expectancy at birth total (years),PK,Pakistan,PAK,2021,65.867,,,0
SP.DYN.LE00.IN,Life expectancy at birth total (years),GB,United Kingdom,GBR,2022,80.693,,,0
SP.DYN.LE00.IN,Life expectancy at birth total (years),US,United States,USA,2022,77.504,,,0
SP.POP.TOTL,Population total,PK,Pakistan,PAK,2022,231402117,,,0
SP.POP.TOTL,Population total,GB,United Kingdom,GBR,2022,67508936,,,0
SP.POP.TOTL,Population total,US,United States,USA,2022,333287557,,,0
SP.POP.TOTL,Population total,CN,China,CHN,2022,1412175000,,,0
```

- [ ] **Step 2: Write failing tests**

```python
# tests/test_normalize.py
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
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
python -m pytest tests/test_normalize.py -v
```

Expected: FAIL (ImportError)

- [ ] **Step 4: Implement normalize.py**

```python
# src/wb_data_lakehouse/normalize.py
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
    """Coerce date → Int64, value → float64."""
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
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
python -m pytest tests/test_normalize.py -v
```

Expected: 7 passed

- [ ] **Step 6: Commit**

```bash
git add src/wb_data_lakehouse/normalize.py tests/test_normalize.py tests/fixtures/wdi_sample.csv
git commit -m "feat: normalize module — validate, coerce, provenance, harmonize_wb"
```

---

### Task 6: Sources (Fetch Engine)

**Files:**
- Create: `src/wb_data_lakehouse/sources/__init__.py`
- Create: `src/wb_data_lakehouse/sources/wdi.py`
- Create: `src/wb_data_lakehouse/sources/hnp.py`
- Create: `src/wb_data_lakehouse/sources/poverty.py`
- Create: `src/wb_data_lakehouse/sources/governance.py`
- Create: `src/wb_data_lakehouse/sources/education.py`
- Create: `src/wb_data_lakehouse/sources/gender.py`
- Create: `tests/test_sources.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_sources.py
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
    # Pre-create the file
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_sources.py -v
```

Expected: FAIL (ImportError)

- [ ] **Step 3: Implement sources/__init__.py**

```python
# src/wb_data_lakehouse/sources/__init__.py
"""Shared fetch engine for all World Bank data sources."""
from __future__ import annotations

from pathlib import Path

from wb_data_lakehouse.api import build_session, fetch_indicator, flatten_observations
from wb_data_lakehouse.config import MANIFEST_DIR, RAW_DIR, REGISTRY_DIR
from wb_data_lakehouse.registry import load_registry
from wb_data_lakehouse.storage import write_json, write_manifest


def fetch_domain(
    domain_name: str,
    raw_dir: Path | None = None,
    registry_path: Path | None = None,
    manifest_dir: Path | None = None,
    skip_existing: bool = False,
) -> dict:
    """Fetch all indicators for a domain from the World Bank API.

    Returns a summary dict suitable for manifest writing.
    """
    if raw_dir is None:
        raw_dir = RAW_DIR
    if registry_path is None:
        registry_path = REGISTRY_DIR / "sources.yaml"
    if manifest_dir is None:
        manifest_dir = MANIFEST_DIR

    registry = load_registry(registry_path)
    if domain_name not in registry:
        return {"domain": domain_name, "status": "error", "reason": f"Unknown domain: {domain_name}"}

    domain = registry[domain_name]
    session = build_session()
    domain_dir = raw_dir / domain_name
    domain_dir.mkdir(parents=True, exist_ok=True)

    results: list[dict] = []

    for indicator_code in domain.indicators:
        result = _fetch_one(session, indicator_code, domain_dir, skip_existing)
        results.append(result)

    summary = {
        "domain": domain_name,
        "description": domain.description,
        "status": "fetched",
        "indicators": results,
        "fetched_count": sum(1 for r in results if r["status"] == "downloaded"),
        "cached_count": sum(1 for r in results if r["status"] == "cached"),
        "error_count": sum(1 for r in results if r["status"] == "error"),
    }

    return write_manifest(f"fetch_{domain_name}", summary, manifest_dir)


def _fetch_one(
    session,
    indicator_code: str,
    domain_dir: Path,
    skip_existing: bool,
) -> dict:
    """Fetch a single indicator from the API and save as JSON."""
    local_path = domain_dir / f"{indicator_code}.json"

    if local_path.exists() and skip_existing:
        return {"indicator": indicator_code, "status": "cached", "path": str(local_path)}

    try:
        raw_records = fetch_indicator(indicator_code, session=session)
        flat = flatten_observations(raw_records)
        write_json(flat, local_path)
        return {
            "indicator": indicator_code,
            "status": "downloaded",
            "records": len(flat),
            "path": str(local_path),
        }
    except Exception as exc:
        return {"indicator": indicator_code, "status": "error", "reason": str(exc)}
```

- [ ] **Step 4: Create the 6 thin domain wrappers**

```python
# src/wb_data_lakehouse/sources/wdi.py
"""World Development Indicators fetcher."""
from __future__ import annotations
from pathlib import Path
from wb_data_lakehouse.sources import fetch_domain

def fetch_wdi(raw_dir: Path | None = None, skip_existing: bool = False) -> dict:
    return fetch_domain("wdi", raw_dir=raw_dir, skip_existing=skip_existing)
```

```python
# src/wb_data_lakehouse/sources/hnp.py
"""Health, Nutrition and Population fetcher."""
from __future__ import annotations
from pathlib import Path
from wb_data_lakehouse.sources import fetch_domain

def fetch_hnp(raw_dir: Path | None = None, skip_existing: bool = False) -> dict:
    return fetch_domain("hnp", raw_dir=raw_dir, skip_existing=skip_existing)
```

```python
# src/wb_data_lakehouse/sources/poverty.py
"""Poverty and Inequality fetcher."""
from __future__ import annotations
from pathlib import Path
from wb_data_lakehouse.sources import fetch_domain

def fetch_poverty(raw_dir: Path | None = None, skip_existing: bool = False) -> dict:
    return fetch_domain("poverty", raw_dir=raw_dir, skip_existing=skip_existing)
```

```python
# src/wb_data_lakehouse/sources/governance.py
"""Worldwide Governance Indicators fetcher."""
from __future__ import annotations
from pathlib import Path
from wb_data_lakehouse.sources import fetch_domain

def fetch_governance(raw_dir: Path | None = None, skip_existing: bool = False) -> dict:
    return fetch_domain("governance", raw_dir=raw_dir, skip_existing=skip_existing)
```

```python
# src/wb_data_lakehouse/sources/education.py
"""Education Statistics fetcher."""
from __future__ import annotations
from pathlib import Path
from wb_data_lakehouse.sources import fetch_domain

def fetch_education(raw_dir: Path | None = None, skip_existing: bool = False) -> dict:
    return fetch_domain("education", raw_dir=raw_dir, skip_existing=skip_existing)
```

```python
# src/wb_data_lakehouse/sources/gender.py
"""Gender Statistics fetcher."""
from __future__ import annotations
from pathlib import Path
from wb_data_lakehouse.sources import fetch_domain

def fetch_gender(raw_dir: Path | None = None, skip_existing: bool = False) -> dict:
    return fetch_domain("gender", raw_dir=raw_dir, skip_existing=skip_existing)
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
python -m pytest tests/test_sources.py -v
```

Expected: 3 passed

- [ ] **Step 6: Commit**

```bash
git add src/wb_data_lakehouse/sources/ tests/test_sources.py
git commit -m "feat: fetch engine — API-driven indicator download with domain wrappers"
```

---

### Task 7: Promote Module (all 6 domains)

**Files:**
- Create: `src/wb_data_lakehouse/promote/__init__.py`
- Create: `src/wb_data_lakehouse/promote/wdi.py`
- Create: `src/wb_data_lakehouse/promote/hnp.py`
- Create: `src/wb_data_lakehouse/promote/poverty.py`
- Create: `src/wb_data_lakehouse/promote/governance.py`
- Create: `src/wb_data_lakehouse/promote/education.py`
- Create: `src/wb_data_lakehouse/promote/gender.py`
- Create: `tests/test_promote.py`
- Create: `tests/fixtures/SP.DYN.LE00.IN.json`
- Create: `tests/fixtures/SP.POP.TOTL.json`

- [ ] **Step 1: Create test fixtures — raw JSON samples**

```json
// tests/fixtures/SP.DYN.LE00.IN.json
[
  {"indicator_id": "SP.DYN.LE00.IN", "indicator_name": "Life expectancy at birth, total (years)", "country_id": "PK", "country_name": "Pakistan", "countryiso3code": "PAK", "date": "2022", "value": 66.094, "unit": "", "obs_status": "", "decimal": 0},
  {"indicator_id": "SP.DYN.LE00.IN", "indicator_name": "Life expectancy at birth, total (years)", "country_id": "PK", "country_name": "Pakistan", "countryiso3code": "PAK", "date": "2021", "value": 65.867, "unit": "", "obs_status": "", "decimal": 0},
  {"indicator_id": "SP.DYN.LE00.IN", "indicator_name": "Life expectancy at birth, total (years)", "country_id": "GB", "country_name": "United Kingdom", "countryiso3code": "GBR", "date": "2022", "value": 80.693, "unit": "", "obs_status": "", "decimal": 0},
  {"indicator_id": "SP.DYN.LE00.IN", "indicator_name": "Life expectancy at birth, total (years)", "country_id": "US", "country_name": "United States", "countryiso3code": "USA", "date": "2022", "value": 77.504, "unit": "", "obs_status": "", "decimal": 0}
]
```

```json
// tests/fixtures/SP.POP.TOTL.json
[
  {"indicator_id": "SP.POP.TOTL", "indicator_name": "Population, total", "country_id": "PK", "country_name": "Pakistan", "countryiso3code": "PAK", "date": "2022", "value": 231402117, "unit": "", "obs_status": "", "decimal": 0},
  {"indicator_id": "SP.POP.TOTL", "indicator_name": "Population, total", "country_id": "GB", "country_name": "United Kingdom", "countryiso3code": "GBR", "date": "2022", "value": 67508936, "unit": "", "obs_status": "", "decimal": 0},
  {"indicator_id": "SP.POP.TOTL", "indicator_name": "Population, total", "country_id": "US", "country_name": "United States", "countryiso3code": "USA", "date": "2022", "value": 333287557, "unit": "", "obs_status": "", "decimal": 0}
]
```

- [ ] **Step 2: Write failing tests**

```python
# tests/test_promote.py
"""Tests for promote modules — raw JSON → bronze CSV → silver Parquet."""
import json
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
    results = promote_wdi(
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
    results = promote_wdi(
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
    results = promote_wdi(
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
    assert len(promoted) == 2  # Two fixture files
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
python -m pytest tests/test_promote.py -v
```

Expected: FAIL (ImportError)

- [ ] **Step 4: Implement promote/__init__.py**

```python
# src/wb_data_lakehouse/promote/__init__.py
"""Promote modules — raw JSON → bronze CSV → silver Parquet for each domain."""
```

- [ ] **Step 5: Implement the shared promote logic in promote/wdi.py**

All 6 domain promoters share the same logic — only the domain name differs. The WDI promoter is the template:

```python
# src/wb_data_lakehouse/promote/wdi.py
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
```

- [ ] **Step 6: Create the other 5 domain promoters**

Each imports and reuses `_promote_domain` from `wdi.py`:

```python
# src/wb_data_lakehouse/promote/hnp.py
"""Promote raw HNP JSON to bronze + silver."""
from __future__ import annotations
from pathlib import Path
from wb_data_lakehouse.promote.wdi import _promote_domain

def promote_hnp(raw_dir: Path, bronze_dir: Path, silver_dir: Path, skip_existing: bool = False) -> list[dict]:
    return _promote_domain("hnp", raw_dir, bronze_dir, silver_dir, skip_existing)
```

```python
# src/wb_data_lakehouse/promote/poverty.py
"""Promote raw Poverty JSON to bronze + silver."""
from __future__ import annotations
from pathlib import Path
from wb_data_lakehouse.promote.wdi import _promote_domain

def promote_poverty(raw_dir: Path, bronze_dir: Path, silver_dir: Path, skip_existing: bool = False) -> list[dict]:
    return _promote_domain("poverty", raw_dir, bronze_dir, silver_dir, skip_existing)
```

```python
# src/wb_data_lakehouse/promote/governance.py
"""Promote raw Governance JSON to bronze + silver."""
from __future__ import annotations
from pathlib import Path
from wb_data_lakehouse.promote.wdi import _promote_domain

def promote_governance(raw_dir: Path, bronze_dir: Path, silver_dir: Path, skip_existing: bool = False) -> list[dict]:
    return _promote_domain("governance", raw_dir, bronze_dir, silver_dir, skip_existing)
```

```python
# src/wb_data_lakehouse/promote/education.py
"""Promote raw Education JSON to bronze + silver."""
from __future__ import annotations
from pathlib import Path
from wb_data_lakehouse.promote.wdi import _promote_domain

def promote_education(raw_dir: Path, bronze_dir: Path, silver_dir: Path, skip_existing: bool = False) -> list[dict]:
    return _promote_domain("education", raw_dir, bronze_dir, silver_dir, skip_existing)
```

```python
# src/wb_data_lakehouse/promote/gender.py
"""Promote raw Gender JSON to bronze + silver."""
from __future__ import annotations
from pathlib import Path
from wb_data_lakehouse.promote.wdi import _promote_domain

def promote_gender(raw_dir: Path, bronze_dir: Path, silver_dir: Path, skip_existing: bool = False) -> list[dict]:
    return _promote_domain("gender", raw_dir, bronze_dir, silver_dir, skip_existing)
```

- [ ] **Step 7: Run tests to verify they pass**

```bash
python -m pytest tests/test_promote.py -v
```

Expected: 5 passed

- [ ] **Step 8: Commit**

```bash
git add src/wb_data_lakehouse/promote/ tests/test_promote.py tests/fixtures/SP.*.json
git commit -m "feat: promote modules — raw JSON to bronze CSV + silver Parquet for all 6 domains"
```

---

### Task 8: Catalog Module

**Files:**
- Create: `src/wb_data_lakehouse/catalog.py`
- Create: `tests/test_catalog.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_catalog.py
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
    hits = search_catalog(catalog, keyword="life")
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_catalog.py -v
```

Expected: FAIL (ImportError)

- [ ] **Step 3: Implement catalog.py**

```python
# src/wb_data_lakehouse/catalog.py
"""Master catalog builder: scans silver layer and builds a unified dataset index."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

DESCRIPTION_HINTS = {
    "wdi": "World Development Indicators",
    "hnp": "Health, Nutrition and Population",
    "poverty": "Poverty and Inequality",
    "governance": "Worldwide Governance Indicators",
    "education": "Education Statistics",
    "gender": "Gender Statistics",
}


def build_catalog(silver_dir: Path, output_path: Path | None = None) -> pd.DataFrame:
    """Scan silver/ for all Parquet files and build a unified catalog."""
    rows: list[dict] = []
    for domain_dir in sorted(silver_dir.iterdir()):
        if not domain_dir.is_dir():
            continue
        domain = domain_dir.name
        hint = DESCRIPTION_HINTS.get(domain, domain)
        for tier_dir in sorted(domain_dir.iterdir()):
            if not tier_dir.is_dir():
                continue
            tier = tier_dir.name
            for pq in sorted(tier_dir.glob("*.parquet")):
                df = pd.read_parquet(pq)
                rows.append({
                    "domain": domain,
                    "tier": tier,
                    "dataset": pq.stem,
                    "description": f"{hint}: {pq.stem} ({tier})",
                    "rows": len(df),
                    "columns": len(df.columns),
                    "column_names": ", ".join(df.columns[:20]),
                    "silver_path": str(pq),
                })
    catalog = pd.DataFrame.from_records(rows)
    if catalog.empty:
        catalog = pd.DataFrame(
            columns=["domain", "tier", "dataset", "description",
                     "rows", "columns", "column_names", "silver_path"]
        )
    catalog = catalog.sort_values(["domain", "tier", "dataset"]).reset_index(drop=True)
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        catalog.to_parquet(output_path, index=False)
    return catalog


def search_catalog(
    catalog: pd.DataFrame,
    keyword: str | None = None,
    domain: str | None = None,
    tier: str | None = None,
) -> pd.DataFrame:
    """Filter catalog by keyword, domain, and/or tier."""
    mask = pd.Series(True, index=catalog.index)
    if domain is not None:
        mask &= catalog["domain"].str.lower() == domain.lower()
    if tier is not None:
        mask &= catalog["tier"].str.lower() == tier.lower()
    if keyword is not None:
        kw = keyword.lower()
        text_match = (
            catalog["dataset"].str.lower().str.contains(kw, na=False)
            | catalog["description"].str.lower().str.contains(kw, na=False)
            | catalog["column_names"].str.lower().str.contains(kw, na=False)
        )
        mask &= text_match
    return catalog.loc[mask].reset_index(drop=True)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_catalog.py -v
```

Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add src/wb_data_lakehouse/catalog.py tests/test_catalog.py
git commit -m "feat: catalog module — build and search silver Parquet index"
```

---

### Task 9: CLI Module

**Files:**
- Create: `src/wb_data_lakehouse/cli.py`
- Create: `tests/test_cli.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_cli.py
"""Tests for CLI argument parsing."""
from wb_data_lakehouse.cli import build_parser


def test_parser_fetch_domain():
    parser = build_parser()
    args = parser.parse_args(["fetch", "wdi", "--skip-existing"])
    assert args.command == "fetch"
    assert args.domain == "wdi"
    assert args.skip_existing is True


def test_parser_fetch_all():
    parser = build_parser()
    args = parser.parse_args(["fetch", "--all"])
    assert args.all is True


def test_parser_promote_domain():
    parser = build_parser()
    args = parser.parse_args(["promote", "hnp"])
    assert args.command == "promote"
    assert args.domain == "hnp"


def test_parser_promote_all():
    parser = build_parser()
    args = parser.parse_args(["promote", "--all"])
    assert args.command == "promote"
    assert args.all is True


def test_parser_search():
    parser = build_parser()
    args = parser.parse_args(["search", "mortality", "--domain", "hnp"])
    assert args.keyword == "mortality"
    assert args.domain == "hnp"


def test_parser_status():
    parser = build_parser()
    args = parser.parse_args(["status"])
    assert args.command == "status"


def test_parser_registry_list():
    parser = build_parser()
    args = parser.parse_args(["registry-list"])
    assert args.command == "registry-list"


def test_parser_catalog():
    parser = build_parser()
    args = parser.parse_args(["catalog"])
    assert args.command == "catalog"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_cli.py -v
```

Expected: FAIL (ImportError)

- [ ] **Step 3: Implement cli.py**

```python
# src/wb_data_lakehouse/cli.py
"""CLI entry point: wb-data command with subcommands."""
from __future__ import annotations

import argparse
import json
import sys

from wb_data_lakehouse.config import (
    BRONZE_DIR, DATA_DIR, DOMAINS, MANIFEST_DIR, RAW_DIR, SILVER_DIR,
    ensure_project_directories,
)


def command_fetch(args) -> dict:
    from wb_data_lakehouse.sources import fetch_domain
    if args.all:
        domains = list(DOMAINS)
    else:
        domains = [args.domain]
    results = {}
    for domain in domains:
        results[domain] = fetch_domain(domain, skip_existing=args.skip_existing)
    return results


def command_promote(args) -> dict:
    from wb_data_lakehouse.promote.wdi import promote_wdi
    from wb_data_lakehouse.promote.hnp import promote_hnp
    from wb_data_lakehouse.promote.poverty import promote_poverty
    from wb_data_lakehouse.promote.governance import promote_governance
    from wb_data_lakehouse.promote.education import promote_education
    from wb_data_lakehouse.promote.gender import promote_gender

    promoters = {
        "wdi": promote_wdi,
        "hnp": promote_hnp,
        "poverty": promote_poverty,
        "governance": promote_governance,
        "education": promote_education,
        "gender": promote_gender,
    }

    if args.all:
        domains = list(DOMAINS)
    else:
        domains = [args.domain]

    results = {}
    for domain in domains:
        raw_dir = RAW_DIR / domain
        fn = promoters[domain]
        results[domain] = fn(raw_dir, BRONZE_DIR, SILVER_DIR, skip_existing=args.skip_existing)
    return results


def command_catalog(args) -> dict:
    from wb_data_lakehouse.catalog import build_catalog
    output = DATA_DIR / "catalog.parquet"
    catalog = build_catalog(SILVER_DIR, output_path=output)
    return {"datasets": len(catalog), "output": str(output)}


def command_search(args) -> dict:
    import pandas as pd
    from wb_data_lakehouse.catalog import search_catalog
    catalog_path = DATA_DIR / "catalog.parquet"
    if not catalog_path.exists():
        return {"error": "Catalog not found. Run 'wb-data catalog' first."}
    catalog = pd.read_parquet(catalog_path)
    hits = search_catalog(catalog, keyword=args.keyword, domain=args.domain)
    records = hits.to_dict(orient="records")
    return {"matches": len(records), "results": records}


def command_registry_list(args) -> dict:
    from wb_data_lakehouse.registry import load_registry
    registry = load_registry()
    rows = []
    for domain_name, domain in registry.items():
        for code in domain.indicators:
            rows.append({
                "domain": domain_name,
                "indicator": code,
                "source_id": domain.source_id,
            })
    return {"total_indicators": len(rows), "indicators": rows}


def command_status(args) -> dict:
    from wb_data_lakehouse.registry import load_registry
    registry = load_registry()
    status = {}
    for domain_name in DOMAINS:
        raw_dir = RAW_DIR / domain_name
        bronze_dir = BRONZE_DIR / domain_name
        native_dir = SILVER_DIR / domain_name / "native"
        harmonized_dir = SILVER_DIR / domain_name / "harmonized"

        raw_files = list(raw_dir.glob("*.json")) if raw_dir.exists() else []
        bronze_files = list(bronze_dir.glob("*.csv")) if bronze_dir.exists() else []
        native_files = list(native_dir.glob("*.parquet")) if native_dir.exists() else []
        harmonized_files = list(harmonized_dir.glob("*.parquet")) if harmonized_dir.exists() else []

        registered = len(registry[domain_name].indicators) if domain_name in registry else 0

        status[domain_name] = {
            "registered_indicators": registered,
            "raw_files": len(raw_files),
            "bronze_files": len(bronze_files),
            "native_parquets": len(native_files),
            "harmonized_parquets": len(harmonized_files),
        }
    return status


def print_summary(summary: dict) -> None:
    print(json.dumps(summary, indent=2, default=str))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wb-data",
        description="World Bank Data Lakehouse — API-driven World Bank data pipeline",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # fetch
    sp = subparsers.add_parser("fetch", help="Fetch indicators from World Bank API")
    sp.add_argument("domain", nargs="?", choices=DOMAINS, help="Domain to fetch")
    sp.add_argument("--all", action="store_true", help="Fetch all domains")
    sp.add_argument("--skip-existing", action="store_true", help="Skip already-downloaded indicators")
    sp.set_defaults(handler=command_fetch)

    # promote
    sp = subparsers.add_parser("promote", help="Promote raw data to bronze + silver")
    sp.add_argument("domain", nargs="?", choices=DOMAINS, help="Domain to promote")
    sp.add_argument("--all", action="store_true", help="Promote all domains")
    sp.add_argument("--skip-existing", action="store_true", help="Skip existing silver files")
    sp.set_defaults(handler=command_promote)

    # catalog
    sp = subparsers.add_parser("catalog", help="Build master catalog from silver layer")
    sp.set_defaults(handler=command_catalog)

    # search
    sp = subparsers.add_parser("search", help="Search the catalog")
    sp.add_argument("keyword", help="Search keyword")
    sp.add_argument("--domain", help="Filter by domain")
    sp.set_defaults(handler=command_search)

    # registry-list
    sp = subparsers.add_parser("registry-list", help="List all registered indicators")
    sp.set_defaults(handler=command_registry_list)

    # status
    sp = subparsers.add_parser("status", help="Show pipeline status for all domains")
    sp.set_defaults(handler=command_status)

    return parser


def main() -> None:
    ensure_project_directories()
    parser = build_parser()
    args = parser.parse_args()

    if args.command in ("fetch", "promote") and not args.all and args.domain is None:
        parser.error("Specify a domain or use --all")

    summary = args.handler(args)
    print_summary(summary)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_cli.py -v
```

Expected: 8 passed

- [ ] **Step 5: Commit**

```bash
git add src/wb_data_lakehouse/cli.py tests/test_cli.py
git commit -m "feat: CLI with fetch, promote, catalog, search, registry-list, status"
```

---

### Task 10: Full Test Suite + E156 Protocol

**Files:**
- Create: `E156-PROTOCOL.md`
- Create: `.gitignore`

- [ ] **Step 1: Run full test suite**

```bash
cd C:\Projects\wb-data-lakehouse
python -m pytest tests/ -v
```

Expected: ~44 tests passed (5 storage + 5 api + 5 registry + 7 normalize + 3 sources + 5 promote + 5 catalog + 8 cli)

- [ ] **Step 2: Create .gitignore**

```gitignore
# Data files (large, not for git)
data/
manifests/

# Python
__pycache__/
*.egg-info/
*.pyc
.eggs/
dist/
build/

# IDE
.vscode/
.idea/

# Claude config
.claude/
```

- [ ] **Step 3: Create E156-PROTOCOL.md**

```markdown
# E156 Protocol — World Bank Data Lakehouse

**Project:** wb-data-lakehouse
**E156 Entry:** (pending workbook addition)
**Date Created:** 2026-04-08
**Date Last Updated:** 2026-04-08

## E156 Body

(To be written after implementation is complete and tests pass.)

## Links

- **Repository:** https://github.com/mahmood726-cyber/wb-data-lakehouse
- **Dashboard:** N/A (pipeline project, no HTML dashboard)
- **Design Spec:** `docs/superpowers/specs/2026-04-08-wb-data-lakehouse-design.md`
```

- [ ] **Step 4: Commit everything**

```bash
git add .gitignore E156-PROTOCOL.md
git commit -m "chore: gitignore, E156 protocol"
```

- [ ] **Step 5: Final full test run to confirm all pass**

```bash
python -m pytest tests/ -v --tb=short
```

Expected: All tests pass, 0 failures.

---

### Task 11: Update INDEX.md + Memory

**Files:**
- Modify: `C:\ProjectIndex\INDEX.md`
- Create/Modify: memory file

- [ ] **Step 1: Add entry to INDEX.md**

Add row to INDEX.md:

```
| NNN | **WB Data Lakehouse** | `C:\Projects\wb-data-lakehouse\` | mahmood726-cyber/wb-data-lakehouse | ACTIVE | ~XXX / ~44 | 2026-04-08 |
```

(Fill in actual line count and row number after implementation.)

- [ ] **Step 2: Update session memory**

Create `wb-data-lakehouse.md` memory file with project location, status, test count, and architecture summary.

- [ ] **Step 3: Commit INDEX.md update**

```bash
cd C:\ProjectIndex
git add INDEX.md
git commit -m "docs: add WB Data Lakehouse to project index"
```
