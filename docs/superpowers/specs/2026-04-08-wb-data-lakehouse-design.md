# World Bank Data Lakehouse — Design Spec

> Date: 2026-04-08
> Status: APPROVED
> Location: `C:\Projects\wb-data-lakehouse\`
> CLI: `wb-data`
> Pattern: Mirrors IHME Data Lakehouse (registry-driven, domain-based, dual silver schema)

## Purpose

API-driven Python pipeline for World Bank open data. Fetches curated indicators from the World Bank API v2, normalizes them through a bronze/silver medallion architecture, and produces harmonized Parquet files compatible with the IHME and WHO lakehouses for cross-source joins. No API key required.

## Domains (6)

| Domain | WB Source ID | Topic ID | Description | ~Indicators |
|--------|-------------|----------|-------------|-------------|
| `wdi` | 2 | — | World Development Indicators (flagship) | ~50 |
| `hnp` | 37 | — | Health, Nutrition & Population | ~40 |
| `poverty` | 2 | 11 | Poverty & Inequality | ~15 |
| `governance` | 3 | — | Worldwide Governance Indicators | ~10 |
| `education` | 2 | 4 | Education Statistics | ~15 |
| `gender` | 14 | — | Gender Statistics | ~15 |

Total: ~145 curated indicators across 6 domains.

## Architecture

### Approach: API-Driven Fetching

The World Bank API v2 (`api.worldbank.org/v2/`) is stable, open, rate-limit-friendly, and returns paginated JSON. Each indicator is fetched for all countries across all available years in a single paginated call.

The registry (`registry/sources.yaml`) defines indicator codes per domain — not file URLs. This differs from the IHME lakehouse (which uses file URLs + checksums) because the WB API returns live data rather than static downloads.

### Data Flow

```
World Bank API v2
       |
       v
  data/raw/{domain}/{indicator_code}.json     (full API response)
       |
       v  [validate columns, coerce types, add provenance]
  data/bronze/{domain}/{indicator_code}.csv    (flat tabular, provenance-tagged)
       |
       v  [partition, write Parquet]
  data/silver/{domain}/native/*.parquet        (WB-native column names)
       |
       v  [harmonize to cross-lakehouse schema]
  data/silver/{domain}/harmonized/*.parquet    (iso3c/year/indicator_code/value)
```

### API Client (`api.py`)

```python
BASE_URL = "https://api.worldbank.org/v2"

def fetch_indicator(indicator_code: str, per_page: int = 1000) -> list[dict]:
    """Fetch all country-year observations for one indicator. Handles pagination."""
    # GET /v2/country/all/indicator/{code}?format=json&per_page=1000&page={n}
    # Returns flat list of observation dicts

def fetch_indicator_metadata(indicator_code: str) -> dict:
    """Fetch metadata (name, source, topic) for one indicator."""
    # GET /v2/indicator/{code}?format=json

def fetch_source_indicators(source_id: int) -> list[dict]:
    """List all indicators for a WB source (for discovery)."""
    # GET /v2/source/{source_id}/indicator?format=json&per_page=1000
```

Retry logic: 3 retries with exponential backoff (1s, 2s, 4s). Timeout: 60s per request.

### Registry (`registry/sources.yaml`)

```yaml
wdi:
  description: "World Development Indicators"
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
    - SI.SPR.PCAP.ZG       # Survey mean consumption/income per capita growth
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
    - SG.LAW.NODC.HR       # Women can get a job the same way as men (1=yes)
    - SL.EMP.MPYR.FE.ZS    # Employers, female (% of female employment)
    - SP.DYN.LE00.FE.IN    # Life expectancy at birth, female
    - SP.DYN.LE00.MA.IN    # Life expectancy at birth, male
```

### Normalize (`normalize.py`)

```python
WB_NATIVE_COLUMNS = {
    "country_id", "country_name", "countryiso3code",
    "date", "indicator_id", "indicator_name",
    "value", "unit", "obs_status", "decimal",
}

def validate_columns(df: pd.DataFrame) -> list[str]:
    """Return list of missing required columns."""

def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """date → Int64, value → float64, country_id/indicator_id → str."""

def add_provenance(df: pd.DataFrame, indicator_code: str) -> pd.DataFrame:
    """Add _source_indicator and _download_timestamp columns."""

def harmonize_wb(df: pd.DataFrame) -> pd.DataFrame:
    """Convert to cross-lakehouse schema.
    WB already provides countryiso3code — no crosswalk needed.
    indicator_code prefixed with 'wb_' to avoid collisions with IHME/WHO.
    """
```

### Catalog (`catalog.py`)

```python
def build_catalog(silver_dir: Path, output_path: Path | None = None) -> pd.DataFrame:
    """Scan silver/ → DataFrame with domain, tier, dataset, rows, columns, column_names, silver_path."""

def search_catalog(catalog: pd.DataFrame, keyword: str | None = None,
                   domain: str | None = None, tier: str | None = None) -> pd.DataFrame:
    """Filter catalog by domain, tier, keyword."""
```

### CLI (`cli.py`)

```
wb-data fetch {domain|--all} [--skip-existing] [--check-only]
wb-data promote {domain|--all} [--skip-existing]
wb-data catalog
wb-data search KEYWORD [--domain DOMAIN] [--tier TIER]
wb-data registry-list [--domain DOMAIN]
wb-data status
```

### Storage (`storage.py`)

Shared with IHME pattern:
- `utc_stamp() -> str`
- `write_json(payload, destination) -> Path`
- `read_json(path) -> dict`
- `write_dataframe(frame, destination, index=False) -> Path`
- `write_manifest(prefix, summary, manifest_dir) -> dict`

## Project Setup

```toml
[project]
name = "wb-data-lakehouse"
version = "0.1.0"
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
```

## Testing (~40 tests)

All offline — test fixtures are small JSON/CSV samples, no API calls in tests.

| Module | Tests | What |
|--------|-------|------|
| `test_api.py` | 5 | Pagination logic, error handling (mocked responses) |
| `test_normalize.py` | 6 | Column validation, type coercion, provenance, harmonize_wb |
| `test_registry.py` | 5 | Load, validate, domain completeness, no duplicates |
| `test_promote_wdi.py` | 4 | Bronze CSV, native Parquet, harmonized Parquet, bad schema rejection |
| `test_promote_hnp.py` | 3 | Same pattern |
| `test_promote_poverty.py` | 3 | Same pattern |
| `test_promote_governance.py` | 3 | Same pattern |
| `test_promote_education.py` | 3 | Same pattern |
| `test_promote_gender.py` | 3 | Same pattern |
| `test_catalog.py` | 4 | Build, search by keyword/domain/tier |
| `test_cli.py` | 5 | Parser for each subcommand |

### Test Fixtures (`tests/fixtures/`)

```
wb_api_response_sample.json     # 5 observations, mimics API pagination format
wdi_sample.csv                  # 8 rows, all WB_NATIVE_COLUMNS
hnp_sample.csv                  # 5 rows
poverty_sample.csv              # 5 rows
governance_sample.csv           # 5 rows
education_sample.csv            # 5 rows
gender_sample.csv               # 5 rows
```

## Downstream Consumers

- **AfricaForecast** (`C:\Models\AfricaForecast\`) — WDI + HNP + Governance indicators for 54 African countries
- **HTNPipeline** (`C:\Models\HTNPipeline\`) — already has a World Bank module, can switch to this lakehouse
- Cross-lakehouse joins with IHME/WHO via harmonized schema (iso3c + year + indicator_code)

## Non-Goals

- Not fetching all ~1,600 WDI indicators — curated list only, extensible via registry
- No authentication or API keys (WB API is fully open)
- No real-time streaming — batch fetch + promote
- No incremental updates in v0.1 (full re-fetch per indicator)
