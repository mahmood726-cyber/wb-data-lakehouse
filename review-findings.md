## Multi-Persona Review: wb-data-lakehouse (core modules)
### Date: 2026-04-08
### Summary: 2 P0, 5 P1, 5 P2

---

#### P0 -- Critical

- **P0-1** [Data Engineer]: Aggregate regions and income groups contaminate harmonized data (normalize.py:49)
  - WB API returns both countries (PAK, USA) and aggregates (AFE=Africa Eastern, WLD=World, XD=High income) in the same response. 256 rows have empty `countryiso3code` (income groups), and ~50 aggregate region codes (AFE, AFW, ARB, CSS, etc.) are mixed with real iso3c codes.
  - Harmonized output passes these through, contaminating downstream joins — IHME/WHO lakehouses only have real countries.
  - Suggested fix: Filter out rows where `countryiso3code` is empty OR matches known WB aggregate codes (3-letter codes not in ISO 3166-1). Add an `is_aggregate` check in `harmonize_wb()`.

- **P0-2** [Software Engineer]: Silent partial data from pagination errors — no truncation flag (api.py:45-51)
  - When a later page returns 400, the graceful fallback keeps earlier pages but returns them without any indication of truncation. Callers cannot distinguish complete vs partial datasets.
  - 10 indicators in the initial fetch were affected — they have partial data silently accepted as complete.
  - Suggested fix: Return a tuple `(records, is_complete: bool)` or add a `_truncated` flag to the result dict in `_fetch_one()`.

#### P1 -- Important

- **P1-1** [Domain Expert]: Sub-annual dates silently become NaN (normalize.py:27, 50)
  - `pd.to_numeric(date, errors="coerce")` converts "2020Q1" or "2020M06" to NaN without warning. Some WB indicators (FX rates, quarterly debt) use sub-annual dates.
  - Suggested fix: Check for non-numeric dates before coercion and log warnings.

- **P1-2** [Software Engineer]: Catalog search uses regex by default (catalog.py:89-91)
  - `.str.contains(kw)` interprets special regex chars. Searching for "X.Y" matches "XAY".
  - Suggested fix: Add `regex=False` parameter to `.str.contains()`.

- **P1-3** [Software Engineer]: Redundant type coercion in harmonize_wb (normalize.py:50, 53)
  - `harmonize_wb()` re-calls `pd.to_numeric()` on date and value even though `coerce_types()` already did this in the promote pipeline. Wasteful and masks errors.
  - Suggested fix: Remove redundant coercion from `harmonize_wb()` — assume input is already coerced.

- **P1-4** [Data Engineer]: Provenance incomplete — no API lastupdated or truncation flag (normalize.py:32-37)
  - Bronze provenance only records indicator code and download timestamp. Missing: API response `lastupdated`, whether fetch was truncated, source URL for reproducibility.
  - Suggested fix: Extend `add_provenance()` or pass metadata from fetch result.

- **P1-5** [Software Engineer]: Session objects never closed (api.py:34, sources/__init__.py:37)
  - `build_session()` creates sessions that are never explicitly closed, risking connection pool exhaustion in long runs.
  - Suggested fix: Use session as context manager or close after domain fetch completes.

#### P2 -- Minor

- **P2-1** [Software Engineer]: Exception swallowing in fetch/promote (sources/__init__.py:77, promote/wdi.py:57)
  - All exceptions become `str(exc)` — stack traces lost.
  - Suggested fix: Log full traceback to stderr or manifest.

- **P2-2** [Data Engineer]: Parquet schema not enforced (storage.py:29-30)
  - No explicit schema, compression, or column ordering. Different domains may produce inconsistent Parquet files.

- **P2-3** [Data Engineer]: Metadata fields (unit, obs_status, decimal) dropped in harmonization (normalize.py:48-58)
  - These fields exist in bronze but are lost in harmonized tier. Minor because harmonized schema is intentionally minimal.

- **P2-4** [Data Engineer]: Catalog truncates column names to first 20 (catalog.py:58)
  - Wider datasets lose metadata in the catalog index. Minor since harmonized datasets have 9 columns.

- **P2-5** [Software Engineer]: CLI handler exceptions not caught (cli.py:207)
  - Unhandled exceptions in command handlers produce raw tracebacks.

#### False Positive Watch
- Path traversal via indicator codes: indicator codes come from our own registry YAML, not API input. Low risk.
- Race condition in mkdir: `exist_ok=True` is safe; CLI is single-threaded.
- Indicator code collision with wb_ prefix: WB codes are well-defined; no real collision risk.
