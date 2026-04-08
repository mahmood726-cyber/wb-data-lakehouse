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
    """Fetch all indicators for a domain from the World Bank API."""
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

    try:
        for indicator_code in domain.indicators:
            result = _fetch_one(session, indicator_code, domain_dir, skip_existing)
            results.append(result)
    finally:
        session.close()

    summary = {
        "domain": domain_name,
        "description": domain.description,
        "status": "fetched",
        "indicators": results,
        "fetched_count": sum(1 for r in results if r["status"] == "downloaded"),
        "cached_count": sum(1 for r in results if r["status"] == "cached"),
        "error_count": sum(1 for r in results if r["status"] == "error"),
        "truncated_count": sum(1 for r in results if r.get("truncated")),
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
        raw_records, is_complete = fetch_indicator(indicator_code, session=session)
        flat = flatten_observations(raw_records)
        write_json(flat, local_path)
        return {
            "indicator": indicator_code,
            "status": "downloaded",
            "records": len(flat),
            "truncated": not is_complete,
            "path": str(local_path),
        }
    except Exception as exc:
        return {"indicator": indicator_code, "status": "error", "reason": str(exc)}
