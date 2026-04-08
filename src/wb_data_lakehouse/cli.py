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
    from wb_data_lakehouse.promote.hci import promote_hci
    from wb_data_lakehouse.promote.uhc import promote_uhc
    from wb_data_lakehouse.promote.esg import promote_esg
    from wb_data_lakehouse.promote.food_nutrition import promote_food_nutrition
    from wb_data_lakehouse.promote.sdg_health import promote_sdg_health
    from wb_data_lakehouse.promote.climate import promote_climate
    from wb_data_lakehouse.promote.population import promote_population
    from wb_data_lakehouse.promote.health_equity import promote_health_equity
    from wb_data_lakehouse.promote.africa_dev import promote_africa_dev
    from wb_data_lakehouse.promote.malnutrition import promote_malnutrition
    from wb_data_lakehouse.promote.energy import promote_energy
    from wb_data_lakehouse.promote.net_savings import promote_net_savings
    from wb_data_lakehouse.promote.doing_business import promote_doing_business
    from wb_data_lakehouse.promote.cpia import promote_cpia
    from wb_data_lakehouse.promote.logistics import promote_logistics
    from wb_data_lakehouse.promote.financial_dev import promote_financial_dev
    from wb_data_lakehouse.promote.stat_performance import promote_stat_performance
    from wb_data_lakehouse.promote.id4d import promote_id4d
    from wb_data_lakehouse.promote.wealth import promote_wealth

    promoters = {
        "wdi": promote_wdi,
        "hnp": promote_hnp,
        "poverty": promote_poverty,
        "governance": promote_governance,
        "education": promote_education,
        "gender": promote_gender,
        "hci": promote_hci,
        "uhc": promote_uhc,
        "esg": promote_esg,
        "food_nutrition": promote_food_nutrition,
        "sdg_health": promote_sdg_health,
        "climate": promote_climate,
        "population": promote_population,
        "health_equity": promote_health_equity,
        "africa_dev": promote_africa_dev,
        "malnutrition": promote_malnutrition,
        "energy": promote_energy,
        "net_savings": promote_net_savings,
        "doing_business": promote_doing_business,
        "cpia": promote_cpia,
        "logistics": promote_logistics,
        "financial_dev": promote_financial_dev,
        "stat_performance": promote_stat_performance,
        "id4d": promote_id4d,
        "wealth": promote_wealth,
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
