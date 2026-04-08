"""Tests for registry loading and validation."""
from pathlib import Path

import yaml

from wb_data_lakehouse.registry import load_registry, validate_registry

REGISTRY_PATH = Path(__file__).resolve().parents[1] / "registry" / "sources.yaml"


def test_registry_loads():
    registry = load_registry(REGISTRY_PATH)
    assert len(registry) == 25


def test_registry_validates_clean():
    registry = load_registry(REGISTRY_PATH)
    errors = validate_registry(registry)
    assert errors == [], f"Registry validation errors: {errors}"


def test_registry_all_domains_present():
    registry = load_registry(REGISTRY_PATH)
    for domain in ("wdi", "hnp", "poverty", "governance", "education", "gender",
                    "hci", "uhc", "esg", "food_nutrition", "sdg_health", "climate",
                    "population", "health_equity", "africa_dev", "malnutrition",
                    "energy", "net_savings", "doing_business", "cpia", "logistics",
                    "financial_dev", "stat_performance", "id4d", "wealth"):
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
