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
