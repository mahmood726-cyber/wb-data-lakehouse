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
    "hci",
    "uhc",
    "esg",
    "food_nutrition",
    "sdg_health",
    "climate",
)


def ensure_project_directories() -> None:
    for domain in DOMAINS:
        (RAW_DIR / domain).mkdir(parents=True, exist_ok=True)
        (BRONZE_DIR / domain).mkdir(parents=True, exist_ok=True)
        (SILVER_DIR / domain / "native").mkdir(parents=True, exist_ok=True)
        (SILVER_DIR / domain / "harmonized").mkdir(parents=True, exist_ok=True)
    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
