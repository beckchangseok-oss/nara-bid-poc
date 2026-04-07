from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
LOG_DIR = DATA_DIR / "logs"
OUTPUT_DIR = DATA_DIR / "output"
DB_PATH = PROJECT_ROOT / "nara_sas_poc.sqlite3"


def ensure_directories() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class Settings:
    bid_service_key: str
    prespec_service_key: str
    keyword: str
    bid_service_url: str
    bid_list_endpoint: str
    inqry_div: str
    page_size: int

    @classmethod
    def from_env(cls) -> "Settings":
        bid_service_key = os.getenv("BID_SERVICE_KEY", "").strip()
        prespec_service_key = os.getenv("PRESPEC_SERVICE_KEY", "").strip()
        keyword = os.getenv("KEYWORD", "SAS").strip()
        bid_service_url = os.getenv("BID_SERVICE_URL", "").strip().rstrip("/")
        bid_list_endpoint = os.getenv("BID_LIST_ENDPOINT", "").strip()
        inqry_div = os.getenv("INQRY_DIV", "1").strip()
        page_size = int(os.getenv("PAGE_SIZE", "50").strip())

        missing = []
        if not bid_service_key:
            missing.append("BID_SERVICE_KEY")
        if not keyword:
            missing.append("KEYWORD")
        if not bid_service_url:
            missing.append("BID_SERVICE_URL")
        if not bid_list_endpoint:
            missing.append("BID_LIST_ENDPOINT")

        if missing:
            raise ValueError(f".env 필수값 누락: {', '.join(missing)}")

        return cls(
            bid_service_key=bid_service_key,
            prespec_service_key=prespec_service_key,
            keyword=keyword,
            bid_service_url=bid_service_url,
            bid_list_endpoint=bid_list_endpoint,
            inqry_div=inqry_div,
            page_size=page_size,
        )