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

BID_ENDPOINTS = {
    "goods_pps": "/getBidPblancListInfoThngPPSSrch",
    "service_pps": "/getBidPblancListInfoServcPPSSrch",
}

PRESPEC_ENDPOINTS = {
    "goods_pps": "/getPublicPrcureThngInfoThngPPSSrch",
    "service_pps": "/getPublicPrcureThngInfoServcPPSSrch",
}


def ensure_directories() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class Settings:
    bid_service_key: str
    prespec_service_key: str
    bid_service_url: str
    prespec_service_url: str
    page_size: int
    lookback_days: int
    request_timeout: int

    @classmethod
    def from_env(cls) -> "Settings":
        bid_service_key = os.getenv("BID_SERVICE_KEY", "").strip()
        prespec_service_key = os.getenv("PRESPEC_SERVICE_KEY", "").strip()
        bid_service_url = os.getenv("BID_SERVICE_URL", "").strip().rstrip("/")
        prespec_service_url = os.getenv("PRESPEC_SERVICE_URL", "").strip().rstrip("/")
        page_size = int(os.getenv("PAGE_SIZE", "50").strip())
        lookback_days = int(os.getenv("LOOKBACK_DAYS", "7").strip())
        request_timeout = int(os.getenv("REQUEST_TIMEOUT", "30").strip())

        missing: list[str] = []
        if not bid_service_key:
            missing.append("BID_SERVICE_KEY")
        if not prespec_service_key:
            missing.append("PRESPEC_SERVICE_KEY")
        if not bid_service_url:
            missing.append("BID_SERVICE_URL")
        if not prespec_service_url:
            missing.append("PRESPEC_SERVICE_URL")

        if missing:
            raise ValueError(f".env 필수값 누락: {', '.join(missing)}")

        return cls(
            bid_service_key=bid_service_key,
            prespec_service_key=prespec_service_key,
            bid_service_url=bid_service_url,
            prespec_service_url=prespec_service_url,
            page_size=page_size,
            lookback_days=lookback_days,
            request_timeout=request_timeout,
        )