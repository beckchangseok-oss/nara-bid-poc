from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import requests

from app.config import DATA_DIR

ATTACHMENTS_DIR = DATA_DIR / "raw" / "attachments"


def _safe_name(value: str) -> str:
    value = value.strip()
    value = re.sub(r'[\\/:*?"<>|]+', "_", value)
    return value[:180] if value else "unknown_file"


def download_attachments(attachment_jobs: list[dict[str, str]], timeout: int = 60) -> list[dict[str, Any]]:
    ATTACHMENTS_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    results: list[dict[str, Any]] = []

    for job in attachment_jobs:
        bid_no = job["bid_ntce_no"]
        bid_ord = job["bid_ntce_ord"]
        seq = job["seq"]
        file_name = _safe_name(job["file_name"] or f"attachment_{seq}")
        file_url = job["file_url"]

        target_dir = ATTACHMENTS_DIR / f"{bid_no}_{bid_ord}"
        target_dir.mkdir(parents=True, exist_ok=True)

        target_path = target_dir / file_name

        if not file_url:
            results.append(
                {
                    "bid_ntce_no": bid_no,
                    "bid_ntce_ord": bid_ord,
                    "seq": seq,
                    "file_name": file_name,
                    "file_url": file_url,
                    "local_path": str(target_path),
                    "status": "SKIPPED_NO_URL",
                    "error": "",
                }
            )
            continue

        try:
            response = session.get(file_url, timeout=timeout)
            response.raise_for_status()
            target_path.write_bytes(response.content)

            results.append(
                {
                    "bid_ntce_no": bid_no,
                    "bid_ntce_ord": bid_ord,
                    "seq": seq,
                    "file_name": file_name,
                    "file_url": file_url,
                    "local_path": str(target_path),
                    "status": "DOWNLOADED",
                    "error": "",
                }
            )
        except Exception as e:
            results.append(
                {
                    "bid_ntce_no": bid_no,
                    "bid_ntce_ord": bid_ord,
                    "seq": seq,
                    "file_name": file_name,
                    "file_url": file_url,
                    "local_path": str(target_path),
                    "status": "FAILED",
                    "error": str(e),
                }
            )

    return results