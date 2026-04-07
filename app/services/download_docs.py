from __future__ import annotations

import re
from typing import Any

import requests

from app.config import DATA_DIR

ATTACHMENTS_DIR = DATA_DIR / "raw" / "attachments"


def _safe_name(value: str) -> str:
    value = value.strip()
    value = re.sub(r'[\\/:*?"<>|]+', "_", value)
    return value[:180] if value else "unknown_file"


def _safe_dir_name(value: str) -> str:
    value = value.strip()
    value = re.sub(r'[\\/:*?"<>|]+', "_", value)
    value = re.sub(r"\s+", "_", value)
    return value[:120] if value else "unknown_dir"


def _guess_suffix_from_url(url: str) -> str:
    lowered = url.lower()
    for suffix in [".pdf", ".hwp", ".hwpx", ".doc", ".docx", ".xls", ".xlsx", ".zip"]:
        if suffix in lowered:
            return suffix
    return ""


def download_attachments(attachment_jobs: list[dict[str, str]], timeout: int = 60) -> list[dict[str, Any]]:
    ATTACHMENTS_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    results: list[dict[str, Any]] = []

    for job in attachment_jobs:
        record_id = job["record_id"]
        safe_record_id = _safe_dir_name(record_id)

        file_name = _safe_name(job["file_name"])
        file_url = job["file_url"]

        if not file_name:
            file_name = f"attachment_{job['seq']}{_guess_suffix_from_url(file_url)}"

        target_dir = ATTACHMENTS_DIR / safe_record_id
        target_dir.mkdir(parents=True, exist_ok=True)

        target_path = target_dir / file_name

        if not file_url:
            results.append(
                {
                    "record_id": record_id,
                    "seq": job["seq"],
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
                    "record_id": record_id,
                    "seq": job["seq"],
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
                    "record_id": record_id,
                    "seq": job["seq"],
                    "file_name": file_name,
                    "file_url": file_url,
                    "local_path": str(target_path),
                    "status": "FAILED",
                    "error": str(e),
                }
            )

    return results