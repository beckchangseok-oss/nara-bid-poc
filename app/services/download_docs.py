from __future__ import annotations

import re
from typing import Any
from urllib.parse import unquote

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


def _detect_extension_from_bytes(content: bytes) -> str:
    if content.startswith(b"%PDF"):
        return ".pdf"
    if content.startswith(bytes.fromhex("D0CF11E0A1B11AE1")):
        return ".hwp"
    if content.startswith(b"PK\x03\x04"):
        return ".zip"
    return ""


def _extract_filename_from_headers(response: requests.Response) -> str:
    content_disposition = response.headers.get("Content-Disposition", "")
    if "filename=" in content_disposition:
        filename = content_disposition.split("filename=")[-1].strip().strip('"').strip("'")
        return unquote(filename)

    content_type = response.headers.get("Content-Type", "").lower()
    if "pdf" in content_type:
        return "downloaded_file.pdf"
    if "hwp" in content_type:
        return "downloaded_file.hwp"
    if "officedocument.wordprocessingml.document" in content_type:
        return "downloaded_file.docx"
    if "msword" in content_type:
        return "downloaded_file.doc"
    if "spreadsheetml" in content_type:
        return "downloaded_file.xlsx"
    if "ms-excel" in content_type:
        return "downloaded_file.xls"
    if "zip" in content_type:
        return "downloaded_file.zip"

    return ""


def _ensure_suffix(file_name: str, suffix: str) -> str:
    if not suffix or "." in file_name:
        return file_name
    return f"{file_name}{suffix}"


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

            resolved_name = file_name
            if file_name.startswith("prespec_attachment_") or "." not in file_name:
                header_name = _extract_filename_from_headers(response)
                if header_name:
                    resolved_name = _safe_name(header_name)
                else:
                    detected_ext = _detect_extension_from_bytes(response.content)
                    resolved_name = _ensure_suffix(file_name, detected_ext)

            target_path = target_dir / resolved_name
            target_path.write_bytes(response.content)

            results.append(
                {
                    "record_id": record_id,
                    "seq": job["seq"],
                    "file_name": resolved_name,
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