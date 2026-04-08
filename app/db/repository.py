from __future__ import annotations

import json
import sqlite3
from typing import Any

from app.config import DB_PATH, PROJECT_ROOT


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    schema_path = PROJECT_ROOT / "app" / "db" / "schema.sql"
    schema_sql = schema_path.read_text(encoding="utf-8")

    with get_connection() as conn:
        conn.executescript(schema_sql)
        conn.commit()


def create_run(started_at: str, keyword: str) -> int:
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO runs (started_at, keyword, status)
            VALUES (?, ?, ?)
            """,
            (started_at, keyword, "RUNNING"),
        )
        conn.commit()
        return int(cur.lastrowid)


def finish_run(
    run_id: int,
    finished_at: str,
    status: str,
    total_found: int,
    raw_json_path: str | None = None,
    error_summary: str | None = None,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE runs
            SET finished_at = ?, status = ?, total_found = ?, raw_json_path = ?, error_summary = ?
            WHERE run_id = ?
            """,
            (finished_at, status, total_found, raw_json_path, error_summary, run_id),
        )
        conn.commit()


def insert_notices(run_id: int, notices: list[dict[str, Any]], raw_json_path: str) -> None:
    rows = []

    for item in notices:
        primary_no = str(
            item.get("_primary_id")
            or item.get("bidNtceNo")
            or item.get("bfSpecRgstNo")
            or ""
        )
        primary_ord = str(item.get("_sub_id") or item.get("bidNtceOrd") or "")

        rows.append(
            (
                run_id,
                primary_no,
                primary_ord,
                str(item.get("_title", "") or item.get("bidNtceNm", "") or item.get("prdctClsfcNoNm", "")),
                str(item.get("_demand_org", "") or item.get("dminsttNm", "") or item.get("rlDminsttNm", "")),
                str(item.get("_pub_org", "") or item.get("ntceInsttNm", "") or item.get("orderInsttNm", "")),
                str(item.get("_close_dt", "") or item.get("bidClseDt", "") or item.get("opninRgstClseDt", "")),
                str(item.get("_detail_url", "") or item.get("bidNtceDtlUrl", "") or item.get("bidNtceUrl", "")),
                raw_json_path,
                "COLLECTED",
                json.dumps(item, ensure_ascii=False),
            )
        )

    with get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO notices (
                run_id,
                bid_ntce_no,
                bid_ntce_ord,
                title,
                demand_org,
                pub_org,
                close_dt,
                notice_url,
                raw_json_path,
                collect_status,
                raw_item_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()