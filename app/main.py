from __future__ import annotations

from datetime import datetime

from app.config import Settings, ensure_directories
from app.db.repository import create_run, finish_run, init_db, insert_notices
from app.services.collect_notices import collect_bid_notices
from app.services.download_docs import download_attachments


def print_section(title: str, items: list[dict], limit: int = 10) -> None:
    print(f"\n[{title}]")
    if not items:
        print("- 없음")
        return

    for item in items[:limit]:
        print(
            f"- id={item.get('_record_id')} "
            f"kind={item.get('_source_kind')} "
            f"type={item.get('_source_type')} "
            f"score={item.get('_score')} "
            f"title={item.get('_title')} "
            f"org={item.get('_pub_org')} "
            f"keyword={','.join(item.get('_query_keywords', []))} "
            f"reasons={item.get('_reasons')}"
        )


def main() -> None:
    ensure_directories()
    init_db()

    settings = Settings.from_env()
    started_at = datetime.now().isoformat(timespec="seconds")
    run_id = create_run(started_at=started_at, keyword="DAY2_PRECISION_TUNING")

    try:
        notices, raw_json_path, attachment_jobs, total_items, deduped_items = collect_bid_notices(settings)
        insert_notices(run_id=run_id, notices=notices, raw_json_path=raw_json_path)

        download_results = download_attachments(attachment_jobs)
        downloaded_count = sum(1 for x in download_results if x["status"] == "DOWNLOADED")
        failed_count = sum(1 for x in download_results if x["status"] == "FAILED")

        direct_items = [x for x in notices if x.get("_label") == "Direct"]
        adjacent_items = [x for x in notices if x.get("_label") == "Adjacent"]
        excluded_items = [x for x in notices if x.get("_label") == "Exclude"]

        finished_at = datetime.now().isoformat(timespec="seconds")
        finish_run(
            run_id=run_id,
            finished_at=finished_at,
            status="SUCCESS",
            total_found=len(notices),
            raw_json_path=raw_json_path,
            error_summary=None,
        )

        print(f"[SUCCESS] total_items={total_items} deduped_items={deduped_items} final_items={len(notices)}")
        print(f"[ATTACHMENTS] jobs={len(attachment_jobs)} downloaded={downloaded_count} failed={failed_count}")
        print(f"[RAW_JSON] {raw_json_path}")

        print_section("Direct Opportunities", direct_items)
        print_section("Adjacent Opportunities", adjacent_items)
        print_section("Excluded", excluded_items, limit=5)

    except Exception as e:
        finished_at = datetime.now().isoformat(timespec="seconds")
        finish_run(
            run_id=run_id,
            finished_at=finished_at,
            status="FAIL",
            total_found=0,
            raw_json_path=None,
            error_summary=str(e),
        )
        raise


if __name__ == "__main__":
    main()