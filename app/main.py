from __future__ import annotations

from datetime import datetime

from app.config import Settings, ensure_directories
from app.db.repository import create_run, finish_run, init_db, insert_notices
from app.services.collect_notices import collect_bid_notices
from app.services.download_docs import download_attachments


def main() -> None:
    ensure_directories()
    init_db()

    settings = Settings.from_env()
    started_at = datetime.now().isoformat(timespec="seconds")
    run_id = create_run(started_at=started_at, keyword=settings.keyword)

    try:
        notices, raw_json_path, attachment_jobs, total_items, active_items = collect_bid_notices(settings)
        insert_notices(run_id=run_id, notices=notices, raw_json_path=raw_json_path)

        download_results = download_attachments(attachment_jobs)
        downloaded_count = sum(1 for x in download_results if x["status"] == "DOWNLOADED")
        failed_count = sum(1 for x in download_results if x["status"] == "FAILED")

        finished_at = datetime.now().isoformat(timespec="seconds")
        finish_run(
            run_id=run_id,
            finished_at=finished_at,
            status="SUCCESS",
            total_found=len(notices),
            raw_json_path=raw_json_path,
            error_summary=None,
        )

        print(f"[SUCCESS] total_items={total_items} active_items={active_items} candidate_items={len(notices)}")
        print(f"[ATTACHMENTS] jobs={len(attachment_jobs)} downloaded={downloaded_count} failed={failed_count}")
        print(f"[RAW_JSON] {raw_json_path}")

        print("\n[TOP CANDIDATES]")
        for item in notices[:10]:
            print(
                f"- grade={item.get('_grade')} "
                f"score={item.get('_score')} "
                f"title={item.get('bidNtceNm', '')} "
                f"reasons={item.get('_reasons', '')}"
            )

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