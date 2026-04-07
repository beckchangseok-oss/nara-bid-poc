from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from app.clients.bid_api import BidApiClient
from app.config import RAW_DIR, Settings

# 1) 핵심 직접 연관 키워드: 사업계획서의 SAS/데이터분석/AI 전환 축
CORE_KEYWORDS = {
    "sas": 12,
    "aml": 10,
    "통계": 8,
    "데이터분석": 8,
    "분석시스템": 8,
    "분석 플랫폼": 8,
    "보건의료인력정보시스템": 10,
    "csv": 6,
    "대용량 csv": 8,
    "ai 전환": 10,
    "agentic ai": 10,
    "renewal": 6,
    "라이선스 갱신": 6,
    "라이선스 구매": 6,
    "pc 3user": 5,
    "pc 5user": 5,
    "서버 전환": 8,
    "VOC 수집": 10,
    "설문": 10,

}

# 2) 인접 기회 키워드: Scailium / Samitech / Medallia / 인프라 확장
ADJACENT_KEYWORDS = {
    "ai": 4,
    "서버": 4,
    "클라우드": 4,
    "컴퓨팅 시스템": 4,
    "병렬처리": 4,
    "데이터 플랫폼": 4,
    "빅데이터": 4,
    "db 전환": 5,
    "sybase": 5,
    "오픈소스 db": 5,
    "cx": 4,
    "nps": 4,
    "dxa": 5,
    "고객경험": 4,
    "환자경험": 4,
    "gen ai": 5,
}

# 3) 타깃 산업/기관 키워드: 공공/금융/통신/유통/물류/교육
SECTOR_KEYWORDS = {
    "공단": 2,
    "공사": 2,
    "정부": 2,
    "행정": 2,
    "건강보험": 3,
    "심사평가": 3,
    "암센터": 3,
    "통계청": 3,
    "법원": 3,
    "은행": 2,
    "카드": 2,
    "보험": 2,
    "증권": 2,
    "저축은행": 2,
    "통신": 2,
    "유통": 2,
    "물류": 2,
    "레저": 2,
    "리조트": 2,
    "대학교": 1,
}

# 4) 강한 제외 키워드: 일반 물품/소모품/농산/선박 등
STRONG_EXCLUDE_KEYWORDS = {
    "우유": -15,
    "급식": -15,
    "부식": -15,
    "농약": -15,
    "골재": -15,
    "자갈": -15,
    "쇄석": -15,
    "소방": -12,
    "축전지": -10,
    "선박": -15,
    "현미경": -12,
    "항응고제": -15,
    "육묘": -15,
    "수산물": -15,
    "공산품": -10,
    "잡석": -15,
}

# 5) 저우선 범용 SW: IT처럼 보이지만 현재 핵심축과 거리가 있음
LOW_PRIORITY_SOFTWARE_KEYWORDS = {
    "adobe": -6,
    "photoshop": -6,
    "illustrator": -6,
    "premiere": -6,
    "acrobat": -4,
    "creative cloud": -6,
    "그래픽소프트웨어": -5,
}

# 최종 판정 기준
A_THRESHOLD = 12
B_THRESHOLD = 6


def get_default_date_range() -> tuple[str, str]:
    now = datetime.now()
    start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    end = now.replace(hour=23, minute=59, second=0, microsecond=0)
    return start.strftime("%Y%m%d%H%M"), end.strftime("%Y%m%d%H%M")


def extract_items(response_json: dict[str, Any]) -> list[dict[str, Any]]:
    response = response_json.get("response", {})
    body = response.get("body", {})
    items = body.get("items", [])

    if isinstance(items, dict):
        if "item" in items:
            item = items["item"]
            if isinstance(item, list):
                return item
            if isinstance(item, dict):
                return [item]
            return []
        return [items]

    if isinstance(items, list):
        return items

    return []


def save_raw_json(response_json: dict[str, Any], keyword: str, page_no: int) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = RAW_DIR / f"bid_list_{keyword}_p{page_no}_{timestamp}.json"
    file_path.write_text(
        json.dumps(response_json, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return str(file_path)


def is_cancel_notice(item: dict[str, Any]) -> bool:
    return str(item.get("ntceKindNm", "")).strip() == "취소공고"


def build_filter_text(item: dict[str, Any]) -> str:
    parts = [
        str(item.get("bidNtceNm", "")),
        str(item.get("prdctSpecNm", "")),
        str(item.get("dtilPrdctClsfcNoNm", "")),
        str(item.get("purchsObjPrdctList", "")),
        str(item.get("ntceInsttNm", "")),
        str(item.get("dminsttNm", "")),
    ]

    for i in range(1, 11):
        parts.append(str(item.get(f"ntceSpecFileNm{i}", "")))

    return " ".join(parts).lower()


def score_by_keyword_map(text: str, keyword_map: dict[str, int]) -> tuple[int, list[str]]:
    score = 0
    matched: list[str] = []

    for keyword, weight in keyword_map.items():
        if keyword.lower() in text:
            score += weight
            matched.append(keyword)

    return score, matched


def score_notice(item: dict[str, Any]) -> dict[str, Any]:
    text = build_filter_text(item)

    score = 0
    reasons: list[str] = []

    core_score, core_matched = score_by_keyword_map(text, CORE_KEYWORDS)
    score += core_score
    if core_matched:
        reasons.append(f"CORE={','.join(core_matched[:5])}")

    adj_score, adj_matched = score_by_keyword_map(text, ADJACENT_KEYWORDS)
    score += adj_score
    if adj_matched:
        reasons.append(f"ADJ={','.join(adj_matched[:5])}")

    sector_score, sector_matched = score_by_keyword_map(text, SECTOR_KEYWORDS)
    score += sector_score
    if sector_matched:
        reasons.append(f"SECTOR={','.join(sector_matched[:5])}")

    exclude_score, exclude_matched = score_by_keyword_map(text, STRONG_EXCLUDE_KEYWORDS)
    score += exclude_score
    if exclude_matched:
        reasons.append(f"EXCLUDE={','.join(exclude_matched[:5])}")

    low_sw_score, low_sw_matched = score_by_keyword_map(text, LOW_PRIORITY_SOFTWARE_KEYWORDS)
    score += low_sw_score
    if low_sw_matched:
        reasons.append(f"LOW_SW={','.join(low_sw_matched[:5])}")

    # 첨부문서가 많고 규격서/공고문이 있으면 보조 가점
    file_name_hits = 0
    for i in range(1, 11):
        file_name = str(item.get(f"ntceSpecFileNm{i}", "")).lower()
        if any(x in file_name for x in ["규격서", "입찰공고", "공고문", "제안요청서", "시방서"]):
            file_name_hits += 1
    if file_name_hits > 0:
        score += min(file_name_hits, 3)
        reasons.append(f"DOC_HINT={file_name_hits}")

    # 취소공고는 즉시 제외
    if is_cancel_notice(item):
        return {
            "score": -999,
            "grade": "EXCLUDE",
            "reasons": ["취소공고"],
            "text": text,
        }

    if score >= A_THRESHOLD:
        grade = "A"
    elif score >= B_THRESHOLD:
        grade = "B"
    elif score > 0:
        grade = "HOLD"
    else:
        grade = "EXCLUDE"

    return {
        "score": score,
        "grade": grade,
        "reasons": reasons,
        "text": text,
    }


def extract_attachments(item: dict[str, Any]) -> list[dict[str, str]]:
    attachments: list[dict[str, str]] = []
    bid_no = str(item.get("bidNtceNo", "")).strip()
    bid_ord = str(item.get("bidNtceOrd", "")).strip()

    for i in range(1, 11):
        file_name = str(item.get(f"ntceSpecFileNm{i}", "")).strip()
        file_url = str(item.get(f"ntceSpecDocUrl{i}", "")).strip()

        if not file_name and not file_url:
            continue

        attachments.append(
            {
                "bid_ntce_no": bid_no,
                "bid_ntce_ord": bid_ord,
                "seq": str(i),
                "file_name": file_name,
                "file_url": file_url,
            }
        )

    return attachments


def collect_bid_notices(
    settings: Settings,
) -> tuple[list[dict[str, Any]], str, list[dict[str, str]], int, int]:
    inqry_bgn_dt, inqry_end_dt = get_default_date_range()

    client = BidApiClient(
        service_key=settings.bid_service_key,
        service_url=settings.bid_service_url,
        endpoint=settings.bid_list_endpoint,
    )

    response_json = client.fetch_bid_list(
        inqry_bgn_dt=inqry_bgn_dt,
        inqry_end_dt=inqry_end_dt,
        page_no=1,
        num_of_rows=settings.page_size,
        inqry_div=settings.inqry_div,
    )

    raw_json_path = save_raw_json(response_json, settings.keyword, page_no=1)
    items = extract_items(response_json)
    active_items = [item for item in items if not is_cancel_notice(item)]

    scored_items: list[dict[str, Any]] = []
    for item in active_items:
        result = score_notice(item)
        item["_score"] = result["score"]
        item["_grade"] = result["grade"]
        item["_reasons"] = " | ".join(result["reasons"])
        scored_items.append(item)

    # A/B 등급만 다운로드 대상으로 사용
    candidate_items = [
        item for item in scored_items
        if item.get("_grade") in ("A", "B")
    ]

    # 점수 높은 순 정렬
    candidate_items.sort(key=lambda x: x.get("_score", 0), reverse=True)

    attachment_jobs: list[dict[str, str]] = []
    for item in candidate_items:
        attachment_jobs.extend(extract_attachments(item))

    return candidate_items, raw_json_path, attachment_jobs, len(items), len(active_items)