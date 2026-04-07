from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any



from app.clients.bid_api import PublicApiClient
from app.config import BID_ENDPOINTS, PRESPEC_ENDPOINTS, RAW_DIR, Settings

DIRECT_QUERY_KEYWORDS = [
    "SAS",
    "AML",
    "통계",
    "데이터분석",
    "CSV",
    "라이선스",
    "리뉴얼",
    "분석 플랫폼",
]

ADJACENT_QUERY_KEYWORDS = [
    "AI",
    "서버",
    "클라우드",
    "빅데이터",
    "DB 전환",
    "CX",
    "VOC",
    "NPS",
    "환자경험",
]

DIRECT_SCORE_MAP = {
    "aml": 35,
    "통계": 18,
    "데이터분석": 22,
    "통계분석": 22,
    "분석 플랫폼": 16,
    "분석플랫폼": 16,
    "csv": 10,
    "리뉴얼": 10,
    "renewal": 10,
    "교육": 6,
    "운영": 6,
    "위탁": 6,
}

ADJACENT_SCORE_MAP = {
    "ai": 10,
    "agentic ai": 14,
    "서버": 8,
    "서버 전환": 12,
    "클라우드": 8,
    "빅데이터": 10,
    "데이터 플랫폼": 10,
    "db 전환": 12,
    "cx": 8,
    "voc": 8,
    "nps": 8,
    "dxa": 8,
    "환자경험": 10,
}

SECTOR_SCORE_MAP = {
    "공단": 4,
    "건강보험": 6,
    "심사평가": 6,
    "암센터": 6,
    "통계청": 6,
    "법원": 6,
    "은행": 4,
    "카드": 4,
    "보험": 4,
    "공공": 4,
    "금융": 4,
    "물류": 4,
    "유통": 4,
}

EXCLUDE_SCORE_MAP = {
    "우유": -100,
    "급식": -100,
    "부식": -100,
    "농약": -100,
    "골재": -100,
    "자갈": -100,
    "쇄석": -100,
    "소방": -80,
    "선박": -100,
    "축전지": -80,
    "현미경": -80,
    "수산물": -100,
    "adobe": -50,
    "photoshop": -50,
    "illustrator": -50,
    "creative cloud": -50,
    "zoom": -40,
    "teams": -30,
    "microsoft 365": -40,
    "office 365": -40,
    "ms ovs": -40,
    "한글과컴퓨터": -35,
    "한컴": -25,
    "v3": -25,
    "알약": -25,
}


def get_default_date_range(lookback_days: int) -> tuple[str, str]:
    now = datetime.now()
    start = (now - timedelta(days=lookback_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    end = now.replace(hour=23, minute=59, second=0, microsecond=0)
    return start.strftime("%Y%m%d%H%M"), end.strftime("%Y%m%d%H%M")


def save_json(payload: dict[str, Any], filename: str) -> str:
    path = RAW_DIR / filename
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def extract_items(response_json: dict[str, Any]) -> list[dict[str, Any]]:
    response = response_json.get("response", {})
    body = response.get("body", {})
    items = body.get("items", {})

    if isinstance(items, dict):
        item = items.get("item")
        if isinstance(item, list):
            return item
        if isinstance(item, dict):
            return [item]
        return []

    if isinstance(items, list):
        return items

    return []


def score_by_map(text: str, score_map: dict[str, int]) -> tuple[int, list[str]]:
    score = 0
    hits: list[str] = []

    for keyword, weight in score_map.items():
        if keyword.lower() in text:
            score += weight
            hits.append(keyword)

    return score, hits

def has_exact_sas(text: str) -> bool:
    patterns = [
        r"(?<![a-z0-9])sas(?![a-z0-9])",
        r"\bsas\s+eg\b",
        r"\(sas\)",
        r"통계패키지\s*sas",
        r"sas\s*라이선스",
        r"sas\s*통계",
    ]
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns)


def has_exact_license_context(text: str) -> bool:
    context_patterns = [
        r"(sas|aml|통계|데이터분석|통계분석|분석\s*플랫폼|ai|cx|voc|nps|dxa|db\s*전환).{0,20}(라이선스|임차|구매|리뉴얼)",
        r"(라이선스|임차|구매|리뉴얼).{0,20}(sas|aml|통계|데이터분석|통계분석|분석\s*플랫폼|ai|cx|voc|nps|dxa|db\s*전환)",
    ]
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in context_patterns)


def count_generic_software_hits(text: str) -> int:
    generic_terms = [
        "zoom", "teams", "microsoft 365", "office 365", "ms ovs",
        "한글과컴퓨터", "한컴", "v3", "알약", "acrobat"
    ]
    return sum(1 for term in generic_terms if term.lower() in text.lower())


def build_bid_ppssrch_params(keyword: str, start_dt: str, end_dt: str) -> dict[str, Any]:
    return {
        "inqryDiv": "1",
        "inqryBgnDt": start_dt,
        "inqryEndDt": end_dt,
        "bidNtceNm": keyword,
        "pageNo": 1,
        "numOfRows": 50,
    }


def build_prespec_ppssrch_params(keyword: str, start_dt: str, end_dt: str) -> dict[str, Any]:
    return {
        "inqryDiv": "1",
        "inqryBgnDt": start_dt,
        "inqryEndDt": end_dt,
        "prdctClsfcNoNm": keyword,
        "pageNo": 1,
        "numOfRows": 50,
    }


def normalize_bid_item(item: dict[str, Any], source_type: str, query_track: str, keyword: str) -> dict[str, Any]:
    normalized = dict(item)
    normalized["_source_kind"] = "bid"
    normalized["_source_type"] = source_type
    normalized["_query_track"] = query_track
    normalized["_query_keyword"] = keyword
    normalized["_primary_id"] = str(item.get("bidNtceNo", "")).strip()
    normalized["_sub_id"] = str(item.get("bidNtceOrd", "")).strip()
    normalized["_record_id"] = f"BID|{normalized['_source_type']}|{normalized['_primary_id']}|{normalized['_sub_id']}"
    normalized["_title"] = str(item.get("bidNtceNm", "")).strip()
    normalized["_pub_org"] = str(item.get("ntceInsttNm", "")).strip()
    normalized["_demand_org"] = str(item.get("dminsttNm", "")).strip()
    normalized["_close_dt"] = str(item.get("bidClseDt", "")).strip()
    normalized["_detail_url"] = str(item.get("bidNtceDtlUrl", "") or item.get("bidNtceUrl", "")).strip()
    normalized["_ref_no"] = str(item.get("refNo", "")).strip()
    return normalized


def normalize_prespec_item(item: dict[str, Any], source_type: str, query_track: str, keyword: str) -> dict[str, Any]:
    normalized = dict(item)
    normalized["_source_kind"] = "prespec"
    normalized["_source_type"] = source_type
    normalized["_query_track"] = query_track
    normalized["_query_keyword"] = keyword
    normalized["_primary_id"] = str(item.get("bfSpecRgstNo", "")).strip()
    normalized["_sub_id"] = ""
    normalized["_record_id"] = f"PRESPEC|{normalized['_source_type']}|{normalized['_primary_id']}"
    normalized["_title"] = str(item.get("prdctClsfcNoNm", "")).strip()
    normalized["_pub_org"] = str(item.get("orderInsttNm", "")).strip()
    normalized["_demand_org"] = str(item.get("rlDminsttNm", "")).strip()
    normalized["_close_dt"] = str(item.get("opninRgstClseDt", "")).strip()
    normalized["_detail_url"] = ""
    normalized["_ref_no"] = str(item.get("refNo", "")).strip()
    return normalized


def build_text(item: dict[str, Any]) -> str:
    parts = [
        str(item.get("_title", "")),
        str(item.get("_pub_org", "")),
        str(item.get("_demand_org", "")),
        str(item.get("_ref_no", "")),
        str(item.get("refNo", "")),
        str(item.get("prdctClsfcNoNm", "")),
        str(item.get("purchsObjPrdctList", "")),
        str(item.get("prdctDtlList", "")),
        str(item.get("bidNtceNoList", "")),
        str(item.get("_query_keyword", "")),
    ]

    for i in range(1, 11):
        parts.append(str(item.get(f"ntceSpecFileNm{i}", "")))
    for i in range(1, 6):
        parts.append(str(item.get(f"specDocFileUrl{i}", "")))

    return " ".join(parts).lower()


def classify_notice(item: dict[str, Any]) -> dict[str, Any]:
    text = build_text(item)

    direct_score, direct_hits = score_by_map(text, DIRECT_SCORE_MAP)
    adjacent_score, adjacent_hits = score_by_map(text, ADJACENT_SCORE_MAP)
    sector_score, sector_hits = score_by_map(text, SECTOR_SCORE_MAP)
    exclude_score, exclude_hits = score_by_map(text, EXCLUDE_SCORE_MAP)

    total_score = direct_score + adjacent_score + sector_score + exclude_score
    reasons: list[str] = []

    exact_sas = has_exact_sas(text)
    license_context = has_exact_license_context(text)
    generic_sw_hits = count_generic_software_hits(text)

    if exact_sas:
        total_score += 50
        reasons.append("EXACT_SAS=Y")

    if license_context:
        total_score += 10
        reasons.append("LICENSE_CONTEXT=Y")

    # swBizObjYn 은 참고 신호만 사용
    if str(item.get("swBizObjYn", "")).strip().upper() == "Y":
        total_score += 4
        reasons.append("SW=Y")

    # direct query로 잡힌 건 약한 보조점수만
    if item.get("_query_track") == "direct":
        total_score += 2

    # 범용SW 다수 포함 시 강하게 내림
    if generic_sw_hits > 0 and not exact_sas:
        total_score -= 20
        reasons.append(f"GENERIC_SW={generic_sw_hits}")

    if direct_hits:
        reasons.append(f"DIRECT={','.join(direct_hits[:5])}")
    if adjacent_hits:
        reasons.append(f"ADJ={','.join(adjacent_hits[:5])}")
    if sector_hits:
        reasons.append(f"SECTOR={','.join(sector_hits[:5])}")
    if exclude_hits:
        reasons.append(f"EXCLUDE={','.join(exclude_hits[:5])}")

    has_strong_direct = exact_sas or any(
        k in text for k in ["aml", "데이터분석", "통계분석", "분석 플랫폼", "분석플랫폼"]
    )

    has_business_fit = has_strong_direct or license_context

    if exclude_score <= -80 and not exact_sas:
        label = "Exclude"
    elif exact_sas:
        label = "Direct"
    elif has_business_fit and total_score >= 20:
        label = "Direct"
    elif adjacent_score > 0 or sector_score > 0:
        label = "Adjacent"
    else:
        label = "Exclude"

    return {
        "label": label,
        "score": total_score,
        "reasons": reasons,
    }


def merge_item(base: dict[str, Any], new_item: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)

    query_keywords = set(merged.get("_query_keywords", []))
    query_keywords.add(str(new_item.get("_query_keyword", "")).strip())
    merged["_query_keywords"] = sorted(x for x in query_keywords if x)

    query_tracks = set(merged.get("_query_tracks", []))
    query_tracks.add(str(new_item.get("_query_track", "")).strip())
    merged["_query_tracks"] = sorted(x for x in query_tracks if x)

    for field in ["_title", "_pub_org", "_demand_org", "_close_dt", "_detail_url", "_ref_no"]:
        if not merged.get(field) and new_item.get(field):
            merged[field] = new_item[field]

    for i in range(1, 11):
        field = f"ntceSpecFileNm{i}"
        if not merged.get(field) and new_item.get(field):
            merged[field] = new_item[field]
        field = f"ntceSpecDocUrl{i}"
        if not merged.get(field) and new_item.get(field):
            merged[field] = new_item[field]

    for i in range(1, 6):
        field = f"specDocFileUrl{i}"
        if not merged.get(field) and new_item.get(field):
            merged[field] = new_item[field]

    if not merged.get("purchsObjPrdctList") and new_item.get("purchsObjPrdctList"):
        merged["purchsObjPrdctList"] = new_item["purchsObjPrdctList"]
    if not merged.get("prdctDtlList") and new_item.get("prdctDtlList"):
        merged["prdctDtlList"] = new_item["prdctDtlList"]

    return merged


def dedupe_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}

    for item in items:
        key = item["_record_id"]
        if key not in deduped:
            item["_query_keywords"] = [item["_query_keyword"]]
            item["_query_tracks"] = [item["_query_track"]]
            deduped[key] = item
        else:
            deduped[key] = merge_item(deduped[key], item)

    return list(deduped.values())


def extract_attachments(item: dict[str, Any]) -> list[dict[str, str]]:
    jobs: list[dict[str, str]] = []
    record_id = str(item.get("_record_id", "")).strip()

    if item.get("_source_kind") == "bid":
        for i in range(1, 11):
            file_name = str(item.get(f"ntceSpecFileNm{i}", "")).strip()
            file_url = str(item.get(f"ntceSpecDocUrl{i}", "")).strip()
            if not file_url:
                continue
            jobs.append(
                {
                    "record_id": record_id,
                    "seq": str(i),
                    "file_name": file_name or f"bid_attachment_{i}",
                    "file_url": file_url,
                }
            )
    else:
        for i in range(1, 6):
            file_url = str(item.get(f"specDocFileUrl{i}", "")).strip()
            if not file_url:
                continue
            jobs.append(
                {
                    "record_id": record_id,
                    "seq": str(i),
                    "file_name": f"prespec_attachment_{i}",
                    "file_url": file_url,
                }
            )

    return jobs


def run_bid_ppssrch(
    client: PublicApiClient,
    endpoint: str,
    source_type: str,
    query_track: str,
    keyword: str,
    start_dt: str,
    end_dt: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    params = build_bid_ppssrch_params(keyword=keyword, start_dt=start_dt, end_dt=end_dt)
    response_json = client.fetch(endpoint=endpoint, params=params)
    items = [
        normalize_bid_item(x, source_type=source_type, query_track=query_track, keyword=keyword)
        for x in extract_items(response_json)
    ]
    return items, response_json


def run_prespec_ppssrch(
    client: PublicApiClient,
    endpoint: str,
    source_type: str,
    query_track: str,
    keyword: str,
    start_dt: str,
    end_dt: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    params = build_prespec_ppssrch_params(keyword=keyword, start_dt=start_dt, end_dt=end_dt)
    response_json = client.fetch(endpoint=endpoint, params=params)
    items = [
        normalize_prespec_item(x, source_type=source_type, query_track=query_track, keyword=keyword)
        for x in extract_items(response_json)
    ]
    return items, response_json


def collect_bid_notices(
    settings: Settings,
) -> tuple[list[dict[str, Any]], str, list[dict[str, str]], int, int]:
    start_dt, end_dt = get_default_date_range(settings.lookback_days)

    bid_client = PublicApiClient(
        service_key=settings.bid_service_key,
        service_url=settings.bid_service_url,
        timeout=settings.request_timeout,
    )
    prespec_client = PublicApiClient(
        service_key=settings.prespec_service_key,
        service_url=settings.prespec_service_url,
        timeout=settings.request_timeout,
    )

    all_items: list[dict[str, Any]] = []
    manifest_calls: list[dict[str, Any]] = []

    retrieval_plan = [
        ("direct", DIRECT_QUERY_KEYWORDS),
        ("adjacent", ADJACENT_QUERY_KEYWORDS),
    ]

    for query_track, keywords in retrieval_plan:
        for keyword in keywords:
            # 입찰공고 - 물품
            bid_goods_items, bid_goods_resp = run_bid_ppssrch(
                client=bid_client,
                endpoint=BID_ENDPOINTS["goods_pps"],
                source_type="goods",
                query_track=query_track,
                keyword=keyword,
                start_dt=start_dt,
                end_dt=end_dt,
            )
            all_items.extend(bid_goods_items)
            manifest_calls.append(
                {
                    "source_kind": "bid",
                    "source_type": "goods",
                    "query_track": query_track,
                    "keyword": keyword,
                    "count": len(bid_goods_items),
                    "endpoint": BID_ENDPOINTS["goods_pps"],
                    "response_header": bid_goods_resp.get("response", {}).get("header", {}),
                }
            )

            # 입찰공고 - 용역
            bid_service_items, bid_service_resp = run_bid_ppssrch(
                client=bid_client,
                endpoint=BID_ENDPOINTS["service_pps"],
                source_type="service",
                query_track=query_track,
                keyword=keyword,
                start_dt=start_dt,
                end_dt=end_dt,
            )
            all_items.extend(bid_service_items)
            manifest_calls.append(
                {
                    "source_kind": "bid",
                    "source_type": "service",
                    "query_track": query_track,
                    "keyword": keyword,
                    "count": len(bid_service_items),
                    "endpoint": BID_ENDPOINTS["service_pps"],
                    "response_header": bid_service_resp.get("response", {}).get("header", {}),
                }
            )

            # 사전규격 - 물품
            prespec_goods_items, prespec_goods_resp = run_prespec_ppssrch(
                client=prespec_client,
                endpoint=PRESPEC_ENDPOINTS["goods_pps"],
                source_type="goods",
                query_track=query_track,
                keyword=keyword,
                start_dt=start_dt,
                end_dt=end_dt,
            )
            all_items.extend(prespec_goods_items)
            manifest_calls.append(
                {
                    "source_kind": "prespec",
                    "source_type": "goods",
                    "query_track": query_track,
                    "keyword": keyword,
                    "count": len(prespec_goods_items),
                    "endpoint": PRESPEC_ENDPOINTS["goods_pps"],
                    "response_header": prespec_goods_resp.get("response", {}).get("header", {}),
                }
            )

            # 사전규격 - 용역
            prespec_service_items, prespec_service_resp = run_prespec_ppssrch(
                client=prespec_client,
                endpoint=PRESPEC_ENDPOINTS["service_pps"],
                source_type="service",
                query_track=query_track,
                keyword=keyword,
                start_dt=start_dt,
                end_dt=end_dt,
            )
            all_items.extend(prespec_service_items)
            manifest_calls.append(
                {
                    "source_kind": "prespec",
                    "source_type": "service",
                    "query_track": query_track,
                    "keyword": keyword,
                    "count": len(prespec_service_items),
                    "endpoint": PRESPEC_ENDPOINTS["service_pps"],
                    "response_header": prespec_service_resp.get("response", {}).get("header", {}),
                }
            )

    deduped_items = dedupe_items(all_items)

    scored_items: list[dict[str, Any]] = []
    for item in deduped_items:
        result = classify_notice(item)
        item["_label"] = result["label"]
        item["_score"] = result["score"]
        item["_reasons"] = " | ".join(result["reasons"])
        scored_items.append(item)

    direct_items = [x for x in scored_items if x["_label"] == "Direct"]
    adjacent_items = [x for x in scored_items if x["_label"] == "Adjacent"]
    excluded_items = [x for x in scored_items if x["_label"] == "Exclude"]

    direct_items.sort(key=lambda x: x["_score"], reverse=True)
    adjacent_items.sort(key=lambda x: x["_score"], reverse=True)
    excluded_items.sort(key=lambda x: x["_score"], reverse=True)

    final_items = direct_items + adjacent_items + excluded_items

    attachment_jobs: list[dict[str, str]] = []
    for item in direct_items:
        attachment_jobs.extend(extract_attachments(item))
    for item in adjacent_items[:10]:
        attachment_jobs.extend(extract_attachments(item))

    manifest = {
        "captured_at": datetime.now().isoformat(timespec="seconds"),
        "range": {"start_dt": start_dt, "end_dt": end_dt},
        "total_raw_items": len(all_items),
        "total_deduped_items": len(deduped_items),
        "direct_count": len(direct_items),
        "adjacent_count": len(adjacent_items),
        "exclude_count": len(excluded_items),
        "calls": manifest_calls,
    }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_json_path = save_json(manifest, f"day2_manifest_{timestamp}.json")

    return final_items, raw_json_path, attachment_jobs, len(all_items), len(deduped_items)