from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from typing import Any

from app.clients.bid_api import PublicApiClient
from app.config import BID_ENDPOINTS, PRESPEC_ENDPOINTS, RAW_DIR, Settings

DIRECT_QUERY_KEYWORDS = [
    "SAS",
    "AML",
    "통계",
    "데이터분석",
    "통계분석",
    "CSV",
    "리뉴얼",
    "분석 플랫폼",
]

ADJACENT_QUERY_KEYWORDS = [
    "AI",
    "클라우드",
    "빅데이터",
    "DB 전환",
    "CX",
    "VOC",
    "NPS",
    "환자경험",
    "서버 전환",
    "분석 서버",
]

DIRECT_SCORE_MAP = {
    "aml": 35,
    "통계": 14,
    "통계분석": 22,
    "데이터분석": 22,
    "통계패키지": 18,
    "통계프로그램": 18,
    "분석 플랫폼": 16,
    "분석플랫폼": 16,
    "csv": 10,
    "리뉴얼": 10,
    "renewal": 10,
    "교육": 4,
    "운영": 4,
    "위탁": 4,
}

ADJACENT_SCORE_MAP = {
    "ai": 10,
    "agentic ai": 14,
    "클라우드": 8,
    "빅데이터": 10,
    "데이터 플랫폼": 10,
    "db 전환": 12,
    "cx": 8,
    "voc": 8,
    "nps": 8,
    "dxa": 8,
    "환자경험": 10,
    "통계시스템": 10,
    "통계플랫폼": 10,
    "분석 서버": 10,
    "서버 전환": 12,
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
    "acrobat": -25,
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


def has_license_context(text: str) -> bool:
    patterns = [
        r"(sas|aml|통계|통계분석|데이터분석|분석\s*플랫폼|분석플랫폼|ai|cx|voc|nps|dxa|db\s*전환).{0,20}(라이선스|임차|구매|리뉴얼)",
        r"(라이선스|임차|구매|리뉴얼).{0,20}(sas|aml|통계|통계분석|데이터분석|분석\s*플랫폼|분석플랫폼|ai|cx|voc|nps|dxa|db\s*전환)",
    ]
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns)


def count_generic_software_hits(text: str) -> int:
    generic_terms = [
        "zoom",
        "teams",
        "microsoft 365",
        "office 365",
        "ms ovs",
        "한글과컴퓨터",
        "한컴",
        "v3",
        "알약",
        "acrobat",
    ]
    return sum(1 for term in generic_terms if term in text.lower())


def is_generic_hardware_goods(item: dict[str, Any], text: str) -> bool:
    if item.get("_source_kind") != "bid":
        return False
    if item.get("_source_type") != "goods":
        return False

    hardware_terms = [
        "서버",
        "gpu",
        "gpu서버",
        "gpu 서버",
        "워크스테이션",
        "맥북",
        "맥북프로",
        "노트북",
        "pc",
        "모니터",
        "장비",
    ]
    business_terms = [
        "sas",
        "aml",
        "통계",
        "통계분석",
        "데이터분석",
        "분석 플랫폼",
        "분석플랫폼",
        "라이선스",
        "소프트웨어",
        "sw",
        "db",
        "클라우드",
        "ai 플랫폼",
    ]

    has_hardware = any(term in text for term in hardware_terms)
    has_business_context = any(term in text for term in business_terms)

    return has_hardware and not has_business_context


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
        str(item.get("bidNtceNo", "")),
        str(item.get("bfSpecRgstNo", "")),
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
    license_context = has_license_context(text)
    generic_sw_hits = count_generic_software_hits(text)
    generic_hw_goods = is_generic_hardware_goods(item, text)

    if exact_sas:
        total_score += 50
        reasons.append("EXACT_SAS=Y")

    if license_context:
        total_score += 10
        reasons.append("LICENSE_CONTEXT=Y")

    if str(item.get("swBizObjYn", "")).strip().upper() == "Y":
        total_score += 4
        reasons.append("SW=Y")

    if item.get("_query_track") == "direct":
        total_score += 2

    if item.get("_source_kind") == "prespec" and item.get("_source_type") == "service":
        total_score += 15
        reasons.append("PRESPEC_SERVICE=Y")
    elif item.get("_source_kind") == "prespec":
        total_score += 6
        reasons.append("PRESPEC=Y")

    if generic_sw_hits > 0 and not exact_sas:
        total_score -= 20
        reasons.append(f"GENERIC_SW={generic_sw_hits}")

    if generic_hw_goods and not exact_sas:
        total_score -= 35
        reasons.append("GENERIC_HARDWARE_GOODS=Y")

    if direct_hits:
        reasons.append(f"DIRECT={','.join(direct_hits[:5])}")
    if adjacent_hits:
        reasons.append(f"ADJ={','.join(adjacent_hits[:5])}")
    if sector_hits:
        reasons.append(f"SECTOR={','.join(sector_hits[:5])}")
    if exclude_hits:
        reasons.append(f"EXCLUDE={','.join(exclude_hits[:5])}")

    has_strong_direct = exact_sas or any(
        term in text
        for term in [
            "aml",
            "데이터분석",
            "통계분석",
            "분석 플랫폼",
            "분석플랫폼",
            "통계프로그램",
            "통계패키지",
        ]
    )

    has_business_fit = has_strong_direct or license_context

    if exclude_score <= -80 and not exact_sas and not has_strong_direct:
        label = "Exclude"
    elif exact_sas:
        label = "Direct"
    elif has_business_fit and total_score >= 20:
        label = "Direct"
    elif total_score >= 10 and (
        adjacent_score > 0
        or sector_score > 0
        or "통계시스템" in text
        or "통계플랫폼" in text
        or item.get("_source_kind") == "prespec"
    ):
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
        file_name_field = f"ntceSpecFileNm{i}"
        file_url_field = f"ntceSpecDocUrl{i}"

        if not merged.get(file_name_field) and new_item.get(file_name_field):
            merged[file_name_field] = new_item[file_name_field]
        if not merged.get(file_url_field) and new_item.get(file_url_field):
            merged[file_url_field] = new_item[file_url_field]

    for i in range(1, 6):
        field = f"specDocFileUrl{i}"
        if not merged.get(field) and new_item.get(field):
            merged[field] = new_item[field]

    if not merged.get("purchsObjPrdctList") and new_item.get("purchsObjPrdctList"):
        merged["purchsObjPrdctList"] = new_item["purchsObjPrdctList"]

    if not merged.get("prdctDtlList") and new_item.get("prdctDtlList"):
        merged["prdctDtlList"] = new_item["prdctDtlList"]

    if not merged.get("bidNtceNoList") and new_item.get("bidNtceNoList"):
        merged["bidNtceNoList"] = new_item["bidNtceNoList"]

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


def attachment_priority(item: dict[str, Any]) -> int:
    score = int(item.get("_score", 0))
    text = build_text(item)

    if item.get("_label") == "Direct":
        score += 20

    if item.get("_source_kind") == "prespec" and item.get("_source_type") == "service":
        score += 40
    elif item.get("_source_kind") == "prespec":
        score += 20
    elif item.get("_source_kind") == "bid" and item.get("_source_type") == "service":
        score += 5
    elif item.get("_source_kind") == "bid" and item.get("_source_type") == "goods":
        score -= 20

    if str(item.get("swBizObjYn", "")).strip().upper() == "Y":
        score += 8

    if is_generic_hardware_goods(item, text) and not has_exact_sas(text):
        score -= 30

    return score


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
    selected_ids: set[str] = set()

    def add_attachment_targets(items: list[dict[str, Any]]) -> None:
        for item in items:
            record_id = str(item.get("_record_id", ""))
            if record_id in selected_ids:
                continue
            selected_ids.add(record_id)
            attachment_jobs.extend(extract_attachments(item))

    add_attachment_targets(direct_items)

    prespec_service_adjacent = sorted(
        [x for x in adjacent_items if x["_source_kind"] == "prespec" and x["_source_type"] == "service"],
        key=attachment_priority,
        reverse=True,
    )[:20]
    add_attachment_targets(prespec_service_adjacent)

    prespec_goods_adjacent_sw = sorted(
        [
            x
            for x in adjacent_items
            if x["_source_kind"] == "prespec"
            and x["_source_type"] == "goods"
            and str(x.get("swBizObjYn", "")).strip().upper() == "Y"
        ],
        key=attachment_priority,
        reverse=True,
    )[:8]
    add_attachment_targets(prespec_goods_adjacent_sw)

    bid_service_adjacent = sorted(
        [x for x in adjacent_items if x["_source_kind"] == "bid" and x["_source_type"] == "service"],
        key=attachment_priority,
        reverse=True,
    )[:5]
    add_attachment_targets(bid_service_adjacent)

    manifest = {
        "captured_at": datetime.now().isoformat(timespec="seconds"),
        "range": {"start_dt": start_dt, "end_dt": end_dt},
        "total_raw_items": len(all_items),
        "total_deduped_items": len(deduped_items),
        "direct_count": len(direct_items),
        "adjacent_count": len(adjacent_items),
        "exclude_count": len(excluded_items),
        "attachment_policy": {
            "direct_all": True,
            "adjacent_prespec_service_top_n": 20,
            "adjacent_prespec_goods_sw_top_n": 8,
            "adjacent_bid_service_top_n": 5,
            "adjacent_bid_goods": "skip",
        },
        "calls": manifest_calls,
    }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_json_path = save_json(manifest, f"day2_manifest_{timestamp}.json")

    return final_items, raw_json_path, attachment_jobs, len(all_items), len(deduped_items)