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
    "medallia": 30,
    "scailium": 30,
    "samitech": 30,
    "사미텍": 30,
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
    "정보시스템": 10,
    "시스템 개선": 10,
    "시스템개선": 10,
    "시스템 구축": 10,
    "시스템구축": 10,
    "db구축": 10,
    "db 구축": 10,
    "분석 서버": 10,
    "서버 전환": 12,
    "통계": 8,
    "통계분석": 10,
    "통계 분석": 10,
    "데이터분석": 10,
    "데이터 분석": 10,
    "분석 플랫폼": 10,
    "분석플랫폼": 10,
    "분석사업": 10,
    "분석 사업": 10,
    "통계패키지": 8,
    "통계프로그램": 8,
    "spss": 8,
    "운영": 4,
    "감리": 6,
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

DIRECT_AXIS_TERMS = [
    "sas",
    "aml",
    "medallia",
    "scailium",
    "samitech",
    "사미텍",
]

DIRECT_LICENSE_TERMS = [
    "sas",
    "aml",
    "medallia",
    "scailium",
    "samitech",
    "사미텍",
]

ADJACENT_LICENSE_TERMS = [
    "ai",
    "클라우드",
    "db 전환",
    "cx",
    "voc",
    "nps",
    "dxa",
]

BID_SERVICE_ADJACENT_TERMS = [
    "정보시스템",
    "시스템 개선",
    "시스템개선",
    "시스템 구축",
    "시스템구축",
    "db구축",
    "db 구축",
    "고도화",
]

PROCUREMENT_TERMS = [
    "라이선스",
    "임차",
    "구매",
    "리뉴얼",
    "renewal",
    "소프트웨어",
    "software",
]



def get_default_date_range(lookback_days: int) -> tuple[str, str]:
    now = datetime.now()
    start = (now - timedelta(days=lookback_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    end = now.replace(hour=23, minute=59, second=0, microsecond=0)
    return start.strftime("%Y%m%d%H%M"), end.strftime("%Y%m%d%H%M")

def resolve_date_range(settings: Settings) -> tuple[str, str, str]:
    start_dt = settings.start_dt_override
    end_dt = settings.end_dt_override

    if start_dt or end_dt:
        if not start_dt or not end_dt:
            raise ValueError("START_DT, END_DT는 둘 다 같이 입력해야 함")
        if len(start_dt) != 12 or len(end_dt) != 12:
            raise ValueError("START_DT, END_DT 형식은 YYYYMMDDHHMM 이어야 함")
        return start_dt, end_dt, "fixed_override"

    start_dt, end_dt = get_default_date_range(settings.lookback_days)
    return start_dt, end_dt, "lookback_days"


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


def has_contextual_license_match(text: str, terms: list[str]) -> bool:
    escaped_terms = "|".join(
        sorted((re.escape(term) for term in terms), key=len, reverse=True)
    )
    patterns = [
        rf"({escaped_terms}).{{0,20}}(라이선스|임차|구매|리뉴얼)",
        rf"(라이선스|임차|구매|리뉴얼).{{0,20}}({escaped_terms})",
    ]
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns)


def has_direct_license_context(text: str) -> bool:
    return has_contextual_license_match(text, DIRECT_LICENSE_TERMS)


def has_adjacent_license_context(text: str) -> bool:
    return has_contextual_license_match(text, ADJACENT_LICENSE_TERMS)

def get_axis_hits(text: str, exact_sas: bool) -> list[str]:
    hits: list[str] = []

    if exact_sas:
        hits.append("sas")

    for term in DIRECT_AXIS_TERMS:
        if term == "sas":
            continue
        if term in text and term not in hits:
            hits.append(term)

    return hits


def get_procurement_hits(text: str) -> list[str]:
    hits: list[str] = []

    for term in PROCUREMENT_TERMS:
        if term in text and term not in hits:
            hits.append(term)

    return hits


def has_procurement_context(text: str) -> bool:
    return len(get_procurement_hits(text)) > 0
def get_bid_detail_url(item: dict[str, Any]) -> str:
    return str(
        item.get("_detail_url")
        or item.get("bidNtceDtlUrl")
        or item.get("bidNtceUrl")
        or ""
    ).strip()


def annotate_detail_url_policy(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    annotated: list[dict[str, Any]] = []

    for item in items:
        enriched = dict(item)

        bid_detail_url = get_bid_detail_url(enriched)
        linked_bid_nos = enriched.get("_linked_bid_ntce_nos") or extract_bid_ntce_numbers(enriched)

        if enriched.get("_source_kind") == "bid":
            if bid_detail_url:
                missing_reason = "BID_DETAIL_URL_AVAILABLE"
            else:
                missing_reason = "BID_DETAIL_URL_MISSING"
        else:
            if bid_detail_url:
                missing_reason = "BID_DETAIL_URL_AVAILABLE"
            elif linked_bid_nos:
                missing_reason = "LINKED_BID_NOT_FOUND"
            else:
                missing_reason = "PRESPEC_NO_NATIVE_DETAIL_URL"

        enriched["_bid_detail_url"] = bid_detail_url
        enriched["_detail_url_missing_reason"] = missing_reason
        annotated.append(enriched)

    return annotated

def has_bid_service_adjacent_context(item: dict[str, Any], text: str) -> bool:
    if item.get("_source_kind") != "bid":
        return False
    if item.get("_source_type") != "service":
        return False
    return any(term in text for term in BID_SERVICE_ADJACENT_TERMS)


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

def is_cloud_infra_only_project(text: str) -> bool:
    has_landingzone = ("랜딩존" in text) or ("landing zone" in text) or ("landingzone" in text)
    has_naver_cloud = ("네이버 클라우드" in text) or ("ncloud" in text) or ("ncp" in text)

    business_terms = [
        "데이터분석",
        "데이터 분석",
        "통계분석",
        "통계 분석",
        "통계시스템",
        "voc 시스템",
        "cx",
        "nps",
        "dxa",
        "sas",
        "medallia",
        "scailium",
        "samitech",
        "사미텍",
    ]
    has_business_context = any(term in text for term in business_terms)

    return has_landingzone and has_naver_cloud and not has_business_context


def is_environmental_vocs_project(text: str) -> bool:
    if "vocs" not in text:
        return False

    env_terms = [
        "장비",
        "보호장비",
        "플라스틱 캡",
        "소음",
        "측정",
        "실외",
        "환경",
        "환경보건",
        "ni-52",
        "octave",
        "sound level meter",
    ]
    return any(term in text for term in env_terms)


def is_generic_ai_tooling_project(item: dict[str, Any], text: str) -> bool:
    if item.get("_source_kind") != "bid":
        return False
    if item.get("_source_type") != "goods":
        return False

    has_labeling = ("라벨링" in text) or ("labeling" in text)
    has_authoring = ("저작도구" in text) or ("annotation" in text)
    has_sw = ("sw" in text) or ("소프트웨어" in text)

    has_axis = has_exact_sas(text) or any(
        term in text for term in ["medallia", "scailium", "samitech", "사미텍"]
    )

    return has_labeling and has_authoring and has_sw and not has_axis

def is_industrial_ai_validation_project(text: str) -> bool:
    has_cnc = "cnc" in text
    has_aircraft_part = ("항공 부품" in text) or ("항공부품" in text)
    has_human_error = "휴먼에러" in text
    has_realtime_validation = ("실시간 검증 시스템" in text) or ("실시간검증시스템" in text)
    has_demo = "실증" in text

    has_axis = has_exact_sas(text) or any(
        term in text for term in ["medallia", "scailium", "samitech", "사미텍"]
    )

    return (
        not has_axis
        and has_cnc
        and has_aircraft_part
        and (has_human_error or has_realtime_validation)
        and has_demo
    )

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


def build_bid_detail_params(bid_ntce_no: str) -> dict[str, Any]:
    return {
        "inqryDiv": "2",
        "bidNtceNo": bid_ntce_no,
        "pageNo": 1,
        "numOfRows": 10,
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
    axis_hits = get_axis_hits(text, exact_sas=exact_sas)
    procurement_hits = get_procurement_hits(text)

    direct_license_context = has_direct_license_context(text)
    adjacent_license_context = has_adjacent_license_context(text)
    bid_service_adjacent_context = has_bid_service_adjacent_context(item, text)
    generic_sw_hits = count_generic_software_hits(text)
    generic_hw_goods = is_generic_hardware_goods(item, text)
    cloud_infra_only = is_cloud_infra_only_project(text)
    environmental_vocs = is_environmental_vocs_project(text)
    generic_ai_tooling = is_generic_ai_tooling_project(item, text)
    industrial_ai_validation = is_industrial_ai_validation_project(text)

    if exact_sas:
        total_score += 50
        reasons.append("EXACT_SAS=Y")

    if direct_license_context:
        total_score += 10
        reasons.append("DIRECT_LICENSE_CONTEXT=Y")

    if adjacent_license_context:
        total_score += 4
        reasons.append("ADJ_LICENSE_CONTEXT=Y")

    if bid_service_adjacent_context:
        total_score += 10
        reasons.append("BID_SERVICE_SYSTEM_CONTEXT=Y")

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
    if cloud_infra_only:
        total_score -= 40
        reasons.append("GENERIC_CLOUD_INFRA=Y")

    if environmental_vocs:
        total_score -= 60
        reasons.append("ENVIRONMENTAL_VOCS=Y")

    if generic_ai_tooling:
        total_score -= 40
        reasons.append("GENERIC_AI_TOOLING=Y")

    if industrial_ai_validation:
        total_score -= 50
        reasons.append("INDUSTRIAL_AI_VALIDATION=Y")    

    if axis_hits:
        reasons.append(f"AXIS={','.join(axis_hits[:5])}")
    if procurement_hits:
        reasons.append(f"PROCUREMENT={','.join(procurement_hits[:5])}")
    if adjacent_hits:
        reasons.append(f"ADJ={','.join(adjacent_hits[:5])}")
    if sector_hits:
        reasons.append(f"SECTOR={','.join(sector_hits[:5])}")
    if exclude_hits:
        reasons.append(f"EXCLUDE={','.join(exclude_hits[:5])}")

    has_axis_term = len(axis_hits) > 0
    has_procurement = len(procurement_hits) > 0

    has_explicit_direct_evidence = (
        exact_sas
        or direct_license_context
        or (has_axis_term and has_procurement)
    )

    has_adjacent_business = (
        adjacent_score > 0
        or sector_score > 0
        or adjacent_license_context
        or bid_service_adjacent_context
        or item.get("_source_kind") == "prespec"
    )

    if exclude_score <= -80 and not has_explicit_direct_evidence:
        label = "Exclude"
    elif has_explicit_direct_evidence:
        label = "Direct"
    elif total_score >= 10 and has_adjacent_business:
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


def merge_bid_detail_into_prespec(prespec_item: dict[str, Any], bid_item: dict[str, Any]) -> dict[str, Any]:
    merged = dict(prespec_item)

    for field in [
        "bidNtceNo",
        "bidNtceOrd",
        "bidNtceDt",
        "bidClseDt",
        "opengDt",
        "bidNtceDtlUrl",
        "bidNtceUrl",
        "presmptPrce",
        "asignBdgtAmt",
        "srvceDivNm",
        "purchsObjPrdctList",
        "infoBizYn",
        "pubPrcrmntClsfcNm",
        "pubPrcrmntMidclsfcNm",
        "pubPrcrmntLrgclsfcNm",
    ]:
        if not merged.get(field) and bid_item.get(field):
            merged[field] = bid_item[field]

    for i in range(1, 11):
        file_name_field = f"ntceSpecFileNm{i}"
        file_url_field = f"ntceSpecDocUrl{i}"

        if not merged.get(file_name_field) and bid_item.get(file_name_field):
            merged[file_name_field] = bid_item[file_name_field]
        if not merged.get(file_url_field) and bid_item.get(file_url_field):
            merged[file_url_field] = bid_item[file_url_field]

    linked_bid_nos = set(merged.get("_linked_bid_ntce_nos", []))
    bid_no = str(bid_item.get("bidNtceNo", "")).strip()
    if bid_no:
        linked_bid_nos.add(bid_no)
    merged["_linked_bid_ntce_nos"] = sorted(x for x in linked_bid_nos if x)

    if bid_item.get("bidNtceDtlUrl") and not merged.get("_detail_url"):
        merged["_detail_url"] = str(bid_item.get("bidNtceDtlUrl", "")).strip()

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


def extract_bid_ntce_numbers(item: dict[str, Any]) -> list[str]:
    raw_value = str(item.get("bidNtceNoList", "")).strip()
    if not raw_value:
        return []

    numbers: list[str] = []
    for part in raw_value.split(","):
        value = part.strip()
        if value:
            numbers.append(value)

    seen: set[str] = set()
    result: list[str] = []
    for value in numbers:
        if value not in seen:
            seen.add(value)
            result.append(value)

    return result


def fetch_bid_detail_by_no(
    client: PublicApiClient,
    bid_ntce_no: str,
    source_type: str,
) -> dict[str, Any] | None:
    if source_type == "service":
        endpoint = BID_ENDPOINTS["service_pps"]
    else:
        endpoint = BID_ENDPOINTS["goods_pps"]

    params = build_bid_detail_params(bid_ntce_no=bid_ntce_no)
    response_json = client.fetch(endpoint=endpoint, params=params)
    items = extract_items(response_json)
    if not items:
        return None
    return items[0]


def enrich_prespec_with_bid_details(
    items: list[dict[str, Any]],
    bid_client: PublicApiClient,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    enriched_items: list[dict[str, Any]] = []
    enrich_logs: list[dict[str, Any]] = []

    for item in items:
        if item.get("_source_kind") != "prespec":
            enriched_items.append(item)
            continue

        if item.get("_label") not in {"Direct", "Adjacent"}:
            enriched_items.append(item)
            continue

        bid_numbers = extract_bid_ntce_numbers(item)
        if not bid_numbers:
            enriched_items.append(item)
            continue

        enriched = dict(item)
        for bid_no in bid_numbers:
            try:
                bid_detail = fetch_bid_detail_by_no(
                    client=bid_client,
                    bid_ntce_no=bid_no,
                    source_type=str(item.get("_source_type", "goods")),
                )
                if bid_detail:
                    enriched = merge_bid_detail_into_prespec(enriched, bid_detail)
                    enrich_logs.append(
                        {
                            "prespec_record_id": item.get("_record_id"),
                            "bid_ntce_no": bid_no,
                            "status": "MERGED",
                        }
                    )
                else:
                    enrich_logs.append(
                        {
                            "prespec_record_id": item.get("_record_id"),
                            "bid_ntce_no": bid_no,
                            "status": "NOT_FOUND",
                        }
                    )
            except Exception as exc:
                enrich_logs.append(
                    {
                        "prespec_record_id": item.get("_record_id"),
                        "bid_ntce_no": bid_no,
                        "status": "ERROR",
                        "error": str(exc),
                    }
                )

        enriched_items.append(enriched)

    return enriched_items, enrich_logs


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
        bid_attachment_added = False
        for i in range(1, 11):
            file_name = str(item.get(f"ntceSpecFileNm{i}", "")).strip()
            file_url = str(item.get(f"ntceSpecDocUrl{i}", "")).strip()
            if not file_url:
                continue
            bid_attachment_added = True
            jobs.append(
                {
                    "record_id": record_id,
                    "seq": f"bid_{i}",
                    "file_name": file_name or f"linked_bid_attachment_{i}",
                    "file_url": file_url,
                }
            )

        for i in range(1, 6):
            file_url = str(item.get(f"specDocFileUrl{i}", "")).strip()
            if not file_url:
                continue
            jobs.append(
                {
                    "record_id": record_id,
                    "seq": str(i) if not bid_attachment_added else f"prespec_{i}",
                    "file_name": f"prespec_attachment_{i}",
                    "file_url": file_url,
                }
            )

    return jobs
def count_bid_attachment_urls(item: dict[str, Any]) -> int:
    return sum(
        1
        for i in range(1, 11)
        if str(item.get(f"ntceSpecDocUrl{i}", "")).strip()
    )


def count_prespec_attachment_urls(item: dict[str, Any]) -> int:
    return sum(
        1
        for i in range(1, 6)
        if str(item.get(f"specDocFileUrl{i}", "")).strip()
    )


def detect_attachment_source(item: dict[str, Any]) -> str:
    has_bid = count_bid_attachment_urls(item) > 0
    has_prespec = count_prespec_attachment_urls(item) > 0

    if has_bid and has_prespec:
        return "bid+prespec"
    if has_bid:
        return "bid"
    if has_prespec:
        return "prespec"
    return "none"


def annotate_attachment_diagnostics(
    items: list[dict[str, Any]],
    selected_ids: set[str],
) -> list[dict[str, Any]]:
    annotated: list[dict[str, Any]] = []

    for item in items:
        enriched = dict(item)

        bid_attachment_count = count_bid_attachment_urls(enriched)
        prespec_attachment_count = count_prespec_attachment_urls(enriched)
        source_attachment_count = bid_attachment_count + prespec_attachment_count

        attachment_jobs = extract_attachments(enriched)
        attachment_job_count = len(attachment_jobs)

        record_id = str(enriched.get("_record_id", "")).strip()
        attachment_selected = record_id in selected_ids

        if source_attachment_count == 0:
            missing_reason = "NO_URL_IN_SOURCE"
        elif attachment_job_count == 0:
            missing_reason = "EXTRACT_LOGIC_MISS"
        elif not attachment_selected:
            missing_reason = "NOT_SELECTED_BY_POLICY"
        else:
            missing_reason = ""

        enriched["_bid_attachment_count"] = bid_attachment_count
        enriched["_prespec_attachment_count"] = prespec_attachment_count
        enriched["_attachment_job_count"] = attachment_job_count
        enriched["_attachment_detected_from"] = detect_attachment_source(enriched)
        enriched["_attachment_selected"] = "Y" if attachment_selected else "N"
        enriched["_attachment_missing_reason"] = missing_reason

        annotated.append(enriched)

    return annotated


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

    if item.get("_linked_bid_ntce_nos"):
        score += 12

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
    start_dt, end_dt, date_range_source = resolve_date_range(settings)

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

    scored_items, enrich_logs = enrich_prespec_with_bid_details(
        items=scored_items,
        bid_client=bid_client,
    )

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

    final_items = annotate_attachment_diagnostics(final_items, selected_ids)
    final_items = annotate_detail_url_policy(final_items)

    manifest = {
        "captured_at": datetime.now().isoformat(timespec="seconds"),
        "range": {"start_dt": start_dt, "end_dt": end_dt},
        "total_raw_items": len(all_items),
        "total_deduped_items": len(deduped_items),
        "direct_count": len(direct_items),
        "adjacent_count": len(adjacent_items),
        "exclude_count": len(excluded_items),
        "enrich_summary": {
            "attempted": len(enrich_logs),
            "merged": sum(1 for x in enrich_logs if x.get("status") == "MERGED"),
            "not_found": sum(1 for x in enrich_logs if x.get("status") == "NOT_FOUND"),
            "error": sum(1 for x in enrich_logs if x.get("status") == "ERROR"),
        },
        "attachment_policy": {
            "direct_all": True,
            "adjacent_prespec_service_top_n": 20,
            "adjacent_prespec_goods_sw_top_n": 8,
            "adjacent_bid_service_top_n": 5,
            "adjacent_bid_goods": "skip",
        },
        "attachments_scope": "selected_download_targets_only",
        "detail_url_policy": "bid_only_or_enriched_bid",
        "classification_policy_version": "direct_axis_plus_procurement_v2",
        "calls": manifest_calls,
        "prespec_bid_enrichment_logs": enrich_logs,
        "date_range_source": date_range_source,
        "lookback_days": settings.lookback_days,
        
    }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_json_path = save_json(manifest, f"day2_manifest_{timestamp}.json")

    return final_items, raw_json_path, attachment_jobs, len(all_items), len(deduped_items)