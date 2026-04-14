from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

OUTPUT_DIR = Path(r"C:\work\nara_sas_poc\data\output")

REVIEW_SCOPE_SHOW_COLS = [
    "review_scope",
    "label",
    "score",
    "record_id",
    "source_kind",
    "source_type",
    "title",
    "pub_org",
    "reasons",
]

REVIEW_CHANGE_SHOW_COLS = [
    "review_key",
    "title_new",
    "original_label_old",
    "original_label_new",
    "final_decision_old",
    "final_decision_new",
    "action_status_old",
    "action_status_new",
    "owner_note_old",
    "owner_note_new",
]


def normalize_text(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def resolve_compare_files() -> tuple[Path, Path]:
    old_override = normalize_text(os.getenv("OLD_XLSX"))
    new_override = normalize_text(os.getenv("NEW_XLSX"))

    if old_override and new_override:
        old_path = Path(old_override)
        new_path = Path(new_override)
        if not old_path.exists():
            raise FileNotFoundError(f"OLD_XLSX not found: {old_path}")
        if not new_path.exists():
            raise FileNotFoundError(f"NEW_XLSX not found: {new_path}")
        return old_path, new_path

    xlsx_files = sorted(OUTPUT_DIR.glob("day2_candidates_*.xlsx"))
    if len(xlsx_files) < 2:
        raise FileNotFoundError("비교 대상 Excel이 2개 이상 필요함")

    if old_override:
        old_path = Path(old_override)
        if not old_path.exists():
            raise FileNotFoundError(f"OLD_XLSX not found: {old_path}")
        return old_path, xlsx_files[-1]

    if new_override:
        new_path = Path(new_override)
        if not new_path.exists():
            raise FileNotFoundError(f"NEW_XLSX not found: {new_path}")
        return xlsx_files[-2], new_path

    return xlsx_files[-2], xlsx_files[-1]


def read_excel_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    try:
        return pd.read_excel(path, sheet_name=sheet_name)
    except ValueError:
        return pd.DataFrame()


def pick_first_existing(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for name in candidates:
        if name in df.columns:
            return name
    return None


def print_df(
    title: str,
    df: pd.DataFrame,
    cols: list[str],
    sort_cols: list[str] | None = None,
    ascending: list[bool] | None = None,
    limit: int | None = None,
) -> None:
    print(f"\n=== {title} ===")
    if df.empty:
        print("없음")
        return

    working_df = df.copy()
    if sort_cols:
        available_sort_cols = [c for c in sort_cols if c in working_df.columns]
        if available_sort_cols:
            sort_ascending = ascending[: len(available_sort_cols)] if ascending else True
            working_df = working_df.sort_values(available_sort_cols, ascending=sort_ascending)

    selected_cols = [c for c in cols if c in working_df.columns]
    output_df = working_df[selected_cols]
    if limit is not None:
        output_df = output_df.head(limit)
    print(output_df.to_string(index=False))


def prepare_candidates_df(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()
    if "record_id" not in prepared.columns:
        raise ValueError("candidates sheet에 record_id 컬럼이 필요함")

    prepared["record_id"] = prepared["record_id"].map(normalize_text)
    for col in ["label", "title", "pub_org", "reasons", "source_kind", "source_type", "review_scope"]:
        if col in prepared.columns:
            prepared[col] = prepared[col].map(normalize_text)
    if "score" in prepared.columns:
        prepared["score"] = pd.to_numeric(prepared["score"], errors="coerce").fillna(0)

    return prepared


def get_review_scope_df(df: pd.DataFrame) -> pd.DataFrame:
    if "review_scope" in df.columns:
        return df[df["review_scope"].str.upper() == "Y"].copy()
    return df[df["label"].isin(["Direct", "Adjacent"])].copy()


def summarize_candidates(old_df: pd.DataFrame, new_df: pd.DataFrame) -> None:
    show_cols = [c for c in REVIEW_SCOPE_SHOW_COLS if c in old_df.columns or c in new_df.columns]

    old_review = get_review_scope_df(old_df)
    new_review = get_review_scope_df(new_df)

    old_review_ids = set(old_review["record_id"])
    new_review_ids = set(new_review["record_id"])

    review_old_only_ids = sorted(old_review_ids - new_review_ids)
    review_new_only_ids = sorted(new_review_ids - old_review_ids)

    review_old_only = old_review[old_review["record_id"].isin(review_old_only_ids)].copy()
    review_new_only = new_review[new_review["record_id"].isin(review_new_only_ids)].copy()

    print("\n=== REVIEW SCOPE SUMMARY ===")
    print(f"old review candidates: {len(old_review)}")
    print(f"new review candidates: {len(new_review)}")
    print(f"review_old_only: {len(review_old_only)}")
    print(f"review_new_only: {len(review_new_only)}")

    print_df("REVIEW OLD ONLY", review_old_only, show_cols, sort_cols=["label", "score"], ascending=[True, False])
    print_df("REVIEW NEW ONLY", review_new_only, show_cols, sort_cols=["label", "score"], ascending=[True, False])

    old_ids = set(old_df["record_id"])
    new_ids = set(new_df["record_id"])

    old_only_ids = sorted(old_ids - new_ids)
    new_only_ids = sorted(new_ids - old_ids)

    old_only = old_df[old_df["record_id"].isin(old_only_ids)].copy()
    new_only = new_df[new_df["record_id"].isin(new_only_ids)].copy()

    merged = old_df[["record_id", "label", "score"]].merge(
        new_df[["record_id", "label", "score"]],
        on="record_id",
        how="inner",
        suffixes=("_old", "_new"),
    )

    label_changed = merged[merged["label_old"] != merged["label_new"]].copy()
    score_changed = merged[merged["score_old"] != merged["score_new"]].copy()

    print("\n=== SUMMARY (ALL CANDIDATES) ===")
    print(f"old candidates: {len(old_df)}")
    print(f"new candidates: {len(new_df)}")
    print(f"old_only: {len(old_only)}")
    print(f"new_only: {len(new_only)}")
    print(f"label_changed: {len(label_changed)}")
    print(f"score_changed: {len(score_changed)}")

    print_df("OLD ONLY", old_only, show_cols, sort_cols=["label", "score"], ascending=[True, False])
    print_df("NEW ONLY", new_only, show_cols, sort_cols=["label", "score"], ascending=[True, False])

    print("\n=== LABEL CHANGED ===")
    if label_changed.empty:
        print("없음")
    else:
        print(label_changed.sort_values("record_id").to_string(index=False))

    print("\n=== TOP SCORE CHANGED ===")
    if score_changed.empty:
        print("없음")
    else:
        print(score_changed.sort_values(["record_id"]).head(50).to_string(index=False))


def prepare_review_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    prepared = df.copy()
    key_col = pick_first_existing(prepared, ["review_item_key", "record_id"])
    if key_col is None:
        raise ValueError("review_candidates sheet에 review_item_key 또는 record_id 컬럼이 필요함")

    prepared["review_key"] = prepared[key_col].map(normalize_text)
    prepared = prepared[prepared["review_key"] != ""].copy()

    for col in ["original_label", "final_decision", "action_status", "owner_note", "title", "pub_org"]:
        if col in prepared.columns:
            prepared[col] = prepared[col].map(normalize_text)
    if "score" in prepared.columns:
        prepared["score"] = pd.to_numeric(prepared["score"], errors="coerce").fillna(0)

    return prepared


def summarize_review_sheet(old_review_df: pd.DataFrame, new_review_df: pd.DataFrame) -> None:
    print("\n=== REVIEW SHEET SUMMARY ===")
    if old_review_df.empty and new_review_df.empty:
        print("review_candidates sheet 없음")
        return

    old_keys = set(old_review_df.get("review_key", pd.Series(dtype=str)))
    new_keys = set(new_review_df.get("review_key", pd.Series(dtype=str)))

    old_only_keys = sorted(old_keys - new_keys)
    new_only_keys = sorted(new_keys - old_keys)

    old_only = old_review_df[old_review_df["review_key"].isin(old_only_keys)].copy()
    new_only = new_review_df[new_review_df["review_key"].isin(new_only_keys)].copy()

    old_final_decision = old_review_df.get("final_decision", pd.Series(dtype=str))
    new_final_decision = new_review_df.get("final_decision", pd.Series(dtype=str))
    old_action_status = old_review_df.get("action_status", pd.Series(dtype=str))
    new_action_status = new_review_df.get("action_status", pd.Series(dtype=str))
    old_owner_note = old_review_df.get("owner_note", pd.Series(dtype=str))
    new_owner_note = new_review_df.get("owner_note", pd.Series(dtype=str))

    old_decided = int((old_final_decision != "").sum())
    new_decided = int((new_final_decision != "").sum())
    old_reviewed = int((~old_action_status.isin(["", "new"])).sum())
    new_reviewed = int((~new_action_status.isin(["", "new"])).sum())
    old_noted = int((old_owner_note != "").sum())
    new_noted = int((new_owner_note != "").sum())

    print(f"old review rows: {len(old_review_df)}")
    print(f"new review rows: {len(new_review_df)}")
    print(f"review_old_only: {len(old_only)}")
    print(f"review_new_only: {len(new_only)}")
    print(f"old final_decision filled: {old_decided}")
    print(f"new final_decision filled: {new_decided}")
    print(f"old action_status reviewed: {old_reviewed}")
    print(f"new action_status reviewed: {new_reviewed}")
    print(f"old owner_note filled: {old_noted}")
    print(f"new owner_note filled: {new_noted}")

    merged_review = old_review_df[[
        "review_key",
        "title",
        "original_label",
        "final_decision",
        "action_status",
        "owner_note",
    ]].merge(
        new_review_df[[
            "review_key",
            "title",
            "original_label",
            "final_decision",
            "action_status",
            "owner_note",
        ]],
        on="review_key",
        how="inner",
        suffixes=("_old", "_new"),
    )

    for col in [
        "title_old",
        "title_new",
        "original_label_old",
        "original_label_new",
        "final_decision_old",
        "final_decision_new",
        "action_status_old",
        "action_status_new",
        "owner_note_old",
        "owner_note_new",
    ]:
        if col in merged_review.columns:
            merged_review[col] = merged_review[col].map(normalize_text)

    review_value_changed = merged_review[
        (merged_review["final_decision_old"] != merged_review["final_decision_new"])
        | (merged_review["action_status_old"] != merged_review["action_status_new"])
        | (merged_review["owner_note_old"] != merged_review["owner_note_new"])
    ].copy()

    review_value_preserved = merged_review[
        (
            (merged_review["final_decision_old"] != "")
            | (merged_review["action_status_old"] != "")
            | (merged_review["owner_note_old"] != "")
        )
        & (merged_review["final_decision_old"] == merged_review["final_decision_new"])
        & (merged_review["action_status_old"] == merged_review["action_status_new"])
        & (merged_review["owner_note_old"] == merged_review["owner_note_new"])
    ].copy()

    print(f"preserved previous review values: {len(review_value_preserved)}")
    print(f"changed review values: {len(review_value_changed)}")

    print_df(
        "REVIEW SHEET OLD ONLY",
        old_only,
        ["review_key", "original_label", "final_decision", "action_status", "owner_note", "title", "pub_org"],
        sort_cols=["original_label", "title"],
        ascending=[True, True],
        limit=50,
    )
    print_df(
        "REVIEW SHEET NEW ONLY",
        new_only,
        ["review_key", "original_label", "final_decision", "action_status", "owner_note", "title", "pub_org"],
        sort_cols=["original_label", "title"],
        ascending=[True, True],
        limit=50,
    )
    print_df(
        "REVIEW VALUE CHANGED",
        review_value_changed,
        REVIEW_CHANGE_SHOW_COLS,
        sort_cols=["review_key"],
        ascending=[True],
        limit=100,
    )


def main() -> None:
    old_xlsx, new_xlsx = resolve_compare_files()

    print(f"OLD_XLSX={old_xlsx}")
    print(f"NEW_XLSX={new_xlsx}")

    old_candidates = prepare_candidates_df(read_excel_sheet(old_xlsx, "candidates"))
    new_candidates = prepare_candidates_df(read_excel_sheet(new_xlsx, "candidates"))
    summarize_candidates(old_candidates, new_candidates)

    old_review_sheet = prepare_review_df(read_excel_sheet(old_xlsx, "review_candidates"))
    new_review_sheet = prepare_review_df(read_excel_sheet(new_xlsx, "review_candidates"))
    summarize_review_sheet(old_review_sheet, new_review_sheet)


if __name__ == "__main__":
    main()
