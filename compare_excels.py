from __future__ import annotations

from pathlib import Path
import pandas as pd

OUTPUT_DIR = Path(r"C:\work\nara_sas_poc\data\output")
OLD_XLSX = OUTPUT_DIR / "day2_candidates_20260409_170036.xlsx"

xlsx_files = sorted(OUTPUT_DIR.glob("day2_candidates_*.xlsx"))
NEW_XLSX = xlsx_files[-1]

print(f"OLD_XLSX={OLD_XLSX}")
print(f"NEW_XLSX={NEW_XLSX}")

old_df = pd.read_excel(OLD_XLSX, sheet_name="candidates")
new_df = pd.read_excel(NEW_XLSX, sheet_name="candidates")

old_df["record_id"] = old_df["record_id"].astype(str).str.strip()
new_df["record_id"] = new_df["record_id"].astype(str).str.strip()

old_ids = set(old_df["record_id"])
new_ids = set(new_df["record_id"])

old_only_ids = sorted(old_ids - new_ids)
new_only_ids = sorted(new_ids - old_ids)
common_ids = sorted(old_ids & new_ids)

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

print("\n=== SUMMARY ===")
print(f"old candidates: {len(old_df)}")
print(f"new candidates: {len(new_df)}")
print(f"old_only: {len(old_only)}")
print(f"new_only: {len(new_only)}")
print(f"label_changed: {len(label_changed)}")
print(f"score_changed: {len(score_changed)}")

show_cols = [c for c in ["label", "score", "record_id", "source_kind", "source_type", "title", "pub_org", "reasons"] if c in old_only.columns]

print("\n=== OLD ONLY ===")
if len(old_only) == 0:
    print("없음")
else:
    print(old_only[show_cols].sort_values(["label", "score"], ascending=[True, False]).to_string(index=False))

print("\n=== NEW ONLY ===")
if len(new_only) == 0:
    print("없음")
else:
    print(new_only[show_cols].sort_values(["label", "score"], ascending=[True, False]).to_string(index=False))

print("\n=== LABEL CHANGED ===")
if len(label_changed) == 0:
    print("없음")
else:
    print(label_changed.sort_values("record_id").to_string(index=False))

print("\n=== TOP SCORE CHANGED ===")
if len(score_changed) == 0:
    print("없음")
else:
    print(score_changed.sort_values(["record_id"]).head(50).to_string(index=False))

old_review = old_df[old_df["label"].isin(["Direct", "Adjacent"])].copy()
new_review = new_df[new_df["label"].isin(["Direct", "Adjacent"])].copy()

old_review_ids = set(old_review["record_id"])
new_review_ids = set(new_review["record_id"])

review_old_only_ids = sorted(old_review_ids - new_review_ids)
review_new_only_ids = sorted(new_review_ids - old_review_ids)

review_old_only = old_review[old_review["record_id"].isin(review_old_only_ids)].copy()
review_new_only = new_review[new_review["record_id"].isin(review_new_only_ids)].copy()

print("\n=== REVIEW SCOPE SUMMARY (Direct+Adjacent only) ===")
print(f"old review candidates: {len(old_review)}")
print(f"new review candidates: {len(new_review)}")
print(f"review_old_only: {len(review_old_only)}")
print(f"review_new_only: {len(review_new_only)}")

print("\n=== REVIEW OLD ONLY ===")
if len(review_old_only) == 0:
    print("없음")
else:
    print(review_old_only[show_cols].sort_values(["label", "score"], ascending=[True, False]).to_string(index=False))

print("\n=== REVIEW NEW ONLY ===")
if len(review_new_only) == 0:
    print("없음")
else:
    print(review_new_only[show_cols].sort_values(["label", "score"], ascending=[True, False]).to_string(index=False))