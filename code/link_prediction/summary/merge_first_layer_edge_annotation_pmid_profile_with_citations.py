"""Merge PMID-level first-layer edge annotation profiles with citation metrics.

This script runs by focal year. It joins the yearly PMID profile table to the
OpenAlex PMID-linked citation table, then writes:

1. a yearly PMID-level merged table for downstream paper-level analyses;
2. a yearly correlation summary between annotation-profile variables and
   citation/reference-count metrics.

The merge key is PMID. Citation-window correlations use only papers with a
complete citation window.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd


PROJECT_DIR = Path("/xdisk/sebratt/jinyugao/projects/innovation_capacity")
INTERIM_DIR = PROJECT_DIR / "data/interim"
OPENALEX_DIR = Path("/xdisk/sebratt/jinyugao/data/products/openalex/flattened_snapshot_2025")

PROFILE_DIR = (
    INTERIM_DIR / "link_prediction/summary/first_layer/edge_annotation_pmid_profile"
)
CITATION_FILE = (
    OPENALEX_DIR / "openalex_work_citations_reference_counts_through_2024_pmid.csv.gz"
)
OUTPUT_DIR = (
    INTERIM_DIR
    / "link_prediction/summary/first_layer/"
    "edge_annotation_pmid_profile_citation"
)
CORRELATION_DIR = OUTPUT_DIR / "correlation_summary_by_year"

PROFILE_FILE_PREFIX = (
    "semmedVER43_R_predications_with_pyear_filtered_first_layer_edge_annotation"
    "_pmid_profile"
)
OUTPUT_FILE_PREFIX = (
    "semmedVER43_R_predications_with_pyear_filtered_first_layer_edge_annotation"
    "_pmid_profile_with_citations"
)
CORRELATION_FILE_PREFIX = (
    "semmedVER43_R_first_layer_edge_annotation_pmid_profile_citation_correlation"
)

BASE_YEAR = 1980
N_YEARS = 40
CHUNK_SIZE = 1_000_000
OVERWRITE = False

PMID_COLUMN = "PMID"
NORMALIZED_PMID_COLUMN = "pmid_normalized"

PROFILE_VALUE_COLUMNS = [
    "n_total_predication_records",
    "n_new_node_combination_predication_records",
    "n_new_combination_predication_records",
    "n_new_relation_predication_records",
    "n_repeated_triple_predication_records",
    "share_new_node_combination_predication_records",
    "share_new_combination_predication_records",
    "share_new_relation_predication_records",
    "share_repeated_triple_predication_records",
]

CITATION_COLUMNS = [
    "n_references",
    "citation_C3",
    "citation_C5",
    "citation_C10",
    "citation_through_2024",
]
CITATION_WINDOW_FLAG = {
    "citation_C3": "has_complete_C3_window",
    "citation_C5": "has_complete_C5_window",
    "citation_C10": "has_complete_C10_window",
}
CITATION_USE_COLUMNS = [
    "pmid",
    "work_id",
    "publication_year",
    *CITATION_COLUMNS,
    "has_complete_C3_window",
    "has_complete_C5_window",
    "has_complete_C10_window",
]


def get_focal_year() -> int:
    task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if task_id is None:
        raise RuntimeError("SLURM_ARRAY_TASK_ID not found.")

    task_index = int(task_id)
    if task_index < 0 or task_index >= N_YEARS:
        raise ValueError(
            f"SLURM_ARRAY_TASK_ID={task_index} is out of range. "
            f"Expected 0-{N_YEARS - 1}."
        )
    return BASE_YEAR + task_index


def normalize_pmid(value: object) -> str:
    if pd.isna(value):
        return ""
    value_text = str(value).strip()
    if value_text.endswith(".0"):
        value_text = value_text[:-2]
    return value_text


def profile_file_for_year(year: int) -> Path:
    return PROFILE_DIR / f"{PROFILE_FILE_PREFIX}_{year}.csv.gz"


def output_file_for_year(year: int) -> Path:
    return OUTPUT_DIR / f"{OUTPUT_FILE_PREFIX}_{year}.csv.gz"


def correlation_file_for_year(year: int) -> Path:
    return CORRELATION_DIR / f"{CORRELATION_FILE_PREFIX}_{year}.csv"


def check_input(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input file: {path}")


def check_output(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not OVERWRITE:
        raise FileExistsError(
            "Output file already exists. Set OVERWRITE = True to replace it:\n"
            f"{path}"
        )
    if path.exists() and OVERWRITE:
        path.unlink()


def load_profile(profile_file: Path) -> pd.DataFrame:
    profile = pd.read_csv(
        profile_file,
        compression="gzip",
        dtype={PMID_COLUMN: "string"},
    )
    profile[NORMALIZED_PMID_COLUMN] = profile[PMID_COLUMN].map(normalize_pmid)
    profile = profile[profile[NORMALIZED_PMID_COLUMN] != ""].copy()
    if profile[NORMALIZED_PMID_COLUMN].duplicated().any():
        duplicate_count = int(profile[NORMALIZED_PMID_COLUMN].duplicated().sum())
        raise ValueError(
            f"Profile table has duplicate PMIDs after normalization: {duplicate_count}"
        )
    return profile


def load_citation_subset(pmids: set[str]) -> pd.DataFrame:
    chunks = []
    total_rows = 0
    matched_rows = 0

    reader = pd.read_csv(
        CITATION_FILE,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        usecols=CITATION_USE_COLUMNS,
        dtype={"pmid": "string", "work_id": "string"},
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        total_rows += len(chunk)
        chunk[NORMALIZED_PMID_COLUMN] = chunk["pmid"].map(normalize_pmid)
        matched = chunk[NORMALIZED_PMID_COLUMN].isin(pmids)
        matched_chunk = chunk.loc[matched].copy()
        matched_rows += len(matched_chunk)
        if not matched_chunk.empty:
            chunks.append(matched_chunk)

        print(
            f"Citation chunk {chunk_number:,}: read {total_rows:,} rows; "
            f"matched {matched_rows:,} rows."
        )

    if not chunks:
        return pd.DataFrame(columns=[*CITATION_USE_COLUMNS, NORMALIZED_PMID_COLUMN])

    citation = pd.concat(chunks, ignore_index=True)
    citation = citation.sort_values(
        [NORMALIZED_PMID_COLUMN, "publication_year", "work_id"],
        kind="stable",
    )
    citation = citation.drop_duplicates(
        subset=[NORMALIZED_PMID_COLUMN], keep="first"
    )
    return citation


def merge_profile_with_citations(profile: pd.DataFrame, citation: pd.DataFrame) -> pd.DataFrame:
    citation = citation.drop(columns=["pmid"], errors="ignore")
    merged = profile.merge(citation, on=NORMALIZED_PMID_COLUMN, how="left")
    merged["pmid_citation_matched"] = merged["work_id"].notna()
    merged["pyear_matches_openalex_publication_year"] = (
        pd.to_numeric(merged["pyear"], errors="coerce")
        == pd.to_numeric(merged["publication_year"], errors="coerce")
    )
    return merged


def complete_window_mask(data: pd.DataFrame, citation_column: str) -> pd.Series:
    flag = CITATION_WINDOW_FLAG.get(citation_column)
    if flag is None:
        return pd.Series(True, index=data.index)
    return data[flag].map(lambda value: bool(value) if pd.notna(value) else False)


def pearson_correlation(left: pd.Series, right: pd.Series) -> float | object:
    if left.nunique(dropna=True) < 2 or right.nunique(dropna=True) < 2:
        return pd.NA
    return left.corr(right, method="pearson")


def spearman_correlation(left: pd.Series, right: pd.Series) -> float:
    left_rank = left.rank(method="average")
    right_rank = right.rank(method="average")
    return pearson_correlation(left_rank, right_rank)


def calculate_correlations(data: pd.DataFrame, focal_year: int) -> pd.DataFrame:
    matched = data[data["pmid_citation_matched"]].copy()
    rows = []

    for profile_column in PROFILE_VALUE_COLUMNS:
        matched[profile_column] = pd.to_numeric(
            matched[profile_column], errors="coerce"
        )

    for citation_column in CITATION_COLUMNS:
        matched[citation_column] = pd.to_numeric(
            matched[citation_column], errors="coerce"
        )

    for profile_column in PROFILE_VALUE_COLUMNS:
        for citation_column in CITATION_COLUMNS:
            subset = matched.loc[
                complete_window_mask(matched, citation_column),
                [profile_column, citation_column],
            ].dropna()

            if len(subset) >= 2:
                pearson = pearson_correlation(
                    subset[profile_column], subset[citation_column]
                )
                spearman = spearman_correlation(
                    subset[profile_column], subset[citation_column]
                )
            else:
                pearson = pd.NA
                spearman = pd.NA

            rows.append(
                {
                    "pyear": focal_year,
                    "profile_variable": profile_column,
                    "citation_variable": citation_column,
                    "n_observations": len(subset),
                    "pearson_correlation": pearson,
                    "spearman_correlation": spearman,
                    "profile_mean": subset[profile_column].mean()
                    if len(subset)
                    else pd.NA,
                    "citation_mean": subset[citation_column].mean()
                    if len(subset)
                    else pd.NA,
                    "complete_window_filter": CITATION_WINDOW_FLAG.get(
                        citation_column, ""
                    ),
                }
            )

    return pd.DataFrame(rows)


def merge_and_correlate(focal_year: int) -> None:
    profile_file = profile_file_for_year(focal_year)
    output_file = output_file_for_year(focal_year)
    correlation_file = correlation_file_for_year(focal_year)

    check_input(profile_file)
    check_input(CITATION_FILE)
    check_output(output_file)
    check_output(correlation_file)

    print(f"Loading PMID profile for focal year {focal_year}: {profile_file}")
    profile = load_profile(profile_file)
    pmids = set(profile[NORMALIZED_PMID_COLUMN])
    print(f"Loaded {len(profile):,} profile PMIDs.")

    print(f"Loading citation subset from {CITATION_FILE}")
    citation = load_citation_subset(pmids)
    print(f"Loaded {len(citation):,} matched citation rows after de-duplication.")

    merged = merge_profile_with_citations(profile, citation)
    merged.to_csv(output_file, index=False, compression="gzip")

    correlations = calculate_correlations(merged, focal_year)
    correlations.to_csv(correlation_file, index=False)

    print(f"Saved merged PMID profile-citation table to {output_file}")
    print(f"Saved yearly correlation summary to {correlation_file}")
    print(
        "Citation match rate: "
        f"{merged['pmid_citation_matched'].mean():.4f} "
        f"({int(merged['pmid_citation_matched'].sum()):,}/{len(merged):,})"
    )


def main() -> None:
    focal_year = get_focal_year()
    merge_and_correlate(focal_year)


if __name__ == "__main__":
    main()
