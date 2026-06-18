"""Merge first-layer edge transitions with PMID-level country lists.

The merge preserves the predication-level structure of the edge-transition
file. Each transition row remains one row and receives the full-counting country
lists associated with its PMID. Country lists are expanded only later when the
country adoption network is constructed.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import pandas as pd


PROJECT_INTERIM_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim"
)
OPENALEX_DIR = Path(
    "/xdisk/sebratt/jinyugao/data/products/openalex/flattened_snapshot_2025"
)

TRANSITION_DIR = PROJECT_INTERIM_DIR / "link_prediction/edge_transition/first_layer"
COUNTRY_FILE = OPENALEX_DIR / "openalex_pmid_country_full_counting_lists.csv.gz"
OUTPUT_DIR = (
    PROJECT_INTERIM_DIR
    / "country_adoption/first_layer_edge_transition_pmid_country_full_counting_lists"
)
SUMMARY_DIR = OUTPUT_DIR / "summary"

TRANSITION_FILE_PREFIX = (
    "semmedVER43_R_predications_with_pyear_filtered_first_layer_edge_transition"
)
OUTPUT_FILE_PREFIX = (
    "semmedVER43_R_predications_with_pyear_filtered_"
    "first_layer_edge_transition_pmid_country_full_counting_lists"
)
SUMMARY_FILE_PREFIX = (
    "first_layer_edge_transition_pmid_country_full_counting_lists_summary"
)

BASE_YEAR = 1980
N_YEARS = 40
CHUNK_SIZE = 100_000
OVERWRITE = False
KEEP_UNMATCHED_TRANSITION_ROWS = True

PMID_COLUMN = "PMID"
PMID_NORMALIZED_COLUMN = "pmid_normalized"
COUNTRY_LIST_COLUMNS = [
    "pmid",
    "pmid_country_codes_full_counting",
    "pmid_country_full_counting",
    "n_countries_for_pmid",
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


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def normalize_pmid(value: object) -> str:
    text = normalize_text(value)
    if not text:
        return ""

    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]

    match = re.search(r"(\d+)(?:/)?$", text)
    if match:
        return match.group(1)

    return text


def transition_file_for_year(focal_year: int) -> Path:
    return TRANSITION_DIR / f"{TRANSITION_FILE_PREFIX}_{focal_year}.csv.gz"


def output_file_for_year(focal_year: int) -> Path:
    return OUTPUT_DIR / f"{OUTPUT_FILE_PREFIX}_{focal_year}.csv.gz"


def summary_file_for_year(focal_year: int) -> Path:
    return SUMMARY_DIR / f"{SUMMARY_FILE_PREFIX}_{focal_year}.csv"


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


def load_pmid_country_lists(country_file: Path) -> pd.DataFrame:
    country_lists = pd.read_csv(
        country_file,
        compression="gzip",
        usecols=COUNTRY_LIST_COLUMNS,
        dtype="string",
        keep_default_na=False,
        na_filter=False,
    )
    country_lists[PMID_NORMALIZED_COLUMN] = country_lists["pmid"].map(
        normalize_pmid
    )
    country_lists = country_lists.drop(columns=["pmid"])
    country_lists["pmid_country_codes_full_counting"] = country_lists[
        "pmid_country_codes_full_counting"
    ].map(normalize_text)
    country_lists["pmid_country_full_counting"] = country_lists[
        "pmid_country_full_counting"
    ].map(normalize_text)
    country_lists["n_countries_for_pmid"] = pd.to_numeric(
        country_lists["n_countries_for_pmid"], errors="coerce"
    ).astype("Int64")

    country_lists = country_lists[
        (country_lists[PMID_NORMALIZED_COLUMN] != "")
        & (country_lists["pmid_country_codes_full_counting"] != "")
    ].copy()

    duplicate_pmids = country_lists[PMID_NORMALIZED_COLUMN].duplicated(keep=False)
    if duplicate_pmids.any():
        examples = ", ".join(
            country_lists.loc[duplicate_pmids, PMID_NORMALIZED_COLUMN]
            .drop_duplicates()
            .head(10)
        )
        raise ValueError(
            "Country-list input must contain one row per PMID. "
            f"Duplicate normalized PMID examples: {examples}"
        )

    print(f"Loaded PMID country-list rows: {len(country_lists):,}.")
    return country_lists


def merge_transition_with_country_lists(
    transition_file: Path,
    country_lists: pd.DataFrame,
    output_file: Path,
) -> dict[str, object]:
    total_transition_rows = 0
    transition_rows_with_pmid = 0
    matched_transition_rows = 0
    output_rows = 0
    transition_pmids: set[str] = set()
    matched_pmids: set[str] = set()
    countries_per_matched_pmid: dict[str, int] = {}
    wrote_header = False

    reader = pd.read_csv(
        transition_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        dtype="string",
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        chunk = chunk.copy()
        total_transition_rows += len(chunk)
        chunk[PMID_NORMALIZED_COLUMN] = chunk[PMID_COLUMN].map(normalize_pmid)

        pmid_mask = chunk[PMID_NORMALIZED_COLUMN] != ""
        transition_rows_with_pmid += int(pmid_mask.sum())
        transition_pmids.update(chunk.loc[pmid_mask, PMID_NORMALIZED_COLUMN])

        merged = chunk.merge(
            country_lists,
            how="left" if KEEP_UNMATCHED_TRANSITION_ROWS else "inner",
            on=PMID_NORMALIZED_COLUMN,
            validate="many_to_one",
        )

        matched_mask = merged["pmid_country_codes_full_counting"].notna() & (
            merged["pmid_country_codes_full_counting"].astype(str).str.strip() != ""
        )
        matched_transition_rows += int(matched_mask.sum())
        matched_pmids.update(merged.loc[matched_mask, PMID_NORMALIZED_COLUMN])
        countries_per_matched_pmid.update(
            merged.loc[
                matched_mask,
                [PMID_NORMALIZED_COLUMN, "n_countries_for_pmid"],
            ]
            .drop_duplicates(subset=[PMID_NORMALIZED_COLUMN])
            .dropna(subset=["n_countries_for_pmid"])
            .set_index(PMID_NORMALIZED_COLUMN)["n_countries_for_pmid"]
            .astype(int)
            .to_dict()
        )

        output_chunk = merged.drop(columns=[PMID_NORMALIZED_COLUMN])
        mode = "w" if not wrote_header else "a"
        output_chunk.to_csv(
            output_file,
            index=False,
            compression="gzip",
            mode=mode,
            header=not wrote_header,
        )
        wrote_header = True
        output_rows += len(output_chunk)

        print(
            f"Chunk {chunk_number:,}: transition rows {len(chunk):,}; "
            f"matched rows {int(matched_mask.sum()):,}; "
            f"output rows {len(output_chunk):,}."
        )

    if not wrote_header:
        raise RuntimeError(f"No rows were read from transition file: {transition_file}")

    n_transition_pmids = len(transition_pmids)
    n_matched_pmids = len(matched_pmids)
    mean_countries = (
        sum(countries_per_matched_pmid.values()) / len(countries_per_matched_pmid)
        if countries_per_matched_pmid
        else 0
    )
    max_countries = (
        max(countries_per_matched_pmid.values())
        if countries_per_matched_pmid
        else 0
    )

    return {
        "n_transition_rows": total_transition_rows,
        "n_transition_rows_with_pmid": transition_rows_with_pmid,
        "n_matched_transition_rows": matched_transition_rows,
        "n_unmatched_transition_rows": (
            total_transition_rows - matched_transition_rows
        ),
        "transition_row_match_rate": (
            matched_transition_rows / total_transition_rows
            if total_transition_rows
            else 0
        ),
        "n_transition_unique_pmids_with_value": n_transition_pmids,
        "n_matched_unique_pmids": n_matched_pmids,
        "n_unmatched_unique_pmids": n_transition_pmids - n_matched_pmids,
        "pmid_match_rate": (
            n_matched_pmids / n_transition_pmids if n_transition_pmids else 0
        ),
        "n_output_rows": output_rows,
        "output_preserves_all_transition_rows": (
            KEEP_UNMATCHED_TRANSITION_ROWS and output_rows == total_transition_rows
        ),
        "mean_countries_per_matched_pmid": mean_countries,
        "max_countries_per_matched_pmid": max_countries,
        "keep_unmatched_transition_rows": KEEP_UNMATCHED_TRANSITION_ROWS,
    }


def write_summary(
    focal_year: int,
    transition_file: Path,
    output_file: Path,
    summary_file: Path,
    stats: dict[str, object],
) -> None:
    summary = {
        "pyear": focal_year,
        "transition_input_file": str(transition_file),
        "country_list_input_file": str(COUNTRY_FILE),
        "output_file": str(output_file),
        **stats,
    }
    pd.DataFrame([summary]).to_csv(summary_file, index=False)
    print(f"Saved summary to {summary_file}")


def main() -> None:
    focal_year = get_focal_year()
    transition_file = transition_file_for_year(focal_year)
    output_file = output_file_for_year(focal_year)
    summary_file = summary_file_for_year(focal_year)

    check_input(transition_file)
    check_input(COUNTRY_FILE)
    check_output(output_file)
    check_output(summary_file)

    country_lists = load_pmid_country_lists(COUNTRY_FILE)
    stats = merge_transition_with_country_lists(
        transition_file,
        country_lists,
        output_file,
    )
    write_summary(
        focal_year,
        transition_file,
        output_file,
        summary_file,
        stats,
    )
    print(f"Saved transition-country-list file to {output_file}")


if __name__ == "__main__":
    main()
