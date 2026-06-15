"""Merge first-layer edge transitions with PMID-country full-counting data.

This script keeps edge transition as a knowledge-structure result and adds
country information only afterward. The output is expanded to one row per
transition row and PMID-country pair, using full counting at the PMID-country
level.
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
COUNTRY_FILE = OPENALEX_DIR / "openalex_pmid_country_full_counting.csv.gz"
OUTPUT_DIR = (
    PROJECT_INTERIM_DIR
    / "country_adoption/first_layer_edge_transition_pmid_country_full_counting"
)
SUMMARY_DIR = OUTPUT_DIR / "summary"

TRANSITION_FILE_PREFIX = (
    "semmedVER43_R_predications_with_pyear_filtered_first_layer_edge_transition"
)
OUTPUT_FILE_PREFIX = (
    "semmedVER43_R_predications_with_pyear_filtered_"
    "first_layer_edge_transition_pmid_country_full_counting"
)
SUMMARY_FILE_PREFIX = "first_layer_edge_transition_pmid_country_full_counting_summary"

BASE_YEAR = 1980
N_YEARS = 40
CHUNK_SIZE = 100_000
OVERWRITE = False
KEEP_UNMATCHED_TRANSITION_ROWS = False

PMID_COLUMN = "PMID"
PMID_NORMALIZED_COLUMN = "pmid_normalized"
TRANSITION_ROW_ID_COLUMN = "_transition_row_id"

COUNTRY_COLUMNS = [
    "pmid",
    "institution_country_code",
    "institution_country",
    "pmid_country_weight",
    "n_work_ids_for_pmid_country",
    "n_authors_for_pmid_country",
    "n_institutions_for_pmid_country",
    "n_authorship_institution_rows_for_pmid_country",
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
    text = str(value).strip()
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


def load_pmid_country_table(country_file: Path) -> pd.DataFrame:
    country = pd.read_csv(
        country_file,
        compression="gzip",
        usecols=COUNTRY_COLUMNS,
        dtype="string",
    )
    country = country.rename(columns={"pmid": "openalex_pmid"})
    country[PMID_NORMALIZED_COLUMN] = country["openalex_pmid"].map(normalize_pmid)
    country = country[
        (country[PMID_NORMALIZED_COLUMN] != "")
        & country["institution_country_code"].notna()
        & (country["institution_country_code"].str.strip() != "")
    ].copy()
    country = country.drop_duplicates(
        subset=[PMID_NORMALIZED_COLUMN, "institution_country_code"]
    )
    country["n_countries_for_pmid"] = country.groupby(PMID_NORMALIZED_COLUMN)[
        "institution_country_code"
    ].transform("nunique")

    print(f"Loaded PMID-country rows: {len(country):,}")
    print(
        "Unique normalized PMIDs with country information: "
        f"{country[PMID_NORMALIZED_COLUMN].nunique():,}"
    )
    return country


def merge_transition_with_country(
    transition_file: Path,
    country: pd.DataFrame,
    output_file: Path,
) -> dict[str, object]:
    total_transition_rows = 0
    transition_rows_with_pmid = 0
    output_rows = 0
    next_row_id = 0
    matched_row_ids: set[int] = set()
    transition_pmids: set[str] = set()
    transition_pmids_with_value: set[str] = set()
    matched_pmids: set[str] = set()
    output_country_codes: set[str] = set()
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
        n_chunk_rows = len(chunk)
        row_ids = range(next_row_id, next_row_id + n_chunk_rows)
        chunk[TRANSITION_ROW_ID_COLUMN] = list(row_ids)
        next_row_id += n_chunk_rows

        total_transition_rows += n_chunk_rows
        chunk[PMID_NORMALIZED_COLUMN] = chunk[PMID_COLUMN].map(normalize_pmid)
        pmid_mask = chunk[PMID_NORMALIZED_COLUMN] != ""
        transition_rows_with_pmid += int(pmid_mask.sum())
        transition_pmids.update(chunk[PMID_NORMALIZED_COLUMN].dropna().unique())
        transition_pmids_with_value.update(chunk.loc[pmid_mask, PMID_NORMALIZED_COLUMN])

        how = "left" if KEEP_UNMATCHED_TRANSITION_ROWS else "inner"
        merged = chunk.merge(
            country,
            how=how,
            on=PMID_NORMALIZED_COLUMN,
            validate="many_to_many",
        )

        if KEEP_UNMATCHED_TRANSITION_ROWS:
            matched = merged["institution_country_code"].notna()
        else:
            matched = pd.Series(True, index=merged.index)

        matched_row_ids.update(
            merged.loc[matched, TRANSITION_ROW_ID_COLUMN].dropna().astype(int)
        )
        matched_pmids.update(merged.loc[matched, PMID_NORMALIZED_COLUMN].dropna())
        output_country_codes.update(
            merged.loc[matched, "institution_country_code"].dropna().astype(str)
        )
        countries_per_matched_pmid.update(
            merged.loc[matched, [PMID_NORMALIZED_COLUMN, "n_countries_for_pmid"]]
            .drop_duplicates()
            .set_index(PMID_NORMALIZED_COLUMN)["n_countries_for_pmid"]
            .astype(int)
            .to_dict()
        )

        merged = merged.drop(columns=[TRANSITION_ROW_ID_COLUMN])
        mode = "w" if not wrote_header else "a"
        merged.to_csv(
            output_file,
            index=False,
            compression="gzip",
            mode=mode,
            header=not wrote_header,
        )
        wrote_header = True
        output_rows += len(merged)

        print(
            f"Chunk {chunk_number:,}: transition rows {n_chunk_rows:,}; "
            f"country-expanded output rows {len(merged):,}."
        )

    if not wrote_header:
        raise RuntimeError(f"No rows were read from transition file: {transition_file}")

    transition_pmids.discard("")
    transition_pmids_with_value.discard("")
    matched_pmids.discard("")

    n_matched_transition_rows = len(matched_row_ids)
    n_unmatched_transition_rows = total_transition_rows - n_matched_transition_rows
    n_transition_pmids_with_value = len(transition_pmids_with_value)
    n_matched_pmids = len(matched_pmids)
    n_unmatched_pmids = n_transition_pmids_with_value - n_matched_pmids

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
        "n_transition_unique_pmids": len(transition_pmids),
        "n_transition_unique_pmids_with_value": n_transition_pmids_with_value,
        "n_matched_transition_rows": n_matched_transition_rows,
        "n_unmatched_transition_rows": n_unmatched_transition_rows,
        "transition_row_match_rate": (
            n_matched_transition_rows / total_transition_rows
            if total_transition_rows
            else 0
        ),
        "n_matched_unique_pmids": n_matched_pmids,
        "n_unmatched_unique_pmids": n_unmatched_pmids,
        "pmid_match_rate": (
            n_matched_pmids / n_transition_pmids_with_value
            if n_transition_pmids_with_value
            else 0
        ),
        "n_country_expanded_output_rows": output_rows,
        "n_unique_country_codes_in_output": len(output_country_codes),
        "mean_countries_per_matched_pmid": mean_countries,
        "max_countries_per_matched_pmid": max_countries,
        "keep_unmatched_transition_rows": KEEP_UNMATCHED_TRANSITION_ROWS,
    }


def write_summary(
    focal_year: int,
    transition_file: Path,
    country_file: Path,
    output_file: Path,
    summary_file: Path,
    stats: dict[str, object],
) -> None:
    summary_file.parent.mkdir(parents=True, exist_ok=True)
    summary = {
        "pyear": focal_year,
        "transition_input_file": str(transition_file),
        "country_input_file": str(country_file),
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

    country = load_pmid_country_table(COUNTRY_FILE)
    stats = merge_transition_with_country(transition_file, country, output_file)
    write_summary(
        focal_year,
        transition_file,
        COUNTRY_FILE,
        output_file,
        summary_file,
        stats,
    )
    print(f"Saved merged transition-country file to {output_file}")


if __name__ == "__main__":
    main()
