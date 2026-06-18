"""Merge yearly SemMedDB predications with PMID-level country lists.

Each output row remains one SemMedDB predication row. The merge adds the
full-counting country-code list, country-name list, and country count associated
with the predication PMID. Predications without matched OpenAlex country data
are retained with empty country fields.
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

INPUT_DIR = (
    PROJECT_INTERIM_DIR / "semmedVER43_R/split_predications_with_pyear_filtered_by_pyear"
)
COUNTRY_FILE = OPENALEX_DIR / "openalex_pmid_country_full_counting_lists.csv.gz"
OUTPUT_DIR = (
    PROJECT_INTERIM_DIR
    / "country_adoption/yearly_semmed_predications_pmid_country_full_counting_lists"
)
SUMMARY_DIR = OUTPUT_DIR / "summary"

INPUT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
OUTPUT_FILE_PREFIX = (
    "semmedVER43_R_predications_with_pyear_filtered_"
    "pmid_country_full_counting_lists"
)
SUMMARY_FILE_PREFIX = "yearly_semmed_predications_pmid_country_full_counting_lists_summary"

BASE_YEAR = 1980
N_YEARS = 45
CHUNK_SIZE = 100_000
OVERWRITE = False

PMID_COLUMN = "PMID"
PMID_NORMALIZED_COLUMN = "pmid_normalized"
COUNTRY_LIST_COLUMNS = [
    "pmid",
    "pmid_country_codes_full_counting",
    "pmid_country_full_counting",
    "n_countries_for_pmid",
]


def get_year() -> int:
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
    return match.group(1) if match else text


def input_file_for_year(year: int) -> Path:
    return INPUT_DIR / f"{INPUT_FILE_PREFIX}_{year}.csv.gz"


def output_file_for_year(year: int) -> Path:
    return OUTPUT_DIR / f"{OUTPUT_FILE_PREFIX}_{year}.csv.gz"


def summary_file_for_year(year: int) -> Path:
    return SUMMARY_DIR / f"{SUMMARY_FILE_PREFIX}_{year}.csv"


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


def load_country_lists(path: Path) -> pd.DataFrame:
    country_lists = pd.read_csv(
        path,
        compression="gzip",
        usecols=COUNTRY_LIST_COLUMNS,
        dtype="string",
        keep_default_na=False,
        na_filter=False,
    )
    country_lists[PMID_NORMALIZED_COLUMN] = country_lists["pmid"].map(normalize_pmid)
    country_lists = country_lists.drop(columns=["pmid"])
    country_lists = country_lists[
        (country_lists[PMID_NORMALIZED_COLUMN] != "")
        & (country_lists["pmid_country_codes_full_counting"] != "")
    ].copy()
    country_lists["n_countries_for_pmid"] = pd.to_numeric(
        country_lists["n_countries_for_pmid"], errors="coerce"
    ).astype("Int64")

    if country_lists[PMID_NORMALIZED_COLUMN].duplicated().any():
        raise ValueError("Country-list input contains duplicate normalized PMIDs.")

    print(f"Loaded PMID country-list rows: {len(country_lists):,}.")
    return country_lists


def merge_year(
    input_file: Path,
    country_lists: pd.DataFrame,
    output_file: Path,
) -> dict[str, object]:
    total_rows = 0
    matched_rows = 0
    all_pmids: set[str] = set()
    matched_pmids: set[str] = set()
    wrote_header = False

    reader = pd.read_csv(
        input_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        dtype="string",
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        chunk = chunk.copy()
        total_rows += len(chunk)
        chunk[PMID_NORMALIZED_COLUMN] = chunk[PMID_COLUMN].map(normalize_pmid)
        all_pmids.update(
            pmid for pmid in chunk[PMID_NORMALIZED_COLUMN].unique() if pmid
        )

        merged = chunk.merge(
            country_lists,
            how="left",
            on=PMID_NORMALIZED_COLUMN,
            validate="many_to_one",
        )
        matched = merged["pmid_country_codes_full_counting"].notna() & (
            merged["pmid_country_codes_full_counting"].astype(str).str.strip() != ""
        )
        matched_rows += int(matched.sum())
        matched_pmids.update(merged.loc[matched, PMID_NORMALIZED_COLUMN])

        output_chunk = merged.drop(columns=[PMID_NORMALIZED_COLUMN])
        output_chunk.to_csv(
            output_file,
            mode="w" if not wrote_header else "a",
            index=False,
            compression="gzip",
            header=not wrote_header,
        )
        wrote_header = True

        print(
            f"Chunk {chunk_number:,}: rows {len(chunk):,}; "
            f"matched country rows {int(matched.sum()):,}."
        )

    if not wrote_header:
        raise RuntimeError(f"No rows were read from yearly predication file: {input_file}")

    return {
        "n_predication_rows": total_rows,
        "n_matched_country_rows": matched_rows,
        "n_unmatched_country_rows": total_rows - matched_rows,
        "predication_row_country_match_rate": (
            matched_rows / total_rows if total_rows else 0
        ),
        "n_unique_pmids": len(all_pmids),
        "n_matched_unique_pmids": len(matched_pmids),
        "n_unmatched_unique_pmids": len(all_pmids) - len(matched_pmids),
        "pmid_country_match_rate": (
            len(matched_pmids) / len(all_pmids) if all_pmids else 0
        ),
        "n_output_rows": total_rows,
    }


def main() -> None:
    year = get_year()
    input_file = input_file_for_year(year)
    output_file = output_file_for_year(year)
    summary_file = summary_file_for_year(year)

    check_input(input_file)
    check_input(COUNTRY_FILE)
    check_output(output_file)
    check_output(summary_file)

    country_lists = load_country_lists(COUNTRY_FILE)
    stats = merge_year(input_file, country_lists, output_file)
    pd.DataFrame(
        [
            {
                "pyear": year,
                "predication_input_file": str(input_file),
                "country_list_input_file": str(COUNTRY_FILE),
                "output_file": str(output_file),
                **stats,
            }
        ]
    ).to_csv(summary_file, index=False)

    print(f"Saved yearly predication-country-list file to {output_file}")
    print(f"Saved merge summary to {summary_file}")


if __name__ == "__main__":
    main()
