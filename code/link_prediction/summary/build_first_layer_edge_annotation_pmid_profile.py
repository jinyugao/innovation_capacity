"""Build PMID-level first-layer edge annotation profiles by focal year.

Each output row is one PMID in one focal year. Counts are based on annotated
non-self-loop SemMedDB predication records, so the four first-layer annotation
counts sum to the PMID's total annotated predication records.
"""

from __future__ import annotations

import os
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd


INTERIM_DIR = Path("/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim")
ANNOTATION_DIR = INTERIM_DIR / "link_prediction/edge_annotation/first_layer"
OUTPUT_DIR = (
    INTERIM_DIR / "link_prediction/summary/first_layer/edge_annotation_pmid_profile"
)
SUMMARY_DIR = OUTPUT_DIR / "summary"

ANNOTATION_FILE_PREFIX = (
    "semmedVER43_R_predications_with_pyear_filtered_first_layer_edge_annotation"
)
OUTPUT_FILE_PREFIX = (
    "semmedVER43_R_predications_with_pyear_filtered_first_layer_edge_annotation"
    "_pmid_profile"
)
SUMMARY_FILE_PREFIX = (
    "semmedVER43_R_first_layer_edge_annotation_pmid_profile_summary"
)

BASE_YEAR = 1980
N_YEARS = 40
CHUNK_SIZE = 100_000
OVERWRITE = False

PMID_COLUMN = "PMID"
ANNOTATION_COLUMN = "first_layer_edge_annotation"

ANNOTATION_CATEGORIES = [
    "New_Node_Combination",
    "New_Combination",
    "New_Relation",
    "Repeated_Triple",
]
CATEGORY_TO_PREFIX = {
    "New_Node_Combination": "new_node_combination",
    "New_Combination": "new_combination",
    "New_Relation": "new_relation",
    "Repeated_Triple": "repeated_triple",
}


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


def normalize_value(value: object) -> str:
    return "" if pd.isna(value) else str(value).strip()


def annotation_file_for_year(year: int) -> Path:
    return ANNOTATION_DIR / f"{ANNOTATION_FILE_PREFIX}_{year}.csv.gz"


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


def share(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def build_pmid_counts(
    input_file: Path,
) -> tuple[dict[str, Counter[str]], dict[str, int]]:
    counts_by_pmid: defaultdict[str, Counter[str]] = defaultdict(Counter)
    category_totals: Counter[str] = Counter()
    n_input_rows = 0
    n_rows_with_missing_pmid = 0
    n_rows_with_unknown_annotation = 0

    reader = pd.read_csv(
        input_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        usecols=[PMID_COLUMN, ANNOTATION_COLUMN],
        dtype={PMID_COLUMN: "string", ANNOTATION_COLUMN: "string"},
    )

    valid_categories = set(ANNOTATION_CATEGORIES)
    for chunk_number, chunk in enumerate(reader, start=1):
        n_input_rows += len(chunk)
        chunk[PMID_COLUMN] = chunk[PMID_COLUMN].map(normalize_value)
        chunk[ANNOTATION_COLUMN] = chunk[ANNOTATION_COLUMN].map(normalize_value)

        missing_pmid_mask = chunk[PMID_COLUMN] == ""
        n_rows_with_missing_pmid += int(missing_pmid_mask.sum())
        chunk = chunk.loc[~missing_pmid_mask, [PMID_COLUMN, ANNOTATION_COLUMN]]

        valid_mask = chunk[ANNOTATION_COLUMN].isin(valid_categories)
        n_rows_with_unknown_annotation += int((~valid_mask).sum())
        chunk = chunk.loc[valid_mask]

        grouped = (
            chunk.groupby([PMID_COLUMN, ANNOTATION_COLUMN], observed=True)
            .size()
            .reset_index(name="n")
        )
        for row in grouped.itertuples(index=False):
            pmid = getattr(row, PMID_COLUMN)
            annotation = getattr(row, ANNOTATION_COLUMN)
            count = int(row.n)
            counts_by_pmid[pmid][annotation] += count
            category_totals[annotation] += count

        print(
            f"Chunk {chunk_number:,}: read {len(chunk):,} usable rows; "
            f"unique PMIDs so far {len(counts_by_pmid):,}."
        )

    summary = {
        "n_input_rows": n_input_rows,
        "n_rows_with_missing_pmid": n_rows_with_missing_pmid,
        "n_rows_with_unknown_annotation": n_rows_with_unknown_annotation,
        "n_output_pmids": len(counts_by_pmid),
    }
    for category in ANNOTATION_CATEGORIES:
        prefix = CATEGORY_TO_PREFIX[category]
        summary[f"n_{prefix}_predication_records"] = category_totals[category]

    return dict(counts_by_pmid), summary


def counts_to_dataframe(
    focal_year: int,
    counts_by_pmid: dict[str, Counter[str]],
) -> pd.DataFrame:
    rows = []
    for pmid in sorted(counts_by_pmid, key=lambda value: (len(value), value)):
        counts = counts_by_pmid[pmid]
        total = sum(counts[category] for category in ANNOTATION_CATEGORIES)
        row = {
            "pyear": focal_year,
            PMID_COLUMN: pmid,
            "n_total_predication_records": total,
        }
        for category in ANNOTATION_CATEGORIES:
            prefix = CATEGORY_TO_PREFIX[category]
            row[f"n_{prefix}_predication_records"] = counts[category]
        for category in ANNOTATION_CATEGORIES:
            prefix = CATEGORY_TO_PREFIX[category]
            row[f"share_{prefix}_predication_records"] = share(
                counts[category], total
            )
        rows.append(row)

    columns = [
        "pyear",
        PMID_COLUMN,
        "n_total_predication_records",
    ]
    columns.extend(
        f"n_{CATEGORY_TO_PREFIX[category]}_predication_records"
        for category in ANNOTATION_CATEGORIES
    )
    columns.extend(
        f"share_{CATEGORY_TO_PREFIX[category]}_predication_records"
        for category in ANNOTATION_CATEGORIES
    )
    return pd.DataFrame(rows, columns=columns)


def build_pmid_profile(focal_year: int) -> None:
    input_file = annotation_file_for_year(focal_year)
    output_file = output_file_for_year(focal_year)
    summary_file = summary_file_for_year(focal_year)

    check_input(input_file)
    check_output(output_file)
    check_output(summary_file)

    counts_by_pmid, summary = build_pmid_counts(input_file)
    output = counts_to_dataframe(focal_year, counts_by_pmid)
    output.to_csv(output_file, index=False, compression="gzip")

    summary_output = pd.DataFrame(
        [
            {
                "pyear": focal_year,
                **summary,
                "n_output_rows": len(output),
                "output_file": str(output_file),
            }
        ]
    )
    summary_output.to_csv(summary_file, index=False)

    print(f"Saved PMID-level edge annotation profile to {output_file}")
    print(f"Saved yearly profile summary to {summary_file}")


def main() -> None:
    focal_year = get_focal_year()
    build_pmid_profile(focal_year)


if __name__ == "__main__":
    main()
