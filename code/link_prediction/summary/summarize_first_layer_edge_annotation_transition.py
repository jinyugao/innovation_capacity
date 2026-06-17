"""Summarize first-layer edge annotation and transition outputs by year.

The summaries are reported at two levels:

1. Predication level: counts and shares of predication rows.
2. PMID level: counts and shares of unique PMIDs containing each category.

PMID-level shares can sum to more than one within a year because one PMID can
contain multiple annotation or transition categories.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd


INTERIM_DIR = Path("/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim")
ANNOTATION_DIR = INTERIM_DIR / "link_prediction/edge_annotation/first_layer"
TRANSITION_DIR = INTERIM_DIR / "link_prediction/edge_transition/first_layer"
OUTPUT_DIR = INTERIM_DIR / "link_prediction/summary/first_layer"

ANNOTATION_FILE_PREFIX = (
    "semmedVER43_R_predications_with_pyear_filtered_first_layer_edge_annotation"
)
TRANSITION_FILE_PREFIX = (
    "semmedVER43_R_predications_with_pyear_filtered_first_layer_edge_transition"
)
ANNOTATION_SUMMARY_FILE = OUTPUT_DIR / "first_layer_edge_annotation_summary_by_year.csv"
TRANSITION_SUMMARY_FILE = OUTPUT_DIR / "first_layer_edge_transition_summary_by_year.csv"

BASE_YEAR = 1980
N_YEARS = 40
CHUNK_SIZE = 100_000
OVERWRITE = False

PMID_COLUMN = "PMID"
ANNOTATION_COLUMN = "first_layer_edge_annotation"
TRANSITION_COLUMN = "first_layer_future_five_year_transition"
ANNOTATION_TO_TRANSITION_COLUMN = "first_layer_annotation_to_transition"

ANNOTATION_CATEGORIES = [
    "New_Node_Combination",
    "New_Combination",
    "New_Relation",
    "Repeated_Triple",
]
TRANSITION_CATEGORIES = [
    ("New_Node_Combination", "Adopted"),
    ("New_Node_Combination", "Disappeared"),
    ("New_Combination", "Adopted"),
    ("New_Combination", "Disappeared"),
    ("New_Relation", "Adopted"),
    ("New_Relation", "Disappeared"),
    ("Repeated_Triple", "Continued"),
    ("Repeated_Triple", "Disappeared"),
]


def normalize_value(value: object) -> str:
    return "" if pd.isna(value) else str(value).strip()


def annotation_file_for_year(year: int) -> Path:
    return ANNOTATION_DIR / f"{ANNOTATION_FILE_PREFIX}_{year}.csv.gz"


def transition_file_for_year(year: int) -> Path:
    return TRANSITION_DIR / f"{TRANSITION_FILE_PREFIX}_{year}.csv.gz"


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


def years() -> range:
    return range(BASE_YEAR, BASE_YEAR + N_YEARS)


def share(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def summarize_annotation_year(year: int) -> list[dict[str, object]]:
    input_file = annotation_file_for_year(year)
    check_input(input_file)

    predication_counts: Counter[str] = Counter()
    pmids_by_category: defaultdict[str, set[str]] = defaultdict(set)
    all_pmids: set[str] = set()
    total_predication_rows = 0

    reader = pd.read_csv(
        input_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        usecols=[PMID_COLUMN, ANNOTATION_COLUMN],
        dtype={PMID_COLUMN: "string", ANNOTATION_COLUMN: "string"},
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        total_predication_rows += len(chunk)
        chunk[ANNOTATION_COLUMN] = chunk[ANNOTATION_COLUMN].map(normalize_value)
        chunk[PMID_COLUMN] = chunk[PMID_COLUMN].map(normalize_value)
        predication_counts.update(chunk[ANNOTATION_COLUMN])

        for category, group in chunk.groupby(ANNOTATION_COLUMN, dropna=False):
            category = normalize_value(category)
            if not category:
                continue
            pmids = set(pmid for pmid in group[PMID_COLUMN] if pmid)
            pmids_by_category[category].update(pmids)
            all_pmids.update(pmids)

        print(
            f"Annotation year {year}, chunk {chunk_number:,}: "
            f"read {len(chunk):,} rows."
        )

    total_unique_pmids = len(all_pmids)
    rows = []
    for category in ANNOTATION_CATEGORIES:
        n_rows = predication_counts[category]
        n_pmids = len(pmids_by_category[category])
        rows.append(
            {
                "pyear": year,
                "first_layer_edge_annotation": category,
                "n_predication_rows": n_rows,
                "predication_row_share": share(n_rows, total_predication_rows),
                "n_unique_pmids": n_pmids,
                "pmid_share": share(n_pmids, total_unique_pmids),
                "total_predication_rows": total_predication_rows,
                "total_unique_pmids": total_unique_pmids,
            }
        )

    return rows


def summarize_transition_year(year: int) -> list[dict[str, object]]:
    input_file = transition_file_for_year(year)
    check_input(input_file)

    transition_counts: Counter[tuple[str, str]] = Counter()
    pmids_by_transition: defaultdict[tuple[str, str], set[str]] = defaultdict(set)
    all_pmids: set[str] = set()
    total_predication_rows = 0

    reader = pd.read_csv(
        input_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        usecols=[PMID_COLUMN, ANNOTATION_COLUMN, TRANSITION_COLUMN],
        dtype={
            PMID_COLUMN: "string",
            ANNOTATION_COLUMN: "string",
            TRANSITION_COLUMN: "string",
        },
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        total_predication_rows += len(chunk)
        chunk[PMID_COLUMN] = chunk[PMID_COLUMN].map(normalize_value)
        chunk[ANNOTATION_COLUMN] = chunk[ANNOTATION_COLUMN].map(normalize_value)
        chunk[TRANSITION_COLUMN] = chunk[TRANSITION_COLUMN].map(normalize_value)

        for row in chunk.itertuples(index=False):
            pmid = normalize_value(getattr(row, PMID_COLUMN))
            annotation = normalize_value(getattr(row, ANNOTATION_COLUMN))
            transition = normalize_value(getattr(row, TRANSITION_COLUMN))
            key = (annotation, transition)
            transition_counts[key] += 1
            if pmid:
                pmids_by_transition[key].add(pmid)
                all_pmids.add(pmid)

        print(
            f"Transition year {year}, chunk {chunk_number:,}: "
            f"read {len(chunk):,} rows."
        )

    total_unique_pmids = len(all_pmids)
    rows = []
    for annotation, transition in TRANSITION_CATEGORIES:
        key = (annotation, transition)
        n_rows = transition_counts[key]
        n_pmids = len(pmids_by_transition[key])
        rows.append(
            {
                "pyear": year,
                "first_layer_edge_annotation": annotation,
                "first_layer_future_five_year_transition": transition,
                ANNOTATION_TO_TRANSITION_COLUMN: f"{annotation} -> {transition}",
                "n_predication_rows": n_rows,
                "predication_row_share": share(n_rows, total_predication_rows),
                "n_unique_pmids": n_pmids,
                "pmid_share": share(n_pmids, total_unique_pmids),
                "total_predication_rows": total_predication_rows,
                "total_unique_pmids": total_unique_pmids,
            }
        )

    return rows


def main() -> None:
    check_output(ANNOTATION_SUMMARY_FILE)
    check_output(TRANSITION_SUMMARY_FILE)

    annotation_rows = []
    transition_rows = []
    for year in years():
        annotation_rows.extend(summarize_annotation_year(year))
        transition_rows.extend(summarize_transition_year(year))

    annotation_summary = pd.DataFrame(annotation_rows)
    transition_summary = pd.DataFrame(transition_rows)

    annotation_summary.to_csv(ANNOTATION_SUMMARY_FILE, index=False)
    transition_summary.to_csv(TRANSITION_SUMMARY_FILE, index=False)

    print(f"Saved annotation summary to {ANNOTATION_SUMMARY_FILE}")
    print(f"Saved transition summary to {TRANSITION_SUMMARY_FILE}")


if __name__ == "__main__":
    main()
