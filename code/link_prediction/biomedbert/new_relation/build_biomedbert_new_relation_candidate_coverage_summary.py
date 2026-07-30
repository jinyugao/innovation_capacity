"""Summarize BiomedBERT new-relation candidate-set coverage.

This script checks whether focal-year first-layer `New_Relation` triples are
inside the BiomedBERT new-relation candidate triple set. It evaluates candidate
coverage before any BiomedBERT scoring or top-percentile threshold is applied.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError


PROJECT_DIR = Path("/xdisk/sebratt/jinyugao/projects/innovation_capacity")
INTERIM_DIR = PROJECT_DIR / "data/interim"
RESULT_DIR = PROJECT_DIR / "results/link_prediction/biomedbert/new_relation"

FIRST_LAYER_ANNOTATION_DIR = (
    INTERIM_DIR / "link_prediction/edge_annotation/first_layer"
)
CANDIDATE_TRIPLE_DIR = (
    INTERIM_DIR / "biomedbert_link_prediction/new_relation/candidate_triples"
)

FIRST_LAYER_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
CANDIDATE_FILE_PREFIX = "biomedbert_new_relation_candidate_triples"
OUTPUT_FILE = RESULT_DIR / "biomedbert_new_relation_candidate_coverage_summary.csv"

BASE_YEAR = 1980
N_YEARS = 40
CHUNK_SIZE = 250_000
OVERWRITE = False

SUBJECT_CUI_COLUMN = "subject_cui_primary"
OBJECT_CUI_COLUMN = "object_cui_primary"
PREDICATE_COLUMN = "PREDICATE"
FIRST_LAYER_ANNOTATION_COLUMN = "first_layer_edge_annotation"
NEW_RELATION_CATEGORY = "New_Relation"


def normalize_value(value: object) -> str:
    return "" if pd.isna(value) else str(value).strip()


def normalize_triple(
    subject_cui: object,
    predicate: object,
    object_cui: object,
) -> tuple[str, str, str]:
    return (
        normalize_value(subject_cui),
        normalize_value(predicate),
        normalize_value(object_cui),
    )


def safe_divide(numerator: int, denominator: int) -> object:
    if denominator == 0:
        return pd.NA
    return numerator / denominator


def f1_score(precision: object, recall: object) -> object:
    if pd.isna(precision) or pd.isna(recall) or precision + recall == 0:
        return pd.NA
    return 2 * precision * recall / (precision + recall)


def first_layer_annotation_file_for_year(focal_year: int) -> Path:
    return (
        FIRST_LAYER_ANNOTATION_DIR
        / f"{FIRST_LAYER_FILE_PREFIX}_first_layer_edge_annotation_{focal_year}.csv.gz"
    )


def candidate_triple_file_for_year(focal_year: int) -> Path:
    return CANDIDATE_TRIPLE_DIR / f"{CANDIDATE_FILE_PREFIX}_{focal_year}.csv.gz"


def check_output() -> None:
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    if OUTPUT_FILE.exists() and not OVERWRITE:
        raise FileExistsError(
            "Output file already exists. Set OVERWRITE = True to replace it:\n"
            f"{OUTPUT_FILE}"
        )
    if OUTPUT_FILE.exists() and OVERWRITE:
        OUTPUT_FILE.unlink()


def check_inputs(paths: list[Path]) -> None:
    missing_files = [str(path) for path in paths if not path.exists()]
    if missing_files:
        missing = "\n".join(missing_files)
        raise FileNotFoundError(f"Missing required input file(s):\n{missing}")


def read_actual_new_relation_triples(path: Path) -> Counter[tuple[str, str, str]]:
    triple_predication_counts: Counter[tuple[str, str, str]] = Counter()

    try:
        reader = pd.read_csv(
            path,
            compression="gzip",
            chunksize=CHUNK_SIZE,
            usecols=[
                SUBJECT_CUI_COLUMN,
                PREDICATE_COLUMN,
                OBJECT_CUI_COLUMN,
                FIRST_LAYER_ANNOTATION_COLUMN,
            ],
            dtype={
                SUBJECT_CUI_COLUMN: "string",
                PREDICATE_COLUMN: "string",
                OBJECT_CUI_COLUMN: "string",
                FIRST_LAYER_ANNOTATION_COLUMN: "string",
            },
        )
    except EmptyDataError:
        return triple_predication_counts

    for chunk in reader:
        chunk = chunk[chunk[FIRST_LAYER_ANNOTATION_COLUMN] == NEW_RELATION_CATEGORY]
        chunk = chunk.dropna(
            subset=[SUBJECT_CUI_COLUMN, PREDICATE_COLUMN, OBJECT_CUI_COLUMN]
        )

        for row in chunk.itertuples(index=False):
            triple = normalize_triple(
                getattr(row, SUBJECT_CUI_COLUMN),
                getattr(row, PREDICATE_COLUMN),
                getattr(row, OBJECT_CUI_COLUMN),
            )
            if all(triple):
                triple_predication_counts[triple] += 1

    return triple_predication_counts


def summarize_candidate_file(
    path: Path,
    actual_new_relation_triples: set[tuple[str, str, str]],
) -> tuple[int, set[tuple[str, str, str]]]:
    n_candidate_triples = 0
    actual_triples_in_candidate_set: set[tuple[str, str, str]] = set()

    try:
        reader = pd.read_csv(
            path,
            compression="gzip",
            chunksize=CHUNK_SIZE,
            usecols=["subject_cui", "predicate", "object_cui"],
            dtype={
                "subject_cui": "string",
                "predicate": "string",
                "object_cui": "string",
            },
        )
    except EmptyDataError:
        return n_candidate_triples, actual_triples_in_candidate_set

    for chunk_number, chunk in enumerate(reader, start=1):
        chunk = chunk.dropna(subset=["subject_cui", "predicate", "object_cui"])
        n_candidate_triples += len(chunk)

        for row in chunk.itertuples(index=False):
            triple = normalize_triple(row.subject_cui, row.predicate, row.object_cui)
            if triple in actual_new_relation_triples:
                actual_triples_in_candidate_set.add(triple)

        print(
            f"Candidate chunk {chunk_number:,}: scanned {n_candidate_triples:,} "
            "candidate triples; "
            f"actual-new-relation hits {len(actual_triples_in_candidate_set):,}."
        )

    return n_candidate_triples, actual_triples_in_candidate_set


def build_summary_row(focal_year: int) -> dict[str, object]:
    first_layer_file = first_layer_annotation_file_for_year(focal_year)
    candidate_file = candidate_triple_file_for_year(focal_year)
    check_inputs([first_layer_file, candidate_file])

    print(
        "Summarizing BiomedBERT new-relation candidate coverage for focal "
        f"year={focal_year}."
    )

    actual_triple_predication_counts = read_actual_new_relation_triples(
        first_layer_file
    )
    actual_new_relation_triples = set(actual_triple_predication_counts)
    n_actual_new_relation_predications = sum(
        actual_triple_predication_counts.values()
    )
    n_actual_new_relation_triples = len(actual_new_relation_triples)

    n_candidate_triples, candidate_hits = summarize_candidate_file(
        candidate_file,
        actual_new_relation_triples,
    )

    n_hit_triples = len(candidate_hits)
    n_missed_triples = n_actual_new_relation_triples - n_hit_triples
    n_hit_predications = sum(
        actual_triple_predication_counts[triple] for triple in candidate_hits
    )
    n_missed_predications = (
        n_actual_new_relation_predications - n_hit_predications
    )
    n_candidate_false_positive_triples = max(0, n_candidate_triples - n_hit_triples)

    candidate_precision = safe_divide(n_hit_triples, n_candidate_triples)
    candidate_recall = safe_divide(n_hit_triples, n_actual_new_relation_triples)
    candidate_predication_recall = safe_divide(
        n_hit_predications,
        n_actual_new_relation_predications,
    )

    return {
        "pyear": focal_year,
        "n_actual_new_relation_predications": n_actual_new_relation_predications,
        "n_actual_new_relation_triples": n_actual_new_relation_triples,
        "n_biomedbert_new_relation_candidate_triples": n_candidate_triples,
        "n_actual_new_relation_triples_in_candidate_set": n_hit_triples,
        "n_actual_new_relation_triples_outside_candidate_set": n_missed_triples,
        "n_actual_new_relation_predications_in_candidate_set": n_hit_predications,
        "n_actual_new_relation_predications_outside_candidate_set": (
            n_missed_predications
        ),
        "n_candidate_false_positive_triples": n_candidate_false_positive_triples,
        "candidate_precision": candidate_precision,
        "candidate_recall": candidate_recall,
        "candidate_predication_recall": candidate_predication_recall,
        "candidate_f1_score": f1_score(candidate_precision, candidate_recall),
    }


def main() -> None:
    check_output()
    rows = []

    for focal_year in range(BASE_YEAR, BASE_YEAR + N_YEARS):
        rows.append(build_summary_row(focal_year))

    summary = pd.DataFrame(rows)
    summary.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved BiomedBERT new-relation candidate coverage to {OUTPUT_FILE}")
    print(f"Rows: {len(summary):,}")


if __name__ == "__main__":
    main()
