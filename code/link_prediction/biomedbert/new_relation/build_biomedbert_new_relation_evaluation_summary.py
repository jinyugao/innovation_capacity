"""Build BiomedBERT new-relation prediction evaluation summaries."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError


PROJECT_DIR = Path("/xdisk/sebratt/jinyugao/projects/innovation_capacity")
INTERIM_DIR = PROJECT_DIR / "data/interim"
RESULT_DIR = PROJECT_DIR / "results/link_prediction/biomedbert/new_relation"

ANNOTATED_PREDICATION_DIR = (
    INTERIM_DIR
    / "link_prediction/annotated_predications/biomedbert/new_relation/10pct"
)
PREDICTED_TRIPLE_DIR = (
    INTERIM_DIR / "link_prediction/predicted_triples/biomedbert/new_relation"
)

ANNOTATED_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
PREDICTED_TRIPLE_FILE_PREFIX = "biomedbert_new_relation_predicted_triples_top_10pct"
OUTPUT_FILE = RESULT_DIR / "biomedbert_new_relation_evaluation_summary.csv"

BASE_YEAR = 1980
N_YEARS = 40
TOP_PERCENTILE = 10
CHUNK_SIZE = 250_000
OVERWRITE = False

SUBJECT_CUI_COLUMN = "subject_cui_primary"
OBJECT_CUI_COLUMN = "object_cui_primary"
PREDICATE_COLUMN = "PREDICATE"
ANNOTATION_COLUMN = "biomedbert_new_relation_annotation"

CATEGORY_NEW_NODE = "New_Node_Combination"
CATEGORY_NEW_COMBINATION = "New_Combination"
CATEGORY_REPEATED_TRIPLE = "Repeated_Triple"
CATEGORY_EXPECTED_NEW_RELATION = "Expected_New_Relation"
CATEGORY_SURPRISED_NEW_RELATION = "Surprised_New_Relation"


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


def top_percentile_label(percentile: int) -> str:
    return f"{percentile}pct"


def annotated_file_for_year(focal_year: int, percentile: int) -> Path:
    label = top_percentile_label(percentile)
    return (
        ANNOTATED_PREDICATION_DIR.parent
        / label
        / (
            f"{ANNOTATED_FILE_PREFIX}_biomedbert_new_relation_top_{label}_annotated_"
            f"{focal_year}.csv.gz"
        )
    )


def predicted_triple_file_for_year(focal_year: int, percentile: int) -> Path:
    label = top_percentile_label(percentile)
    return (
        PREDICTED_TRIPLE_DIR
        / f"{PREDICTED_TRIPLE_FILE_PREFIX}_{focal_year}.csv.gz"
    )


def check_output() -> None:
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    if OUTPUT_FILE.exists() and not OVERWRITE:
        raise FileExistsError(
            "Output file already exists. Set OVERWRITE = True to replace it:\n"
            f"{OUTPUT_FILE}"
        )
    if OUTPUT_FILE.exists() and OVERWRITE:
        OUTPUT_FILE.unlink()


def read_predicted_triple_count(path: Path) -> int:
    try:
        predicted_triples = pd.read_csv(
            path,
            compression="gzip",
            usecols=["subject_cui", "predicate", "object_cui"],
            dtype={
                "subject_cui": "string",
                "predicate": "string",
                "object_cui": "string",
            },
        )
    except EmptyDataError:
        return 0

    predicted_triples = predicted_triples.dropna(
        subset=["subject_cui", "predicate", "object_cui"]
    )
    predicted_triple_set = {
        normalize_triple(row.subject_cui, row.predicate, row.object_cui)
        for row in predicted_triples.itertuples(index=False)
    }
    predicted_triple_set = {triple for triple in predicted_triple_set if all(triple)}
    return len(predicted_triple_set)


def empty_annotation_summary() -> dict[str, object]:
    return {
        "n_predications": 0,
        "n_new_node_combination_predications": 0,
        "n_new_combination_predications": 0,
        "n_repeated_triple_predications": 0,
        "n_expected_new_relation_predications": 0,
        "n_surprised_new_relation_predications": 0,
        "n_actual_new_relation_predications": 0,
        "n_actual_new_relation_triples": 0,
        "n_expected_new_relation_triples": 0,
        "n_surprised_new_relation_triples": 0,
    }


def summarize_annotated_predications(path: Path) -> dict[str, object]:
    category_counts: Counter[str] = Counter()
    actual_new_relation_triples: set[tuple[str, str, str]] = set()
    expected_new_relation_triples: set[tuple[str, str, str]] = set()
    surprised_new_relation_triples: set[tuple[str, str, str]] = set()
    total_predications = 0

    try:
        reader = pd.read_csv(
            path,
            compression="gzip",
            chunksize=CHUNK_SIZE,
            usecols=[
                SUBJECT_CUI_COLUMN,
                PREDICATE_COLUMN,
                OBJECT_CUI_COLUMN,
                ANNOTATION_COLUMN,
            ],
            dtype={
                SUBJECT_CUI_COLUMN: "string",
                PREDICATE_COLUMN: "string",
                OBJECT_CUI_COLUMN: "string",
                ANNOTATION_COLUMN: "string",
            },
        )
    except EmptyDataError:
        return empty_annotation_summary()

    for chunk in reader:
        total_predications += len(chunk)
        category_counts.update(chunk[ANNOTATION_COLUMN].dropna().tolist())

        for row in chunk.itertuples(index=False):
            category = getattr(row, ANNOTATION_COLUMN)
            if pd.isna(category):
                continue
            if category not in {
                CATEGORY_EXPECTED_NEW_RELATION,
                CATEGORY_SURPRISED_NEW_RELATION,
            }:
                continue

            triple = normalize_triple(
                getattr(row, SUBJECT_CUI_COLUMN),
                getattr(row, PREDICATE_COLUMN),
                getattr(row, OBJECT_CUI_COLUMN),
            )
            if not all(triple):
                continue

            actual_new_relation_triples.add(triple)
            if category == CATEGORY_EXPECTED_NEW_RELATION:
                expected_new_relation_triples.add(triple)
            else:
                surprised_new_relation_triples.add(triple)

    return {
        "n_predications": total_predications,
        "n_new_node_combination_predications": category_counts[CATEGORY_NEW_NODE],
        "n_new_combination_predications": category_counts[CATEGORY_NEW_COMBINATION],
        "n_repeated_triple_predications": category_counts[CATEGORY_REPEATED_TRIPLE],
        "n_expected_new_relation_predications": category_counts[
            CATEGORY_EXPECTED_NEW_RELATION
        ],
        "n_surprised_new_relation_predications": category_counts[
            CATEGORY_SURPRISED_NEW_RELATION
        ],
        "n_actual_new_relation_predications": (
            category_counts[CATEGORY_EXPECTED_NEW_RELATION]
            + category_counts[CATEGORY_SURPRISED_NEW_RELATION]
        ),
        "n_actual_new_relation_triples": len(actual_new_relation_triples),
        "n_expected_new_relation_triples": len(expected_new_relation_triples),
        "n_surprised_new_relation_triples": len(surprised_new_relation_triples),
    }


def build_summary_row(focal_year: int, percentile: int) -> dict[str, object]:
    annotated_file = annotated_file_for_year(focal_year, percentile)
    predicted_triple_file = predicted_triple_file_for_year(focal_year, percentile)

    missing_files = [
        str(path)
        for path in [annotated_file, predicted_triple_file]
        if not path.exists()
    ]
    if missing_files:
        missing = "\n".join(missing_files)
        raise FileNotFoundError(f"Missing required input file(s):\n{missing}")

    print(
        "Summarizing BiomedBERT new-relation evaluation for "
        f"focal year={focal_year}; top_percentile={percentile}."
    )
    annotated_summary = summarize_annotated_predications(annotated_file)
    n_predicted_triples = read_predicted_triple_count(predicted_triple_file)
    n_expected_triples = int(annotated_summary["n_expected_new_relation_triples"])
    n_actual_triples = int(annotated_summary["n_actual_new_relation_triples"])
    n_false_positive_triples = max(0, n_predicted_triples - n_expected_triples)

    precision = safe_divide(n_expected_triples, n_predicted_triples)
    recall = safe_divide(n_expected_triples, n_actual_triples)

    return {
        "pyear": focal_year,
        "method": "biomedbert",
        "prediction_task": "new_relation",
        "top_percentile": percentile,
        **annotated_summary,
        "n_predicted_triples": n_predicted_triples,
        "n_false_positive_triples": n_false_positive_triples,
        "precision": precision,
        "recall": recall,
        "f1_score": f1_score(precision, recall),
    }


def main() -> None:
    check_output()
    rows = []

    for focal_year in range(BASE_YEAR, BASE_YEAR + N_YEARS):
        rows.append(build_summary_row(focal_year, TOP_PERCENTILE))

    summary = pd.DataFrame(rows)
    summary.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved BiomedBERT new-relation evaluation summary to {OUTPUT_FILE}")
    print(f"Rows: {len(summary):,}")


if __name__ == "__main__":
    main()
