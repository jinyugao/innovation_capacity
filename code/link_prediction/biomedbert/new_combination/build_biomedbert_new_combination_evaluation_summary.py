"""Build BiomedBERT new-combination prediction evaluation summaries."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError


PROJECT_DIR = Path("/xdisk/sebratt/jinyugao/projects/innovation_capacity")
INTERIM_DIR = PROJECT_DIR / "data/interim"
RESULT_DIR = PROJECT_DIR / "results/link_prediction/biomedbert/new_combination"

ANNOTATED_PREDICATION_DIR = (
    INTERIM_DIR
    / "link_prediction/annotated_predications/biomedbert/new_combination/10pct"
)
PREDICTED_EDGE_DIR = INTERIM_DIR / "link_prediction/predicted_edges/biomedbert"

ANNOTATED_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
PREDICTED_EDGE_FILE_PREFIX = (
    "biomedbert_new_combination_predicted_edges_top_10pct"
)
OUTPUT_FILE = RESULT_DIR / "biomedbert_new_combination_evaluation_summary.csv"

BASE_YEAR = 1980
N_YEARS = 40
TOP_PERCENTILE = 10
CHUNK_SIZE = 250_000
OVERWRITE = False

CONCEPT_PAIR_NODE_A_COLUMN = "concept_pair_node_a"
CONCEPT_PAIR_NODE_B_COLUMN = "concept_pair_node_b"
ANNOTATION_COLUMN = "biomedbert_new_combination_annotation"

CATEGORY_NEW_NODE = "New_Node_Combination"
CATEGORY_NEW_RELATION = "New_Relation"
CATEGORY_REPEATED_TRIPLE = "Repeated_Triple"
CATEGORY_EXPECTED_NEW_COMBINATION = "Expected_New_Combination"
CATEGORY_SURPRISED_NEW_COMBINATION = "Surprised_New_Combination"


def normalize_node(node: object) -> str:
    return "" if pd.isna(node) else str(node).strip()


def normalize_edge(node_a: object, node_b: object) -> tuple[str, str]:
    node_a_text = normalize_node(node_a)
    node_b_text = normalize_node(node_b)
    return tuple(sorted((node_a_text, node_b_text)))


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
            f"{ANNOTATED_FILE_PREFIX}_biomedbert_new_combination_top_{label}_"
            f"annotated_{focal_year}.csv.gz"
        )
    )


def predicted_edge_file_for_year(focal_year: int, percentile: int) -> Path:
    label = top_percentile_label(percentile)
    return PREDICTED_EDGE_DIR / f"{PREDICTED_EDGE_FILE_PREFIX}_{focal_year}.csv.gz"


def check_output() -> None:
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    if OUTPUT_FILE.exists() and not OVERWRITE:
        raise FileExistsError(
            "Output file already exists. Set OVERWRITE = True to replace it:\n"
            f"{OUTPUT_FILE}"
        )
    if OUTPUT_FILE.exists() and OVERWRITE:
        OUTPUT_FILE.unlink()


def read_predicted_edge_count(path: Path) -> int:
    try:
        predicted_edges = pd.read_csv(
            path,
            compression="gzip",
            usecols=["node_a", "node_b"],
            dtype={"node_a": "string", "node_b": "string"},
        )
    except EmptyDataError:
        return 0

    predicted_edges = predicted_edges.dropna(subset=["node_a", "node_b"])
    predicted_edge_set = {
        normalize_edge(row.node_a, row.node_b)
        for row in predicted_edges.itertuples(index=False)
    }
    predicted_edge_set = {edge for edge in predicted_edge_set if all(edge)}
    return len(predicted_edge_set)


def empty_annotation_summary() -> dict[str, object]:
    return {
        "n_predications": 0,
        "n_new_node_combination_predications": 0,
        "n_expected_new_combination_predications": 0,
        "n_surprised_new_combination_predications": 0,
        "n_actual_new_combination_predications": 0,
        "n_new_relation_predications": 0,
        "n_repeated_triple_predications": 0,
        "n_actual_new_combination_edges": 0,
        "n_expected_new_combination_edges": 0,
        "n_surprised_new_combination_edges": 0,
    }


def summarize_annotated_predications(path: Path) -> dict[str, object]:
    category_counts: Counter[str] = Counter()
    actual_new_combination_edges: set[tuple[str, str]] = set()
    expected_new_combination_edges: set[tuple[str, str]] = set()
    surprised_new_combination_edges: set[tuple[str, str]] = set()
    total_predications = 0

    try:
        reader = pd.read_csv(
            path,
            compression="gzip",
            chunksize=CHUNK_SIZE,
            usecols=[
                CONCEPT_PAIR_NODE_A_COLUMN,
                CONCEPT_PAIR_NODE_B_COLUMN,
                ANNOTATION_COLUMN,
            ],
            dtype={
                CONCEPT_PAIR_NODE_A_COLUMN: "string",
                CONCEPT_PAIR_NODE_B_COLUMN: "string",
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
                CATEGORY_EXPECTED_NEW_COMBINATION,
                CATEGORY_SURPRISED_NEW_COMBINATION,
            }:
                continue

            edge = normalize_edge(
                getattr(row, CONCEPT_PAIR_NODE_A_COLUMN),
                getattr(row, CONCEPT_PAIR_NODE_B_COLUMN),
            )
            if not all(edge):
                continue

            actual_new_combination_edges.add(edge)
            if category == CATEGORY_EXPECTED_NEW_COMBINATION:
                expected_new_combination_edges.add(edge)
            else:
                surprised_new_combination_edges.add(edge)

    return {
        "n_predications": total_predications,
        "n_new_node_combination_predications": category_counts[CATEGORY_NEW_NODE],
        "n_expected_new_combination_predications": category_counts[
            CATEGORY_EXPECTED_NEW_COMBINATION
        ],
        "n_surprised_new_combination_predications": category_counts[
            CATEGORY_SURPRISED_NEW_COMBINATION
        ],
        "n_actual_new_combination_predications": (
            category_counts[CATEGORY_EXPECTED_NEW_COMBINATION]
            + category_counts[CATEGORY_SURPRISED_NEW_COMBINATION]
        ),
        "n_new_relation_predications": category_counts[CATEGORY_NEW_RELATION],
        "n_repeated_triple_predications": category_counts[CATEGORY_REPEATED_TRIPLE],
        "n_actual_new_combination_edges": len(actual_new_combination_edges),
        "n_expected_new_combination_edges": len(expected_new_combination_edges),
        "n_surprised_new_combination_edges": len(surprised_new_combination_edges),
    }


def build_summary_row(focal_year: int, percentile: int) -> dict[str, object]:
    annotated_file = annotated_file_for_year(focal_year, percentile)
    predicted_edge_file = predicted_edge_file_for_year(focal_year, percentile)

    missing_files = [
        str(path)
        for path in [annotated_file, predicted_edge_file]
        if not path.exists()
    ]
    if missing_files:
        missing = "\n".join(missing_files)
        raise FileNotFoundError(f"Missing required input file(s):\n{missing}")

    print(
        "Summarizing BiomedBERT new-combination evaluation for "
        f"focal year={focal_year}; top_percentile={percentile}."
    )
    annotated_summary = summarize_annotated_predications(annotated_file)
    n_predicted_edges = read_predicted_edge_count(predicted_edge_file)
    n_expected_edges = int(annotated_summary["n_expected_new_combination_edges"])
    n_actual_edges = int(annotated_summary["n_actual_new_combination_edges"])
    n_false_positive_edges = max(0, n_predicted_edges - n_expected_edges)

    precision = safe_divide(n_expected_edges, n_predicted_edges)
    recall = safe_divide(n_expected_edges, n_actual_edges)

    return {
        "pyear": focal_year,
        "method": "biomedbert",
        "prediction_task": "new_combination",
        "top_percentile": percentile,
        **annotated_summary,
        "n_predicted_edges": n_predicted_edges,
        "n_false_positive_edges": n_false_positive_edges,
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
    print(f"Saved BiomedBERT new-combination evaluation summary to {OUTPUT_FILE}")
    print(f"Rows: {len(summary):,}")


if __name__ == "__main__":
    main()
