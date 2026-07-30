"""Summarize BiomedBERT new-combination candidate-set coverage.

This script checks whether focal-year first-layer `New_Combination` edges are
inside the two-hop CUI-CUI candidate edge set used by the BiomedBERT
new-combination prediction track. It evaluates candidate coverage before any
BiomedBERT scoring or top-percentile threshold is applied.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError


PROJECT_DIR = Path("/xdisk/sebratt/jinyugao/projects/innovation_capacity")
INTERIM_DIR = PROJECT_DIR / "data/interim"
RESULT_DIR = PROJECT_DIR / "results/link_prediction/biomedbert/new_combination"

FIRST_LAYER_ANNOTATION_DIR = (
    INTERIM_DIR / "link_prediction/edge_annotation/first_layer"
)
CANDIDATE_EDGE_DIR = (
    INTERIM_DIR / "link_prediction/candidate_edges/two_hop_candidate_edges"
)

FIRST_LAYER_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
CANDIDATE_FILE_PREFIX = "two_hop_candidate_edges_prior_5y"
OUTPUT_FILE = RESULT_DIR / "biomedbert_new_combination_candidate_coverage_summary.csv"

BASE_YEAR = 1980
N_YEARS = 40
CHUNK_SIZE = 250_000
OVERWRITE = False

CONCEPT_PAIR_NODE_A_COLUMN = "concept_pair_node_a"
CONCEPT_PAIR_NODE_B_COLUMN = "concept_pair_node_b"
FIRST_LAYER_ANNOTATION_COLUMN = "first_layer_edge_annotation"
NEW_COMBINATION_CATEGORY = "New_Combination"


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


def first_layer_annotation_file_for_year(focal_year: int) -> Path:
    return (
        FIRST_LAYER_ANNOTATION_DIR
        / f"{FIRST_LAYER_FILE_PREFIX}_first_layer_edge_annotation_{focal_year}.csv.gz"
    )


def candidate_edge_file_for_year(focal_year: int) -> Path:
    return CANDIDATE_EDGE_DIR / f"{CANDIDATE_FILE_PREFIX}_{focal_year}.csv.gz"


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


def read_actual_new_combination_edges(path: Path) -> Counter[tuple[str, str]]:
    edge_predication_counts: Counter[tuple[str, str]] = Counter()

    try:
        reader = pd.read_csv(
            path,
            compression="gzip",
            chunksize=CHUNK_SIZE,
            usecols=[
                CONCEPT_PAIR_NODE_A_COLUMN,
                CONCEPT_PAIR_NODE_B_COLUMN,
                FIRST_LAYER_ANNOTATION_COLUMN,
            ],
            dtype={
                CONCEPT_PAIR_NODE_A_COLUMN: "string",
                CONCEPT_PAIR_NODE_B_COLUMN: "string",
                FIRST_LAYER_ANNOTATION_COLUMN: "string",
            },
        )
    except EmptyDataError:
        return edge_predication_counts

    for chunk in reader:
        chunk = chunk[
            chunk[FIRST_LAYER_ANNOTATION_COLUMN] == NEW_COMBINATION_CATEGORY
        ]
        chunk = chunk.dropna(
            subset=[CONCEPT_PAIR_NODE_A_COLUMN, CONCEPT_PAIR_NODE_B_COLUMN]
        )

        for row in chunk.itertuples(index=False):
            edge = normalize_edge(
                getattr(row, CONCEPT_PAIR_NODE_A_COLUMN),
                getattr(row, CONCEPT_PAIR_NODE_B_COLUMN),
            )
            if all(edge):
                edge_predication_counts[edge] += 1

    return edge_predication_counts


def summarize_candidate_file(
    path: Path,
    actual_new_combination_edges: set[tuple[str, str]],
) -> tuple[int, set[tuple[str, str]]]:
    n_candidate_edges = 0
    actual_edges_in_candidate_set: set[tuple[str, str]] = set()

    try:
        reader = pd.read_csv(
            path,
            compression="gzip",
            chunksize=CHUNK_SIZE,
            usecols=["node_a", "node_b"],
            dtype={"node_a": "string", "node_b": "string"},
        )
    except EmptyDataError:
        return n_candidate_edges, actual_edges_in_candidate_set

    for chunk_number, chunk in enumerate(reader, start=1):
        chunk = chunk.dropna(subset=["node_a", "node_b"])
        n_candidate_edges += len(chunk)

        for row in chunk.itertuples(index=False):
            edge = normalize_edge(row.node_a, row.node_b)
            if edge in actual_new_combination_edges:
                actual_edges_in_candidate_set.add(edge)

        print(
            f"Candidate chunk {chunk_number:,}: scanned {n_candidate_edges:,} "
            "candidate edges; "
            f"actual-new-combination hits {len(actual_edges_in_candidate_set):,}."
        )

    return n_candidate_edges, actual_edges_in_candidate_set


def build_summary_row(focal_year: int) -> dict[str, object]:
    first_layer_file = first_layer_annotation_file_for_year(focal_year)
    candidate_file = candidate_edge_file_for_year(focal_year)
    check_inputs([first_layer_file, candidate_file])

    print(
        "Summarizing BiomedBERT new-combination candidate coverage for focal "
        f"year={focal_year}."
    )

    actual_edge_predication_counts = read_actual_new_combination_edges(
        first_layer_file
    )
    actual_new_combination_edges = set(actual_edge_predication_counts)
    n_actual_new_combination_predications = sum(
        actual_edge_predication_counts.values()
    )
    n_actual_new_combination_edges = len(actual_new_combination_edges)

    n_candidate_edges, candidate_hits = summarize_candidate_file(
        candidate_file,
        actual_new_combination_edges,
    )

    n_hit_edges = len(candidate_hits)
    n_missed_edges = n_actual_new_combination_edges - n_hit_edges
    n_hit_predications = sum(
        actual_edge_predication_counts[edge] for edge in candidate_hits
    )
    n_missed_predications = (
        n_actual_new_combination_predications - n_hit_predications
    )
    n_candidate_false_positive_edges = max(0, n_candidate_edges - n_hit_edges)

    candidate_precision = safe_divide(n_hit_edges, n_candidate_edges)
    candidate_recall = safe_divide(n_hit_edges, n_actual_new_combination_edges)
    candidate_predication_recall = safe_divide(
        n_hit_predications,
        n_actual_new_combination_predications,
    )

    return {
        "pyear": focal_year,
        "n_actual_new_combination_predications": (
            n_actual_new_combination_predications
        ),
        "n_actual_new_combination_edges": n_actual_new_combination_edges,
        "n_biomedbert_new_combination_candidate_edges": n_candidate_edges,
        "n_actual_new_combination_edges_in_candidate_set": n_hit_edges,
        "n_actual_new_combination_edges_outside_candidate_set": n_missed_edges,
        "n_actual_new_combination_predications_in_candidate_set": (
            n_hit_predications
        ),
        "n_actual_new_combination_predications_outside_candidate_set": (
            n_missed_predications
        ),
        "n_candidate_false_positive_edges": n_candidate_false_positive_edges,
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
    print(f"Saved BiomedBERT new-combination candidate coverage to {OUTPUT_FILE}")
    print(f"Rows: {len(summary):,}")


if __name__ == "__main__":
    main()
