"""Annotate first-layer predications with BiomedBERT new-combination predictions."""

from __future__ import annotations

import os
from collections import Counter
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError


INTERIM_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim"
)
FIRST_LAYER_ANNOTATION_DIR = (
    INTERIM_DIR / "link_prediction/edge_annotation/first_layer"
)
PREDICTED_EDGE_DIR = INTERIM_DIR / "link_prediction/predicted_edges/biomedbert"
OUTPUT_DIR = (
    INTERIM_DIR
    / "link_prediction/annotated_predications/biomedbert/new_combination/10pct"
)

FIRST_LAYER_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
PREDICTED_EDGE_FILE_PREFIX = (
    "biomedbert_new_combination_predicted_edges_top_10pct"
)
OUTPUT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"

BASE_YEAR = 1980
N_YEARS = 40
TOP_PERCENTILE = 10
CHUNK_SIZE = 100_000
OVERWRITE = False

CONCEPT_PAIR_NODE_A_COLUMN = "concept_pair_node_a"
CONCEPT_PAIR_NODE_B_COLUMN = "concept_pair_node_b"
FIRST_LAYER_ANNOTATION_COLUMN = "first_layer_edge_annotation"
OUTPUT_ANNOTATION_COLUMN = "biomedbert_new_combination_annotation"

CATEGORY_NEW_COMBINATION = "New_Combination"
CATEGORY_EXPECTED_NEW_COMBINATION = "Expected_New_Combination"
CATEGORY_SURPRISED_NEW_COMBINATION = "Surprised_New_Combination"

MINIMUM_INPUT_COLUMNS = [
    CONCEPT_PAIR_NODE_A_COLUMN,
    CONCEPT_PAIR_NODE_B_COLUMN,
    FIRST_LAYER_ANNOTATION_COLUMN,
]
ADDED_OUTPUT_COLUMNS = [
    OUTPUT_ANNOTATION_COLUMN,
    "biomedbert_new_combination_method",
    "biomedbert_new_combination_top_percentile",
    "biomedbert_new_combination_predicted",
    "biomedbert_new_combination_score",
    "biomedbert_new_combination_rank_within_method_year",
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


def normalize_node(node: object) -> str:
    return "" if pd.isna(node) else str(node).strip()


def normalize_edge(node_a: object, node_b: object) -> tuple[str, str]:
    node_a_text = normalize_node(node_a)
    node_b_text = normalize_node(node_b)
    return tuple(sorted((node_a_text, node_b_text)))


def first_layer_annotation_file_for_year(focal_year: int) -> Path:
    return (
        FIRST_LAYER_ANNOTATION_DIR
        / f"{FIRST_LAYER_FILE_PREFIX}_first_layer_edge_annotation_{focal_year}.csv.gz"
    )


def predicted_edge_file_for_year(focal_year: int) -> Path:
    return PREDICTED_EDGE_DIR / f"{PREDICTED_EDGE_FILE_PREFIX}_{focal_year}.csv.gz"


def output_file_for_year(focal_year: int) -> Path:
    return (
        OUTPUT_DIR
        / (
            f"{OUTPUT_FILE_PREFIX}_biomedbert_new_combination_top_10pct_"
            f"annotated_{focal_year}.csv.gz"
        )
    )


def check_inputs(paths: list[Path]) -> None:
    missing_files = [str(path) for path in paths if not path.exists()]
    if missing_files:
        missing = "\n".join(missing_files)
        raise FileNotFoundError(f"Missing required input file(s):\n{missing}")


def check_output(output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    if output_file.exists() and not OVERWRITE:
        raise FileExistsError(
            "Output file already exists. Set OVERWRITE = True to replace it:\n"
            f"{output_file}"
        )
    if output_file.exists() and OVERWRITE:
        output_file.unlink()


def output_columns_for_empty_file(first_layer_file: Path) -> list[str]:
    try:
        input_columns = pd.read_csv(
            first_layer_file,
            compression="gzip",
            nrows=0,
        ).columns.tolist()
    except EmptyDataError:
        input_columns = MINIMUM_INPUT_COLUMNS.copy()

    return input_columns + [
        column for column in ADDED_OUTPUT_COLUMNS if column not in input_columns
    ]


def read_predicted_edges(path: Path) -> dict[tuple[str, str], tuple[float, int]]:
    try:
        predicted_edges = pd.read_csv(
            path,
            compression="gzip",
            usecols=["node_a", "node_b", "score", "rank_within_method_year"],
            dtype={"node_a": "string", "node_b": "string"},
        )
    except EmptyDataError:
        return {}

    predicted_edges = predicted_edges.dropna(
        subset=["node_a", "node_b", "score", "rank_within_method_year"]
    )
    predicted_edges["score"] = pd.to_numeric(
        predicted_edges["score"], errors="coerce"
    )
    predicted_edges["rank_within_method_year"] = pd.to_numeric(
        predicted_edges["rank_within_method_year"], errors="coerce"
    )
    predicted_edges = predicted_edges.dropna(
        subset=["score", "rank_within_method_year"]
    )

    edge_map: dict[tuple[str, str], tuple[float, int]] = {}
    for row in predicted_edges.itertuples(index=False):
        edge = normalize_edge(row.node_a, row.node_b)
        if all(edge):
            edge_map[edge] = (
                float(row.score),
                int(row.rank_within_method_year),
            )

    return edge_map


def annotate_chunk(
    chunk: pd.DataFrame,
    predicted_edges: dict[tuple[str, str], tuple[float, int]],
) -> pd.DataFrame:
    annotated = chunk.copy()
    categories = []
    prediction_scores = []
    prediction_ranks = []
    is_predicted_values = []

    for row in annotated.itertuples(index=False):
        first_layer_annotation = getattr(row, FIRST_LAYER_ANNOTATION_COLUMN)

        if first_layer_annotation != CATEGORY_NEW_COMBINATION:
            categories.append(first_layer_annotation)
            prediction_scores.append(pd.NA)
            prediction_ranks.append(pd.NA)
            is_predicted_values.append(False)
            continue

        edge = normalize_edge(
            getattr(row, CONCEPT_PAIR_NODE_A_COLUMN),
            getattr(row, CONCEPT_PAIR_NODE_B_COLUMN),
        )

        if edge in predicted_edges:
            score, rank = predicted_edges[edge]
            categories.append(CATEGORY_EXPECTED_NEW_COMBINATION)
            prediction_scores.append(score)
            prediction_ranks.append(rank)
            is_predicted_values.append(True)
        else:
            categories.append(CATEGORY_SURPRISED_NEW_COMBINATION)
            prediction_scores.append(pd.NA)
            prediction_ranks.append(pd.NA)
            is_predicted_values.append(False)

    annotated[OUTPUT_ANNOTATION_COLUMN] = categories
    annotated["biomedbert_new_combination_method"] = "biomedbert"
    annotated["biomedbert_new_combination_top_percentile"] = TOP_PERCENTILE
    annotated["biomedbert_new_combination_predicted"] = is_predicted_values
    annotated["biomedbert_new_combination_score"] = prediction_scores
    annotated["biomedbert_new_combination_rank_within_method_year"] = prediction_ranks
    return annotated


def annotate_focal_year(focal_year: int) -> None:
    first_layer_file = first_layer_annotation_file_for_year(focal_year)
    predicted_edge_file = predicted_edge_file_for_year(focal_year)
    output_file = output_file_for_year(focal_year)

    check_inputs([first_layer_file, predicted_edge_file])
    check_output(output_file)

    print(f"Annotating BiomedBERT new-combination predictions for {focal_year}.")
    predicted_edges = read_predicted_edges(predicted_edge_file)
    print(f"Loaded {len(predicted_edges):,} predicted edge(s).")

    category_counts: Counter[str] = Counter()
    total_rows = 0
    wrote_header = False

    reader = pd.read_csv(
        first_layer_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        dtype={
            CONCEPT_PAIR_NODE_A_COLUMN: "string",
            CONCEPT_PAIR_NODE_B_COLUMN: "string",
            FIRST_LAYER_ANNOTATION_COLUMN: "string",
        },
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        annotated_chunk = annotate_chunk(chunk, predicted_edges)
        chunk_counts = Counter(annotated_chunk[OUTPUT_ANNOTATION_COLUMN])
        category_counts.update(chunk_counts)
        total_rows += len(annotated_chunk)

        annotated_chunk.to_csv(
            output_file,
            mode="a",
            index=False,
            compression="gzip",
            header=not wrote_header,
        )
        wrote_header = True

        print(
            f"Chunk {chunk_number:,}: annotated {len(annotated_chunk):,} rows; "
            f"category counts {dict(chunk_counts)}."
        )

    if not wrote_header:
        pd.DataFrame(columns=output_columns_for_empty_file(first_layer_file)).to_csv(
            output_file,
            index=False,
            compression="gzip",
        )

    print(f"Saved BiomedBERT new-combination annotation to {output_file}")
    print(f"Total rows annotated: {total_rows:,}")
    print(f"Final category counts: {dict(category_counts)}")


def main() -> None:
    focal_year = get_focal_year()
    annotate_focal_year(focal_year)


if __name__ == "__main__":
    main()
