"""Annotate SemMedDB predications using top-percentile predicted edges."""

from __future__ import annotations

import os
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError


INTERIM_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim"
)
PRIOR_FIVE_YEAR_EDGE_DIR = (
    INTERIM_DIR / "link_prediction/candidate_edges/prior_five_year_edges"
)
PREDICTED_EDGE_DIR = INTERIM_DIR / "link_prediction/predicted_edges"
SPLIT_PREDICATION_DIR = (
    INTERIM_DIR
    / "semmedVER43_R/split_predications_with_pyear_filtered_by_pyear"
)
OUTPUT_DIR = INTERIM_DIR / "link_prediction/annotated_predications"

INPUT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
OUTPUT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"

BASE_YEAR = 1980
N_YEARS = 40
CHUNK_SIZE = 100_000
TOP_PERCENTILES = [5, 10]
OVERWRITE = False

SUBJECT_CUI_COLUMN = "subject_cui_primary"
OBJECT_CUI_COLUMN = "object_cui_primary"


@dataclass(frozen=True)
class MethodConfig:
    method: str


METHODS = [
    MethodConfig(method="common_neighbor"),
    MethodConfig(method="jaccard"),
    MethodConfig(method="adamic_adar"),
    MethodConfig(method="resource_allocation"),
    MethodConfig(method="preferential_attachment"),
]


def get_task_config() -> tuple[int, MethodConfig, int]:
    task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if task_id is None:
        raise RuntimeError("SLURM_ARRAY_TASK_ID not found.")

    task_index = int(task_id)
    method_percentile_count = len(METHODS) * len(TOP_PERCENTILES)
    max_task_index = N_YEARS * method_percentile_count - 1

    if task_index > max_task_index:
        raise ValueError(
            f"SLURM_ARRAY_TASK_ID={task_index} is out of range. "
            f"Expected 0-{max_task_index}."
        )

    year_offset = task_index % N_YEARS
    combo_index = task_index // N_YEARS
    method_index = combo_index // len(TOP_PERCENTILES)
    percentile_index = combo_index % len(TOP_PERCENTILES)

    focal_year = BASE_YEAR + year_offset
    method_config = METHODS[method_index]
    percentile = TOP_PERCENTILES[percentile_index]
    return focal_year, method_config, percentile


def normalize_node(node: object) -> str:
    return "" if pd.isna(node) else str(node).strip()


def normalize_edge(node_a: object, node_b: object) -> tuple[str, str]:
    node_a_text = normalize_node(node_a)
    node_b_text = normalize_node(node_b)
    return tuple(sorted((node_a_text, node_b_text)))


def top_percentile_label(percentile: int) -> str:
    return f"{percentile}pct"


def prior_five_year_edge_file_for_year(focal_year: int) -> Path:
    return PRIOR_FIVE_YEAR_EDGE_DIR / f"prior_five_year_edges_{focal_year}.csv.gz"


def predicted_edge_file_for_year(
    method_config: MethodConfig,
    focal_year: int,
    percentile: int,
) -> Path:
    label = top_percentile_label(percentile)
    return (
        PREDICTED_EDGE_DIR
        / method_config.method
        / f"{method_config.method}_predicted_edges_top_{label}_{focal_year}.csv.gz"
    )


def predication_file_for_year(focal_year: int) -> Path:
    return SPLIT_PREDICATION_DIR / f"{INPUT_FILE_PREFIX}_{focal_year}.csv.gz"


def output_file_for_year(
    method_config: MethodConfig,
    focal_year: int,
    percentile: int,
) -> Path:
    label = top_percentile_label(percentile)
    return (
        OUTPUT_DIR
        / method_config.method
        / label
        / (
            f"{OUTPUT_FILE_PREFIX}_{method_config.method}_top_{label}_annotated_"
            f"{focal_year}.csv.gz"
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


def read_prior_five_year_edges_and_nodes(
    path: Path,
) -> tuple[set[tuple[str, str]], set[str]]:
    try:
        prior_five_year_edges = pd.read_csv(
            path,
            compression="gzip",
            usecols=["node_a", "node_b"],
            dtype={"node_a": "string", "node_b": "string"},
        )
    except EmptyDataError:
        return set(), set()

    prior_five_year_edges = prior_five_year_edges.dropna(subset=["node_a", "node_b"])
    edge_set = {
        normalize_edge(node_a, node_b)
        for node_a, node_b in zip(
            prior_five_year_edges["node_a"],
            prior_five_year_edges["node_b"],
        )
    }
    node_set = set(prior_five_year_edges["node_a"].astype("string").str.strip())
    node_set.update(prior_five_year_edges["node_b"].astype("string").str.strip())
    node_set.discard("")
    return edge_set, node_set


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

    predicted_edges = predicted_edges.dropna(subset=["node_a", "node_b", "score"])
    predicted_edges["score"] = pd.to_numeric(
        predicted_edges["score"], errors="coerce"
    )
    predicted_edges["rank_within_method_year"] = pd.to_numeric(
        predicted_edges["rank_within_method_year"], errors="coerce"
    )
    predicted_edges = predicted_edges.dropna(
        subset=["score", "rank_within_method_year"]
    )

    return {
        normalize_edge(row.node_a, row.node_b): (
            float(row.score),
            int(row.rank_within_method_year),
        )
        for row in predicted_edges.itertuples(index=False)
    }


def classify_edges(
    chunk: pd.DataFrame,
    prior_five_year_edges: set[tuple[str, str]],
    prior_five_year_nodes: set[str],
    predicted_edges: dict[tuple[str, str], tuple[float, int]],
    method_config: MethodConfig,
    percentile: int,
) -> pd.DataFrame:
    annotated = chunk.copy()
    categories = []
    subject_seen_in_prior_five_year_window = []
    object_seen_in_prior_five_year_window = []
    has_node_absent_from_prior_five_year_window = []
    prediction_scores = []
    prediction_ranks = []

    for subject_cui, object_cui in zip(
        annotated[SUBJECT_CUI_COLUMN], annotated[OBJECT_CUI_COLUMN]
    ):
        subject_node = normalize_node(subject_cui)
        object_node = normalize_node(object_cui)
        edge = normalize_edge(subject_node, object_node)
        subject_seen = subject_node in prior_five_year_nodes
        object_seen = object_node in prior_five_year_nodes
        has_node_absent = not (subject_seen and object_seen)

        if not subject_node or not object_node or subject_node == object_node:
            categories.append("Self_Loop")
            prediction_scores.append(pd.NA)
            prediction_ranks.append(pd.NA)
        elif has_node_absent:
            categories.append("New_Node_Combination")
            prediction_scores.append(pd.NA)
            prediction_ranks.append(pd.NA)
        elif edge in prior_five_year_edges:
            categories.append("Repeated_Combination")
            prediction_scores.append(pd.NA)
            prediction_ranks.append(pd.NA)
        elif edge in predicted_edges:
            score, rank = predicted_edges[edge]
            categories.append("Expected_New_Combination")
            prediction_scores.append(score)
            prediction_ranks.append(rank)
        else:
            categories.append("Surprised_New_Combination")
            prediction_scores.append(pd.NA)
            prediction_ranks.append(pd.NA)

        subject_seen_in_prior_five_year_window.append(subject_seen)
        object_seen_in_prior_five_year_window.append(object_seen)
        has_node_absent_from_prior_five_year_window.append(has_node_absent)

    annotated["category"] = categories
    annotated["subject_seen_in_prior_five_year_window"] = (
        subject_seen_in_prior_five_year_window
    )
    annotated["object_seen_in_prior_five_year_window"] = (
        object_seen_in_prior_five_year_window
    )
    annotated["has_node_absent_from_prior_five_year_window"] = (
        has_node_absent_from_prior_five_year_window
    )
    annotated["link_prediction_method"] = method_config.method
    annotated["top_percentile"] = percentile
    annotated["prediction_score"] = prediction_scores
    annotated["prediction_rank_within_method_year"] = prediction_ranks
    return annotated


def annotate_focal_year(
    focal_year: int,
    method_config: MethodConfig,
    percentile: int,
) -> None:
    prior_five_year_edges_file = prior_five_year_edge_file_for_year(focal_year)
    predicted_edges_file = predicted_edge_file_for_year(
        method_config,
        focal_year,
        percentile,
    )
    predication_file = predication_file_for_year(focal_year)
    output_file = output_file_for_year(method_config, focal_year, percentile)

    check_inputs([prior_five_year_edges_file, predicted_edges_file, predication_file])
    check_output(output_file)

    print(
        f"Annotating focal year {focal_year}; method={method_config.method}; "
        f"top_percentile={percentile}."
    )
    print(f"Reading prior-five-year edges from {prior_five_year_edges_file}")
    prior_five_year_edges, prior_five_year_nodes = (
        read_prior_five_year_edges_and_nodes(prior_five_year_edges_file)
    )
    print(f"Loaded {len(prior_five_year_edges):,} prior-five-year edge(s).")
    print(f"Loaded {len(prior_five_year_nodes):,} prior-five-year node(s).")

    print(f"Reading predicted edges from {predicted_edges_file}")
    predicted_edges = read_predicted_edges(predicted_edges_file)
    print(f"Loaded {len(predicted_edges):,} predicted edge(s).")

    category_counts: Counter[str] = Counter()
    total_rows = 0

    reader = pd.read_csv(
        predication_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        dtype={SUBJECT_CUI_COLUMN: "string", OBJECT_CUI_COLUMN: "string"},
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        annotated_chunk = classify_edges(
            chunk,
            prior_five_year_edges,
            prior_five_year_nodes,
            predicted_edges,
            method_config,
            percentile,
        )
        chunk_counts = Counter(annotated_chunk["category"])
        category_counts.update(chunk_counts)
        total_rows += len(annotated_chunk)

        annotated_chunk.to_csv(
            output_file,
            mode="a",
            index=False,
            compression="gzip",
            header=(chunk_number == 1),
        )

        print(
            f"Chunk {chunk_number:,}: annotated {len(annotated_chunk):,} rows; "
            f"category counts {dict(chunk_counts)}."
        )

    print(f"Saved annotated data to {output_file}")
    print(f"Total rows annotated: {total_rows:,}")
    print(f"Final category counts: {dict(category_counts)}")


def main() -> None:
    focal_year, method_config, percentile = get_task_config()
    annotate_focal_year(focal_year, method_config, percentile)


if __name__ == "__main__":
    main()
