"""Build top-percentile predicted edges from scored candidate edge files."""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


INTERIM_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim"
)
OUTPUT_DIR = INTERIM_DIR / "link_prediction/predicted_edges"

BASE_YEAR = 1980
CHUNK_SIZE = 1_000_000
TOP_PERCENTILES = [5, 10]
OVERWRITE = False


@dataclass(frozen=True)
class MethodConfig:
    method: str
    input_dir: Path
    input_file_prefix: str
    score_column: str


METHODS = [
    MethodConfig(
        method="common_neighbor",
        input_dir=INTERIM_DIR / "common_neighbor_link_prediction/candidate_edges",
        input_file_prefix="common_neighbor_scored_candidate_edges",
        score_column="common_neighbor_score",
    ),
    MethodConfig(
        method="jaccard",
        input_dir=INTERIM_DIR / "jaccard_link_prediction/candidate_edges",
        input_file_prefix="jaccard_scored_candidate_edges",
        score_column="jaccard_score",
    ),
    MethodConfig(
        method="adamic_adar",
        input_dir=INTERIM_DIR / "adamic_adar_link_prediction/candidate_edges",
        input_file_prefix="adamic_adar_scored_candidate_edges",
        score_column="adamic_adar_score",
    ),
    MethodConfig(
        method="resource_allocation",
        input_dir=INTERIM_DIR / "resource_allocation_link_prediction/candidate_edges",
        input_file_prefix="resource_allocation_scored_candidate_edges",
        score_column="resource_allocation_score",
    ),
    MethodConfig(
        method="preferential_attachment",
        input_dir=INTERIM_DIR
        / "preferential_attachment_link_prediction/candidate_edges",
        input_file_prefix="preferential_attachment_scored_candidate_edges",
        score_column="preferential_attachment_score",
    ),
]


def get_focal_year() -> int:
    task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if task_id is None:
        raise RuntimeError("SLURM_ARRAY_TASK_ID not found.")
    return int(task_id) + BASE_YEAR


def input_file_for_year(method_config: MethodConfig, focal_year: int) -> Path:
    return (
        method_config.input_dir
        / f"{method_config.input_file_prefix}_{focal_year}.csv.gz"
    )


def top_percentile_label(percentile: int) -> str:
    return f"{percentile}pct"


def output_file_for_year(
    method_config: MethodConfig,
    focal_year: int,
    percentile: int,
) -> Path:
    label = top_percentile_label(percentile)
    return (
        OUTPUT_DIR
        / method_config.method
        / f"{method_config.method}_predicted_edges_top_{label}_{focal_year}.csv.gz"
    )


def check_outputs(method_config: MethodConfig, focal_year: int) -> None:
    for percentile in TOP_PERCENTILES:
        output_file = output_file_for_year(method_config, focal_year, percentile)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        if output_file.exists() and not OVERWRITE:
            raise FileExistsError(
                "Output file already exists. Set OVERWRITE = True to replace it:\n"
                f"{output_file}"
            )

        if output_file.exists() and OVERWRITE:
            output_file.unlink()


def count_rows(input_file: Path, score_column: str) -> int:
    total_rows = 0
    reader = pd.read_csv(
        input_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        usecols=["node_a", "node_b", score_column],
    )

    for chunk in reader:
        total_rows += len(chunk)

    return total_rows


def n_keep_by_percentile(total_rows: int) -> dict[int, int]:
    if total_rows == 0:
        return {percentile: 0 for percentile in TOP_PERCENTILES}

    return {
        percentile: max(1, math.ceil(total_rows * percentile / 100))
        for percentile in TOP_PERCENTILES
    }


def standardize_chunk(
    chunk: pd.DataFrame,
    method_config: MethodConfig,
) -> pd.DataFrame:
    chunk = chunk.dropna(subset=["node_a", "node_b", method_config.score_column])
    chunk = chunk.copy()
    chunk["node_a"] = chunk["node_a"].astype("string").str.strip()
    chunk["node_b"] = chunk["node_b"].astype("string").str.strip()
    chunk["score"] = pd.to_numeric(
        chunk[method_config.score_column],
        errors="coerce",
    )
    chunk = chunk.dropna(subset=["node_a", "node_b", "score"])
    return chunk[["node_a", "node_b", "score"]]


def sort_candidate_edges(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(
        by=["score", "node_a", "node_b"],
        ascending=[False, True, True],
        kind="mergesort",
    ).reset_index(drop=True)


def load_top_candidate_edges(
    input_file: Path,
    method_config: MethodConfig,
    max_keep: int,
) -> pd.DataFrame:
    top_edges = pd.DataFrame(columns=["node_a", "node_b", "score"])
    reader = pd.read_csv(
        input_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        usecols=["node_a", "node_b", method_config.score_column],
        dtype={"node_a": "string", "node_b": "string"},
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        chunk = standardize_chunk(chunk, method_config)
        combined = pd.concat([top_edges, chunk], ignore_index=True)
        top_edges = sort_candidate_edges(combined).head(max_keep)
        print(
            f"{method_config.method}: chunk {chunk_number:,}; "
            f"current top pool {len(top_edges):,} rows."
        )

    return sort_candidate_edges(top_edges)


def save_empty_outputs(method_config: MethodConfig, focal_year: int) -> None:
    columns = [
        "node_a",
        "node_b",
        "pyear",
        "method",
        "top_percentile",
        "rank_within_method_year",
        "score",
    ]
    empty_df = pd.DataFrame(columns=columns)

    for percentile in TOP_PERCENTILES:
        output_file = output_file_for_year(method_config, focal_year, percentile)
        empty_df.to_csv(output_file, index=False, compression="gzip")
        print(f"Saved empty predicted-edge file: {output_file}")


def save_top_percentile_outputs(
    top_edges: pd.DataFrame,
    method_config: MethodConfig,
    focal_year: int,
    keep_counts: dict[int, int],
) -> None:
    top_edges = top_edges.copy()
    top_edges["pyear"] = focal_year
    top_edges["method"] = method_config.method
    top_edges["rank_within_method_year"] = range(1, len(top_edges) + 1)

    for percentile in TOP_PERCENTILES:
        n_keep = keep_counts[percentile]
        output_file = output_file_for_year(method_config, focal_year, percentile)

        predicted_edges = top_edges.head(n_keep).copy()
        predicted_edges["top_percentile"] = percentile
        predicted_edges = predicted_edges[
            [
                "node_a",
                "node_b",
                "pyear",
                "method",
                "top_percentile",
                "rank_within_method_year",
                "score",
            ]
        ]
        predicted_edges.to_csv(output_file, index=False, compression="gzip")
        print(
            f"Saved {method_config.method} top {percentile}% predicted edges: "
            f"{len(predicted_edges):,} rows to {output_file}"
        )


def build_predicted_edges_for_method(
    method_config: MethodConfig,
    focal_year: int,
) -> None:
    input_file = input_file_for_year(method_config, focal_year)

    if not input_file.exists():
        raise FileNotFoundError(f"Missing scored candidate edge file: {input_file}")

    check_outputs(method_config, focal_year)
    total_rows = count_rows(input_file, method_config.score_column)
    keep_counts = n_keep_by_percentile(total_rows)

    print(
        f"{method_config.method}: found {total_rows:,} scored candidate edges "
        f"for focal year {focal_year}."
    )
    print(f"{method_config.method}: keep counts by percentile {keep_counts}.")

    if total_rows == 0:
        save_empty_outputs(method_config, focal_year)
        return

    max_keep = max(keep_counts.values())
    top_edges = load_top_candidate_edges(input_file, method_config, max_keep)
    save_top_percentile_outputs(top_edges, method_config, focal_year, keep_counts)


def main() -> None:
    focal_year = get_focal_year()
    print(f"Starting top-percentile predicted-edge build for {focal_year}.")

    for method_config in METHODS:
        build_predicted_edges_for_method(method_config, focal_year)

    print(f"Finished top-percentile predicted-edge build for {focal_year}.")


if __name__ == "__main__":
    main()
