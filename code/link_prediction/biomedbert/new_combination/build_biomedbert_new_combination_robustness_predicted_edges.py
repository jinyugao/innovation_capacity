"""Build BiomedBERT robustness predicted edges for new-combination prediction.

This robustness script is separate from the main top-10% analysis. It reads the
same scored BiomedBERT candidate-edge files and writes top 1%, 5%, and 20%
predicted-edge files under a dedicated robustness directory.
"""

from __future__ import annotations

import math
import os
from pathlib import Path

import pandas as pd


INTERIM_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim"
)
INPUT_DIR = INTERIM_DIR / "biomedbert_link_prediction/candidate_edges"
OUTPUT_ROOT = (
    INTERIM_DIR / "link_prediction/predicted_edges/biomedbert/new_combination/robustness"
)

INPUT_FILE_PREFIX = "biomedbert_scored_candidate_edges"
OUTPUT_FILE_PREFIX = "biomedbert_new_combination_robustness_predicted_edges_top"
SCORE_COLUMN = "biomedbert_score"

BASE_YEAR = 1980
CHUNK_SIZE = 1_000_000
ROBUSTNESS_PERCENTILES = [1, 5, 20]
OVERWRITE = False


def get_focal_year() -> int:
    task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if task_id is None:
        raise RuntimeError("SLURM_ARRAY_TASK_ID not found.")
    return int(task_id) + BASE_YEAR


def percentile_label(percentile: int) -> str:
    return f"{percentile}pct"


def input_file_for_year(focal_year: int) -> Path:
    return INPUT_DIR / f"{INPUT_FILE_PREFIX}_{focal_year}.csv.gz"


def output_file_for_year(focal_year: int, percentile: int) -> Path:
    label = percentile_label(percentile)
    return OUTPUT_ROOT / label / f"{OUTPUT_FILE_PREFIX}_{label}_{focal_year}.csv.gz"


def check_input(input_file: Path) -> None:
    if not input_file.exists():
        raise FileNotFoundError(f"Missing scored BiomedBERT file: {input_file}")


def check_outputs(output_files: list[Path]) -> None:
    for output_file in output_files:
        output_file.parent.mkdir(parents=True, exist_ok=True)

    existing_files = [str(path) for path in output_files if path.exists()]
    if existing_files and not OVERWRITE:
        existing = "\n".join(existing_files)
        raise FileExistsError(
            "Output file(s) already exist. Set OVERWRITE = True to replace them:\n"
            f"{existing}"
        )

    for output_file in output_files:
        if output_file.exists() and OVERWRITE:
            output_file.unlink()


def count_rows(input_file: Path) -> int:
    total_rows = 0
    reader = pd.read_csv(
        input_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        usecols=["node_a", "node_b", SCORE_COLUMN],
    )

    for chunk in reader:
        scores = pd.to_numeric(chunk[SCORE_COLUMN], errors="coerce")
        total_rows += int(scores.notna().sum())

    return total_rows


def n_keep_top_percentile(total_rows: int, percentile: int) -> int:
    if total_rows == 0:
        return 0
    return max(1, math.ceil(total_rows * percentile / 100))


def standardize_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    chunk = chunk.dropna(subset=["node_a", "node_b", SCORE_COLUMN]).copy()
    chunk["node_a"] = chunk["node_a"].astype("string").str.strip()
    chunk["node_b"] = chunk["node_b"].astype("string").str.strip()
    chunk["score"] = pd.to_numeric(chunk[SCORE_COLUMN], errors="coerce")
    chunk = chunk.dropna(subset=["node_a", "node_b", "score"])
    return chunk[["node_a", "node_b", "score"]]


def sort_candidate_edges(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(
        by=["score", "node_a", "node_b"],
        ascending=[False, True, True],
        kind="mergesort",
    ).reset_index(drop=True)


def load_top_candidate_edges(input_file: Path, n_keep: int) -> pd.DataFrame:
    top_edges = pd.DataFrame(columns=["node_a", "node_b", "score"])
    reader = pd.read_csv(
        input_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        usecols=["node_a", "node_b", SCORE_COLUMN],
        dtype={"node_a": "string", "node_b": "string"},
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        chunk = standardize_chunk(chunk)
        combined = pd.concat([top_edges, chunk], ignore_index=True)
        top_edges = sort_candidate_edges(combined).head(n_keep)
        print(
            f"Chunk {chunk_number:,}: current BiomedBERT robustness pool "
            f"{len(top_edges):,} rows."
        )

    return sort_candidate_edges(top_edges)


def output_columns() -> list[str]:
    return [
        "node_a",
        "node_b",
        "pyear",
        "method",
        "prediction_task",
        "analysis_type",
        "top_percentile",
        "rank_within_method_year",
        "score",
    ]


def save_empty_output(output_file: Path) -> None:
    pd.DataFrame(columns=output_columns()).to_csv(
        output_file,
        index=False,
        compression="gzip",
    )
    print(f"Saved empty BiomedBERT robustness predicted-edge file: {output_file}")


def save_percentile_output(
    top_edges: pd.DataFrame,
    output_file: Path,
    focal_year: int,
    percentile: int,
) -> None:
    output = top_edges.copy()
    output["pyear"] = focal_year
    output["method"] = "biomedbert"
    output["prediction_task"] = "new_combination"
    output["analysis_type"] = "robustness"
    output["top_percentile"] = percentile
    output["rank_within_method_year"] = range(1, len(output) + 1)
    output = output[output_columns()]
    output.to_csv(output_file, index=False, compression="gzip")
    print(
        f"Saved BiomedBERT new-combination robustness top {percentile}% "
        f"predicted edges: {len(output):,} rows to {output_file}"
    )


def build_robustness_predicted_edges(focal_year: int) -> None:
    input_file = input_file_for_year(focal_year)
    output_files = [
        output_file_for_year(focal_year, percentile)
        for percentile in ROBUSTNESS_PERCENTILES
    ]

    check_input(input_file)
    check_outputs(output_files)

    total_rows = count_rows(input_file)
    n_keep_by_percentile = {
        percentile: n_keep_top_percentile(total_rows, percentile)
        for percentile in ROBUSTNESS_PERCENTILES
    }
    max_keep = max(n_keep_by_percentile.values())

    print(
        f"BiomedBERT new combination: found {total_rows:,} scored candidate "
        f"edges for focal year {focal_year}."
    )
    print(f"Robustness keep counts: {n_keep_by_percentile}")

    if total_rows == 0:
        for output_file in output_files:
            save_empty_output(output_file)
        return

    top_edges = load_top_candidate_edges(input_file, max_keep)
    for percentile in ROBUSTNESS_PERCENTILES:
        n_keep = n_keep_by_percentile[percentile]
        percentile_edges = top_edges.head(n_keep).reset_index(drop=True)
        save_percentile_output(
            percentile_edges,
            output_file_for_year(focal_year, percentile),
            focal_year,
            percentile,
        )


def main() -> None:
    focal_year = get_focal_year()
    print(
        "Starting BiomedBERT new-combination robustness predicted-edge build "
        f"for {focal_year}."
    )
    build_robustness_predicted_edges(focal_year)
    print(
        "Finished BiomedBERT new-combination robustness predicted-edge build "
        f"for {focal_year}."
    )


if __name__ == "__main__":
    main()
