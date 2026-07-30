"""Build BiomedBERT top 10% predicted edges for new-combination prediction."""

from __future__ import annotations

import math
import os
from pathlib import Path

import pandas as pd


INTERIM_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim"
)
INPUT_DIR = INTERIM_DIR / "biomedbert_link_prediction/candidate_edges"
OUTPUT_DIR = INTERIM_DIR / "link_prediction/predicted_edges/biomedbert"

INPUT_FILE_PREFIX = "biomedbert_scored_candidate_edges"
OUTPUT_FILE_PREFIX = "biomedbert_new_combination_predicted_edges_top_10pct"
SCORE_COLUMN = "biomedbert_score"

BASE_YEAR = 1980
CHUNK_SIZE = 1_000_000
TOP_PERCENTILE = 10
OVERWRITE = False


def get_focal_year() -> int:
    task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if task_id is None:
        raise RuntimeError("SLURM_ARRAY_TASK_ID not found.")
    return int(task_id) + BASE_YEAR


def input_file_for_year(focal_year: int) -> Path:
    return INPUT_DIR / f"{INPUT_FILE_PREFIX}_{focal_year}.csv.gz"


def output_file_for_year(focal_year: int) -> Path:
    return OUTPUT_DIR / f"{OUTPUT_FILE_PREFIX}_{focal_year}.csv.gz"


def check_input(input_file: Path) -> None:
    if not input_file.exists():
        raise FileNotFoundError(f"Missing scored BiomedBERT file: {input_file}")


def check_output(output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    if output_file.exists() and not OVERWRITE:
        raise FileExistsError(
            "Output file already exists. Set OVERWRITE = True to replace it:\n"
            f"{output_file}"
        )
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


def n_keep_top_10pct(total_rows: int) -> int:
    if total_rows == 0:
        return 0
    return max(1, math.ceil(total_rows * TOP_PERCENTILE / 100))


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


def load_top_10pct_candidate_edges(input_file: Path, n_keep: int) -> pd.DataFrame:
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
            f"Chunk {chunk_number:,}: current BiomedBERT top 10% pool "
            f"{len(top_edges):,} rows."
        )

    return sort_candidate_edges(top_edges)


def save_empty_output(output_file: Path) -> None:
    columns = [
        "node_a",
        "node_b",
        "pyear",
        "method",
        "prediction_task",
        "top_percentile",
        "rank_within_method_year",
        "score",
    ]
    pd.DataFrame(columns=columns).to_csv(
        output_file,
        index=False,
        compression="gzip",
    )
    print(f"Saved empty BiomedBERT top 10% predicted-edge file: {output_file}")


def save_top_10pct_output(
    top_edges: pd.DataFrame,
    output_file: Path,
    focal_year: int,
) -> None:
    top_edges = top_edges.copy()
    top_edges["pyear"] = focal_year
    top_edges["method"] = "biomedbert"
    top_edges["prediction_task"] = "new_combination"
    top_edges["top_percentile"] = TOP_PERCENTILE
    top_edges["rank_within_method_year"] = range(1, len(top_edges) + 1)
    top_edges = top_edges[
        [
            "node_a",
            "node_b",
            "pyear",
            "method",
            "prediction_task",
            "top_percentile",
            "rank_within_method_year",
            "score",
        ]
    ]
    top_edges.to_csv(output_file, index=False, compression="gzip")
    print(
        f"Saved BiomedBERT top 10% predicted edges: {len(top_edges):,} rows to "
        f"{output_file}"
    )


def build_biomedbert_top_10pct_predicted_edges(focal_year: int) -> None:
    input_file = input_file_for_year(focal_year)
    output_file = output_file_for_year(focal_year)

    check_input(input_file)
    check_output(output_file)

    total_rows = count_rows(input_file)
    n_keep = n_keep_top_10pct(total_rows)

    print(
        f"BiomedBERT: found {total_rows:,} scored candidate edges for focal "
        f"year {focal_year}."
    )
    print(f"BiomedBERT: keeping top 10% = {n_keep:,} edges.")

    if total_rows == 0:
        save_empty_output(output_file)
        return

    top_edges = load_top_10pct_candidate_edges(input_file, n_keep)
    save_top_10pct_output(top_edges, output_file, focal_year)


def main() -> None:
    focal_year = get_focal_year()
    print(
        f"Starting BiomedBERT new-combination top 10% predicted-edge build for "
        f"{focal_year}."
    )
    build_biomedbert_top_10pct_predicted_edges(focal_year)
    print(
        f"Finished BiomedBERT new-combination top 10% predicted-edge build for "
        f"{focal_year}."
    )


if __name__ == "__main__":
    main()
