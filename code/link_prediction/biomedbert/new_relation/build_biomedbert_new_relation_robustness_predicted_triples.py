"""Build BiomedBERT robustness predicted triples for new-relation prediction."""

from __future__ import annotations

import math
import os
from pathlib import Path

import pandas as pd


INTERIM_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim"
)
INPUT_DIR = INTERIM_DIR / "biomedbert_link_prediction/new_relation/scored_candidate_triples"
OUTPUT_ROOT = (
    INTERIM_DIR / "link_prediction/predicted_triples/biomedbert/new_relation/robustness"
)

INPUT_FILE_PREFIX = "biomedbert_new_relation_scored_candidate_triples"
OUTPUT_FILE_PREFIX = "biomedbert_new_relation_robustness_predicted_triples_top"
SCORE_COLUMN = "biomedbert_new_relation_score"

BASE_YEAR = 1980
CHUNK_SIZE = 100_000
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
        raise FileNotFoundError(
            f"Missing scored BiomedBERT new-relation file: {input_file}"
        )


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


def standardize_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    chunk = chunk.dropna(
        subset=["subject_cui", "predicate", "object_cui", SCORE_COLUMN]
    ).copy()
    chunk["subject_cui"] = chunk["subject_cui"].astype("string").str.strip()
    chunk["predicate"] = chunk["predicate"].astype("string").str.strip()
    chunk["object_cui"] = chunk["object_cui"].astype("string").str.strip()
    chunk["score"] = pd.to_numeric(chunk[SCORE_COLUMN], errors="coerce")
    chunk = chunk.dropna(subset=["subject_cui", "predicate", "object_cui", "score"])
    return chunk


def count_unique_scored_triples(input_file: Path) -> int:
    scored_triples: set[tuple[str, str, str]] = set()
    reader = pd.read_csv(
        input_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        usecols=["subject_cui", "predicate", "object_cui", SCORE_COLUMN],
    )

    for chunk in reader:
        chunk = standardize_chunk(chunk)
        scored_triples.update(
            zip(
                chunk["subject_cui"].astype(str),
                chunk["predicate"].astype(str),
                chunk["object_cui"].astype(str),
            )
        )

    return len(scored_triples)


def n_keep_top_percentile(total_rows: int, percentile: int) -> int:
    if total_rows == 0:
        return 0
    return max(1, math.ceil(total_rows * percentile / 100))


def sort_candidate_triples(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(
        by=["score", "subject_cui", "predicate", "object_cui"],
        ascending=[False, True, True, True],
        kind="mergesort",
    ).reset_index(drop=True)


def deduplicate_triples_keep_best(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return sort_candidate_triples(df).drop_duplicates(
        subset=["subject_cui", "predicate", "object_cui"],
        keep="first",
    )


def load_top_candidate_triples(input_file: Path, n_keep: int) -> pd.DataFrame:
    top_triples = pd.DataFrame()
    reader = pd.read_csv(
        input_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        dtype={
            "subject_cui": "string",
            "predicate": "string",
            "object_cui": "string",
        },
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        chunk = standardize_chunk(chunk)
        combined = pd.concat([top_triples, chunk], ignore_index=True)
        top_triples = deduplicate_triples_keep_best(combined).head(n_keep)
        print(
            f"Chunk {chunk_number:,}: current BiomedBERT new-relation "
            f"robustness pool {len(top_triples):,} rows."
        )

    return sort_candidate_triples(top_triples)


def front_columns() -> list[str]:
    return [
        "subject_cui",
        "predicate",
        "object_cui",
        "pyear",
        "method",
        "prediction_task",
        "analysis_type",
        "top_percentile",
        "rank_within_method_year",
        "score",
    ]


def save_empty_output(output_file: Path) -> None:
    pd.DataFrame(columns=front_columns()).to_csv(
        output_file,
        index=False,
        compression="gzip",
    )
    print(f"Saved empty BiomedBERT new-relation robustness file: {output_file}")


def save_percentile_output(
    top_triples: pd.DataFrame,
    output_file: Path,
    focal_year: int,
    percentile: int,
) -> None:
    output = top_triples.copy()
    output["pyear"] = focal_year
    output["method"] = "biomedbert"
    output["prediction_task"] = "new_relation"
    output["analysis_type"] = "robustness"
    output["top_percentile"] = percentile
    output["rank_within_method_year"] = range(1, len(output) + 1)

    front = front_columns()
    remaining = [column for column in output.columns if column not in front]
    output = output[front + remaining]
    output.to_csv(output_file, index=False, compression="gzip")
    print(
        f"Saved BiomedBERT new-relation robustness top {percentile}% "
        f"predicted triples: {len(output):,} rows to {output_file}"
    )


def build_robustness_predicted_triples(focal_year: int) -> None:
    input_file = input_file_for_year(focal_year)
    output_files = [
        output_file_for_year(focal_year, percentile)
        for percentile in ROBUSTNESS_PERCENTILES
    ]

    check_input(input_file)
    check_outputs(output_files)

    total_triples = count_unique_scored_triples(input_file)
    n_keep_by_percentile = {
        percentile: n_keep_top_percentile(total_triples, percentile)
        for percentile in ROBUSTNESS_PERCENTILES
    }
    max_keep = max(n_keep_by_percentile.values())

    print(
        f"BiomedBERT new relation: found {total_triples:,} unique scored "
        f"candidate triples for focal year {focal_year}."
    )
    print(f"Robustness keep counts: {n_keep_by_percentile}")

    if total_triples == 0:
        for output_file in output_files:
            save_empty_output(output_file)
        return

    top_triples = load_top_candidate_triples(input_file, max_keep)
    for percentile in ROBUSTNESS_PERCENTILES:
        n_keep = n_keep_by_percentile[percentile]
        percentile_triples = top_triples.head(n_keep).reset_index(drop=True)
        save_percentile_output(
            percentile_triples,
            output_file_for_year(focal_year, percentile),
            focal_year,
            percentile,
        )


def main() -> None:
    focal_year = get_focal_year()
    print(
        "Starting BiomedBERT new-relation robustness predicted-triple build "
        f"for {focal_year}."
    )
    build_robustness_predicted_triples(focal_year)
    print(
        "Finished BiomedBERT new-relation robustness predicted-triple build "
        f"for {focal_year}."
    )


if __name__ == "__main__":
    main()
