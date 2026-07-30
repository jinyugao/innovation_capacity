"""Build BiomedBERT top 10% predicted triples for new-relation prediction."""

from __future__ import annotations

import math
import os
from pathlib import Path

import pandas as pd


INTERIM_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim"
)
INPUT_DIR = INTERIM_DIR / "biomedbert_link_prediction/new_relation/scored_candidate_triples"
OUTPUT_DIR = INTERIM_DIR / "link_prediction/predicted_triples/biomedbert/new_relation"

INPUT_FILE_PREFIX = "biomedbert_new_relation_scored_candidate_triples"
OUTPUT_FILE_PREFIX = "biomedbert_new_relation_predicted_triples_top_10pct"
SCORE_COLUMN = "biomedbert_new_relation_score"

BASE_YEAR = 1980
CHUNK_SIZE = 100_000
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
        raise FileNotFoundError(
            f"Missing scored BiomedBERT new-relation file: {input_file}"
        )


def check_output(output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    if output_file.exists() and not OVERWRITE:
        raise FileExistsError(
            "Output file already exists. Set OVERWRITE = True to replace it:\n"
            f"{output_file}"
        )
    if output_file.exists() and OVERWRITE:
        output_file.unlink()


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


def n_keep_top_10pct(total_rows: int) -> int:
    if total_rows == 0:
        return 0
    return max(1, math.ceil(total_rows * TOP_PERCENTILE / 100))


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


def load_top_10pct_candidate_triples(input_file: Path, n_keep: int) -> pd.DataFrame:
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
            f"Chunk {chunk_number:,}: current BiomedBERT new-relation top 10% "
            f"pool {len(top_triples):,} rows."
        )

    return sort_candidate_triples(top_triples)


def save_empty_output(output_file: Path) -> None:
    columns = [
        "subject_cui",
        "predicate",
        "object_cui",
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
    print(f"Saved empty BiomedBERT new-relation top 10% file: {output_file}")


def save_top_10pct_output(
    top_triples: pd.DataFrame,
    output_file: Path,
    focal_year: int,
) -> None:
    top_triples = top_triples.copy()
    top_triples["pyear"] = focal_year
    top_triples["method"] = "biomedbert"
    top_triples["prediction_task"] = "new_relation"
    top_triples["top_percentile"] = TOP_PERCENTILE
    top_triples["rank_within_method_year"] = range(1, len(top_triples) + 1)

    front_columns = [
        "subject_cui",
        "predicate",
        "object_cui",
        "pyear",
        "method",
        "prediction_task",
        "top_percentile",
        "rank_within_method_year",
        "score",
    ]
    remaining_columns = [
        column for column in top_triples.columns if column not in front_columns
    ]
    top_triples = top_triples[front_columns + remaining_columns]
    top_triples.to_csv(output_file, index=False, compression="gzip")
    print(
        f"Saved BiomedBERT new-relation top 10% predicted triples: "
        f"{len(top_triples):,} rows to {output_file}"
    )


def build_top_10pct_predicted_triples(focal_year: int) -> None:
    input_file = input_file_for_year(focal_year)
    output_file = output_file_for_year(focal_year)

    check_input(input_file)
    check_output(output_file)

    total_triples = count_unique_scored_triples(input_file)
    n_keep = n_keep_top_10pct(total_triples)
    print(
        f"BiomedBERT new relation: found {total_triples:,} unique scored candidate "
        f"triples for focal year {focal_year}."
    )
    print(f"BiomedBERT new relation: keeping top 10% = {n_keep:,} triples.")

    if total_triples == 0:
        save_empty_output(output_file)
        return

    top_triples = load_top_10pct_candidate_triples(input_file, n_keep)
    save_top_10pct_output(top_triples, output_file, focal_year)


def main() -> None:
    focal_year = get_focal_year()
    print(
        f"Starting BiomedBERT new-relation top 10% predicted-triple build for "
        f"{focal_year}."
    )
    build_top_10pct_predicted_triples(focal_year)
    print(
        f"Finished BiomedBERT new-relation top 10% predicted-triple build for "
        f"{focal_year}."
    )


if __name__ == "__main__":
    main()
