"""Score semantic proximity for focal-year New_Combination edges."""

from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    import duckdb
except ImportError as exc:  # pragma: no cover - exercised on HPC if missing.
    raise SystemExit(
        "Missing required Python package: duckdb. Install it in the HPC "
        "environment before running this script."
    ) from exc


DEFAULT_PROJECT_ROOT = Path(
    "/xdisk/sebratt/jinyugao/research/projects/innovation_capacity"
)

BASE_YEAR = 1980
N_YEARS = 40

EDGE_ANNOTATION_SUBDIR = Path("data/processed/knowledge_combination/edge_annotation")
EMBEDDING_SUBDIR = Path(
    "data/processed/knowledge_combination/semantic_proximity/cui_embeddings"
)
OUTPUT_SUBDIR = Path(
    "data/processed/knowledge_combination/semantic_proximity/new_combination"
)
SUMMARY_SUBDIR = OUTPUT_SUBDIR / "summary"

EDGE_ANNOTATION_FILE_STEM = "edge_annotation"
EMBEDDING_FILE_NAME = "cui_embeddings.npy"
EMBEDDING_METADATA_FILE_NAME = "cui_embedding_metadata.parquet"
OUTPUT_FILE_STEM = "semantic_proximity_new_combination"
SUMMARY_FILE_STEM = "semantic_proximity_new_combination_summary"

NEW_COMBINATION = "New_Combination"
DEFAULT_SIMILARITY_BATCH_SIZE = 500_000


def get_env_path(name: str, default: Path) -> Path:
    value = os.environ.get(name)
    return Path(value).expanduser() if value else default


def get_env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


def get_env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    return int(value) if value else default


def sql_literal(value: Path | str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def get_focal_year() -> int:
    task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if task_id is None:
        focal_year = os.environ.get("IC_FOCAL_YEAR")
        if focal_year is None:
            raise RuntimeError("Set SLURM_ARRAY_TASK_ID or IC_FOCAL_YEAR.")
        return int(focal_year)

    task_index = int(task_id)
    if task_index < 0 or task_index >= N_YEARS:
        raise ValueError(
            f"SLURM_ARRAY_TASK_ID={task_index} is out of range. "
            f"Expected 0-{N_YEARS - 1}."
        )
    return BASE_YEAR + task_index


def edge_annotation_file(project_root: Path, focal_year: int) -> Path:
    return (
        project_root
        / EDGE_ANNOTATION_SUBDIR
        / f"{EDGE_ANNOTATION_FILE_STEM}_{focal_year}.parquet"
    )


def output_file(project_root: Path, focal_year: int) -> Path:
    return project_root / OUTPUT_SUBDIR / f"{OUTPUT_FILE_STEM}_{focal_year}.parquet"


def summary_file(project_root: Path, focal_year: int) -> Path:
    return project_root / SUMMARY_SUBDIR / f"{SUMMARY_FILE_STEM}_{focal_year}.csv"


def check_paths(
    input_files: list[Path],
    output_files: list[Path],
    overwrite: bool,
) -> None:
    missing = [path for path in input_files if not path.exists()]
    if missing:
        missing_text = "\n".join(str(path) for path in missing)
        raise FileNotFoundError(f"Missing required input file(s):\n{missing_text}")

    for path in output_files:
        path.parent.mkdir(parents=True, exist_ok=True)

    existing = [path for path in output_files if path.exists()]
    if existing and not overwrite:
        existing_text = "\n".join(str(path) for path in existing)
        raise FileExistsError(
            "Output file(s) already exist. Move them, delete them, or rerun "
            "with IC_NEW_COMBINATION_SEMANTIC_PROXIMITY_OVERWRITE=1:\n"
            f"{existing_text}"
        )
    if overwrite:
        for path in existing:
            path.unlink()


def int_scalar(con: duckdb.DuckDBPyConnection, query: str) -> int:
    value = con.execute(query).fetchone()[0]
    return 0 if value is None else int(value)


def load_new_combination_edges(input_file: Path) -> tuple[pd.DataFrame, int]:
    con = duckdb.connect(database=":memory:")
    try:
        source = f"read_parquet({sql_literal(input_file)})"
        n_edge_annotation_rows = int_scalar(con, f"SELECT COUNT(*) FROM {source}")
        edges = con.execute(
            f"""
            SELECT *
            FROM {source}
            WHERE edge_annotation = {sql_literal(NEW_COMBINATION)}
            """
        ).fetchdf()
    finally:
        con.close()

    return edges, n_edge_annotation_rows


def load_embedding_lookup(metadata_file: Path) -> pd.Series:
    metadata = pd.read_parquet(
        metadata_file,
        columns=["primary_cui", "embedding_row_id"],
    )
    metadata = metadata.dropna(subset=["primary_cui", "embedding_row_id"]).copy()
    metadata["primary_cui"] = metadata["primary_cui"].astype("string").str.strip()
    metadata = metadata[metadata["primary_cui"] != ""]
    duplicate_rows = int(metadata.duplicated(subset=["primary_cui"]).sum())
    if duplicate_rows:
        raise ValueError(
            "Embedding metadata is not unique by primary_cui. "
            f"Duplicate rows: {duplicate_rows:,}"
        )
    return metadata.set_index("primary_cui")["embedding_row_id"]


def nullable_ids_to_numpy(ids: pd.Series) -> np.ndarray:
    output = np.full(len(ids), -1, dtype=np.int64)
    valid = ids.notna().to_numpy()
    if valid.any():
        output[valid] = ids[valid].astype(np.int64).to_numpy()
    return output


def map_embedding_ids(
    edges: pd.DataFrame,
    lookup: pd.Series,
) -> tuple[pd.Series, pd.Series, np.ndarray, np.ndarray]:
    subject_cui = edges["subject_cui_primary"].astype("string").str.strip()
    object_cui = edges["object_cui_primary"].astype("string").str.strip()
    subject_ids = subject_cui.map(lookup)
    object_ids = object_cui.map(lookup)
    return (
        subject_ids.astype("Int64"),
        object_ids.astype("Int64"),
        nullable_ids_to_numpy(subject_ids),
        nullable_ids_to_numpy(object_ids),
    )


def cosine_similarity_by_row(
    embeddings: np.ndarray,
    subject_ids: np.ndarray,
    object_ids: np.ndarray,
    batch_size: int,
) -> np.ndarray:
    scores = np.full(len(subject_ids), np.nan, dtype=np.float32)
    valid_rows = np.flatnonzero((subject_ids >= 0) & (object_ids >= 0))

    for start in range(0, len(valid_rows), batch_size):
        row_ids = valid_rows[start : start + batch_size]
        subject_vectors = embeddings[subject_ids[row_ids]]
        object_vectors = embeddings[object_ids[row_ids]]
        numerator = np.einsum("ij,ij->i", subject_vectors, object_vectors)
        subject_norm = np.linalg.norm(subject_vectors, axis=1)
        object_norm = np.linalg.norm(object_vectors, axis=1)
        denominator = subject_norm * object_norm
        batch_scores = np.divide(
            numerator,
            denominator,
            out=np.full(len(row_ids), np.nan, dtype=np.float32),
            where=denominator != 0,
        )
        scores[row_ids] = batch_scores.astype(np.float32)
        print(f"Scored New_Combination rows {min(start + batch_size, len(valid_rows)):,} / {len(valid_rows):,}.")

    return scores


def summary_statistics(scores: np.ndarray) -> dict[str, Any]:
    valid_scores = scores[~np.isnan(scores)]
    if len(valid_scores) == 0:
        return {
            "score_min": "",
            "score_mean": "",
            "score_std": "",
            "score_p01": "",
            "score_p05": "",
            "score_p10": "",
            "score_p25": "",
            "score_p50": "",
            "score_p75": "",
            "score_p90": "",
            "score_p95": "",
            "score_p99": "",
            "score_max": "",
        }

    percentiles = np.percentile(
        valid_scores,
        [1, 5, 10, 25, 50, 75, 90, 95, 99],
    )
    return {
        "score_min": float(valid_scores.min()),
        "score_mean": float(valid_scores.mean()),
        "score_std": float(valid_scores.std()),
        "score_p01": float(percentiles[0]),
        "score_p05": float(percentiles[1]),
        "score_p10": float(percentiles[2]),
        "score_p25": float(percentiles[3]),
        "score_p50": float(percentiles[4]),
        "score_p75": float(percentiles[5]),
        "score_p90": float(percentiles[6]),
        "score_p95": float(percentiles[7]),
        "score_p99": float(percentiles[8]),
        "score_max": float(valid_scores.max()),
    }


def safe_share(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def write_single_row_csv(path: Path, row: dict[str, Any]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)


def score_new_combination_semantic_proximity(
    project_root: Path,
    focal_year: int,
    overwrite: bool,
    similarity_batch_size: int,
) -> None:
    annotation_file = edge_annotation_file(project_root, focal_year)
    embedding_file = project_root / EMBEDDING_SUBDIR / EMBEDDING_FILE_NAME
    metadata_file = project_root / EMBEDDING_SUBDIR / EMBEDDING_METADATA_FILE_NAME
    scored_file = output_file(project_root, focal_year)
    year_summary_file = summary_file(project_root, focal_year)

    check_paths(
        [annotation_file, embedding_file, metadata_file],
        [scored_file, year_summary_file],
        overwrite,
    )

    print(f"Reading edge annotation from {annotation_file}")
    edges, n_edge_annotation_rows = load_new_combination_edges(annotation_file)
    print(f"Edge annotation rows: {n_edge_annotation_rows:,}")
    print(f"New_Combination rows: {len(edges):,}")

    print(f"Reading embedding metadata from {metadata_file}")
    lookup = load_embedding_lookup(metadata_file)
    print(f"Embedding metadata CUIs: {len(lookup):,}")

    print(f"Reading embedding matrix from {embedding_file}")
    embeddings = np.load(embedding_file, mmap_mode="r")
    if embeddings.ndim != 2:
        raise ValueError(f"Expected 2D embedding matrix, found shape {embeddings.shape}")
    if int(lookup.max()) >= embeddings.shape[0]:
        raise ValueError(
            "Embedding metadata contains row ids outside the embedding matrix. "
            f"Max row id: {int(lookup.max())}; matrix rows: {embeddings.shape[0]:,}"
        )
    print(f"Embedding matrix shape: {embeddings.shape}")

    subject_ids, object_ids, subject_id_array, object_id_array = map_embedding_ids(
        edges,
        lookup,
    )
    subject_has_embedding = subject_ids.notna()
    object_has_embedding = object_ids.notna()
    has_both_embeddings = subject_has_embedding & object_has_embedding
    print(f"Rows with both endpoint embeddings: {int(has_both_embeddings.sum()):,}")

    scores = cosine_similarity_by_row(
        embeddings,
        subject_id_array,
        object_id_array,
        similarity_batch_size,
    )

    edges = edges.copy()
    edges["subject_embedding_row_id"] = subject_ids
    edges["object_embedding_row_id"] = object_ids
    edges["subject_has_cui_embedding"] = subject_has_embedding.to_numpy(dtype=bool)
    edges["object_has_cui_embedding"] = object_has_embedding.to_numpy(dtype=bool)
    edges["new_combination_semantic_proximity"] = scores

    print(f"Writing scored New_Combination rows to {scored_file}")
    edges.to_parquet(scored_file, index=False, compression="zstd")

    n_new_combination_rows = len(edges)
    n_scored_rows = int(np.sum(~np.isnan(scores)))
    n_missing_subject_embedding = int((~subject_has_embedding).sum())
    n_missing_object_embedding = int((~object_has_embedding).sum())
    n_missing_any_embedding = int((~has_both_embeddings).sum())
    summary: dict[str, Any] = {
        "pyear": focal_year,
        "n_edge_annotation_rows": n_edge_annotation_rows,
        "n_new_combination_rows": n_new_combination_rows,
        "n_scored_rows": n_scored_rows,
        "n_missing_subject_embedding": n_missing_subject_embedding,
        "n_missing_object_embedding": n_missing_object_embedding,
        "n_missing_any_embedding": n_missing_any_embedding,
        "share_scored_rows": safe_share(n_scored_rows, n_new_combination_rows),
        "n_unique_subject_cui": int(edges["subject_cui_primary"].nunique()),
        "n_unique_object_cui": int(edges["object_cui_primary"].nunique()),
        "n_unique_undirected_pairs": int(
            edges[["node_a", "node_b"]].drop_duplicates().shape[0]
        ),
        "embedding_rows": int(embeddings.shape[0]),
        "embedding_dim": int(embeddings.shape[1]),
        "similarity_batch_size": similarity_batch_size,
        **summary_statistics(scores),
    }
    write_single_row_csv(year_summary_file, summary)
    print(f"Saved summary to {year_summary_file}")


def main() -> None:
    project_root = get_env_path("IC_PROJECT_ROOT", DEFAULT_PROJECT_ROOT)
    focal_year = get_focal_year()
    overwrite = get_env_bool(
        "IC_NEW_COMBINATION_SEMANTIC_PROXIMITY_OVERWRITE",
        False,
    )
    similarity_batch_size = get_env_int(
        "IC_SEMANTIC_PROXIMITY_BATCH_SIZE",
        DEFAULT_SIMILARITY_BATCH_SIZE,
    )

    print(f"Project root: {project_root}")
    print(f"Focal year: {focal_year}")
    print(f"Overwrite outputs: {overwrite}")
    print(f"Similarity batch size: {similarity_batch_size}")

    score_new_combination_semantic_proximity(
        project_root,
        focal_year,
        overwrite,
        similarity_batch_size,
    )
    print("New_Combination semantic proximity scoring complete.")


if __name__ == "__main__":
    main()
