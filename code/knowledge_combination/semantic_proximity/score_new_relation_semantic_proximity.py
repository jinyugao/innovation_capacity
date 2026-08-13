"""Score semantic proximity for focal-year New_Relation edges.

For each focal-year New_Relation edge, this script compares the focal directed
typed triple with prior five-year triples that have the same subject CUI, same
object CUI, same direction, and a different predicate.
"""

from __future__ import annotations

import csv
import os
import re
import time
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

try:
    import torch
    from transformers import AutoModel, AutoTokenizer
except ImportError as exc:  # pragma: no cover - exercised on HPC if missing.
    raise SystemExit(
        "Missing required Python packages: torch and transformers. Install them "
        "in the HPC environment before running this script."
    ) from exc


DEFAULT_PROJECT_ROOT = Path(
    "/xdisk/sebratt/jinyugao/research/projects/innovation_capacity"
)

BASE_YEAR = 1980
N_YEARS = 40
PRIOR_WINDOW_YEARS = 5

SEMMED_YEARLY_SUBDIR = Path("data/processed/semmedVER43_R/semmeddb_analysis_sample")
EDGE_ANNOTATION_SUBDIR = Path("data/processed/knowledge_combination/edge_annotation")
CUI_TEXT_TABLE_FILE = Path(
    "data/processed/knowledge_combination/semantic_proximity/"
    "cui_text/cui_text_table.parquet"
)
OUTPUT_SUBDIR = Path(
    "data/processed/knowledge_combination/semantic_proximity/new_relation"
)
SUMMARY_SUBDIR = OUTPUT_SUBDIR / "summary"

SEMMED_YEARLY_FILE_STEM = "semmeddb_analysis_sample"
EDGE_ANNOTATION_FILE_STEM = "edge_annotation"
OUTPUT_FILE_STEM = "semantic_proximity_new_relation"
SUMMARY_FILE_STEM = "semantic_proximity_new_relation_summary"

NEW_RELATION = "New_Relation"
DEFAULT_MODEL_NAME = "microsoft/BiomedNLP-BiomedBERT-base-uncased-abstract-fulltext"
DEFAULT_BATCH_SIZE = 128
DEFAULT_MAX_LENGTH = 96
DEFAULT_SIMILARITY_BATCH_SIZE = 500_000

TRIPLE_COLUMNS = ["subject_cui_primary", "PREDICATE", "object_cui_primary"]
PAIR_COLUMNS = ["subject_cui_primary", "object_cui_primary"]


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


def get_env_int_any(names: list[str], default: int) -> int:
    for name in names:
        value = os.environ.get(name)
        if value:
            return int(value)
    return default


def get_env_str_any(names: list[str], default: str) -> str:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return default


def sql_literal(value: Path | str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def safe_share(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


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


def prior_years_for_focal_year(focal_year: int) -> list[int]:
    return list(range(focal_year - PRIOR_WINDOW_YEARS, focal_year))


def edge_annotation_file(project_root: Path, focal_year: int) -> Path:
    return (
        project_root
        / EDGE_ANNOTATION_SUBDIR
        / f"{EDGE_ANNOTATION_FILE_STEM}_{focal_year}.parquet"
    )


def yearly_input_file(project_root: Path, year: int) -> Path:
    return (
        project_root
        / SEMMED_YEARLY_SUBDIR
        / f"{SEMMED_YEARLY_FILE_STEM}_{year}.parquet"
    )


def output_file(project_root: Path, focal_year: int) -> Path:
    return project_root / OUTPUT_SUBDIR / f"{OUTPUT_FILE_STEM}_{focal_year}.parquet"


def summary_file(project_root: Path, focal_year: int) -> Path:
    return project_root / SUMMARY_SUBDIR / f"{SUMMARY_FILE_STEM}_{focal_year}.csv"


def parquet_relation(paths: list[Path] | Path) -> str:
    if isinstance(paths, Path):
        return f"read_parquet({sql_literal(paths)})"
    path_list = ", ".join(sql_literal(path) for path in paths)
    return f"read_parquet([{path_list}])"


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
            "with IC_NEW_RELATION_SEMANTIC_PROXIMITY_OVERWRITE=1:\n"
            f"{existing_text}"
        )
    if overwrite:
        for path in existing:
            path.unlink()


def int_scalar(con: duckdb.DuckDBPyConnection, query: str) -> int:
    value = con.execute(query).fetchone()[0]
    return 0 if value is None else int(value)


def load_new_relation_edges(input_file: Path) -> tuple[pd.DataFrame, int]:
    con = duckdb.connect(database=":memory:")
    try:
        source = f"read_parquet({sql_literal(input_file)})"
        n_edge_annotation_rows = int_scalar(con, f"SELECT COUNT(*) FROM {source}")
        edges = con.execute(
            f"""
            SELECT *
            FROM {source}
            WHERE edge_annotation = {sql_literal(NEW_RELATION)}
            """
        ).fetchdf()
    finally:
        con.close()

    return edges, n_edge_annotation_rows


def load_prior_unique_triples(prior_files: list[Path]) -> pd.DataFrame:
    con = duckdb.connect(database=":memory:")
    try:
        prior = con.execute(
            f"""
            SELECT DISTINCT
                subject_cui_primary,
                PREDICATE,
                object_cui_primary
            FROM {parquet_relation(prior_files)}
            WHERE
                subject_cui_primary IS NOT NULL
                AND object_cui_primary IS NOT NULL
                AND PREDICATE IS NOT NULL
                AND trim(subject_cui_primary) != ''
                AND trim(object_cui_primary) != ''
                AND trim(PREDICATE) != ''
                AND subject_cui_primary != object_cui_primary
            """
        ).fetchdf()
    finally:
        con.close()
    return normalize_triple_columns(prior)


def normalize_triple_columns(data: pd.DataFrame) -> pd.DataFrame:
    data = data.copy()
    for column in TRIPLE_COLUMNS:
        data[column] = data[column].astype("string").str.strip()
    data = data.dropna(subset=TRIPLE_COLUMNS)
    data = data[
        (data["subject_cui_primary"] != "")
        & (data["PREDICATE"] != "")
        & (data["object_cui_primary"] != "")
    ]
    return data.reset_index(drop=True)


def load_cui_text_lookup(input_file: Path) -> pd.Series:
    data = pd.read_parquet(input_file, columns=["primary_cui", "cui_text"])
    data = data.dropna(subset=["primary_cui", "cui_text"]).copy()
    data["primary_cui"] = data["primary_cui"].astype("string").str.strip()
    data["cui_text"] = data["cui_text"].astype("string").str.strip()
    data = data[(data["primary_cui"] != "") & (data["cui_text"] != "")]
    duplicate_rows = int(data.duplicated(subset=["primary_cui"]).sum())
    if duplicate_rows:
        raise ValueError(
            "CUI text table is not unique by primary_cui. "
            f"Duplicate rows: {duplicate_rows:,}"
        )
    return data.set_index("primary_cui")["cui_text"]


def select_device() -> torch.device:
    if torch.cuda.is_available():
        return torch.device("cuda")

    require_cuda = (
        os.environ.get("REQUIRE_CUDA") == "1"
        or os.environ.get("IC_REQUIRE_CUDA") == "1"
    )
    if require_cuda:
        raise RuntimeError(
            "REQUIRE_CUDA=1 but torch.cuda.is_available() is False. "
            "Check the GPU Slurm allocation and CUDA-enabled torch installation."
        )

    return torch.device("cpu")


def mean_pool(
    last_hidden_state: torch.Tensor,
    attention_mask: torch.Tensor,
) -> torch.Tensor:
    mask = attention_mask.unsqueeze(-1).expand(last_hidden_state.size()).float()
    masked_embeddings = last_hidden_state * mask
    summed_embeddings = masked_embeddings.sum(dim=1)
    token_counts = mask.sum(dim=1).clamp(min=1e-9)
    return summed_embeddings / token_counts


def load_text_encoder(
    model_name: str,
    device: torch.device,
) -> tuple[Any, Any]:
    print(f"Loading BiomedBERT model: {model_name}")
    print(f"Using device: {device}")
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name)
    model.to(device)
    model.eval()
    return tokenizer, model


def embed_texts(
    texts: list[str],
    tokenizer: Any,
    model: Any,
    batch_size: int,
    max_length: int,
    device: torch.device,
) -> np.ndarray:
    if not texts:
        return np.empty((0, 0), dtype=np.float32)

    embeddings: list[np.ndarray] = []
    n_texts = len(texts)
    with torch.no_grad():
        for start in range(0, n_texts, batch_size):
            end = min(start + batch_size, n_texts)
            encoded = tokenizer(
                texts[start:end],
                padding=True,
                truncation=True,
                max_length=max_length,
                return_tensors="pt",
            )
            encoded = {key: value.to(device) for key, value in encoded.items()}
            output = model(**encoded)
            pooled = mean_pool(output.last_hidden_state, encoded["attention_mask"])
            embeddings.append(pooled.cpu().numpy().astype("float32"))
            print(f"Embedded relation texts {end:,} / {n_texts:,}.")

    return np.vstack(embeddings)


def predicate_to_phrase(predicate: Any) -> str:
    text = "" if pd.isna(predicate) else str(predicate)
    text = text.strip().lower().replace("_", " ")
    return re.sub(r"\s+", " ", text)


def cui_labels(cuis: pd.Series, lookup: pd.Series) -> pd.Series:
    cui_text = cuis.map(lookup)
    return cui_text.fillna(cuis).astype("string")


def add_relation_text_columns(data: pd.DataFrame, lookup: pd.Series) -> pd.DataFrame:
    data = data.copy()
    subject_labels = cui_labels(data["subject_cui_primary"], lookup)
    object_labels = cui_labels(data["object_cui_primary"], lookup)
    data["predicate_phrase"] = data["PREDICATE"].map(predicate_to_phrase)
    data["full_triple_text"] = (
        subject_labels
        + " "
        + data["predicate_phrase"].astype("string")
        + " "
        + object_labels
    )
    data["full_triple_text"] = data["full_triple_text"].astype("string").str.strip()
    return data


def unique_preserving_order(values: pd.Series) -> list[str]:
    return list(dict.fromkeys(values.dropna().astype(str).tolist()))


def map_text_ids(texts: pd.Series, text_to_id: dict[str, int]) -> np.ndarray:
    return texts.astype(str).map(text_to_id).to_numpy(dtype=np.int64)


def cosine_similarity_by_id_pair(
    embeddings: np.ndarray,
    left_ids: np.ndarray,
    right_ids: np.ndarray,
    batch_size: int,
    label: str,
) -> np.ndarray:
    scores = np.full(len(left_ids), np.nan, dtype=np.float32)
    valid_rows = np.flatnonzero((left_ids >= 0) & (right_ids >= 0))

    for start in range(0, len(valid_rows), batch_size):
        row_ids = valid_rows[start : start + batch_size]
        left_vectors = embeddings[left_ids[row_ids]]
        right_vectors = embeddings[right_ids[row_ids]]
        numerator = np.einsum("ij,ij->i", left_vectors, right_vectors)
        left_norm = np.linalg.norm(left_vectors, axis=1)
        right_norm = np.linalg.norm(right_vectors, axis=1)
        denominator = left_norm * right_norm
        batch_scores = np.divide(
            numerator,
            denominator,
            out=np.full(len(row_ids), np.nan, dtype=np.float32),
            where=denominator != 0,
        )
        scores[row_ids] = batch_scores.astype(np.float32)
        print(
            f"Scored {label} comparisons "
            f"{min(start + batch_size, len(valid_rows)):,} / {len(valid_rows):,}."
        )

    return scores


def prefixed_summary_statistics(
    scores: pd.Series | np.ndarray,
    prefix: str,
) -> dict[str, Any]:
    score_array = np.asarray(scores, dtype=np.float32)
    valid_scores = score_array[~np.isnan(score_array)]
    if len(valid_scores) == 0:
        return {
            f"{prefix}_min": "",
            f"{prefix}_mean": "",
            f"{prefix}_std": "",
            f"{prefix}_p01": "",
            f"{prefix}_p05": "",
            f"{prefix}_p10": "",
            f"{prefix}_p25": "",
            f"{prefix}_p50": "",
            f"{prefix}_p75": "",
            f"{prefix}_p90": "",
            f"{prefix}_p95": "",
            f"{prefix}_p99": "",
            f"{prefix}_max": "",
        }

    percentiles = np.percentile(
        valid_scores,
        [1, 5, 10, 25, 50, 75, 90, 95, 99],
    )
    return {
        f"{prefix}_min": float(valid_scores.min()),
        f"{prefix}_mean": float(valid_scores.mean()),
        f"{prefix}_std": float(valid_scores.std()),
        f"{prefix}_p01": float(percentiles[0]),
        f"{prefix}_p05": float(percentiles[1]),
        f"{prefix}_p10": float(percentiles[2]),
        f"{prefix}_p25": float(percentiles[3]),
        f"{prefix}_p50": float(percentiles[4]),
        f"{prefix}_p75": float(percentiles[5]),
        f"{prefix}_p90": float(percentiles[6]),
        f"{prefix}_p95": float(percentiles[7]),
        f"{prefix}_p99": float(percentiles[8]),
        f"{prefix}_max": float(valid_scores.max()),
    }


def closest_prior_by_score(
    comparisons: pd.DataFrame,
    score_column: str,
    score_output_prefix: str,
    closest_output_suffix: str,
) -> pd.DataFrame:
    valid = comparisons.dropna(subset=[score_column]).copy()
    if valid.empty:
        return pd.DataFrame(
            columns=[
                "focal_triple_id",
                f"{score_output_prefix}_max",
                f"{score_output_prefix}_mean",
                f"{score_output_prefix}_min",
                f"new_relation_closest_prior_predicate_{closest_output_suffix}",
                f"new_relation_closest_prior_similarity_{closest_output_suffix}",
            ]
        )

    valid = valid.sort_values(
        ["focal_triple_id", score_column, "PREDICATE_prior"],
        ascending=[True, False, True],
    )
    grouped = valid.groupby("focal_triple_id", sort=False)
    summary = grouped[score_column].agg(["max", "mean", "min"]).reset_index()
    summary = summary.rename(
        columns={
            "max": f"{score_output_prefix}_max",
            "mean": f"{score_output_prefix}_mean",
            "min": f"{score_output_prefix}_min",
        }
    )
    closest = grouped.head(1)[
        ["focal_triple_id", "PREDICATE_prior", score_column]
    ].rename(
        columns={
            "PREDICATE_prior": f"new_relation_closest_prior_predicate_{closest_output_suffix}",
            score_column: f"new_relation_closest_prior_similarity_{closest_output_suffix}",
        }
    )
    return summary.merge(closest, on="focal_triple_id", how="left")


def build_focal_and_comparisons(
    edges: pd.DataFrame,
    prior_unique_triples: pd.DataFrame,
    lookup: pd.Series,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    edges = normalize_triple_columns(edges)
    edges = edges.reset_index(drop=True)
    edges.insert(0, "new_relation_row_id", np.arange(len(edges), dtype=np.int64))

    focal_triples = edges[TRIPLE_COLUMNS].drop_duplicates().reset_index(drop=True)
    focal_triples.insert(0, "focal_triple_id", np.arange(len(focal_triples), dtype=np.int64))
    focal_triples = add_relation_text_columns(focal_triples, lookup)

    edges = edges.merge(focal_triples[["focal_triple_id", *TRIPLE_COLUMNS]], on=TRIPLE_COLUMNS, how="left")
    edges = edges.sort_values("new_relation_row_id").reset_index(drop=True)

    prior_triples = prior_unique_triples[TRIPLE_COLUMNS].drop_duplicates().reset_index(drop=True)
    prior_triples.insert(0, "prior_triple_id", np.arange(len(prior_triples), dtype=np.int64))
    prior_triples = add_relation_text_columns(prior_triples, lookup)

    comparisons = focal_triples[["focal_triple_id", *PAIR_COLUMNS, "PREDICATE"]].merge(
        prior_triples[["prior_triple_id", *PAIR_COLUMNS, "PREDICATE"]],
        on=PAIR_COLUMNS,
        how="inner",
        suffixes=("_focal", "_prior"),
    )
    comparisons = comparisons[
        comparisons["PREDICATE_focal"] != comparisons["PREDICATE_prior"]
    ].copy()
    comparisons = comparisons.sort_values(
        ["focal_triple_id", "PREDICATE_prior", "prior_triple_id"]
    ).reset_index(drop=True)

    return edges, focal_triples, prior_triples, comparisons


def add_similarity_scores(
    comparisons: pd.DataFrame,
    focal_triples: pd.DataFrame,
    prior_triples: pd.DataFrame,
    model_name: str,
    batch_size: int,
    max_length: int,
    similarity_batch_size: int,
    device: torch.device,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if comparisons.empty:
        comparisons = comparisons.copy()
        comparisons["full_triple_similarity"] = np.nan
        comparisons["predicate_only_similarity"] = np.nan
        return comparisons, {
            "n_unique_full_triple_texts_embedded": 0,
            "n_unique_predicate_texts_embedded": 0,
            "embedding_dim_full_triple": "",
            "embedding_dim_predicate_only": "",
        }

    tokenizer, model = load_text_encoder(model_name, device)
    focal_full_texts = focal_triples.set_index("focal_triple_id")["full_triple_text"]
    prior_full_texts = prior_triples.set_index("prior_triple_id")["full_triple_text"]
    focal_predicate_texts = focal_triples.set_index("focal_triple_id")["predicate_phrase"]
    prior_predicate_texts = prior_triples.set_index("prior_triple_id")["predicate_phrase"]

    comparison_focal_full_texts = comparisons["focal_triple_id"].map(focal_full_texts)
    comparison_prior_full_texts = comparisons["prior_triple_id"].map(prior_full_texts)
    comparison_focal_predicate_texts = comparisons["focal_triple_id"].map(focal_predicate_texts)
    comparison_prior_predicate_texts = comparisons["prior_triple_id"].map(prior_predicate_texts)

    unique_full_texts = unique_preserving_order(
        pd.concat(
            [comparison_focal_full_texts, comparison_prior_full_texts],
            ignore_index=True,
        )
    )
    full_text_to_id = {text: i for i, text in enumerate(unique_full_texts)}
    print(f"Unique full-triple texts to embed: {len(unique_full_texts):,}")
    full_embeddings = embed_texts(
        unique_full_texts,
        tokenizer,
        model,
        batch_size,
        max_length,
        device,
    )

    full_focal_ids = map_text_ids(comparison_focal_full_texts, full_text_to_id)
    full_prior_ids = map_text_ids(comparison_prior_full_texts, full_text_to_id)
    full_scores = cosine_similarity_by_id_pair(
        full_embeddings,
        full_focal_ids,
        full_prior_ids,
        similarity_batch_size,
        "full-triple",
    )

    unique_predicate_texts = unique_preserving_order(
        pd.concat(
            [comparison_focal_predicate_texts, comparison_prior_predicate_texts],
            ignore_index=True,
        )
    )
    predicate_text_to_id = {text: i for i, text in enumerate(unique_predicate_texts)}
    print(f"Unique predicate-only texts to embed: {len(unique_predicate_texts):,}")
    predicate_embeddings = embed_texts(
        unique_predicate_texts,
        tokenizer,
        model,
        batch_size,
        max_length,
        device,
    )

    predicate_focal_ids = map_text_ids(
        comparison_focal_predicate_texts,
        predicate_text_to_id,
    )
    predicate_prior_ids = map_text_ids(
        comparison_prior_predicate_texts,
        predicate_text_to_id,
    )
    predicate_scores = cosine_similarity_by_id_pair(
        predicate_embeddings,
        predicate_focal_ids,
        predicate_prior_ids,
        similarity_batch_size,
        "predicate-only",
    )

    comparisons = comparisons.copy()
    comparisons["full_triple_similarity"] = full_scores
    comparisons["predicate_only_similarity"] = predicate_scores
    embedding_summary = {
        "n_unique_full_triple_texts_embedded": len(unique_full_texts),
        "n_unique_predicate_texts_embedded": len(unique_predicate_texts),
        "embedding_dim_full_triple": int(full_embeddings.shape[1]),
        "embedding_dim_predicate_only": int(predicate_embeddings.shape[1]),
    }
    return comparisons, embedding_summary


def aggregate_scores(
    focal_triples: pd.DataFrame,
    comparisons: pd.DataFrame,
) -> pd.DataFrame:
    focal_score = focal_triples[["focal_triple_id"]].copy()

    if comparisons.empty:
        focal_score["new_relation_n_prior_triples"] = 0
        focal_score["new_relation_n_prior_predicates"] = 0
    else:
        counts = (
            comparisons.groupby("focal_triple_id")
            .agg(
                new_relation_n_prior_triples=("prior_triple_id", "count"),
                new_relation_n_prior_predicates=("PREDICATE_prior", "nunique"),
            )
            .reset_index()
        )
        focal_score = focal_score.merge(counts, on="focal_triple_id", how="left")
        focal_score["new_relation_n_prior_triples"] = focal_score[
            "new_relation_n_prior_triples"
        ].fillna(0).astype(np.int64)
        focal_score["new_relation_n_prior_predicates"] = focal_score[
            "new_relation_n_prior_predicates"
        ].fillna(0).astype(np.int64)

    full_summary = closest_prior_by_score(
        comparisons,
        "full_triple_similarity",
        "semantic_proximity_full_triple",
        "full_triple",
    )
    predicate_summary = closest_prior_by_score(
        comparisons,
        "predicate_only_similarity",
        "semantic_proximity_predicate_only",
        "predicate_only",
    )

    focal_score = focal_score.merge(full_summary, on="focal_triple_id", how="left")
    focal_score = focal_score.merge(predicate_summary, on="focal_triple_id", how="left")
    focal_score["new_relation_has_prior_same_direction_different_predicate"] = (
        focal_score["new_relation_n_prior_predicates"] > 0
    )

    rename_map = {
        "semantic_proximity_full_triple_max": "new_relation_semantic_proximity_full_triple_max",
        "semantic_proximity_full_triple_mean": "new_relation_semantic_proximity_full_triple_mean",
        "semantic_proximity_full_triple_min": "new_relation_semantic_proximity_full_triple_min",
        "semantic_proximity_predicate_only_max": "new_relation_semantic_proximity_predicate_only_max",
        "semantic_proximity_predicate_only_mean": "new_relation_semantic_proximity_predicate_only_mean",
        "semantic_proximity_predicate_only_min": "new_relation_semantic_proximity_predicate_only_min",
    }
    return focal_score.rename(columns=rename_map)


def write_single_row_csv(path: Path, row: dict[str, Any]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)


def build_summary(
    scored_edges: pd.DataFrame,
    comparisons: pd.DataFrame,
    n_edge_annotation_rows: int,
    n_prior_unique_triples_total: int,
    focal_year: int,
    prior_years: list[int],
    model_name: str,
    batch_size: int,
    max_length: int,
    similarity_batch_size: int,
    device: torch.device,
    embedding_summary: dict[str, Any],
    elapsed_seconds: float,
) -> dict[str, Any]:
    n_new_relation_rows = len(scored_edges)
    n_unique_new_relation_triples = int(scored_edges["focal_triple_id"].nunique())
    has_prior = scored_edges["new_relation_has_prior_same_direction_different_predicate"]
    full_score_column = "new_relation_semantic_proximity_full_triple_max"
    predicate_score_column = "new_relation_semantic_proximity_predicate_only_max"

    n_scored_rows_full = int(scored_edges[full_score_column].notna().sum())
    n_scored_rows_predicate = int(scored_edges[predicate_score_column].notna().sum())
    scored_unique_full = int(
        scored_edges.loc[scored_edges[full_score_column].notna(), "focal_triple_id"].nunique()
    )
    scored_unique_predicate = int(
        scored_edges.loc[scored_edges[predicate_score_column].notna(), "focal_triple_id"].nunique()
    )

    gpu_name = torch.cuda.get_device_name(0) if torch.cuda.is_available() else ""
    summary: dict[str, Any] = {
        "pyear": focal_year,
        "prior_years": "|".join(str(year) for year in prior_years),
        "n_prior_years_found": len(prior_years),
        "n_edge_annotation_rows": n_edge_annotation_rows,
        "n_new_relation_rows": n_new_relation_rows,
        "n_unique_new_relation_triples": n_unique_new_relation_triples,
        "n_unique_directed_pairs": int(scored_edges[PAIR_COLUMNS].drop_duplicates().shape[0]),
        "n_unique_subject_cui": int(scored_edges["subject_cui_primary"].nunique()),
        "n_unique_object_cui": int(scored_edges["object_cui_primary"].nunique()),
        "n_unique_focal_predicates": int(scored_edges["PREDICATE"].nunique()),
        "n_prior_unique_triples_total": n_prior_unique_triples_total,
        "n_prior_comparison_triples": len(comparisons),
        "n_unique_prior_predicates_in_comparisons": int(
            comparisons["PREDICATE_prior"].nunique()
        )
        if not comparisons.empty
        else 0,
        "mean_n_prior_predicates": float(
            scored_edges.drop_duplicates("focal_triple_id")[
                "new_relation_n_prior_predicates"
            ].mean()
        )
        if n_unique_new_relation_triples
        else "",
        "max_n_prior_predicates": int(
            scored_edges["new_relation_n_prior_predicates"].max()
        )
        if n_new_relation_rows
        else 0,
        "n_rows_with_prior_same_direction_different_predicate": int(has_prior.sum()),
        "n_rows_without_prior_same_direction_different_predicate": int((~has_prior).sum()),
        "share_rows_with_prior_same_direction_different_predicate": safe_share(
            int(has_prior.sum()),
            n_new_relation_rows,
        ),
        "n_scored_rows_full_triple": n_scored_rows_full,
        "n_scored_unique_triples_full_triple": scored_unique_full,
        "share_scored_rows_full_triple": safe_share(
            n_scored_rows_full,
            n_new_relation_rows,
        ),
        "n_scored_rows_predicate_only": n_scored_rows_predicate,
        "n_scored_unique_triples_predicate_only": scored_unique_predicate,
        "share_scored_rows_predicate_only": safe_share(
            n_scored_rows_predicate,
            n_new_relation_rows,
        ),
        "model_name": model_name,
        "batch_size": batch_size,
        "max_length": max_length,
        "similarity_batch_size": similarity_batch_size,
        "device": str(device),
        "cuda_available": bool(torch.cuda.is_available()),
        "cuda_version": torch.version.cuda or "",
        "gpu_name": gpu_name,
        "elapsed_seconds": round(elapsed_seconds, 3),
        **embedding_summary,
        **prefixed_summary_statistics(
            scored_edges[full_score_column],
            "score_full_triple",
        ),
        **prefixed_summary_statistics(
            scored_edges[predicate_score_column],
            "score_predicate_only",
        ),
    }
    return summary


def score_new_relation_semantic_proximity(
    project_root: Path,
    focal_year: int,
    overwrite: bool,
    model_name: str,
    batch_size: int,
    max_length: int,
    similarity_batch_size: int,
) -> None:
    start_time = time.time()
    prior_years = prior_years_for_focal_year(focal_year)
    annotation_file = edge_annotation_file(project_root, focal_year)
    prior_files = [yearly_input_file(project_root, year) for year in prior_years]
    cui_text_file = project_root / CUI_TEXT_TABLE_FILE
    scored_file = output_file(project_root, focal_year)
    year_summary_file = summary_file(project_root, focal_year)

    check_paths(
        [annotation_file, *prior_files, cui_text_file],
        [scored_file, year_summary_file],
        overwrite,
    )

    print(f"Reading edge annotation from {annotation_file}")
    edges, n_edge_annotation_rows = load_new_relation_edges(annotation_file)
    print(f"Edge annotation rows: {n_edge_annotation_rows:,}")
    print(f"New_Relation rows: {len(edges):,}")

    print(f"Reading prior unique triples from {len(prior_files):,} yearly files")
    prior_unique_triples = load_prior_unique_triples(prior_files)
    print(f"Prior unique directed typed triples: {len(prior_unique_triples):,}")

    print(f"Reading CUI text table from {cui_text_file}")
    cui_text_lookup = load_cui_text_lookup(cui_text_file)
    print(f"CUI text labels: {len(cui_text_lookup):,}")

    edges, focal_triples, prior_triples, comparisons = build_focal_and_comparisons(
        edges,
        prior_unique_triples,
        cui_text_lookup,
    )
    print(f"Unique New_Relation focal triples: {len(focal_triples):,}")
    print(f"Prior comparison triples: {len(comparisons):,}")

    device = select_device()
    comparisons, embedding_summary = add_similarity_scores(
        comparisons,
        focal_triples,
        prior_triples,
        model_name,
        batch_size,
        max_length,
        similarity_batch_size,
        device,
    )
    focal_scores = aggregate_scores(focal_triples, comparisons)

    scored_edges = edges.merge(focal_scores, on="focal_triple_id", how="left")
    scored_edges = scored_edges.sort_values("new_relation_row_id").reset_index(drop=True)

    print(f"Writing scored New_Relation rows to {scored_file}")
    scored_edges.to_parquet(scored_file, index=False, compression="zstd")

    elapsed_seconds = time.time() - start_time
    summary = build_summary(
        scored_edges,
        comparisons,
        n_edge_annotation_rows,
        len(prior_unique_triples),
        focal_year,
        prior_years,
        model_name,
        batch_size,
        max_length,
        similarity_batch_size,
        device,
        embedding_summary,
        elapsed_seconds,
    )
    write_single_row_csv(year_summary_file, summary)
    print(f"Saved summary to {year_summary_file}")


def main() -> None:
    project_root = get_env_path("IC_PROJECT_ROOT", DEFAULT_PROJECT_ROOT)
    focal_year = get_focal_year()
    overwrite = get_env_bool(
        "IC_NEW_RELATION_SEMANTIC_PROXIMITY_OVERWRITE",
        False,
    )
    model_name = get_env_str_any(
        ["IC_NEW_RELATION_BIOMEDBERT_MODEL_NAME", "IC_BIOMEDBERT_MODEL_NAME"],
        DEFAULT_MODEL_NAME,
    )
    batch_size = get_env_int_any(
        ["IC_NEW_RELATION_BIOMEDBERT_BATCH_SIZE", "IC_BIOMEDBERT_BATCH_SIZE"],
        DEFAULT_BATCH_SIZE,
    )
    max_length = get_env_int_any(
        ["IC_NEW_RELATION_BIOMEDBERT_MAX_LENGTH", "IC_BIOMEDBERT_MAX_LENGTH"],
        DEFAULT_MAX_LENGTH,
    )
    similarity_batch_size = get_env_int(
        "IC_SEMANTIC_PROXIMITY_BATCH_SIZE",
        DEFAULT_SIMILARITY_BATCH_SIZE,
    )

    print(f"Project root: {project_root}")
    print(f"Focal year: {focal_year}")
    print(f"Overwrite outputs: {overwrite}")
    print(f"Model: {model_name}")
    print(f"Embedding batch size: {batch_size}")
    print(f"Max token length: {max_length}")
    print(f"Similarity batch size: {similarity_batch_size}")

    score_new_relation_semantic_proximity(
        project_root,
        focal_year,
        overwrite,
        model_name,
        batch_size,
        max_length,
        similarity_batch_size,
    )
    print("New_Relation semantic proximity scoring complete.")


if __name__ == "__main__":
    main()
