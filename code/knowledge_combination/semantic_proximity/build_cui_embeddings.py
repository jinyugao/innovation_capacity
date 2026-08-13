"""Build BiomedBERT embeddings for CUI text labels.

This script reads the semantic-proximity CUI text table and writes a row-aligned
embedding matrix plus metadata. The matrix row order is recorded by
``embedding_row_id`` in the metadata parquet.
"""

from __future__ import annotations

import csv
import os
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

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

INPUT_FILE_NAME = (
    "data/processed/knowledge_combination/semantic_proximity/"
    "cui_text/cui_text_table.parquet"
)
OUTPUT_SUBDIR = Path(
    "data/processed/knowledge_combination/semantic_proximity/cui_embeddings"
)
EMBEDDING_FILE_NAME = "cui_embeddings.npy"
METADATA_FILE_NAME = "cui_embedding_metadata.parquet"
SUMMARY_FILE_NAME = "cui_embedding_summary.csv"

DEFAULT_MODEL_NAME = "microsoft/BiomedNLP-BiomedBERT-base-uncased-abstract-fulltext"
DEFAULT_BATCH_SIZE = 128
DEFAULT_MAX_LENGTH = 64

REQUIRED_COLUMNS = ["primary_cui", "cui_text"]


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


def get_env_str(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value else default


def check_paths(input_file: Path, output_files: list[Path], overwrite: bool) -> None:
    if not input_file.exists():
        raise FileNotFoundError(f"Missing input parquet: {input_file}")

    for output_file in output_files:
        output_file.parent.mkdir(parents=True, exist_ok=True)

    existing = [path for path in output_files if path.exists()]
    if existing and not overwrite:
        existing_text = "\n".join(str(path) for path in existing)
        raise FileExistsError(
            "Output file(s) already exist. Move them, delete them, or rerun "
            f"with IC_CUI_EMBEDDINGS_OVERWRITE=1:\n{existing_text}"
        )
    if overwrite:
        for path in existing:
            path.unlink()


def write_single_row_csv(path: Path, row: dict[str, Any]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)


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


def load_cui_text_table(input_file: Path) -> pd.DataFrame:
    data = pd.read_parquet(input_file)
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in data.columns]
    if missing_columns:
        raise ValueError(
            "Input CUI text table is missing required column(s): "
            + ", ".join(missing_columns)
        )

    data = data.copy()
    data["primary_cui"] = data["primary_cui"].astype("string").str.strip()
    data["cui_text"] = data["cui_text"].astype("string").str.strip()
    data = data.dropna(subset=["primary_cui", "cui_text"])
    data = data[(data["primary_cui"] != "") & (data["cui_text"] != "")]
    data = data.reset_index(drop=True)
    data.insert(0, "embedding_row_id", np.arange(len(data), dtype=np.int64))

    duplicate_rows = int(data.duplicated(subset=["primary_cui"]).sum())
    if duplicate_rows:
        raise ValueError(
            "CUI text table is not unique by primary_cui. "
            f"Duplicate rows: {duplicate_rows:,}"
        )

    return data


def build_embeddings(
    texts: list[str],
    model_name: str,
    batch_size: int,
    max_length: int,
    device: torch.device,
) -> np.ndarray:
    print(f"Loading BiomedBERT model: {model_name}")
    print(f"Using device: {device}")

    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name)
    model.to(device)
    model.eval()

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
            print(f"Embedded CUI texts {end:,} / {n_texts:,}.")

    if not embeddings:
        raise ValueError("No CUI texts available for embedding.")

    return np.vstack(embeddings)


def save_outputs(
    metadata: pd.DataFrame,
    embedding_matrix: np.ndarray,
    embedding_file: Path,
    metadata_file: Path,
    summary_file: Path,
    summary: dict[str, Any],
) -> None:
    metadata = metadata.copy()
    metadata["model_name"] = summary["model_name"]
    metadata["embedding_dim"] = summary["embedding_dim"]
    metadata["embedding_file"] = embedding_file.name

    np.save(embedding_file, embedding_matrix)
    metadata.to_parquet(metadata_file, index=False, compression="zstd")
    write_single_row_csv(summary_file, summary)


def main() -> None:
    start_time = time.time()
    project_root = get_env_path("IC_PROJECT_ROOT", DEFAULT_PROJECT_ROOT)
    input_file = project_root / INPUT_FILE_NAME
    output_dir = project_root / OUTPUT_SUBDIR
    embedding_file = output_dir / EMBEDDING_FILE_NAME
    metadata_file = output_dir / METADATA_FILE_NAME
    summary_file = output_dir / SUMMARY_FILE_NAME

    model_name = get_env_str("IC_BIOMEDBERT_MODEL_NAME", DEFAULT_MODEL_NAME)
    batch_size = get_env_int("IC_BIOMEDBERT_BATCH_SIZE", DEFAULT_BATCH_SIZE)
    max_length = get_env_int("IC_BIOMEDBERT_MAX_LENGTH", DEFAULT_MAX_LENGTH)
    overwrite = get_env_bool("IC_CUI_EMBEDDINGS_OVERWRITE", False)

    print(f"Project root: {project_root}")
    print(f"Input CUI text table: {input_file}")
    print(f"Output embedding matrix: {embedding_file}")
    print(f"Output metadata: {metadata_file}")
    print(f"Output summary: {summary_file}")
    print(f"Model: {model_name}")
    print(f"Batch size: {batch_size}")
    print(f"Max token length: {max_length}")
    print(f"Overwrite outputs: {overwrite}")

    check_paths(input_file, [embedding_file, metadata_file, summary_file], overwrite)
    metadata = load_cui_text_table(input_file)
    print(f"CUI texts to embed: {len(metadata):,}")

    device = select_device()
    embedding_matrix = build_embeddings(
        metadata["cui_text"].astype(str).tolist(),
        model_name,
        batch_size,
        max_length,
        device,
    )

    norms = np.linalg.norm(embedding_matrix, axis=1)
    elapsed_seconds = time.time() - start_time
    gpu_name = torch.cuda.get_device_name(0) if torch.cuda.is_available() else ""
    summary = {
        "n_cui_text_rows": len(metadata),
        "n_unique_primary_cui": int(metadata["primary_cui"].nunique()),
        "model_name": model_name,
        "embedding_dim": int(embedding_matrix.shape[1]),
        "embedding_dtype": str(embedding_matrix.dtype),
        "embeddings_l2_normalized": False,
        "batch_size": batch_size,
        "max_length": max_length,
        "device": str(device),
        "cuda_available": bool(torch.cuda.is_available()),
        "cuda_version": torch.version.cuda or "",
        "gpu_name": gpu_name,
        "embedding_norm_min": float(norms.min()),
        "embedding_norm_mean": float(norms.mean()),
        "embedding_norm_max": float(norms.max()),
        "elapsed_seconds": round(elapsed_seconds, 3),
    }

    save_outputs(metadata, embedding_matrix, embedding_file, metadata_file, summary_file, summary)

    print(f"Saved embedding matrix to {embedding_file}")
    print(f"Saved embedding metadata to {metadata_file}")
    print(f"Saved embedding summary to {summary_file}")
    print(f"Embedding matrix shape: {embedding_matrix.shape}")
    print("CUI embedding construction complete.")


if __name__ == "__main__":
    main()
