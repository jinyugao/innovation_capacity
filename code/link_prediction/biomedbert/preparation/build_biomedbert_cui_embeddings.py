"""Build BiomedBERT embeddings for selected CUI labels."""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import torch
    from transformers import AutoModel, AutoTokenizer
except ImportError as exc:
    raise ImportError(
        "This script needs torch and transformers. Install them with:\n"
        "/xdisk/sebratt/jinyugao/envs/innovation_capacity/bin/pip install "
        "torch transformers"
    ) from exc


INPUT_FILE = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/"
    "biomedbert_link_prediction/cui_labels/biomedbert_cui_labels.csv.gz"
)
OUTPUT_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/"
    "biomedbert_link_prediction/cui_embeddings"
)
EMBEDDING_FILE = OUTPUT_DIR / "biomedbert_cui_embeddings.npz"
METADATA_FILE = OUTPUT_DIR / "biomedbert_cui_embedding_metadata.csv.gz"

MODEL_NAME = "microsoft/BiomedNLP-BiomedBERT-base-uncased-abstract-fulltext"
BATCH_SIZE = 128
MAX_LENGTH = 64
OVERWRITE = False


def select_device() -> torch.device:
    if torch.cuda.is_available():
        return torch.device("cuda")
    if os.environ.get("REQUIRE_CUDA") == "1":
        raise RuntimeError(
            "REQUIRE_CUDA=1 but torch.cuda.is_available() is False. "
            "Check the GPU Slurm allocation and CUDA-enabled torch installation."
        )
    return torch.device("cpu")


def check_input(input_file: Path) -> None:
    if not input_file.exists():
        raise FileNotFoundError(f"Missing required input file: {input_file}")


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


def mean_pool(last_hidden_state: torch.Tensor, attention_mask: torch.Tensor) -> torch.Tensor:
    mask = attention_mask.unsqueeze(-1).expand(last_hidden_state.size()).float()
    masked_embeddings = last_hidden_state * mask
    summed_embeddings = masked_embeddings.sum(dim=1)
    token_counts = mask.sum(dim=1).clamp(min=1e-9)
    return summed_embeddings / token_counts


def build_embeddings(input_file: Path) -> tuple[pd.DataFrame, np.ndarray]:
    labels = pd.read_csv(
        input_file,
        compression="gzip",
        dtype={"cui": "string", "selected_cui_name": "string"},
    )
    labels = labels.dropna(subset=["cui", "selected_cui_name"]).copy()
    labels["cui"] = labels["cui"].astype("string").str.strip()
    labels["selected_cui_name"] = (
        labels["selected_cui_name"].astype("string").str.strip()
    )
    labels = labels[(labels["cui"] != "") & (labels["selected_cui_name"] != "")]
    labels = labels.sort_values("cui", kind="mergesort").reset_index(drop=True)

    device = select_device()
    print(f"Loading BiomedBERT model: {MODEL_NAME}")
    print(f"Using device: {device}")

    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModel.from_pretrained(MODEL_NAME)
    model.to(device)
    model.eval()

    embeddings = []
    names = labels["selected_cui_name"].tolist()

    with torch.no_grad():
        for start in range(0, len(labels), BATCH_SIZE):
            end = min(start + BATCH_SIZE, len(labels))
            batch_names = names[start:end]
            encoded = tokenizer(
                batch_names,
                padding=True,
                truncation=True,
                max_length=MAX_LENGTH,
                return_tensors="pt",
            )
            encoded = {key: value.to(device) for key, value in encoded.items()}
            output = model(**encoded)
            pooled = mean_pool(output.last_hidden_state, encoded["attention_mask"])
            embeddings.append(pooled.cpu().numpy().astype("float32"))

            print(f"Embedded CUI labels {end:,} / {len(labels):,}.")

    embedding_matrix = np.vstack(embeddings)
    return labels, embedding_matrix


def save_embeddings(
    labels: pd.DataFrame,
    embedding_matrix: np.ndarray,
    embedding_file: Path,
    metadata_file: Path,
) -> None:
    np.savez_compressed(
        embedding_file,
        cui=labels["cui"].astype(str).to_numpy(),
        embeddings=embedding_matrix,
        model_name=np.array([MODEL_NAME]),
    )

    metadata = labels[
        [
            "cui",
            "selected_cui_name",
            "selected_name_occurrences",
            "frequency_rank",
            "selection_rank_after_quality_filter",
            "name_quality_flag",
        ]
    ].copy()
    metadata["model_name"] = MODEL_NAME
    metadata["embedding_dimensions"] = embedding_matrix.shape[1]
    metadata.to_csv(metadata_file, index=False, compression="gzip")

    print(f"Saved embedding matrix to {embedding_file}")
    print(f"Saved embedding metadata to {metadata_file}")
    print(f"Embedding matrix shape: {embedding_matrix.shape}")


def main() -> None:
    check_input(INPUT_FILE)
    check_outputs([EMBEDDING_FILE, METADATA_FILE])
    labels, embedding_matrix = build_embeddings(INPUT_FILE)
    save_embeddings(labels, embedding_matrix, EMBEDDING_FILE, METADATA_FILE)


if __name__ == "__main__":
    main()
