"""Calculate BiomedBERT semantic scores for two-hop candidate edges."""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd


CANDIDATE_EDGE_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/"
    "link_prediction/candidate_edges/two_hop_candidate_edges"
)
EMBEDDING_FILE = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/"
    "biomedbert_link_prediction/cui_embeddings/biomedbert_cui_embeddings.npz"
)
OUTPUT_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/"
    "biomedbert_link_prediction/candidate_edges"
)

BASE_YEAR = 1980
CHUNK_SIZE = 1_000_000
OVERWRITE = False


def get_focal_year() -> int:
    task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if task_id is None:
        raise RuntimeError("SLURM_ARRAY_TASK_ID not found.")
    return int(task_id) + BASE_YEAR


def candidate_file_for_year(focal_year: int) -> Path:
    return (
        CANDIDATE_EDGE_DIR
        / f"two_hop_candidate_edges_prior_5y_{focal_year}.csv.gz"
    )


def output_file_for_year(focal_year: int) -> Path:
    return OUTPUT_DIR / f"biomedbert_scored_candidate_edges_{focal_year}.csv.gz"


def check_input(input_file: Path) -> None:
    if not input_file.exists():
        raise FileNotFoundError(f"Missing required input file: {input_file}")


def check_output(output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    if output_file.exists() and not OVERWRITE:
        raise FileExistsError(
            "Output file already exists. Set OVERWRITE = True to replace it:\n"
            f"{output_file}"
        )
    if output_file.exists() and OVERWRITE:
        output_file.unlink()


def load_embeddings(embedding_file: Path) -> tuple[dict[str, int], np.ndarray]:
    data = np.load(embedding_file, allow_pickle=False)
    cuis = data["cui"].astype(str)
    embeddings = data["embeddings"].astype("float32")

    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    embeddings = embeddings / norms

    cui_to_index = {cui: index for index, cui in enumerate(cuis)}
    print(f"Loaded BiomedBERT embeddings for {len(cui_to_index):,} CUIs.")
    return cui_to_index, embeddings


def score_chunk(
    chunk: pd.DataFrame,
    cui_to_index: dict[str, int],
    embeddings: np.ndarray,
) -> pd.DataFrame:
    chunk = chunk.dropna(subset=["node_a", "node_b"]).copy()
    chunk["node_a"] = chunk["node_a"].astype("string").str.strip()
    chunk["node_b"] = chunk["node_b"].astype("string").str.strip()

    node_a_indices = np.array(
        [cui_to_index.get(str(node_a), -1) for node_a in chunk["node_a"]],
        dtype=np.int64,
    )
    node_b_indices = np.array(
        [cui_to_index.get(str(node_b), -1) for node_b in chunk["node_b"]],
        dtype=np.int64,
    )
    valid = (node_a_indices >= 0) & (node_b_indices >= 0)

    scores = np.full(len(chunk), np.nan, dtype="float32")
    if valid.any():
        scores[valid] = np.sum(
            embeddings[node_a_indices[valid]] * embeddings[node_b_indices[valid]],
            axis=1,
        )

    chunk["node_a_has_biomedbert_embedding"] = node_a_indices >= 0
    chunk["node_b_has_biomedbert_embedding"] = node_b_indices >= 0
    chunk["biomedbert_score"] = scores
    return chunk


def score_candidate_edges(focal_year: int) -> None:
    candidate_file = candidate_file_for_year(focal_year)
    output_file = output_file_for_year(focal_year)

    check_input(candidate_file)
    check_input(EMBEDDING_FILE)
    check_output(output_file)

    cui_to_index, embeddings = load_embeddings(EMBEDDING_FILE)

    total_rows = 0
    missing_node_a = 0
    missing_node_b = 0
    wrote_header = False

    reader = pd.read_csv(
        candidate_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        dtype={"node_a": "string", "node_b": "string"},
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        scored = score_chunk(chunk, cui_to_index, embeddings)
        scored["pyear"] = focal_year

        total_rows += len(scored)
        missing_node_a += int((~scored["node_a_has_biomedbert_embedding"]).sum())
        missing_node_b += int((~scored["node_b_has_biomedbert_embedding"]).sum())

        scored.to_csv(
            output_file,
            mode="a",
            index=False,
            compression="gzip",
            header=not wrote_header,
        )
        wrote_header = True

        print(f"Chunk {chunk_number:,}: scored {total_rows:,} candidate edges.")

    if not wrote_header:
        pd.DataFrame(
            columns=[
                "node_a",
                "node_b",
                "pyear",
                "node_a_has_biomedbert_embedding",
                "node_b_has_biomedbert_embedding",
                "biomedbert_score",
            ]
        ).to_csv(output_file, index=False, compression="gzip")

    print(f"Saved BiomedBERT scores to {output_file}")
    print(f"Total candidate edges scored: {total_rows:,}")
    print(f"Rows missing node_a embedding: {missing_node_a:,}")
    print(f"Rows missing node_b embedding: {missing_node_b:,}")


def main() -> None:
    focal_year = get_focal_year()
    print(f"Starting BiomedBERT scoring for focal year {focal_year}.")
    score_candidate_edges(focal_year)
    print(f"Finished BiomedBERT scoring for focal year {focal_year}.")


if __name__ == "__main__":
    main()
