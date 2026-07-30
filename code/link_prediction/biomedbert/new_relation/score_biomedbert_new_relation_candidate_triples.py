"""Score BiomedBERT candidate triples for new-relation prediction.

Each candidate triple is compared with prior 5-year reference triples using
BiomedBERT text embeddings. The primary score is the maximum cosine similarity
to historical triples with the same predicate and the same directed semantic
type pair. If that reference set is empty, the script falls back to historical
triples with the same predicate only.
"""

from __future__ import annotations

import gc
import os
from collections import defaultdict
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


INTERIM_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim"
)
SPLIT_PREDICATION_DIR = (
    INTERIM_DIR
    / "semmedVER43_R/split_predications_with_pyear_filtered_by_pyear"
)
CANDIDATE_DIR = INTERIM_DIR / "biomedbert_link_prediction/new_relation/candidate_triples"
CUI_LABEL_FILE = (
    INTERIM_DIR / "biomedbert_link_prediction/cui_labels/biomedbert_cui_labels.csv.gz"
)
OUTPUT_DIR = INTERIM_DIR / "biomedbert_link_prediction/new_relation/scored_candidate_triples"

SPLIT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
CANDIDATE_FILE_PREFIX = "biomedbert_new_relation_candidate_triples"
OUTPUT_FILE_PREFIX = "biomedbert_new_relation_scored_candidate_triples"

MODEL_NAME = "microsoft/BiomedNLP-BiomedBERT-base-uncased-abstract-fulltext"
BASE_YEAR = 1980
PRIOR_FIVE_YEAR_WINDOW_YEARS = 5
REFERENCE_CHUNK_SIZE = 500_000
CANDIDATE_CHUNK_SIZE = 10_000
TEXT_BATCH_SIZE = 128
MAX_LENGTH = 96
SIMILARITY_QUERY_BLOCK_SIZE = 256
OVERWRITE = False

PREDICATION_ID_COLUMN = "PREDICATION_ID"
SUBJECT_CUI_COLUMN = "subject_cui_primary"
OBJECT_CUI_COLUMN = "object_cui_primary"
PREDICATE_COLUMN = "PREDICATE"
SUBJECT_SEMTYPE_COLUMN = "SUBJECT_SEMTYPE"
OBJECT_SEMTYPE_COLUMN = "OBJECT_SEMTYPE"

SCORED_OUTPUT_COLUMNS = [
    "subject_cui",
    "predicate",
    "object_cui",
    "subject_semtype",
    "object_semtype",
    "n_supporting_semtype_pairs",
    "n_supporting_semtype_pair_observations",
    "subject_name",
    "predicate_phrase",
    "object_name",
    "candidate_text",
    "pyear",
    "prior_five_year_window_years",
    "candidate_pair_source",
    "candidate_unit",
    "biomedbert_new_relation_score",
    "reference_scope",
    "n_reference_triples",
    "max_similarity_reference_predication_id",
    "max_similarity_reference_text",
]


def select_device() -> torch.device:
    if torch.cuda.is_available():
        return torch.device("cuda")
    if os.environ.get("REQUIRE_CUDA") == "1":
        raise RuntimeError(
            "REQUIRE_CUDA=1 but torch.cuda.is_available() is False. "
            "Check the GPU Slurm allocation and CUDA-enabled torch installation."
        )
    return torch.device("cpu")


def get_focal_year() -> int:
    task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if task_id is None:
        raise RuntimeError("SLURM_ARRAY_TASK_ID not found.")
    return int(task_id) + BASE_YEAR


def split_file_for_year(year: int) -> Path:
    return SPLIT_PREDICATION_DIR / f"{SPLIT_FILE_PREFIX}_{year}.csv.gz"


def candidate_file_for_year(focal_year: int) -> Path:
    return CANDIDATE_DIR / f"{CANDIDATE_FILE_PREFIX}_{focal_year}.csv.gz"


def output_file_for_year(focal_year: int) -> Path:
    return OUTPUT_DIR / f"{OUTPUT_FILE_PREFIX}_{focal_year}.csv.gz"


def check_input(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input file: {path}")


def check_output(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not OVERWRITE:
        raise FileExistsError(
            "Output file already exists. Set OVERWRITE = True to replace it:\n"
            f"{path}"
        )
    if path.exists() and OVERWRITE:
        path.unlink()


def normalize_value(value: object) -> str:
    return "" if pd.isna(value) else str(value).strip()


def predicate_to_phrase(predicate: object) -> str:
    return normalize_value(predicate).replace("_", " ").lower()


def build_text(subject_name: str, predicate: str, object_name: str) -> str:
    return f"{subject_name} {predicate_to_phrase(predicate)} {object_name}".strip()


def load_cui_labels(path: Path) -> dict[str, str]:
    check_input(path)
    labels = pd.read_csv(
        path,
        compression="gzip",
        usecols=["cui", "selected_cui_name"],
        dtype={"cui": "string", "selected_cui_name": "string"},
    )
    labels = labels.dropna(subset=["cui", "selected_cui_name"]).copy()
    labels["cui"] = labels["cui"].astype("string").str.strip()
    labels["selected_cui_name"] = labels["selected_cui_name"].astype("string").str.strip()
    labels = labels[(labels["cui"] != "") & (labels["selected_cui_name"] != "")]
    label_map = dict(zip(labels["cui"].astype(str), labels["selected_cui_name"].astype(str)))
    print(f"Loaded BiomedBERT CUI labels for {len(label_map):,} CUIs.")
    return label_map


def load_model() -> tuple[AutoTokenizer, AutoModel, torch.device]:
    device = select_device()
    print(f"Loading BiomedBERT model: {MODEL_NAME}")
    print(f"Using device: {device}")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModel.from_pretrained(MODEL_NAME)
    model.to(device)
    model.eval()
    return tokenizer, model, device


def mean_pool(last_hidden_state: torch.Tensor, attention_mask: torch.Tensor) -> torch.Tensor:
    mask = attention_mask.unsqueeze(-1).expand(last_hidden_state.size()).float()
    masked_embeddings = last_hidden_state * mask
    summed_embeddings = masked_embeddings.sum(dim=1)
    token_counts = mask.sum(dim=1).clamp(min=1e-9)
    return summed_embeddings / token_counts


def encode_texts(
    texts: list[str],
    tokenizer: AutoTokenizer,
    model: AutoModel,
    device: torch.device,
) -> np.ndarray:
    embeddings = []
    with torch.no_grad():
        for start in range(0, len(texts), TEXT_BATCH_SIZE):
            end = min(start + TEXT_BATCH_SIZE, len(texts))
            encoded = tokenizer(
                texts[start:end],
                padding=True,
                truncation=True,
                max_length=MAX_LENGTH,
                return_tensors="pt",
            )
            encoded = {key: value.to(device) for key, value in encoded.items()}
            output = model(**encoded)
            pooled = mean_pool(output.last_hidden_state, encoded["attention_mask"])
            batch = pooled.cpu().numpy().astype("float32")
            embeddings.append(batch)

    if not embeddings:
        return np.empty((0, 0), dtype="float32")

    matrix = np.vstack(embeddings)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return matrix / norms


def read_reference_triples(
    focal_year: int,
    label_map: dict[str, str],
) -> pd.DataFrame:
    rows = []
    found_years = []
    missing_years = []

    for year in range(focal_year - PRIOR_FIVE_YEAR_WINDOW_YEARS, focal_year):
        input_file = split_file_for_year(year)
        if not input_file.exists():
            missing_years.append(year)
            continue

        found_years.append(year)
        reader = pd.read_csv(
            input_file,
            compression="gzip",
            chunksize=REFERENCE_CHUNK_SIZE,
            usecols=[
                PREDICATION_ID_COLUMN,
                SUBJECT_CUI_COLUMN,
                OBJECT_CUI_COLUMN,
                PREDICATE_COLUMN,
                SUBJECT_SEMTYPE_COLUMN,
                OBJECT_SEMTYPE_COLUMN,
            ],
            dtype={
                PREDICATION_ID_COLUMN: "string",
                SUBJECT_CUI_COLUMN: "string",
                OBJECT_CUI_COLUMN: "string",
                PREDICATE_COLUMN: "string",
                SUBJECT_SEMTYPE_COLUMN: "string",
                OBJECT_SEMTYPE_COLUMN: "string",
            },
        )

        year_rows = 0
        for chunk in reader:
            chunk = chunk.dropna(
                subset=[
                    SUBJECT_CUI_COLUMN,
                    OBJECT_CUI_COLUMN,
                    PREDICATE_COLUMN,
                    SUBJECT_SEMTYPE_COLUMN,
                    OBJECT_SEMTYPE_COLUMN,
                ]
            )
            year_rows += len(chunk)

            for row in chunk.itertuples(index=False):
                predication_id = normalize_value(getattr(row, PREDICATION_ID_COLUMN))
                subject_cui = normalize_value(getattr(row, SUBJECT_CUI_COLUMN))
                object_cui = normalize_value(getattr(row, OBJECT_CUI_COLUMN))
                predicate = normalize_value(getattr(row, PREDICATE_COLUMN))
                subject_semtype = normalize_value(getattr(row, SUBJECT_SEMTYPE_COLUMN))
                object_semtype = normalize_value(getattr(row, OBJECT_SEMTYPE_COLUMN))

                if not subject_cui or not object_cui or subject_cui == object_cui:
                    continue
                if not predicate or not subject_semtype or not object_semtype:
                    continue

                subject_name = label_map.get(subject_cui, subject_cui)
                object_name = label_map.get(object_cui, object_cui)
                reference_text = build_text(subject_name, predicate, object_name)

                rows.append(
                    {
                        "reference_predication_id": predication_id,
                        "reference_subject_cui": subject_cui,
                        "reference_predicate": predicate,
                        "reference_object_cui": object_cui,
                        "reference_subject_semtype": subject_semtype,
                        "reference_object_semtype": object_semtype,
                        "reference_text": reference_text,
                    }
                )

        print(f"Prior year {year}: collected {year_rows:,} usable reference rows.")
        gc.collect()

    if not found_years:
        raise FileNotFoundError(
            f"No prior-five-year files found for focal year {focal_year} in "
            f"{SPLIT_PREDICATION_DIR}."
        )

    print(f"Focal year {focal_year}: found prior years {found_years}.")
    if missing_years:
        print(f"Focal year {focal_year}: missing prior years {missing_years}.")

    references = pd.DataFrame(rows)
    if references.empty:
        raise ValueError(f"No valid reference triples found for focal year {focal_year}.")

    references = references.drop_duplicates(
        subset=[
            "reference_predicate",
            "reference_subject_semtype",
            "reference_object_semtype",
            "reference_text",
        ],
        keep="first",
    ).reset_index(drop=True)
    print(f"Unique reference texts: {len(references):,}.")
    return references


def build_reference_indexes(
    references: pd.DataFrame,
) -> tuple[
    defaultdict[tuple[str, str, str], list[int]],
    defaultdict[str, list[int]],
]:
    semtype_index: defaultdict[tuple[str, str, str], list[int]] = defaultdict(list)
    predicate_index: defaultdict[str, list[int]] = defaultdict(list)

    for index, row in enumerate(references.itertuples(index=False)):
        predicate = row.reference_predicate
        subject_semtype = row.reference_subject_semtype
        object_semtype = row.reference_object_semtype
        semtype_index[(predicate, subject_semtype, object_semtype)].append(index)
        predicate_index[predicate].append(index)

    print(f"Reference same-predicate-semtype groups: {len(semtype_index):,}.")
    print(f"Reference same-predicate groups: {len(predicate_index):,}.")
    return semtype_index, predicate_index


def max_similarity_to_references(
    query_matrix: np.ndarray,
    reference_matrix: np.ndarray,
    reference_indices: list[int],
) -> tuple[np.ndarray, np.ndarray]:
    group_matrix = reference_matrix[reference_indices]
    best_scores = np.full(query_matrix.shape[0], np.nan, dtype="float32")
    best_reference_positions = np.full(query_matrix.shape[0], -1, dtype=np.int64)

    for start in range(0, query_matrix.shape[0], SIMILARITY_QUERY_BLOCK_SIZE):
        end = min(start + SIMILARITY_QUERY_BLOCK_SIZE, query_matrix.shape[0])
        similarities = query_matrix[start:end] @ group_matrix.T
        local_best_positions = np.argmax(similarities, axis=1)
        best_scores[start:end] = similarities[
            np.arange(end - start),
            local_best_positions,
        ]
        best_reference_positions[start:end] = [
            reference_indices[position] for position in local_best_positions
        ]

    return best_scores, best_reference_positions


def order_scored_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()
    for column in SCORED_OUTPUT_COLUMNS:
        if column not in output.columns:
            output[column] = pd.NA
    remaining_columns = [
        column for column in output.columns if column not in SCORED_OUTPUT_COLUMNS
    ]
    return output[SCORED_OUTPUT_COLUMNS + remaining_columns]


def score_candidate_chunk(
    chunk: pd.DataFrame,
    tokenizer: AutoTokenizer,
    model: AutoModel,
    device: torch.device,
    references: pd.DataFrame,
    reference_matrix: np.ndarray,
    semtype_index: defaultdict[tuple[str, str, str], list[int]],
    predicate_index: defaultdict[str, list[int]],
) -> pd.DataFrame:
    chunk = chunk.dropna(
        subset=[
            "subject_cui",
            "predicate",
            "object_cui",
            "subject_semtype",
            "object_semtype",
            "candidate_text",
        ]
    ).copy()
    chunk["candidate_text"] = chunk["candidate_text"].astype("string").str.strip()
    chunk = chunk[chunk["candidate_text"] != ""].reset_index(drop=True)

    if chunk.empty:
        return order_scored_output_columns(chunk)

    query_matrix = encode_texts(
        chunk["candidate_text"].astype(str).tolist(),
        tokenizer,
        model,
        device,
    )
    scores = np.full(len(chunk), np.nan, dtype="float32")
    reference_scope = np.array(["no_reference"] * len(chunk), dtype=object)
    n_reference_triples = np.zeros(len(chunk), dtype=np.int64)
    best_reference_indices = np.full(len(chunk), -1, dtype=np.int64)

    primary_keys = [
        (
            normalize_value(predicate),
            normalize_value(subject_semtype),
            normalize_value(object_semtype),
        )
        for predicate, subject_semtype, object_semtype in zip(
            chunk["predicate"],
            chunk["subject_semtype"],
            chunk["object_semtype"],
        )
    ]
    predicate_keys = [normalize_value(predicate) for predicate in chunk["predicate"]]

    for key in sorted(set(primary_keys)):
        row_positions = [index for index, row_key in enumerate(primary_keys) if row_key == key]
        reference_indices = semtype_index.get(key, [])
        if not reference_indices:
            continue

        group_scores, group_refs = max_similarity_to_references(
            query_matrix[row_positions],
            reference_matrix,
            reference_indices,
        )
        scores[row_positions] = group_scores
        best_reference_indices[row_positions] = group_refs
        reference_scope[row_positions] = "same_predicate_same_semtype_pair"
        n_reference_triples[row_positions] = len(reference_indices)

    fallback_positions = [index for index, score in enumerate(scores) if np.isnan(score)]
    for predicate in sorted({predicate_keys[index] for index in fallback_positions}):
        row_positions = [
            index
            for index in fallback_positions
            if predicate_keys[index] == predicate
        ]
        reference_indices = predicate_index.get(predicate, [])
        if not reference_indices:
            continue

        group_scores, group_refs = max_similarity_to_references(
            query_matrix[row_positions],
            reference_matrix,
            reference_indices,
        )
        scores[row_positions] = group_scores
        best_reference_indices[row_positions] = group_refs
        reference_scope[row_positions] = "same_predicate_only"
        n_reference_triples[row_positions] = len(reference_indices)

    best_reference_texts = []
    best_reference_predication_ids = []
    for reference_index in best_reference_indices:
        if reference_index < 0:
            best_reference_texts.append(pd.NA)
            best_reference_predication_ids.append(pd.NA)
            continue

        reference_row = references.iloc[int(reference_index)]
        best_reference_texts.append(reference_row["reference_text"])
        best_reference_predication_ids.append(reference_row["reference_predication_id"])

    chunk["biomedbert_new_relation_score"] = scores
    chunk["reference_scope"] = reference_scope
    chunk["n_reference_triples"] = n_reference_triples
    chunk["max_similarity_reference_predication_id"] = best_reference_predication_ids
    chunk["max_similarity_reference_text"] = best_reference_texts
    return order_scored_output_columns(chunk)


def score_candidate_triples(focal_year: int) -> None:
    candidate_file = candidate_file_for_year(focal_year)
    output_file = output_file_for_year(focal_year)

    check_input(candidate_file)
    check_output(output_file)

    label_map = load_cui_labels(CUI_LABEL_FILE)
    references = read_reference_triples(focal_year, label_map)
    tokenizer, model, device = load_model()

    print("Encoding reference triple texts.")
    reference_matrix = encode_texts(
        references["reference_text"].astype(str).tolist(),
        tokenizer,
        model,
        device,
    )
    semtype_index, predicate_index = build_reference_indexes(references)

    total_rows = 0
    wrote_header = False
    reader = pd.read_csv(
        candidate_file,
        compression="gzip",
        chunksize=CANDIDATE_CHUNK_SIZE,
        dtype={
            "subject_cui": "string",
            "predicate": "string",
            "object_cui": "string",
            "subject_semtype": "string",
            "object_semtype": "string",
            "candidate_text": "string",
        },
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        scored = score_candidate_chunk(
            chunk,
            tokenizer,
            model,
            device,
            references,
            reference_matrix,
            semtype_index,
            predicate_index,
        )
        total_rows += len(scored)
        scored.to_csv(
            output_file,
            mode="a",
            index=False,
            compression="gzip",
            header=not wrote_header,
        )
        wrote_header = True
        print(f"Chunk {chunk_number:,}: scored {total_rows:,} candidate triples.")

    if not wrote_header:
        pd.DataFrame(columns=SCORED_OUTPUT_COLUMNS).to_csv(
            output_file,
            index=False,
            compression="gzip",
        )

    print(f"Saved BiomedBERT new-relation scores to {output_file}")
    print(f"Total candidate triples scored: {total_rows:,}")


def main() -> None:
    focal_year = get_focal_year()
    print(f"Starting BiomedBERT new-relation scoring for {focal_year}.")
    score_candidate_triples(focal_year)
    print(f"Finished BiomedBERT new-relation scoring for {focal_year}.")


if __name__ == "__main__":
    main()
