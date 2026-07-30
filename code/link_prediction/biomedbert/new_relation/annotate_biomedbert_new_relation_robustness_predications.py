"""Annotate first-layer predications for BiomedBERT new-relation robustness."""

from __future__ import annotations

import os
from collections import Counter
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError


INTERIM_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim"
)
FIRST_LAYER_ANNOTATION_DIR = (
    INTERIM_DIR / "link_prediction/edge_annotation/first_layer"
)
PREDICTED_TRIPLE_ROOT = (
    INTERIM_DIR / "link_prediction/predicted_triples/biomedbert/new_relation/robustness"
)
OUTPUT_ROOT = (
    INTERIM_DIR
    / "link_prediction/annotated_predications/biomedbert/new_relation/robustness"
)

FIRST_LAYER_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
PREDICTED_TRIPLE_FILE_PREFIX = (
    "biomedbert_new_relation_robustness_predicted_triples_top"
)
OUTPUT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"

BASE_YEAR = 1980
N_YEARS = 40
ROBUSTNESS_PERCENTILES = [1, 5, 20]
CHUNK_SIZE = 100_000
OVERWRITE = False

SUBJECT_CUI_COLUMN = "subject_cui_primary"
OBJECT_CUI_COLUMN = "object_cui_primary"
PREDICATE_COLUMN = "PREDICATE"
FIRST_LAYER_ANNOTATION_COLUMN = "first_layer_edge_annotation"
OUTPUT_ANNOTATION_COLUMN = "biomedbert_new_relation_robustness_annotation"

CATEGORY_NEW_RELATION = "New_Relation"
CATEGORY_EXPECTED_NEW_RELATION = "Expected_New_Relation"
CATEGORY_SURPRISED_NEW_RELATION = "Surprised_New_Relation"

MINIMUM_INPUT_COLUMNS = [
    SUBJECT_CUI_COLUMN,
    PREDICATE_COLUMN,
    OBJECT_CUI_COLUMN,
    FIRST_LAYER_ANNOTATION_COLUMN,
]
ADDED_OUTPUT_COLUMNS = [
    OUTPUT_ANNOTATION_COLUMN,
    "biomedbert_new_relation_robustness_method",
    "biomedbert_new_relation_robustness_top_percentile",
    "biomedbert_new_relation_robustness_predicted",
    "biomedbert_new_relation_robustness_score",
    "biomedbert_new_relation_robustness_rank_within_method_year",
]


def get_focal_year() -> int:
    task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if task_id is None:
        raise RuntimeError("SLURM_ARRAY_TASK_ID not found.")

    task_index = int(task_id)
    if task_index < 0 or task_index >= N_YEARS:
        raise ValueError(
            f"SLURM_ARRAY_TASK_ID={task_index} is out of range. "
            f"Expected 0-{N_YEARS - 1}."
        )
    return BASE_YEAR + task_index


def percentile_label(percentile: int) -> str:
    return f"{percentile}pct"


def normalize_value(value: object) -> str:
    return "" if pd.isna(value) else str(value).strip()


def normalize_triple(
    subject_cui: object,
    predicate: object,
    object_cui: object,
) -> tuple[str, str, str]:
    return (
        normalize_value(subject_cui),
        normalize_value(predicate),
        normalize_value(object_cui),
    )


def first_layer_annotation_file_for_year(focal_year: int) -> Path:
    return (
        FIRST_LAYER_ANNOTATION_DIR
        / f"{FIRST_LAYER_FILE_PREFIX}_first_layer_edge_annotation_{focal_year}.csv.gz"
    )


def predicted_triple_file_for_year(focal_year: int, percentile: int) -> Path:
    label = percentile_label(percentile)
    return (
        PREDICTED_TRIPLE_ROOT
        / label
        / f"{PREDICTED_TRIPLE_FILE_PREFIX}_{label}_{focal_year}.csv.gz"
    )


def output_file_for_year(focal_year: int, percentile: int) -> Path:
    label = percentile_label(percentile)
    return (
        OUTPUT_ROOT
        / label
        / (
            f"{OUTPUT_FILE_PREFIX}_biomedbert_new_relation_robustness_top_"
            f"{label}_annotated_{focal_year}.csv.gz"
        )
    )


def check_inputs(paths: list[Path]) -> None:
    missing_files = [str(path) for path in paths if not path.exists()]
    if missing_files:
        missing = "\n".join(missing_files)
        raise FileNotFoundError(f"Missing required input file(s):\n{missing}")


def check_output(output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    if output_file.exists() and not OVERWRITE:
        raise FileExistsError(
            "Output file already exists. Set OVERWRITE = True to replace it:\n"
            f"{output_file}"
        )
    if output_file.exists() and OVERWRITE:
        output_file.unlink()


def output_columns_for_empty_file(first_layer_file: Path) -> list[str]:
    try:
        input_columns = pd.read_csv(
            first_layer_file,
            compression="gzip",
            nrows=0,
        ).columns.tolist()
    except EmptyDataError:
        input_columns = MINIMUM_INPUT_COLUMNS.copy()

    return input_columns + [
        column for column in ADDED_OUTPUT_COLUMNS if column not in input_columns
    ]


def read_predicted_triples(path: Path) -> dict[tuple[str, str, str], tuple[float, int]]:
    try:
        predicted_triples = pd.read_csv(
            path,
            compression="gzip",
            usecols=[
                "subject_cui",
                "predicate",
                "object_cui",
                "score",
                "rank_within_method_year",
            ],
            dtype={
                "subject_cui": "string",
                "predicate": "string",
                "object_cui": "string",
            },
        )
    except EmptyDataError:
        return {}

    predicted_triples = predicted_triples.dropna(
        subset=[
            "subject_cui",
            "predicate",
            "object_cui",
            "score",
            "rank_within_method_year",
        ]
    )
    predicted_triples["score"] = pd.to_numeric(
        predicted_triples["score"], errors="coerce"
    )
    predicted_triples["rank_within_method_year"] = pd.to_numeric(
        predicted_triples["rank_within_method_year"], errors="coerce"
    )
    predicted_triples = predicted_triples.dropna(
        subset=["score", "rank_within_method_year"]
    )

    triple_map: dict[tuple[str, str, str], tuple[float, int]] = {}
    for row in predicted_triples.itertuples(index=False):
        triple = normalize_triple(row.subject_cui, row.predicate, row.object_cui)
        if all(triple):
            triple_map[triple] = (
                float(row.score),
                int(row.rank_within_method_year),
            )

    return triple_map


def annotate_chunk(
    chunk: pd.DataFrame,
    predicted_triples: dict[tuple[str, str, str], tuple[float, int]],
    percentile: int,
) -> pd.DataFrame:
    annotated = chunk.copy()
    categories = []
    prediction_scores = []
    prediction_ranks = []
    is_predicted_values = []

    for row in annotated.itertuples(index=False):
        first_layer_annotation = getattr(row, FIRST_LAYER_ANNOTATION_COLUMN)

        if first_layer_annotation != CATEGORY_NEW_RELATION:
            categories.append(first_layer_annotation)
            prediction_scores.append(pd.NA)
            prediction_ranks.append(pd.NA)
            is_predicted_values.append(False)
            continue

        triple = normalize_triple(
            getattr(row, SUBJECT_CUI_COLUMN),
            getattr(row, PREDICATE_COLUMN),
            getattr(row, OBJECT_CUI_COLUMN),
        )

        if triple in predicted_triples:
            score, rank = predicted_triples[triple]
            categories.append(CATEGORY_EXPECTED_NEW_RELATION)
            prediction_scores.append(score)
            prediction_ranks.append(rank)
            is_predicted_values.append(True)
        else:
            categories.append(CATEGORY_SURPRISED_NEW_RELATION)
            prediction_scores.append(pd.NA)
            prediction_ranks.append(pd.NA)
            is_predicted_values.append(False)

    annotated[OUTPUT_ANNOTATION_COLUMN] = categories
    annotated["biomedbert_new_relation_robustness_method"] = "biomedbert"
    annotated["biomedbert_new_relation_robustness_top_percentile"] = percentile
    annotated["biomedbert_new_relation_robustness_predicted"] = is_predicted_values
    annotated["biomedbert_new_relation_robustness_score"] = prediction_scores
    annotated[
        "biomedbert_new_relation_robustness_rank_within_method_year"
    ] = prediction_ranks
    return annotated


def annotate_focal_year_percentile(focal_year: int, percentile: int) -> None:
    first_layer_file = first_layer_annotation_file_for_year(focal_year)
    predicted_triple_file = predicted_triple_file_for_year(focal_year, percentile)
    output_file = output_file_for_year(focal_year, percentile)

    check_inputs([first_layer_file, predicted_triple_file])
    check_output(output_file)

    print(
        "Annotating BiomedBERT new-relation robustness predictions for "
        f"{focal_year}; top_percentile={percentile}."
    )
    predicted_triples = read_predicted_triples(predicted_triple_file)
    print(f"Loaded {len(predicted_triples):,} predicted triple(s).")

    category_counts: Counter[str] = Counter()
    total_rows = 0
    wrote_header = False

    reader = pd.read_csv(
        first_layer_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        dtype={
            SUBJECT_CUI_COLUMN: "string",
            PREDICATE_COLUMN: "string",
            OBJECT_CUI_COLUMN: "string",
            FIRST_LAYER_ANNOTATION_COLUMN: "string",
        },
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        annotated_chunk = annotate_chunk(chunk, predicted_triples, percentile)
        chunk_counts = Counter(annotated_chunk[OUTPUT_ANNOTATION_COLUMN])
        category_counts.update(chunk_counts)
        total_rows += len(annotated_chunk)

        annotated_chunk.to_csv(
            output_file,
            mode="a",
            index=False,
            compression="gzip",
            header=not wrote_header,
        )
        wrote_header = True

        print(
            f"Chunk {chunk_number:,}: annotated {len(annotated_chunk):,} rows; "
            f"category counts {dict(chunk_counts)}."
        )

    if not wrote_header:
        pd.DataFrame(columns=output_columns_for_empty_file(first_layer_file)).to_csv(
            output_file,
            index=False,
            compression="gzip",
        )

    print(f"Saved BiomedBERT new-relation robustness annotation to {output_file}")
    print(f"Total rows annotated: {total_rows:,}")
    print(f"Final category counts: {dict(category_counts)}")


def main() -> None:
    focal_year = get_focal_year()
    for percentile in ROBUSTNESS_PERCENTILES:
        annotate_focal_year_percentile(focal_year, percentile)


if __name__ == "__main__":
    main()
