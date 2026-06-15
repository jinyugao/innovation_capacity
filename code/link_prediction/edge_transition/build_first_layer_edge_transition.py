"""Build first-layer edge transitions for SemMedDB predications.

This script uses the first-layer broad annotation output and checks whether each
knowledge element appears again in the following five-year window.

Transition units depend on the first-layer annotation:

1. New_Node_Combination and New_Combination use undirected CUI-CUI combinations.
2. New_Relation and Repeated_Triple use directed semantic relations.

Self-loops should already be excluded from the first-layer annotation output. If
they are encountered here, they are kept as diagnostic rows and marked
Not_Analyzed.
"""

from __future__ import annotations

import os
from collections import Counter
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError


INTERIM_DIR = Path("/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim")
SPLIT_PREDICATION_DIR = (
    INTERIM_DIR / "semmedVER43_R/split_predications_with_pyear_filtered_by_pyear"
)
ANNOTATION_DIR = INTERIM_DIR / "link_prediction/edge_annotation/first_layer"
OUTPUT_DIR = INTERIM_DIR / "link_prediction/edge_transition/first_layer"

SPLIT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
ANNOTATION_FILE_PREFIX = (
    "semmedVER43_R_predications_with_pyear_filtered_first_layer_edge_annotation"
)
OUTPUT_FILE_PREFIX = (
    "semmedVER43_R_predications_with_pyear_filtered_first_layer_edge_transition"
)

BASE_YEAR = 1980
N_YEARS = 40
FUTURE_WINDOW_YEARS = 5
CHUNK_SIZE = 100_000
OVERWRITE = False

SUBJECT_CUI_COLUMN = "subject_cui_primary"
OBJECT_CUI_COLUMN = "object_cui_primary"
PREDICATE_COLUMN = "PREDICATE"
ANNOTATION_COLUMN = "first_layer_edge_annotation"

CATEGORY_NEW_NODE = "New_Node_Combination"
CATEGORY_NEW_COMBINATION = "New_Combination"
CATEGORY_NEW_RELATION = "New_Relation"
CATEGORY_REPEATED_TRIPLE = "Repeated_Triple"
CATEGORY_SELF_LOOP = "Self_Loop"

TRANSITION_ADOPTED = "Adopted"
TRANSITION_CONTINUED = "Continued"
TRANSITION_DISAPPEARED = "Disappeared"
TRANSITION_NOT_ANALYZED = "Not_Analyzed"


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


def normalize_value(value: object) -> str:
    return "" if pd.isna(value) else str(value).strip()


def normalize_combination(node_a: object, node_b: object) -> tuple[str, str]:
    node_a_text = normalize_value(node_a)
    node_b_text = normalize_value(node_b)
    return tuple(sorted((node_a_text, node_b_text)))


def normalize_directed_relation(
    subject_cui: object,
    predicate: object,
    object_cui: object,
) -> tuple[str, str, str]:
    return (
        normalize_value(subject_cui),
        normalize_value(predicate),
        normalize_value(object_cui),
    )


def is_self_loop(subject_cui: object, object_cui: object) -> bool:
    subject_node = normalize_value(subject_cui)
    object_node = normalize_value(object_cui)
    return not subject_node or not object_node or subject_node == object_node


def split_file_for_year(year: int) -> Path:
    return SPLIT_PREDICATION_DIR / f"{SPLIT_FILE_PREFIX}_{year}.csv.gz"


def annotation_file_for_year(focal_year: int) -> Path:
    return ANNOTATION_DIR / f"{ANNOTATION_FILE_PREFIX}_{focal_year}.csv.gz"


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


def future_years_for_focal_year(focal_year: int) -> range:
    return range(focal_year + 1, focal_year + FUTURE_WINDOW_YEARS + 1)


def read_future_window_sets(
    focal_year: int,
) -> tuple[set[tuple[str, str]], set[tuple[str, str, str]]]:
    future_combinations: set[tuple[str, str]] = set()
    future_directed_relations: set[tuple[str, str, str]] = set()
    found_years = []
    missing_years = []

    for year in future_years_for_focal_year(focal_year):
        input_file = split_file_for_year(year)
        if not input_file.exists():
            missing_years.append(year)
            continue

        found_years.append(year)
        try:
            reader = pd.read_csv(
                input_file,
                compression="gzip",
                chunksize=CHUNK_SIZE,
                usecols=[SUBJECT_CUI_COLUMN, OBJECT_CUI_COLUMN, PREDICATE_COLUMN],
                dtype={
                    SUBJECT_CUI_COLUMN: "string",
                    OBJECT_CUI_COLUMN: "string",
                    PREDICATE_COLUMN: "string",
                },
            )
        except EmptyDataError:
            continue

        year_rows = 0
        for chunk in reader:
            year_rows += len(chunk)
            for row in chunk.itertuples(index=False):
                subject_cui = normalize_value(getattr(row, SUBJECT_CUI_COLUMN))
                object_cui = normalize_value(getattr(row, OBJECT_CUI_COLUMN))
                predicate = normalize_value(getattr(row, PREDICATE_COLUMN))

                if not subject_cui or not object_cui or subject_cui == object_cui:
                    continue
                if not predicate:
                    continue

                future_combinations.add(normalize_combination(subject_cui, object_cui))
                future_directed_relations.add(
                    normalize_directed_relation(subject_cui, predicate, object_cui)
                )

        print(f"Future year {year}: scanned {year_rows:,} predication rows.")

    if not found_years:
        raise FileNotFoundError(
            f"No future-year files found for focal year {focal_year} in "
            f"{SPLIT_PREDICATION_DIR}."
        )

    print(f"Focal year {focal_year}: future years found {found_years}.")
    if missing_years:
        print(f"Focal year {focal_year}: future years missing {missing_years}.")
    print(f"Future CUI-CUI combinations: {len(future_combinations):,}.")
    print(f"Future directed semantic relations: {len(future_directed_relations):,}.")

    return future_combinations, future_directed_relations


def classify_transition(
    category: str,
    subject_cui: object,
    predicate: object,
    object_cui: object,
    future_combinations: set[tuple[str, str]],
    future_directed_relations: set[tuple[str, str, str]],
) -> tuple[str, str, bool]:
    if is_self_loop(subject_cui, object_cui) or category == CATEGORY_SELF_LOOP:
        return "self_loop", TRANSITION_NOT_ANALYZED, False

    if category in {CATEGORY_NEW_NODE, CATEGORY_NEW_COMBINATION}:
        comparison_unit = "undirected_cui_cui_combination"
        appears_in_future = (
            normalize_combination(subject_cui, object_cui) in future_combinations
        )
        transition = TRANSITION_ADOPTED if appears_in_future else TRANSITION_DISAPPEARED
        return comparison_unit, transition, appears_in_future

    if category in {CATEGORY_NEW_RELATION, CATEGORY_REPEATED_TRIPLE}:
        comparison_unit = "directed_semantic_relation"
        appears_in_future = (
            normalize_directed_relation(subject_cui, predicate, object_cui)
            in future_directed_relations
        )

        if category == CATEGORY_REPEATED_TRIPLE and appears_in_future:
            transition = TRANSITION_CONTINUED
        elif appears_in_future:
            transition = TRANSITION_ADOPTED
        else:
            transition = TRANSITION_DISAPPEARED
        return comparison_unit, transition, appears_in_future

    return "unknown", TRANSITION_NOT_ANALYZED, False


def annotate_transition_chunk(
    chunk: pd.DataFrame,
    future_combinations: set[tuple[str, str]],
    future_directed_relations: set[tuple[str, str, str]],
) -> pd.DataFrame:
    annotated = chunk.copy()
    comparison_units = []
    appears_in_future_values = []
    transitions = []
    annotation_to_transition_values = []

    for row in annotated.itertuples(index=False):
        category = normalize_value(getattr(row, ANNOTATION_COLUMN))
        comparison_unit, transition, appears_in_future = classify_transition(
            category=category,
            subject_cui=getattr(row, SUBJECT_CUI_COLUMN),
            predicate=getattr(row, PREDICATE_COLUMN),
            object_cui=getattr(row, OBJECT_CUI_COLUMN),
            future_combinations=future_combinations,
            future_directed_relations=future_directed_relations,
        )

        comparison_units.append(comparison_unit)
        appears_in_future_values.append(appears_in_future)
        transitions.append(transition)
        annotation_to_transition_values.append(f"{category} -> {transition}")

    annotated["first_layer_transition_comparison_unit"] = comparison_units
    annotated["appears_in_future_five_year_window"] = appears_in_future_values
    annotated["first_layer_future_five_year_transition"] = transitions
    annotated["first_layer_annotation_to_transition"] = annotation_to_transition_values
    return annotated


def build_first_layer_edge_transition(focal_year: int) -> None:
    annotation_file = annotation_file_for_year(focal_year)
    output_file = output_file_for_year(focal_year)

    check_input(annotation_file)
    check_output(output_file)

    print(f"Building future-window sets for focal year {focal_year}.")
    future_combinations, future_directed_relations = read_future_window_sets(focal_year)

    print(f"Building first-layer edge transitions for focal year {focal_year}.")
    transition_counts: Counter[str] = Counter()
    total_rows = 0
    wrote_header = False

    reader = pd.read_csv(
        annotation_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        dtype={
            SUBJECT_CUI_COLUMN: "string",
            OBJECT_CUI_COLUMN: "string",
            PREDICATE_COLUMN: "string",
            ANNOTATION_COLUMN: "string",
        },
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        transition_chunk = annotate_transition_chunk(
            chunk,
            future_combinations=future_combinations,
            future_directed_relations=future_directed_relations,
        )

        chunk_counts = Counter(transition_chunk["first_layer_annotation_to_transition"])
        transition_counts.update(chunk_counts)
        total_rows += len(transition_chunk)

        transition_chunk.to_csv(
            output_file,
            mode="a",
            index=False,
            compression="gzip",
            header=not wrote_header,
        )
        wrote_header = True

        print(
            f"Chunk {chunk_number:,}: wrote {len(transition_chunk):,} rows; "
            f"transition counts {dict(chunk_counts)}."
        )

    if not wrote_header:
        pd.DataFrame(columns=["first_layer_annotation_to_transition"]).to_csv(
            output_file, index=False, compression="gzip"
        )

    print(f"Saved first-layer edge transition data to {output_file}")
    print(f"Total rows written: {total_rows:,}")
    print(f"Final transition counts: {dict(transition_counts)}")


def main() -> None:
    focal_year = get_focal_year()
    build_first_layer_edge_transition(focal_year)


if __name__ == "__main__":
    main()
