"""Build first-layer edge annotations for SemMedDB predications.

This script assigns each non-self-loop focal-year predication to one broad
annotation category:

1. New_Node_Combination
2. New_Combination
3. New_Relation
4. Repeated_Triple

Self-loop predications are counted before annotation and written to a yearly
diagnostic summary, but they are excluded from the annotated output used for
downstream analysis.
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
OUTPUT_DIR = INTERIM_DIR / "link_prediction/edge_annotation/first_layer"
SELF_LOOP_SUMMARY_DIR = OUTPUT_DIR / "self_loop_summary"

INPUT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
OUTPUT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
SELF_LOOP_SUMMARY_PREFIX = "semmedVER43_R_first_layer_self_loop_summary"

BASE_YEAR = 1980
N_YEARS = 40
PRIOR_WINDOW_YEARS = 5
CHUNK_SIZE = 100_000
OVERWRITE = False

SUBJECT_CUI_COLUMN = "subject_cui_primary"
OBJECT_CUI_COLUMN = "object_cui_primary"
PREDICATE_COLUMN = "PREDICATE"

CATEGORY_NEW_NODE = "New_Node_Combination"
CATEGORY_NEW_COMBINATION = "New_Combination"
CATEGORY_NEW_RELATION = "New_Relation"
CATEGORY_REPEATED_TRIPLE = "Repeated_Triple"


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


def predication_file_for_year(year: int) -> Path:
    return SPLIT_PREDICATION_DIR / f"{INPUT_FILE_PREFIX}_{year}.csv.gz"


def output_file_for_year(focal_year: int) -> Path:
    return (
        OUTPUT_DIR
        / f"{OUTPUT_FILE_PREFIX}_first_layer_edge_annotation_{focal_year}.csv.gz"
    )


def self_loop_summary_file_for_year(focal_year: int) -> Path:
    return SELF_LOOP_SUMMARY_DIR / f"{SELF_LOOP_SUMMARY_PREFIX}_{focal_year}.csv"


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


def prior_years_for_focal_year(focal_year: int) -> range:
    return range(focal_year - PRIOR_WINDOW_YEARS, focal_year)


def read_prior_window_network(
    focal_year: int,
) -> tuple[set[str], set[tuple[str, str]], set[tuple[str, str, str]]]:
    prior_nodes: set[str] = set()
    prior_combinations: set[tuple[str, str]] = set()
    prior_directed_relations: set[tuple[str, str, str]] = set()
    found_years = []
    missing_years = []

    for year in prior_years_for_focal_year(focal_year):
        input_file = predication_file_for_year(year)
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

                prior_nodes.add(subject_cui)
                prior_nodes.add(object_cui)
                prior_combinations.add(normalize_combination(subject_cui, object_cui))
                prior_directed_relations.add(
                    normalize_directed_relation(subject_cui, predicate, object_cui)
                )

        print(f"Prior year {year}: scanned {year_rows:,} predication rows.")

    if not found_years:
        raise FileNotFoundError(
            f"No prior-year files found for focal year {focal_year} in "
            f"{SPLIT_PREDICATION_DIR}."
        )

    print(f"Focal year {focal_year}: prior years found {found_years}.")
    if missing_years:
        print(f"Focal year {focal_year}: prior years missing {missing_years}.")
    print(f"Prior nodes: {len(prior_nodes):,}.")
    print(f"Prior CUI-CUI combinations: {len(prior_combinations):,}.")
    print(f"Prior directed semantic relations: {len(prior_directed_relations):,}.")

    return prior_nodes, prior_combinations, prior_directed_relations


def is_self_loop(subject_cui: object, object_cui: object) -> bool:
    subject_node = normalize_value(subject_cui)
    object_node = normalize_value(object_cui)
    return not subject_node or not object_node or subject_node == object_node


def count_self_loops(predication_file: Path) -> dict[str, object]:
    total_predications = 0
    self_loop_predications = 0

    reader = pd.read_csv(
        predication_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        usecols=[SUBJECT_CUI_COLUMN, OBJECT_CUI_COLUMN],
        dtype={SUBJECT_CUI_COLUMN: "string", OBJECT_CUI_COLUMN: "string"},
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        total_predications += len(chunk)
        chunk_self_loops = 0

        for subject_cui, object_cui in zip(
            chunk[SUBJECT_CUI_COLUMN], chunk[OBJECT_CUI_COLUMN]
        ):
            if is_self_loop(subject_cui, object_cui):
                chunk_self_loops += 1

        self_loop_predications += chunk_self_loops
        print(
            f"Self-loop pass chunk {chunk_number:,}: "
            f"read {len(chunk):,} rows; self loops {chunk_self_loops:,}."
        )

    non_self_loop_predications = total_predications - self_loop_predications
    self_loop_share = (
        self_loop_predications / total_predications if total_predications else pd.NA
    )

    return {
        "n_predications": total_predications,
        "n_self_loop_predications": self_loop_predications,
        "n_non_self_loop_predications": non_self_loop_predications,
        "self_loop_predication_share": self_loop_share,
    }


def write_self_loop_summary(focal_year: int, summary: dict[str, object]) -> None:
    output_file = self_loop_summary_file_for_year(focal_year)
    check_output(output_file)
    output = pd.DataFrame([{"pyear": focal_year, **summary}])
    output.to_csv(output_file, index=False)
    print(f"Saved self-loop summary to {output_file}")


def annotate_chunk(
    chunk: pd.DataFrame,
    prior_nodes: set[str],
    prior_combinations: set[tuple[str, str]],
    prior_directed_relations: set[tuple[str, str, str]],
) -> pd.DataFrame:
    non_self_loop = chunk[
        [
            not is_self_loop(subject_cui, object_cui)
            for subject_cui, object_cui in zip(
                chunk[SUBJECT_CUI_COLUMN], chunk[OBJECT_CUI_COLUMN]
            )
        ]
    ].copy()

    categories = []
    subject_seen_values = []
    object_seen_values = []
    has_node_absent_values = []
    combination_seen_values = []
    directed_relation_seen_values = []
    combination_node_a_values = []
    combination_node_b_values = []

    for row in non_self_loop.itertuples(index=False):
        subject_cui = normalize_value(getattr(row, SUBJECT_CUI_COLUMN))
        object_cui = normalize_value(getattr(row, OBJECT_CUI_COLUMN))
        predicate = normalize_value(getattr(row, PREDICATE_COLUMN))

        combination = normalize_combination(subject_cui, object_cui)
        directed_relation = normalize_directed_relation(subject_cui, predicate, object_cui)

        subject_seen = subject_cui in prior_nodes
        object_seen = object_cui in prior_nodes
        has_node_absent = not (subject_seen and object_seen)
        combination_seen = combination in prior_combinations
        directed_relation_seen = directed_relation in prior_directed_relations

        if has_node_absent:
            category = CATEGORY_NEW_NODE
        elif not combination_seen:
            category = CATEGORY_NEW_COMBINATION
        elif not directed_relation_seen:
            category = CATEGORY_NEW_RELATION
        else:
            category = CATEGORY_REPEATED_TRIPLE

        categories.append(category)
        subject_seen_values.append(subject_seen)
        object_seen_values.append(object_seen)
        has_node_absent_values.append(has_node_absent)
        combination_seen_values.append(combination_seen)
        directed_relation_seen_values.append(directed_relation_seen)
        combination_node_a_values.append(combination[0])
        combination_node_b_values.append(combination[1])

    non_self_loop["first_layer_edge_annotation"] = categories
    non_self_loop["concept_pair_node_a"] = combination_node_a_values
    non_self_loop["concept_pair_node_b"] = combination_node_b_values
    non_self_loop["subject_seen_in_prior_five_year_window"] = subject_seen_values
    non_self_loop["object_seen_in_prior_five_year_window"] = object_seen_values
    non_self_loop["has_node_absent_from_prior_five_year_window"] = (
        has_node_absent_values
    )
    non_self_loop["concept_pair_seen_in_prior_five_year_window"] = (
        combination_seen_values
    )
    non_self_loop["directed_semantic_relation_seen_in_prior_five_year_window"] = (
        directed_relation_seen_values
    )
    return non_self_loop


def build_first_layer_annotation(focal_year: int) -> None:
    predication_file = predication_file_for_year(focal_year)
    output_file = output_file_for_year(focal_year)

    check_input(predication_file)
    check_output(output_file)

    print(f"Counting self-loop predications for focal year {focal_year}.")
    self_loop_summary = count_self_loops(predication_file)
    write_self_loop_summary(focal_year, self_loop_summary)

    print(f"Building prior-five-year network for focal year {focal_year}.")
    prior_nodes, prior_combinations, prior_directed_relations = read_prior_window_network(
        focal_year
    )

    print(f"Annotating non-self-loop predications for focal year {focal_year}.")
    category_counts: Counter[str] = Counter()
    total_output_rows = 0

    reader = pd.read_csv(
        predication_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        dtype={
            SUBJECT_CUI_COLUMN: "string",
            OBJECT_CUI_COLUMN: "string",
            PREDICATE_COLUMN: "string",
        },
    )

    wrote_header = False
    for chunk_number, chunk in enumerate(reader, start=1):
        annotated_chunk = annotate_chunk(
            chunk,
            prior_nodes=prior_nodes,
            prior_combinations=prior_combinations,
            prior_directed_relations=prior_directed_relations,
        )

        if annotated_chunk.empty:
            print(f"Annotation chunk {chunk_number:,}: no non-self-loop rows.")
            continue

        chunk_counts = Counter(annotated_chunk["first_layer_edge_annotation"])
        category_counts.update(chunk_counts)
        total_output_rows += len(annotated_chunk)

        annotated_chunk.to_csv(
            output_file,
            mode="a",
            index=False,
            compression="gzip",
            header=not wrote_header,
        )
        wrote_header = True

        print(
            f"Annotation chunk {chunk_number:,}: wrote {len(annotated_chunk):,} rows; "
            f"category counts {dict(chunk_counts)}."
        )

    if not wrote_header:
        pd.DataFrame(columns=["first_layer_edge_annotation"]).to_csv(
            output_file, index=False, compression="gzip"
        )

    print(f"Saved first-layer edge annotation to {output_file}")
    print(f"Total non-self-loop rows written: {total_output_rows:,}")
    print(f"Final first-layer annotation counts: {dict(category_counts)}")


def main() -> None:
    focal_year = get_focal_year()
    build_first_layer_annotation(focal_year)


if __name__ == "__main__":
    main()
