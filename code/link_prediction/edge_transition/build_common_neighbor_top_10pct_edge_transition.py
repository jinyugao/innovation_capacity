"""Build common-neighbor edge-transition labels using future five-year edges.

The focal-year CUI-CUI edge is treated as undirected. For each annotated
focal-year predication, this script checks whether the same undirected edge
appears in the following five years and labels the transition from the focal-year
annotation to its future status.
"""

from __future__ import annotations

import os
from collections import Counter
from pathlib import Path

import pandas as pd


INTERIM_DIR = Path("/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim")
ANNOTATED_PREDICATION_DIR = (
    INTERIM_DIR / "link_prediction/annotated_predications/common_neighbor/10pct"
)
SPLIT_PREDICATION_DIR = (
    INTERIM_DIR / "semmedVER43_R/split_predications_with_pyear_filtered_by_pyear"
)
OUTPUT_DIR = INTERIM_DIR / "link_prediction/edge_transition/common_neighbor/10pct"

ANNOTATED_FILE_PREFIX = (
    "semmedVER43_R_predications_with_pyear_filtered_common_neighbor_top_10pct_annotated"
)
SPLIT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
OUTPUT_FILE_PREFIX = (
    "semmedVER43_R_predications_with_pyear_filtered_common_neighbor_top_10pct_"
    "edge_transition"
)

BASE_YEAR = 1980
FUTURE_WINDOW_YEARS = 5
CHUNK_SIZE = 100_000
OVERWRITE = False

SUBJECT_CUI_COLUMN = "subject_cui_primary"
OBJECT_CUI_COLUMN = "object_cui_primary"


def get_focal_year() -> int:
    task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if task_id is None:
        raise RuntimeError("SLURM_ARRAY_TASK_ID not found.")
    return int(task_id) + BASE_YEAR


def normalize_edge(node_a: object, node_b: object) -> tuple[str, str]:
    node_a_text = "" if pd.isna(node_a) else str(node_a).strip()
    node_b_text = "" if pd.isna(node_b) else str(node_b).strip()
    return tuple(sorted((node_a_text, node_b_text)))


def split_file_for_year(year: int) -> Path:
    return SPLIT_PREDICATION_DIR / f"{SPLIT_FILE_PREFIX}_{year}.csv.gz"


def check_input(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input file: {path}")


def check_output(output_file: Path, overwrite: bool) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    if output_file.exists() and not overwrite:
        raise FileExistsError(
            "Output file already exists. Set OVERWRITE = True to replace it:\n"
            f"{output_file}"
        )
    if output_file.exists() and overwrite:
        output_file.unlink()


def build_future_edge_set(focal_year: int) -> set[tuple[str, str]]:
    future_years = range(focal_year + 1, focal_year + FUTURE_WINDOW_YEARS + 1)
    future_edges: set[tuple[str, str]] = set()
    found_years = []
    missing_years = []

    for year in future_years:
        input_file = split_file_for_year(year)

        if not input_file.exists():
            missing_years.append(year)
            continue

        found_years.append(year)
        reader = pd.read_csv(
            input_file,
            compression="gzip",
            chunksize=CHUNK_SIZE,
            usecols=[SUBJECT_CUI_COLUMN, OBJECT_CUI_COLUMN],
            dtype={SUBJECT_CUI_COLUMN: "string", OBJECT_CUI_COLUMN: "string"},
        )

        year_rows = 0
        for chunk in reader:
            chunk = chunk.dropna(subset=[SUBJECT_CUI_COLUMN, OBJECT_CUI_COLUMN])
            year_rows += len(chunk)

            for subject_cui, object_cui in zip(
                chunk[SUBJECT_CUI_COLUMN], chunk[OBJECT_CUI_COLUMN]
            ):
                edge = normalize_edge(subject_cui, object_cui)
                node_a, node_b = edge

                if not node_a or not node_b:
                    continue

                future_edges.add(edge)

        print(f"Future year {year}: loaded {year_rows:,} non-missing CUI rows.")

    if not found_years:
        raise FileNotFoundError(
            f"No future files found for focal year {focal_year} in "
            f"{SPLIT_PREDICATION_DIR}."
        )

    print(f"Focal year {focal_year}: found future years {found_years}.")
    if missing_years:
        print(f"Focal year {focal_year}: missing future years {missing_years}.")
    print(f"Future edge set size: {len(future_edges):,}.")

    return future_edges


def classify_transition(
    chunk: pd.DataFrame,
    future_edges: set[tuple[str, str]],
) -> pd.DataFrame:
    annotated = chunk.copy()
    transition_statuses = []
    transitions = []

    for subject_cui, object_cui, category in zip(
        annotated[SUBJECT_CUI_COLUMN],
        annotated[OBJECT_CUI_COLUMN],
        annotated["category"],
    ):
        edge = normalize_edge(subject_cui, object_cui)
        node_a, node_b = edge

        if not node_a or not node_b or node_a == node_b or category == "Self_Loop":
            transition_statuses.append("Not_Analyzed")
            transitions.append(f"{category} -> Not_Analyzed")
            continue

        appears_in_future_five_year_window = edge in future_edges
        if category == "Repeated_Combination" and appears_in_future_five_year_window:
            transition_status = "Continued"
        elif appears_in_future_five_year_window:
            transition_status = "Adopted"
        else:
            transition_status = "Disappeared"

        transition_statuses.append(transition_status)
        transitions.append(f"{category} -> {transition_status}")

    annotated["future_five_year_transition"] = transition_statuses
    annotated["focal_annotation_to_future_transition"] = transitions
    return annotated


def build_common_neighbor_edge_transition(focal_year: int) -> None:
    annotated_file = (
        ANNOTATED_PREDICATION_DIR / f"{ANNOTATED_FILE_PREFIX}_{focal_year}.csv.gz"
    )
    output_file = OUTPUT_DIR / f"{OUTPUT_FILE_PREFIX}_{focal_year}.csv.gz"

    check_input(annotated_file)
    check_output(output_file, OVERWRITE)

    print(f"Building edge transitions for focal year {focal_year}.")
    future_edges = build_future_edge_set(focal_year)

    category_counts: Counter[str] = Counter()
    total_rows = 0

    reader = pd.read_csv(
        annotated_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        dtype={
            SUBJECT_CUI_COLUMN: "string",
            OBJECT_CUI_COLUMN: "string",
            "category": "string",
        },
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        transition_chunk = classify_transition(chunk, future_edges)
        chunk_counts = Counter(transition_chunk["focal_annotation_to_future_transition"])
        category_counts.update(chunk_counts)
        total_rows += len(transition_chunk)

        transition_chunk.to_csv(
            output_file,
            mode="a",
            index=False,
            compression="gzip",
            header=(chunk_number == 1),
        )

        print(
            f"Chunk {chunk_number:,}: labeled {len(transition_chunk):,} rows; "
            f"transition counts {dict(chunk_counts)}."
        )

    print(f"Saved edge-transition data to {output_file}")
    print(f"Total rows labeled: {total_rows:,}")
    print(f"Final edge-transition counts: {dict(category_counts)}")


def main() -> None:
    focal_year = get_focal_year()
    build_common_neighbor_edge_transition(focal_year)


if __name__ == "__main__":
    main()
