"""Annotate focal-year SemMedDB predications against two-hop candidate edges."""

from __future__ import annotations

import os
from collections import Counter
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError


INTERIM_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim"
)
PRIOR_FIVE_YEAR_EDGE_DIR = (
    INTERIM_DIR / "link_prediction/candidate_edges/prior_five_year_edges"
)
TWO_HOP_CANDIDATE_EDGE_DIR = (
    INTERIM_DIR / "link_prediction/candidate_edges/two_hop_candidate_edges"
)
SPLIT_PREDICATION_DIR = (
    INTERIM_DIR
    / "semmedVER43_R/split_predications_with_pyear_filtered_by_pyear"
)
OUTPUT_DIR = INTERIM_DIR / "link_prediction/candidate_edges/annotated_predications"

INPUT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
OUTPUT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"

BASE_YEAR = 1980
N_YEARS = 40
CHUNK_SIZE = 100_000
OVERWRITE = False

SUBJECT_CUI_COLUMN = "subject_cui_primary"
OBJECT_CUI_COLUMN = "object_cui_primary"


CATEGORY_SELF_LOOP = "Self_Loop"
CATEGORY_NEW_NODE = "New_Node_Combination"
CATEGORY_REPEATED = "Repeated_Combination"
CATEGORY_IN_CANDIDATE = "Two_Hop_Candidate_New_Combination"
CATEGORY_OUTSIDE_CANDIDATE = "Outside_Two_Hop_Candidate_New_Combination"


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


def normalize_node(node: object) -> str:
    return "" if pd.isna(node) else str(node).strip()


def normalize_edge(node_a: object, node_b: object) -> tuple[str, str]:
    node_a_text = normalize_node(node_a)
    node_b_text = normalize_node(node_b)
    return tuple(sorted((node_a_text, node_b_text)))


def prior_five_year_edge_file_for_year(focal_year: int) -> Path:
    return PRIOR_FIVE_YEAR_EDGE_DIR / f"prior_five_year_edges_{focal_year}.csv.gz"


def two_hop_candidate_edge_file_for_year(focal_year: int) -> Path:
    return (
        TWO_HOP_CANDIDATE_EDGE_DIR
        / f"two_hop_candidate_edges_prior_5y_{focal_year}.csv.gz"
    )


def predication_file_for_year(focal_year: int) -> Path:
    return SPLIT_PREDICATION_DIR / f"{INPUT_FILE_PREFIX}_{focal_year}.csv.gz"


def output_file_for_year(focal_year: int) -> Path:
    return (
        OUTPUT_DIR
        / f"{OUTPUT_FILE_PREFIX}_two_hop_candidate_edges_annotated_{focal_year}.csv.gz"
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


def read_prior_five_year_edges_and_nodes(
    path: Path,
) -> tuple[set[tuple[str, str]], set[str]]:
    try:
        prior_five_year_edges = pd.read_csv(
            path,
            compression="gzip",
            usecols=["node_a", "node_b"],
            dtype={"node_a": "string", "node_b": "string"},
        )
    except EmptyDataError:
        return set(), set()

    prior_five_year_edges = prior_five_year_edges.dropna(subset=["node_a", "node_b"])
    edge_set = {
        normalize_edge(node_a, node_b)
        for node_a, node_b in zip(
            prior_five_year_edges["node_a"],
            prior_five_year_edges["node_b"],
        )
    }
    node_set = set(prior_five_year_edges["node_a"].astype("string").str.strip())
    node_set.update(prior_five_year_edges["node_b"].astype("string").str.strip())
    node_set.discard("")
    return edge_set, node_set


def classify_edges(
    chunk: pd.DataFrame,
    prior_five_year_edges: set[tuple[str, str]],
    prior_five_year_nodes: set[str],
    two_hop_candidate_new_combination_edges: set[tuple[str, str]],
) -> pd.DataFrame:
    annotated = chunk.copy()
    categories = []
    subject_seen_in_prior_five_year_window = []
    object_seen_in_prior_five_year_window = []
    has_node_absent_from_prior_five_year_window = []
    in_two_hop_candidate_edge_set = []

    for subject_cui, object_cui in zip(
        annotated[SUBJECT_CUI_COLUMN], annotated[OBJECT_CUI_COLUMN]
    ):
        subject_node = normalize_node(subject_cui)
        object_node = normalize_node(object_cui)
        edge = normalize_edge(subject_node, object_node)
        subject_seen = subject_node in prior_five_year_nodes
        object_seen = object_node in prior_five_year_nodes
        has_node_absent = not (subject_seen and object_seen)
        in_candidate_set = edge in two_hop_candidate_new_combination_edges

        if not subject_node or not object_node or subject_node == object_node:
            categories.append(CATEGORY_SELF_LOOP)
            in_candidate_set = False
        elif has_node_absent:
            categories.append(CATEGORY_NEW_NODE)
            in_candidate_set = False
        elif edge in prior_five_year_edges:
            categories.append(CATEGORY_REPEATED)
            in_candidate_set = False
        elif in_candidate_set:
            categories.append(CATEGORY_IN_CANDIDATE)
        else:
            categories.append(CATEGORY_OUTSIDE_CANDIDATE)

        subject_seen_in_prior_five_year_window.append(subject_seen)
        object_seen_in_prior_five_year_window.append(object_seen)
        has_node_absent_from_prior_five_year_window.append(has_node_absent)
        in_two_hop_candidate_edge_set.append(in_candidate_set)

    annotated["candidate_edge_category"] = categories
    annotated["subject_seen_in_prior_five_year_window"] = (
        subject_seen_in_prior_five_year_window
    )
    annotated["object_seen_in_prior_five_year_window"] = (
        object_seen_in_prior_five_year_window
    )
    annotated["has_node_absent_from_prior_five_year_window"] = (
        has_node_absent_from_prior_five_year_window
    )
    annotated["in_two_hop_candidate_edge_set"] = in_two_hop_candidate_edge_set
    return annotated


def find_actual_new_combination_edges(
    predication_file: Path,
    prior_five_year_edges: set[tuple[str, str]],
    prior_five_year_nodes: set[str],
) -> set[tuple[str, str]]:
    actual_new_edges: set[tuple[str, str]] = set()

    reader = pd.read_csv(
        predication_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        usecols=[SUBJECT_CUI_COLUMN, OBJECT_CUI_COLUMN],
        dtype={SUBJECT_CUI_COLUMN: "string", OBJECT_CUI_COLUMN: "string"},
    )

    for chunk in reader:
        for subject_cui, object_cui in zip(
            chunk[SUBJECT_CUI_COLUMN], chunk[OBJECT_CUI_COLUMN]
        ):
            subject_node = normalize_node(subject_cui)
            object_node = normalize_node(object_cui)
            edge = normalize_edge(subject_node, object_node)

            if not subject_node or not object_node or subject_node == object_node:
                continue
            if subject_node not in prior_five_year_nodes:
                continue
            if object_node not in prior_five_year_nodes:
                continue
            if edge in prior_five_year_edges:
                continue

            actual_new_edges.add(edge)

    return actual_new_edges


def read_candidate_hits_for_actual_new_edges(
    path: Path,
    actual_new_edges: set[tuple[str, str]],
) -> set[tuple[str, str]]:
    candidate_hits: set[tuple[str, str]] = set()

    try:
        reader = pd.read_csv(
            path,
            compression="gzip",
            chunksize=CHUNK_SIZE,
            usecols=["node_a", "node_b"],
            dtype={"node_a": "string", "node_b": "string"},
        )
    except EmptyDataError:
        return candidate_hits

    for chunk_number, chunk in enumerate(reader, start=1):
        chunk = chunk.dropna(subset=["node_a", "node_b"])
        for node_a, node_b in zip(chunk["node_a"], chunk["node_b"]):
            edge = normalize_edge(node_a, node_b)
            if edge in actual_new_edges:
                candidate_hits.add(edge)

        print(
            f"Candidate chunk {chunk_number:,}: "
            f"current actual-new-edge hits {len(candidate_hits):,}."
        )

    return candidate_hits


def annotate_focal_year(focal_year: int) -> None:
    prior_five_year_edges_file = prior_five_year_edge_file_for_year(focal_year)
    two_hop_candidate_edges_file = two_hop_candidate_edge_file_for_year(focal_year)
    predication_file = predication_file_for_year(focal_year)
    output_file = output_file_for_year(focal_year)

    check_inputs(
        [prior_five_year_edges_file, two_hop_candidate_edges_file, predication_file]
    )
    check_output(output_file)

    print(f"Annotating focal year {focal_year} against two-hop candidate edges.")
    print(f"Reading prior-five-year edges from {prior_five_year_edges_file}")
    prior_five_year_edges, prior_five_year_nodes = (
        read_prior_five_year_edges_and_nodes(prior_five_year_edges_file)
    )
    print(f"Loaded {len(prior_five_year_edges):,} prior-five-year edge(s).")
    print(f"Loaded {len(prior_five_year_nodes):,} prior-five-year node(s).")

    print("Finding actual new combinations in focal-year predications.")
    actual_new_edges = find_actual_new_combination_edges(
        predication_file,
        prior_five_year_edges,
        prior_five_year_nodes,
    )
    print(f"Found {len(actual_new_edges):,} actual new combination edge(s).")

    print(f"Scanning two-hop candidate edges from {two_hop_candidate_edges_file}")
    two_hop_candidate_new_combination_edges = read_candidate_hits_for_actual_new_edges(
        two_hop_candidate_edges_file,
        actual_new_edges,
    )
    print(
        "Found "
        f"{len(two_hop_candidate_new_combination_edges):,} actual new "
        "combination edge(s) inside the two-hop candidate set."
    )

    category_counts: Counter[str] = Counter()
    total_rows = 0

    reader = pd.read_csv(
        predication_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        dtype={SUBJECT_CUI_COLUMN: "string", OBJECT_CUI_COLUMN: "string"},
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        annotated_chunk = classify_edges(
            chunk,
            prior_five_year_edges,
            prior_five_year_nodes,
            two_hop_candidate_new_combination_edges,
        )
        chunk_counts = Counter(annotated_chunk["candidate_edge_category"])
        category_counts.update(chunk_counts)
        total_rows += len(annotated_chunk)

        annotated_chunk.to_csv(
            output_file,
            mode="a",
            index=False,
            compression="gzip",
            header=(chunk_number == 1),
        )

        print(
            f"Chunk {chunk_number:,}: annotated {len(annotated_chunk):,} rows; "
            f"category counts {dict(chunk_counts)}."
        )

    print(f"Saved annotated data to {output_file}")
    print(f"Total rows annotated: {total_rows:,}")
    print(f"Final category counts: {dict(category_counts)}")


def main() -> None:
    focal_year = get_focal_year()
    annotate_focal_year(focal_year)


if __name__ == "__main__":
    main()
