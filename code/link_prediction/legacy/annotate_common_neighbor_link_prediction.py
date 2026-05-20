"""Legacy common-neighbor-only annotation for yearly SemMedDB predications."""

from __future__ import annotations

import os
from collections import Counter
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError


PRIOR_FIVE_YEAR_EDGE_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/"
    "link_prediction/candidate_edges/prior_five_year_edges"
)
COMMON_NEIGHBOR_SCORE_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/"
    "common_neighbor_link_prediction/candidate_edges"
)
SPLIT_PREDICATION_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/semmedVER43_R/"
    "split_predications_with_pyear_filtered_by_pyear"
)
OUTPUT_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/"
    "annotated_predications"
)

INPUT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
OUTPUT_FILE_PREFIX = (
    "semmedVER43_R_predications_with_pyear_filtered_common_neighbor_annotated"
)

BASE_YEAR = 1980
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
    node_a_text = str(node_a).strip()
    node_b_text = str(node_b).strip()
    return tuple(sorted((node_a_text, node_b_text)))


def read_past_edges(path: Path) -> set[tuple[str, str]]:
    try:
        past_edges = pd.read_csv(
            path,
            compression="gzip",
            usecols=["node_a", "node_b"],
            dtype={"node_a": "string", "node_b": "string"},
        )
    except EmptyDataError:
        return set()

    past_edges = past_edges.dropna(subset=["node_a", "node_b"])
    return {
        normalize_edge(node_a, node_b)
        for node_a, node_b in zip(past_edges["node_a"], past_edges["node_b"])
    }


def read_candidate_edges(path: Path) -> dict[tuple[str, str], int]:
    try:
        candidate_edges = pd.read_csv(
            path,
            compression="gzip",
            usecols=["node_a", "node_b", "common_neighbor_score"],
            dtype={"node_a": "string", "node_b": "string"},
        )
    except EmptyDataError:
        return {}

    candidate_edges = candidate_edges.dropna(subset=["node_a", "node_b"])
    candidate_edges["common_neighbor_score"] = pd.to_numeric(
        candidate_edges["common_neighbor_score"], errors="coerce"
    ).fillna(0)

    return {
        normalize_edge(row.node_a, row.node_b): int(row.common_neighbor_score)
        for row in candidate_edges.itertuples(index=False)
    }


def check_inputs(paths: list[Path]) -> None:
    missing_files = [str(path) for path in paths if not path.exists()]
    if missing_files:
        missing = "\n".join(missing_files)
        raise FileNotFoundError(f"Missing required input file(s):\n{missing}")


def check_output(output_file: Path, overwrite: bool) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    if output_file.exists() and not overwrite:
        raise FileExistsError(
            "Output file already exists. Set OVERWRITE = True to replace it:\n"
            f"{output_file}"
        )
    if output_file.exists() and overwrite:
        output_file.unlink()


def classify_edges(
    chunk: pd.DataFrame,
    past_edges: set[tuple[str, str]],
    candidate_edges: dict[tuple[str, str], int],
) -> pd.DataFrame:
    annotated = chunk.copy()
    edges = [
        normalize_edge(subject_cui, object_cui)
        for subject_cui, object_cui in zip(
            annotated[SUBJECT_CUI_COLUMN], annotated[OBJECT_CUI_COLUMN]
        )
    ]

    categories = []
    common_neighbor_scores = []

    for edge in edges:
        node_a, node_b = edge

        if not node_a or not node_b or node_a == node_b:
            categories.append("Self_Loop")
            common_neighbor_scores.append(0)
        elif edge in past_edges:
            categories.append("Repeated")
            common_neighbor_scores.append(0)
        elif edge in candidate_edges:
            categories.append("Expected")
            common_neighbor_scores.append(candidate_edges[edge])
        else:
            categories.append("Surprised")
            common_neighbor_scores.append(0)

    annotated["category"] = categories
    annotated["common_neighbor_score"] = common_neighbor_scores
    return annotated


def annotate_focal_year(focal_year: int) -> None:
    past_edges_file = (
        PRIOR_FIVE_YEAR_EDGE_DIR / f"prior_five_year_edges_{focal_year}.csv.gz"
    )
    candidate_edges_file = (
        COMMON_NEIGHBOR_SCORE_DIR
        / f"common_neighbor_scored_candidate_edges_{focal_year}.csv.gz"
    )
    predication_file = (
        SPLIT_PREDICATION_DIR / f"{INPUT_FILE_PREFIX}_{focal_year}.csv.gz"
    )
    output_file = OUTPUT_DIR / f"{OUTPUT_FILE_PREFIX}_{focal_year}.csv.gz"

    check_inputs([past_edges_file, candidate_edges_file, predication_file])
    check_output(output_file, OVERWRITE)

    print(f"Annotating focal year {focal_year}.")
    print(f"Reading past edges from {past_edges_file}")
    past_edges = read_past_edges(past_edges_file)
    print(f"Loaded {len(past_edges):,} past edge(s).")

    print(f"Reading candidate edges from {candidate_edges_file}")
    candidate_edges = read_candidate_edges(candidate_edges_file)
    print(f"Loaded {len(candidate_edges):,} candidate edge(s).")

    category_counts: Counter[str] = Counter()
    total_rows = 0

    reader = pd.read_csv(
        predication_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        dtype={SUBJECT_CUI_COLUMN: "string", OBJECT_CUI_COLUMN: "string"},
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        annotated_chunk = classify_edges(chunk, past_edges, candidate_edges)
        chunk_counts = Counter(annotated_chunk["category"])
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
