"""Build evaluation summaries for two-hop candidate edge coverage."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError


PROJECT_DIR = Path("/xdisk/sebratt/jinyugao/projects/innovation_capacity")
INTERIM_DIR = PROJECT_DIR / "data/interim"
RESULT_DIR = PROJECT_DIR / "results/link_prediction/candidate_edges"

ANNOTATED_PREDICATION_DIR = (
    INTERIM_DIR / "link_prediction/candidate_edges/annotated_predications"
)
TWO_HOP_CANDIDATE_EDGE_DIR = (
    INTERIM_DIR / "link_prediction/candidate_edges/two_hop_candidate_edges"
)

INPUT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
OUTPUT_FILE = RESULT_DIR / "two_hop_candidate_edge_evaluation_summary.csv"

BASE_YEAR = 1980
N_YEARS = 40
CHUNK_SIZE = 250_000
OVERWRITE = False

SUBJECT_CUI_COLUMN = "subject_cui_primary"
OBJECT_CUI_COLUMN = "object_cui_primary"

CATEGORY_SELF_LOOP = "Self_Loop"
CATEGORY_NEW_NODE = "New_Node_Combination"
CATEGORY_REPEATED = "Repeated_Combination"
CATEGORY_IN_CANDIDATE = "Two_Hop_Candidate_New_Combination"
CATEGORY_OUTSIDE_CANDIDATE = "Outside_Two_Hop_Candidate_New_Combination"


def normalize_edge(node_a: object, node_b: object) -> tuple[str, str]:
    node_a_text = "" if pd.isna(node_a) else str(node_a).strip()
    node_b_text = "" if pd.isna(node_b) else str(node_b).strip()
    return tuple(sorted((node_a_text, node_b_text)))


def annotated_file_for_year(focal_year: int) -> Path:
    return (
        ANNOTATED_PREDICATION_DIR
        / f"{INPUT_FILE_PREFIX}_two_hop_candidate_edges_annotated_{focal_year}.csv.gz"
    )


def two_hop_candidate_edge_file_for_year(focal_year: int) -> Path:
    return (
        TWO_HOP_CANDIDATE_EDGE_DIR
        / f"two_hop_candidate_edges_prior_5y_{focal_year}.csv.gz"
    )


def check_output() -> None:
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    if OUTPUT_FILE.exists() and not OVERWRITE:
        raise FileExistsError(
            "Output file already exists. Set OVERWRITE = True to replace it:\n"
            f"{OUTPUT_FILE}"
        )
    if OUTPUT_FILE.exists() and OVERWRITE:
        OUTPUT_FILE.unlink()


def safe_divide(numerator: int, denominator: int) -> object:
    if denominator == 0:
        return pd.NA
    return numerator / denominator


def f1_score(precision: object, recall: object) -> object:
    if pd.isna(precision) or pd.isna(recall) or precision + recall == 0:
        return pd.NA
    return 2 * precision * recall / (precision + recall)


def read_two_hop_candidate_edge_count(path: Path) -> int:
    total_rows = 0

    try:
        reader = pd.read_csv(
            path,
            compression="gzip",
            chunksize=CHUNK_SIZE,
            usecols=["node_a", "node_b"],
            dtype={"node_a": "string", "node_b": "string"},
        )
    except EmptyDataError:
        return 0

    for chunk in reader:
        total_rows += len(chunk.dropna(subset=["node_a", "node_b"]))

    return total_rows


def summarize_annotated_predications(path: Path) -> dict[str, object]:
    category_counts: Counter[str] = Counter()
    actual_new_combination_edges: set[tuple[str, str]] = set()
    candidate_new_combination_edges: set[tuple[str, str]] = set()
    outside_candidate_new_combination_edges: set[tuple[str, str]] = set()
    repeated_combination_edges: set[tuple[str, str]] = set()
    new_node_combination_edges: set[tuple[str, str]] = set()
    self_loop_edges: set[tuple[str, str]] = set()
    total_predications = 0

    try:
        reader = pd.read_csv(
            path,
            compression="gzip",
            chunksize=CHUNK_SIZE,
            usecols=[
                SUBJECT_CUI_COLUMN,
                OBJECT_CUI_COLUMN,
                "candidate_edge_category",
            ],
            dtype={
                SUBJECT_CUI_COLUMN: "string",
                OBJECT_CUI_COLUMN: "string",
                "candidate_edge_category": "string",
            },
        )
    except EmptyDataError:
        return {
            "n_predications": 0,
            "n_self_loop_predications": 0,
            "n_new_node_combination_predications": 0,
            "n_repeated_combination_predications": 0,
            "n_two_hop_candidate_new_combination_predications": 0,
            "n_outside_two_hop_candidate_new_combination_predications": 0,
            "n_actual_new_combination_edges": 0,
            "n_two_hop_candidate_new_combination_edges": 0,
            "n_outside_two_hop_candidate_new_combination_edges": 0,
            "n_repeated_combination_edges": 0,
            "n_new_node_combination_edges": 0,
            "n_self_loop_edges": 0,
        }

    for chunk in reader:
        total_predications += len(chunk)
        category_counts.update(chunk["candidate_edge_category"].dropna().tolist())

        for row in chunk.itertuples(index=False):
            edge = normalize_edge(
                getattr(row, SUBJECT_CUI_COLUMN),
                getattr(row, OBJECT_CUI_COLUMN),
            )
            category = getattr(row, "candidate_edge_category")
            if pd.isna(category):
                continue

            if category == CATEGORY_IN_CANDIDATE:
                actual_new_combination_edges.add(edge)
                candidate_new_combination_edges.add(edge)
            elif category == CATEGORY_OUTSIDE_CANDIDATE:
                actual_new_combination_edges.add(edge)
                outside_candidate_new_combination_edges.add(edge)
            elif category == CATEGORY_REPEATED:
                repeated_combination_edges.add(edge)
            elif category == CATEGORY_NEW_NODE:
                new_node_combination_edges.add(edge)
            elif category == CATEGORY_SELF_LOOP:
                self_loop_edges.add(edge)

    return {
        "n_predications": total_predications,
        "n_self_loop_predications": category_counts[CATEGORY_SELF_LOOP],
        "n_new_node_combination_predications": category_counts[CATEGORY_NEW_NODE],
        "n_repeated_combination_predications": category_counts[CATEGORY_REPEATED],
        "n_two_hop_candidate_new_combination_predications": category_counts[
            CATEGORY_IN_CANDIDATE
        ],
        "n_outside_two_hop_candidate_new_combination_predications": category_counts[
            CATEGORY_OUTSIDE_CANDIDATE
        ],
        "n_actual_new_combination_edges": len(actual_new_combination_edges),
        "n_two_hop_candidate_new_combination_edges": len(candidate_new_combination_edges),
        "n_outside_two_hop_candidate_new_combination_edges": len(
            outside_candidate_new_combination_edges
        ),
        "n_repeated_combination_edges": len(repeated_combination_edges),
        "n_new_node_combination_edges": len(new_node_combination_edges),
        "n_self_loop_edges": len(self_loop_edges),
    }


def build_summary_row(focal_year: int) -> dict[str, object]:
    annotated_file = annotated_file_for_year(focal_year)
    candidate_edge_file = two_hop_candidate_edge_file_for_year(focal_year)

    missing_files = [
        str(path)
        for path in [annotated_file, candidate_edge_file]
        if not path.exists()
    ]
    if missing_files:
        missing = "\n".join(missing_files)
        raise FileNotFoundError(f"Missing required input file(s):\n{missing}")

    print(f"Summarizing two-hop candidate edge coverage for focal year={focal_year}.")
    annotated_summary = summarize_annotated_predications(annotated_file)
    n_two_hop_candidate_edges = read_two_hop_candidate_edge_count(candidate_edge_file)
    n_candidate_hits = int(
        annotated_summary["n_two_hop_candidate_new_combination_edges"]
    )
    n_actual_new_combination_edges = int(
        annotated_summary["n_actual_new_combination_edges"]
    )
    n_candidate_false_positive_edges = max(
        0,
        n_two_hop_candidate_edges - n_candidate_hits,
    )
    candidate_precision = safe_divide(n_candidate_hits, n_two_hop_candidate_edges)
    candidate_recall = safe_divide(n_candidate_hits, n_actual_new_combination_edges)

    return {
        "pyear": focal_year,
        **annotated_summary,
        "n_two_hop_candidate_edges": n_two_hop_candidate_edges,
        "n_two_hop_candidate_false_positive_edges": n_candidate_false_positive_edges,
        "candidate_precision": candidate_precision,
        "candidate_recall": candidate_recall,
        "candidate_f1_score": f1_score(candidate_precision, candidate_recall),
    }


def main() -> None:
    check_output()
    rows = []

    for focal_year in range(BASE_YEAR, BASE_YEAR + N_YEARS):
        rows.append(build_summary_row(focal_year))

    summary = pd.DataFrame(rows)
    summary.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved two-hop candidate edge evaluation summary to {OUTPUT_FILE}")
    print(f"Rows: {len(summary):,}")


if __name__ == "__main__":
    main()
