"""Build evaluation summaries for top-percentile link prediction results."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError


PROJECT_DIR = Path("/xdisk/sebratt/jinyugao/projects/innovation_capacity")
INTERIM_DIR = PROJECT_DIR / "data/interim"
RESULT_DIR = PROJECT_DIR / "results/link_prediction"

ANNOTATED_PREDICATION_DIR = INTERIM_DIR / "link_prediction/annotated_predications"
PREDICTED_EDGE_DIR = INTERIM_DIR / "link_prediction/predicted_edges"

INPUT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
OUTPUT_FILE = RESULT_DIR / "link_prediction_evaluation_summary.csv"

BASE_YEAR = 1980
N_YEARS = 40
CHUNK_SIZE = 250_000
TOP_PERCENTILES = [5, 10]
OVERWRITE = False

SUBJECT_CUI_COLUMN = "subject_cui_primary"
OBJECT_CUI_COLUMN = "object_cui_primary"


@dataclass(frozen=True)
class MethodConfig:
    method: str


METHODS = [
    MethodConfig(method="common_neighbor"),
    MethodConfig(method="jaccard"),
    MethodConfig(method="adamic_adar"),
    MethodConfig(method="resource_allocation"),
    MethodConfig(method="preferential_attachment"),
]


def normalize_edge(node_a: object, node_b: object) -> tuple[str, str]:
    node_a_text = "" if pd.isna(node_a) else str(node_a).strip()
    node_b_text = "" if pd.isna(node_b) else str(node_b).strip()
    return tuple(sorted((node_a_text, node_b_text)))


def top_percentile_label(percentile: int) -> str:
    return f"{percentile}pct"


def annotated_file_for_year(
    method_config: MethodConfig,
    focal_year: int,
    percentile: int,
) -> Path:
    label = top_percentile_label(percentile)
    return (
        ANNOTATED_PREDICATION_DIR
        / method_config.method
        / label
        / (
            f"{INPUT_FILE_PREFIX}_{method_config.method}_top_{label}_annotated_"
            f"{focal_year}.csv.gz"
        )
    )


def predicted_edge_file_for_year(
    method_config: MethodConfig,
    focal_year: int,
    percentile: int,
) -> Path:
    label = top_percentile_label(percentile)
    return (
        PREDICTED_EDGE_DIR
        / method_config.method
        / f"{method_config.method}_predicted_edges_top_{label}_{focal_year}.csv.gz"
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


def read_predicted_edge_count(path: Path) -> int:
    try:
        predicted_edges = pd.read_csv(
            path,
            compression="gzip",
            usecols=["node_a", "node_b"],
            dtype={"node_a": "string", "node_b": "string"},
        )
    except EmptyDataError:
        return 0

    predicted_edges = predicted_edges.dropna(subset=["node_a", "node_b"])
    predicted_edge_set = {
        normalize_edge(node_a, node_b)
        for node_a, node_b in zip(
            predicted_edges["node_a"],
            predicted_edges["node_b"],
        )
    }
    return len(predicted_edge_set)


def summarize_annotated_predications(path: Path) -> dict[str, object]:
    category_counts: Counter[str] = Counter()
    actual_new_combination_edges: set[tuple[str, str]] = set()
    expected_new_combination_edges: set[tuple[str, str]] = set()
    repeated_combination_edges: set[tuple[str, str]] = set()
    new_node_combination_edges: set[tuple[str, str]] = set()
    self_loop_edges: set[tuple[str, str]] = set()
    total_predications = 0

    try:
        reader = pd.read_csv(
            path,
            compression="gzip",
            chunksize=CHUNK_SIZE,
            usecols=[SUBJECT_CUI_COLUMN, OBJECT_CUI_COLUMN, "category"],
            dtype={
                SUBJECT_CUI_COLUMN: "string",
                OBJECT_CUI_COLUMN: "string",
                "category": "string",
            },
        )
    except EmptyDataError:
        return {
            "n_predications": 0,
            "n_self_loop_predications": 0,
            "n_new_node_combination_predications": 0,
            "n_repeated_combination_predications": 0,
            "n_expected_new_combination_predications": 0,
            "n_surprised_new_combination_predications": 0,
            "n_actual_new_combination_edges": 0,
            "n_expected_new_combination_edges": 0,
            "n_repeated_combination_edges": 0,
            "n_new_node_combination_edges": 0,
            "n_self_loop_edges": 0,
        }

    for chunk in reader:
        total_predications += len(chunk)
        category_counts.update(chunk["category"].dropna().tolist())

        for row in chunk.itertuples(index=False):
            edge = normalize_edge(
                getattr(row, SUBJECT_CUI_COLUMN),
                getattr(row, OBJECT_CUI_COLUMN),
            )
            category = getattr(row, "category")
            if pd.isna(category):
                continue

            if category == "Expected_New_Combination":
                actual_new_combination_edges.add(edge)
                expected_new_combination_edges.add(edge)
            elif category == "Surprised_New_Combination":
                actual_new_combination_edges.add(edge)
            elif category == "Repeated_Combination":
                repeated_combination_edges.add(edge)
            elif category == "New_Node_Combination":
                new_node_combination_edges.add(edge)
            elif category == "Self_Loop":
                self_loop_edges.add(edge)

    return {
        "n_predications": total_predications,
        "n_self_loop_predications": category_counts["Self_Loop"],
        "n_new_node_combination_predications": category_counts[
            "New_Node_Combination"
        ],
        "n_repeated_combination_predications": category_counts[
            "Repeated_Combination"
        ],
        "n_expected_new_combination_predications": category_counts[
            "Expected_New_Combination"
        ],
        "n_surprised_new_combination_predications": category_counts[
            "Surprised_New_Combination"
        ],
        "n_actual_new_combination_edges": len(actual_new_combination_edges),
        "n_expected_new_combination_edges": len(expected_new_combination_edges),
        "n_repeated_combination_edges": len(repeated_combination_edges),
        "n_new_node_combination_edges": len(new_node_combination_edges),
        "n_self_loop_edges": len(self_loop_edges),
    }


def safe_divide(numerator: int, denominator: int) -> object:
    if denominator == 0:
        return pd.NA
    return numerator / denominator


def f1_score(precision: object, recall: object) -> object:
    if pd.isna(precision) or pd.isna(recall) or precision + recall == 0:
        return pd.NA
    return 2 * precision * recall / (precision + recall)


def build_summary_row(
    method_config: MethodConfig,
    focal_year: int,
    percentile: int,
) -> dict[str, object]:
    annotated_file = annotated_file_for_year(method_config, focal_year, percentile)
    predicted_edge_file = predicted_edge_file_for_year(
        method_config,
        focal_year,
        percentile,
    )

    missing_files = [
        str(path)
        for path in [annotated_file, predicted_edge_file]
        if not path.exists()
    ]
    if missing_files:
        missing = "\n".join(missing_files)
        raise FileNotFoundError(f"Missing required input file(s):\n{missing}")

    print(
        f"Summarizing focal year={focal_year}; method={method_config.method}; "
        f"top_percentile={percentile}."
    )

    annotated_summary = summarize_annotated_predications(annotated_file)
    n_predicted_edges = read_predicted_edge_count(predicted_edge_file)
    n_expected_new_combination_edges = int(
        annotated_summary["n_expected_new_combination_edges"]
    )
    n_actual_new_combination_edges = int(
        annotated_summary["n_actual_new_combination_edges"]
    )
    n_false_positive_edges = max(
        0,
        n_predicted_edges - n_expected_new_combination_edges,
    )
    precision = safe_divide(n_expected_new_combination_edges, n_predicted_edges)
    recall = safe_divide(
        n_expected_new_combination_edges,
        n_actual_new_combination_edges,
    )

    return {
        "pyear": focal_year,
        "method": method_config.method,
        "top_percentile": percentile,
        **annotated_summary,
        "n_predicted_edges": n_predicted_edges,
        "n_false_positive_edges": n_false_positive_edges,
        "precision": precision,
        "recall": recall,
        "f1_score": f1_score(precision, recall),
    }


def main() -> None:
    check_output()
    rows = []

    for method_config in METHODS:
        for percentile in TOP_PERCENTILES:
            for focal_year in range(BASE_YEAR, BASE_YEAR + N_YEARS):
                row = build_summary_row(method_config, focal_year, percentile)
                rows.append(row)

    summary = pd.DataFrame(rows)
    summary.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved evaluation summary to {OUTPUT_FILE}")
    print(f"Rows: {len(summary):,}")


if __name__ == "__main__":
    main()
