"""Build common-neighbor link-prediction candidates from yearly SemMedDB predication files."""

from __future__ import annotations

import gc
import os
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path

import pandas as pd


INPUT_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/semmedVER43_R/"
    "split_predications_with_pyear_filtered_by_pyear"
)
OUTPUT_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/"
    "common_neighbor_link_prediction"
)

INPUT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
HISTORY_WINDOW_YEARS = 5
MAX_DEGREE = 500
BASE_YEAR = 1980

SUBJECT_CUI_COLUMN = "subject_cui_primary"
OBJECT_CUI_COLUMN = "object_cui_primary"


def get_focal_year() -> int:
    task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if task_id is None:
        raise RuntimeError("SLURM_ARRAY_TASK_ID not found.")
    return int(task_id) + BASE_YEAR


def input_file_for_year(input_dir: Path, year: int) -> Path:
    return input_dir / f"{INPUT_FILE_PREFIX}_{year}.csv.gz"


def build_history_network(
    focal_year: int,
    input_dir: Path,
    max_degree: int,
) -> tuple[defaultdict[str, set[str]], list[int], list[int]]:
    history_years = range(focal_year - HISTORY_WINDOW_YEARS, focal_year)
    adj: defaultdict[str, set[str]] = defaultdict(set)
    found_years = []
    missing_years = []

    for year in history_years:
        input_file = input_file_for_year(input_dir, year)

        if not input_file.exists():
            missing_years.append(year)
            continue

        found_years.append(year)
        df = pd.read_csv(
            input_file,
            compression="gzip",
            usecols=[SUBJECT_CUI_COLUMN, OBJECT_CUI_COLUMN],
            dtype={SUBJECT_CUI_COLUMN: "string", OBJECT_CUI_COLUMN: "string"},
        )
        df = df.dropna(subset=[SUBJECT_CUI_COLUMN, OBJECT_CUI_COLUMN])
        print(f"Year {year}: loaded {len(df):,} rows.")

        for subject_cui, object_cui in zip(
            df[SUBJECT_CUI_COLUMN], df[OBJECT_CUI_COLUMN]
        ):
            subject_cui = str(subject_cui).strip()
            object_cui = str(object_cui).strip()

            if not subject_cui or not object_cui or subject_cui == object_cui:
                continue

            adj[subject_cui].add(object_cui)
            adj[object_cui].add(subject_cui)

        del df
        gc.collect()

    if not found_years:
        raise FileNotFoundError(
            f"No history files found for focal year {focal_year} in {input_dir}."
        )

    print(f"Focal year {focal_year}: found history years {found_years}.")
    if missing_years:
        print(f"Focal year {focal_year}: missing history years {missing_years}.")
    print(f"History network nodes before degree filter: {len(adj):,}.")

    if max_degree is not None:
        high_degree_nodes = [
            node for node, neighbors in adj.items() if len(neighbors) > max_degree
        ]
        if high_degree_nodes:
            print(
                f"Nodes above max_degree={max_degree:,}: {len(high_degree_nodes):,}. "
                "They are skipped during candidate generation."
            )

    return adj, found_years, missing_years


def save_past_edges(adj: defaultdict[str, set[str]], output_dir: Path, focal_year: int) -> None:
    past_edges = []
    seen_edges = set()

    for node, neighbors in adj.items():
        for neighbor in neighbors:
            edge = tuple(sorted((node, neighbor)))
            if edge not in seen_edges:
                past_edges.append({"node_a": edge[0], "node_b": edge[1]})
                seen_edges.add(edge)

    output_file = (
        output_dir
        / "past_edges"
        / f"common_neighbor_past_edges_5y_{focal_year}.csv.gz"
    )
    pd.DataFrame(past_edges).to_csv(output_file, index=False, compression="gzip")
    print(f"Past edges saved: {len(past_edges):,} rows to {output_file}")

    del past_edges
    gc.collect()


def save_candidate_edges(
    adj: defaultdict[str, set[str]],
    output_dir: Path,
    focal_year: int,
    max_degree: int,
) -> None:
    candidate_counts: Counter[tuple[str, str]] = Counter()

    print(f"Calculating candidate edges for {focal_year}...")
    for node, neighbors in adj.items():
        if len(neighbors) > max_degree:
            continue

        for node_a, node_b in combinations(sorted(neighbors), 2):
            if node_b not in adj[node_a]:
                candidate_counts[(node_a, node_b)] += 1

    output_file = (
        output_dir
        / "candidate_edges"
        / f"common_neighbor_candidate_edges_{focal_year}.csv.gz"
    )

    if not candidate_counts:
        pd.DataFrame(columns=["node_a", "node_b", "cn_count", "pyear"]).to_csv(
            output_file, index=False, compression="gzip"
        )
        print(f"No candidate edges found. Empty file saved to {output_file}")
        return

    candidate_edges = pd.DataFrame(
        [
            {"node_a": pair[0], "node_b": pair[1], "cn_count": count}
            for pair, count in candidate_counts.items()
        ]
    )
    candidate_edges["pyear"] = focal_year
    candidate_edges.to_csv(output_file, index=False, compression="gzip")
    print(f"Candidate edges saved: {len(candidate_edges):,} rows to {output_file}")

    del candidate_edges
    candidate_counts.clear()
    gc.collect()


def find_links(focal_year: int) -> None:
    (OUTPUT_DIR / "past_edges").mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "candidate_edges").mkdir(parents=True, exist_ok=True)

    adj, _, _ = build_history_network(
        focal_year=focal_year,
        input_dir=INPUT_DIR,
        max_degree=MAX_DEGREE,
    )
    save_past_edges(adj=adj, output_dir=OUTPUT_DIR, focal_year=focal_year)
    save_candidate_edges(
        adj=adj,
        output_dir=OUTPUT_DIR,
        focal_year=focal_year,
        max_degree=MAX_DEGREE,
    )


def main() -> None:
    focal_year = get_focal_year()
    print(f"Starting link prediction for focal year {focal_year}.")
    find_links(focal_year)
    print(f"Finished link prediction for focal year {focal_year}.")


if __name__ == "__main__":
    main()
