"""Build generic two-hop candidate edges from yearly SemMedDB predication files."""

from __future__ import annotations

import gc
import os
from collections import defaultdict
from itertools import combinations
from pathlib import Path

import pandas as pd


INPUT_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/semmedVER43_R/"
    "split_predications_with_pyear_filtered_by_pyear"
)
OUTPUT_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/"
    "link_prediction/candidate_edges"
)

INPUT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
PRIOR_FIVE_YEAR_WINDOW_YEARS = 5
MAX_COMMON_NEIGHBOR_DEGREE = 500
BASE_YEAR = 1980

SUBJECT_CUI_COLUMN = "subject_cui_primary"
OBJECT_CUI_COLUMN = "object_cui_primary"


def get_focal_year() -> int:
    task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if task_id is None:
        raise RuntimeError("SLURM_ARRAY_TASK_ID not found.")
    return int(task_id) + BASE_YEAR


def input_file_for_year(year: int) -> Path:
    return INPUT_DIR / f"{INPUT_FILE_PREFIX}_{year}.csv.gz"


def build_prior_five_year_network(focal_year: int) -> defaultdict[str, set[str]]:
    prior_five_years = range(focal_year - PRIOR_FIVE_YEAR_WINDOW_YEARS, focal_year)
    adj: defaultdict[str, set[str]] = defaultdict(set)
    found_years = []
    missing_years = []

    for year in prior_five_years:
        input_file = input_file_for_year(year)

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
            "No prior-five-year files found for focal year "
            f"{focal_year} in {INPUT_DIR}."
        )

    print(f"Focal year {focal_year}: found prior-five-year files {found_years}.")
    if missing_years:
        print(
            f"Focal year {focal_year}: missing prior-five-year files "
            f"{missing_years}."
        )
    print(f"Prior-five-year network nodes: {len(adj):,}.")

    high_degree_nodes = [
        node
        for node, neighbors in adj.items()
        if len(neighbors) > MAX_COMMON_NEIGHBOR_DEGREE
    ]
    if high_degree_nodes:
        print(
            "Common-neighbor nodes above "
            f"MAX_COMMON_NEIGHBOR_DEGREE={MAX_COMMON_NEIGHBOR_DEGREE:,}: "
            f"{len(high_degree_nodes):,}. They are skipped during candidate "
            "generation."
        )

    return adj


def save_prior_five_year_edges(
    adj: defaultdict[str, set[str]],
    focal_year: int,
) -> None:
    prior_five_year_edges = []
    seen_edges = set()

    for node, neighbors in adj.items():
        for neighbor in neighbors:
            edge = tuple(sorted((node, neighbor)))
            if edge not in seen_edges:
                prior_five_year_edges.append({"node_a": edge[0], "node_b": edge[1]})
                seen_edges.add(edge)

    output_file = (
        OUTPUT_DIR
        / "prior_five_year_edges"
        / f"prior_five_year_edges_{focal_year}.csv.gz"
    )
    output_file.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(prior_five_year_edges).to_csv(
        output_file, index=False, compression="gzip"
    )
    print(
        "Prior-five-year edges saved: "
        f"{len(prior_five_year_edges):,} rows to {output_file}"
    )

    del prior_five_year_edges
    gc.collect()


def save_two_hop_candidate_edges(
    adj: defaultdict[str, set[str]],
    focal_year: int,
) -> None:
    candidate_edges = set()

    print(f"Finding two-hop candidate edges for {focal_year}...")
    for common_neighbor, neighbors in adj.items():
        if len(neighbors) > MAX_COMMON_NEIGHBOR_DEGREE:
            continue

        for node_a, node_b in combinations(sorted(neighbors), 2):
            if node_b not in adj[node_a]:
                candidate_edges.add((node_a, node_b))

    output_file = (
        OUTPUT_DIR
        / "two_hop_candidate_edges"
        / f"two_hop_candidate_edges_5y_{focal_year}.csv.gz"
    )
    output_file.parent.mkdir(parents=True, exist_ok=True)

    if not candidate_edges:
        pd.DataFrame(columns=["node_a", "node_b", "pyear"]).to_csv(
            output_file, index=False, compression="gzip"
        )
        print(f"No candidate edges found. Empty file saved to {output_file}")
        return

    candidate_edges_df = pd.DataFrame(
        [{"node_a": edge[0], "node_b": edge[1]} for edge in sorted(candidate_edges)]
    )
    candidate_edges_df["pyear"] = focal_year
    candidate_edges_df.to_csv(output_file, index=False, compression="gzip")
    print(
        f"Two-hop candidate edges saved: {len(candidate_edges_df):,} rows to "
        f"{output_file}"
    )

    del candidate_edges_df
    candidate_edges.clear()
    gc.collect()


def main() -> None:
    focal_year = get_focal_year()
    print(f"Starting two-hop candidate edge generation for focal year {focal_year}.")
    adj = build_prior_five_year_network(focal_year)
    save_prior_five_year_edges(adj, focal_year)
    save_two_hop_candidate_edges(adj, focal_year)
    print(f"Finished two-hop candidate edge generation for focal year {focal_year}.")


if __name__ == "__main__":
    main()
