"""Calculate Jaccard scores for generic two-hop candidate edges."""

from __future__ import annotations

import gc
import os
from collections import defaultdict
from pathlib import Path

import pandas as pd


SPLIT_PREDICATION_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/semmedVER43_R/"
    "split_predications_with_pyear_filtered_by_pyear"
)
CANDIDATE_EDGE_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/"
    "link_prediction/candidate_edges/two_hop_candidate_edges"
)
OUTPUT_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/"
    "jaccard_link_prediction/candidate_edges"
)

SPLIT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
PRIOR_FIVE_YEAR_WINDOW_YEARS = 5
BASE_YEAR = 1980
CHUNK_SIZE = 1_000_000
OVERWRITE = False

SUBJECT_CUI_COLUMN = "subject_cui_primary"
OBJECT_CUI_COLUMN = "object_cui_primary"


def get_focal_year() -> int:
    task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if task_id is None:
        raise RuntimeError("SLURM_ARRAY_TASK_ID not found.")
    return int(task_id) + BASE_YEAR


def split_file_for_year(year: int) -> Path:
    return SPLIT_PREDICATION_DIR / f"{SPLIT_FILE_PREFIX}_{year}.csv.gz"


def candidate_file_for_year(focal_year: int) -> Path:
    return (
        CANDIDATE_EDGE_DIR
        / f"two_hop_candidate_edges_prior_5y_{focal_year}.csv.gz"
    )


def output_file_for_year(focal_year: int) -> Path:
    return OUTPUT_DIR / f"jaccard_scored_candidate_edges_{focal_year}.csv.gz"


def check_output(output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    if output_file.exists() and not OVERWRITE:
        raise FileExistsError(
            "Output file already exists. Set OVERWRITE = True to replace it:\n"
            f"{output_file}"
        )
    if output_file.exists() and OVERWRITE:
        output_file.unlink()


def build_prior_five_year_network(focal_year: int) -> defaultdict[str, set[str]]:
    prior_five_years = range(focal_year - PRIOR_FIVE_YEAR_WINDOW_YEARS, focal_year)
    adj: defaultdict[str, set[str]] = defaultdict(set)
    found_years = []
    missing_years = []

    for year in prior_five_years:
        input_file = split_file_for_year(year)

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
            f"No prior-five-year files found for focal year {focal_year} in "
            f"{SPLIT_PREDICATION_DIR}."
        )

    print(f"Focal year {focal_year}: found prior-five-year files {found_years}.")
    if missing_years:
        print(
            f"Focal year {focal_year}: missing prior-five-year files "
            f"{missing_years}."
        )
    print(f"Prior-five-year network nodes: {len(adj):,}.")
    return adj


def count_common_neighbors(neighbors_a: set[str], neighbors_b: set[str]) -> int:
    if len(neighbors_a) > len(neighbors_b):
        neighbors_a, neighbors_b = neighbors_b, neighbors_a
    return sum(1 for neighbor in neighbors_a if neighbor in neighbors_b)


def jaccard_score(
    node_a: str,
    node_b: str,
    adj: defaultdict[str, set[str]],
) -> float:
    neighbors_a = adj.get(node_a, set())
    neighbors_b = adj.get(node_b, set())
    common_count = count_common_neighbors(neighbors_a, neighbors_b)
    union_count = len(neighbors_a) + len(neighbors_b) - common_count

    if union_count == 0:
        return 0.0

    return common_count / union_count


def score_candidate_edges(
    focal_year: int,
    adj: defaultdict[str, set[str]],
) -> None:
    candidate_file = candidate_file_for_year(focal_year)
    output_file = output_file_for_year(focal_year)

    if not candidate_file.exists():
        raise FileNotFoundError(f"Missing candidate edge file: {candidate_file}")

    check_output(output_file)

    total_rows = 0
    wrote_header = False

    reader = pd.read_csv(
        candidate_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        dtype={"node_a": "string", "node_b": "string"},
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        chunk = chunk.dropna(subset=["node_a", "node_b"]).copy()
        chunk["node_a"] = chunk["node_a"].astype("string").str.strip()
        chunk["node_b"] = chunk["node_b"].astype("string").str.strip()
        chunk["jaccard_score"] = [
            jaccard_score(node_a, node_b, adj)
            for node_a, node_b in zip(chunk["node_a"], chunk["node_b"])
        ]
        chunk["pyear"] = focal_year

        total_rows += len(chunk)
        chunk.to_csv(
            output_file,
            mode="a",
            index=False,
            compression="gzip",
            header=not wrote_header,
        )
        wrote_header = True

        print(f"Chunk {chunk_number:,}: scored {total_rows:,} candidate edges.")

    if not wrote_header:
        pd.DataFrame(columns=["node_a", "node_b", "pyear", "jaccard_score"]).to_csv(
            output_file, index=False, compression="gzip"
        )

    print(f"Saved Jaccard scores to {output_file}")
    print(f"Total candidate edges scored: {total_rows:,}")


def main() -> None:
    focal_year = get_focal_year()
    print(f"Starting Jaccard scoring for focal year {focal_year}.")
    adj = build_prior_five_year_network(focal_year)
    score_candidate_edges(focal_year, adj)
    print(f"Finished Jaccard scoring for focal year {focal_year}.")


if __name__ == "__main__":
    main()
