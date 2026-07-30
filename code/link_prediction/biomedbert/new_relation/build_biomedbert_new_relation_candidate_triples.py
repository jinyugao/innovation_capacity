"""Build BiomedBERT candidate triples for new-relation prediction.

For each focal year, candidates are built from prior 5-year undirected
CUI-CUI combinations. A candidate is a unique directed triple
(subject, predicate, object) where the undirected CUI-CUI pair was observed
in the prior 5-year window, the predicate was observed for a compatible
directed semantic-type pair, and the exact directed triple was not observed
in the prior 5-year window.
"""

from __future__ import annotations

import gc
import os
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd


INTERIM_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim"
)
SPLIT_PREDICATION_DIR = (
    INTERIM_DIR
    / "semmedVER43_R/split_predications_with_pyear_filtered_by_pyear"
)
CUI_LABEL_FILE = (
    INTERIM_DIR / "biomedbert_link_prediction/cui_labels/biomedbert_cui_labels.csv.gz"
)
OUTPUT_DIR = INTERIM_DIR / "biomedbert_link_prediction/new_relation/candidate_triples"

SPLIT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
OUTPUT_FILE_PREFIX = "biomedbert_new_relation_candidate_triples"

BASE_YEAR = 1980
PRIOR_FIVE_YEAR_WINDOW_YEARS = 5
CHUNK_SIZE = 500_000
WRITE_BUFFER_SIZE = 500_000
OVERWRITE = False

PREDICATION_ID_COLUMN = "PREDICATION_ID"
SUBJECT_CUI_COLUMN = "subject_cui_primary"
OBJECT_CUI_COLUMN = "object_cui_primary"
PREDICATE_COLUMN = "PREDICATE"
SUBJECT_NAME_COLUMN = "SUBJECT_NAME"
OBJECT_NAME_COLUMN = "OBJECT_NAME"
SUBJECT_SEMTYPE_COLUMN = "SUBJECT_SEMTYPE"
OBJECT_SEMTYPE_COLUMN = "OBJECT_SEMTYPE"


def get_focal_year() -> int:
    task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if task_id is None:
        raise RuntimeError("SLURM_ARRAY_TASK_ID not found.")
    return int(task_id) + BASE_YEAR


def split_file_for_year(year: int) -> Path:
    return SPLIT_PREDICATION_DIR / f"{SPLIT_FILE_PREFIX}_{year}.csv.gz"


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


def normalize_value(value: object) -> str:
    return "" if pd.isna(value) else str(value).strip()


def normalize_combination(node_a: object, node_b: object) -> tuple[str, str]:
    node_a_text = normalize_value(node_a)
    node_b_text = normalize_value(node_b)
    return tuple(sorted((node_a_text, node_b_text)))


def predicate_to_phrase(predicate: object) -> str:
    return normalize_value(predicate).replace("_", " ").lower()


def candidate_text(
    subject_name: str,
    predicate: str,
    object_name: str,
) -> str:
    return f"{subject_name} {predicate_to_phrase(predicate)} {object_name}".strip()


def load_cui_labels(path: Path) -> dict[str, str]:
    check_input(path)
    labels = pd.read_csv(
        path,
        compression="gzip",
        usecols=["cui", "selected_cui_name"],
        dtype={"cui": "string", "selected_cui_name": "string"},
    )
    labels = labels.dropna(subset=["cui", "selected_cui_name"]).copy()
    labels["cui"] = labels["cui"].astype("string").str.strip()
    labels["selected_cui_name"] = labels["selected_cui_name"].astype("string").str.strip()
    labels = labels[(labels["cui"] != "") & (labels["selected_cui_name"] != "")]
    label_map = dict(zip(labels["cui"].astype(str), labels["selected_cui_name"].astype(str)))
    print(f"Loaded BiomedBERT CUI labels for {len(label_map):,} CUIs.")
    return label_map


def read_prior_five_year_network(
    focal_year: int,
) -> tuple[
    set[tuple[str, str, str]],
    defaultdict[tuple[str, str], Counter[tuple[str, str]]],
    defaultdict[tuple[str, str], set[str]],
    set[tuple[str, str]],
]:
    exact_triples: set[tuple[str, str, str]] = set()
    directed_pair_semtype_counts: defaultdict[
        tuple[str, str], Counter[tuple[str, str]]
    ] = defaultdict(Counter)
    semtype_predicates: defaultdict[tuple[str, str], set[str]] = defaultdict(set)
    undirected_combinations: set[tuple[str, str]] = set()
    found_years = []
    missing_years = []

    prior_years = range(focal_year - PRIOR_FIVE_YEAR_WINDOW_YEARS, focal_year)
    for year in prior_years:
        input_file = split_file_for_year(year)
        if not input_file.exists():
            missing_years.append(year)
            continue

        found_years.append(year)
        reader = pd.read_csv(
            input_file,
            compression="gzip",
            chunksize=CHUNK_SIZE,
            usecols=[
                SUBJECT_CUI_COLUMN,
                OBJECT_CUI_COLUMN,
                PREDICATE_COLUMN,
                SUBJECT_SEMTYPE_COLUMN,
                OBJECT_SEMTYPE_COLUMN,
            ],
            dtype={
                SUBJECT_CUI_COLUMN: "string",
                OBJECT_CUI_COLUMN: "string",
                PREDICATE_COLUMN: "string",
                SUBJECT_SEMTYPE_COLUMN: "string",
                OBJECT_SEMTYPE_COLUMN: "string",
            },
        )

        year_rows = 0
        for chunk in reader:
            chunk = chunk.dropna(
                subset=[
                    SUBJECT_CUI_COLUMN,
                    OBJECT_CUI_COLUMN,
                    PREDICATE_COLUMN,
                    SUBJECT_SEMTYPE_COLUMN,
                    OBJECT_SEMTYPE_COLUMN,
                ]
            )
            year_rows += len(chunk)

            for row in chunk.itertuples(index=False):
                subject_cui = normalize_value(getattr(row, SUBJECT_CUI_COLUMN))
                object_cui = normalize_value(getattr(row, OBJECT_CUI_COLUMN))
                predicate = normalize_value(getattr(row, PREDICATE_COLUMN))
                subject_semtype = normalize_value(getattr(row, SUBJECT_SEMTYPE_COLUMN))
                object_semtype = normalize_value(getattr(row, OBJECT_SEMTYPE_COLUMN))

                if not subject_cui or not object_cui or subject_cui == object_cui:
                    continue
                if not predicate or not subject_semtype or not object_semtype:
                    continue

                exact_triples.add((subject_cui, predicate, object_cui))
                undirected_combinations.add(
                    normalize_combination(subject_cui, object_cui)
                )

                directed_pair_semtype_counts[(subject_cui, object_cui)][
                    (subject_semtype, object_semtype)
                ] += 1
                directed_pair_semtype_counts[(object_cui, subject_cui)][
                    (object_semtype, subject_semtype)
                ] += 1
                semtype_predicates[(subject_semtype, object_semtype)].add(predicate)

        print(f"Prior year {year}: scanned {year_rows:,} usable predication rows.")
        gc.collect()

    if not found_years:
        raise FileNotFoundError(
            f"No prior-five-year files found for focal year {focal_year} in "
            f"{SPLIT_PREDICATION_DIR}."
        )

    print(f"Focal year {focal_year}: found prior years {found_years}.")
    if missing_years:
        print(f"Focal year {focal_year}: missing prior years {missing_years}.")
    print(f"Prior exact triples: {len(exact_triples):,}.")
    print(f"Prior undirected CUI-CUI combinations: {len(undirected_combinations):,}.")
    print(
        "Candidate directed subject-object pairs: "
        f"{len(directed_pair_semtype_counts):,}."
    )
    print(f"Prior directed semtype pairs: {len(semtype_predicates):,}.")

    return (
        exact_triples,
        directed_pair_semtype_counts,
        semtype_predicates,
        undirected_combinations,
    )


def empty_candidate_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "subject_cui",
            "predicate",
            "object_cui",
            "subject_semtype",
            "object_semtype",
            "n_supporting_semtype_pairs",
            "n_supporting_semtype_pair_observations",
            "subject_name",
            "predicate_phrase",
            "object_name",
            "candidate_text",
            "pyear",
            "prior_five_year_window_years",
            "candidate_pair_source",
            "candidate_unit",
        ]
    )


def flush_rows(rows: list[dict[str, object]], output_file: Path, wrote_header: bool) -> bool:
    if not rows:
        return wrote_header
    pd.DataFrame(rows).to_csv(
        output_file,
        mode="a",
        index=False,
        compression="gzip",
        header=not wrote_header,
    )
    rows.clear()
    return True


def build_candidate_triples(focal_year: int) -> None:
    output_file = output_file_for_year(focal_year)
    check_output(output_file)
    label_map = load_cui_labels(CUI_LABEL_FILE)
    (
        exact_triples,
        directed_pair_semtype_counts,
        semtype_predicates,
        undirected_combinations,
    ) = read_prior_five_year_network(focal_year)

    rows: list[dict[str, object]] = []
    wrote_header = False
    total_candidates = 0

    for subject_cui, object_cui in sorted(directed_pair_semtype_counts):
        if normalize_combination(subject_cui, object_cui) not in undirected_combinations:
            continue

        subject_name = label_map.get(subject_cui, subject_cui)
        object_name = label_map.get(object_cui, object_cui)
        semtype_counts = directed_pair_semtype_counts[(subject_cui, object_cui)]
        predicate_support: defaultdict[
            str, list[tuple[str, str, int]]
        ] = defaultdict(list)

        for (subject_semtype, object_semtype), support_count in semtype_counts.items():
            candidate_predicates = semtype_predicates[(subject_semtype, object_semtype)]
            for predicate in sorted(candidate_predicates):
                predicate_support[predicate].append(
                    (subject_semtype, object_semtype, support_count)
                )

        for predicate in sorted(predicate_support):
            if (subject_cui, predicate, object_cui) in exact_triples:
                continue

            supporting_semtype_pairs = sorted(
                predicate_support[predicate],
                key=lambda item: (-item[2], item[0], item[1]),
            )
            subject_semtype, object_semtype, _ = supporting_semtype_pairs[0]
            n_supporting_observations = sum(
                support_count
                for _, _, support_count in supporting_semtype_pairs
            )

            predicate_phrase = predicate_to_phrase(predicate)
            rows.append(
                {
                    "subject_cui": subject_cui,
                    "predicate": predicate,
                    "object_cui": object_cui,
                    "subject_semtype": subject_semtype,
                    "object_semtype": object_semtype,
                    "n_supporting_semtype_pairs": len(supporting_semtype_pairs),
                    "n_supporting_semtype_pair_observations": n_supporting_observations,
                    "subject_name": subject_name,
                    "predicate_phrase": predicate_phrase,
                    "object_name": object_name,
                    "candidate_text": candidate_text(
                        subject_name,
                        predicate,
                        object_name,
                    ),
                    "pyear": focal_year,
                    "prior_five_year_window_years": PRIOR_FIVE_YEAR_WINDOW_YEARS,
                    "candidate_pair_source": "prior_undirected_cui_pair",
                    "candidate_unit": "unique_directed_triple",
                }
            )
            total_candidates += 1

            if len(rows) >= WRITE_BUFFER_SIZE:
                wrote_header = flush_rows(rows, output_file, wrote_header)
                print(f"Wrote {total_candidates:,} candidate triples so far.")

    if total_candidates == 0:
        empty_candidate_frame().to_csv(output_file, index=False, compression="gzip")
        print(f"No candidate triples found. Empty file saved to {output_file}")
        return

    wrote_header = flush_rows(rows, output_file, wrote_header)
    print(f"Saved {total_candidates:,} candidate triples to {output_file}")


def main() -> None:
    focal_year = get_focal_year()
    print(f"Starting BiomedBERT new-relation candidate build for {focal_year}.")
    build_candidate_triples(focal_year)
    print(f"Finished BiomedBERT new-relation candidate build for {focal_year}.")


if __name__ == "__main__":
    main()
