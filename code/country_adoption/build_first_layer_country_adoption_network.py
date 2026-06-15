"""Build first-layer country adoption networks from SemMedDB edge transitions.

The network direction is adopter country -> source country. Source countries are
identified from focal-year predications, while adopter countries are identified
from future five-year predications that re-use the focal knowledge element.

The matching unit follows the first-layer annotation:

1. New_Node_Combination and New_Combination use undirected CUI-CUI combinations.
2. New_Relation uses directed subject-predicate-object relations.

Repeated_Triple is not included in the adoption network because it describes
continuation of an existing relation rather than adoption of new knowledge.
"""

from __future__ import annotations

import os
import re
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError


PROJECT_INTERIM_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim"
)
OPENALEX_DIR = Path(
    "/xdisk/sebratt/jinyugao/data/products/openalex/flattened_snapshot_2025"
)

SOURCE_TRANSITION_COUNTRY_DIR = (
    PROJECT_INTERIM_DIR
    / "country_adoption/first_layer_edge_transition_pmid_country_full_counting"
)
SPLIT_PREDICATION_DIR = (
    PROJECT_INTERIM_DIR / "semmedVER43_R/split_predications_with_pyear_filtered_by_pyear"
)
COUNTRY_FILE = OPENALEX_DIR / "openalex_pmid_country_full_counting.csv.gz"
OUTPUT_DIR = PROJECT_INTERIM_DIR / "country_adoption/first_layer_country_adoption_network"
SUMMARY_DIR = OUTPUT_DIR / "summary"

SOURCE_FILE_PREFIX = (
    "semmedVER43_R_predications_with_pyear_filtered_"
    "first_layer_edge_transition_pmid_country_full_counting"
)
SPLIT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
OUTPUT_FILE_PREFIX = "first_layer_country_adoption_network"
SUMMARY_FILE_PREFIX = "first_layer_country_adoption_network_summary"

BASE_YEAR = 1980
N_YEARS = 40
FUTURE_WINDOW_YEARS = 5
CHUNK_SIZE = 100_000
OVERWRITE = False

SUBJECT_CUI_COLUMN = "subject_cui_primary"
OBJECT_CUI_COLUMN = "object_cui_primary"
PREDICATE_COLUMN = "PREDICATE"
PMID_COLUMN = "PMID"
PREDICATION_ID_COLUMN = "PREDICATION_ID"
PMID_NORMALIZED_COLUMN = "pmid_normalized"
ANNOTATION_COLUMN = "first_layer_edge_annotation"
TRANSITION_COLUMN = "first_layer_future_five_year_transition"
COUNTRY_CODE_COLUMN = "institution_country_code"
COUNTRY_NAME_COLUMN = "institution_country"
MATCH_KEY_COLUMN = "adoption_match_key"

CATEGORY_NEW_NODE = "New_Node_Combination"
CATEGORY_NEW_COMBINATION = "New_Combination"
CATEGORY_NEW_RELATION = "New_Relation"
POOLED_CATEGORY = "Pooled_New_Knowledge"
TRANSITION_ADOPTED = "Adopted"

ADOPTION_CATEGORIES = [
    CATEGORY_NEW_NODE,
    CATEGORY_NEW_COMBINATION,
    CATEGORY_NEW_RELATION,
]
COMBINATION_CATEGORIES = {CATEGORY_NEW_NODE, CATEGORY_NEW_COMBINATION}
OUTPUT_CATEGORY_SLUGS = {
    CATEGORY_NEW_NODE: "new_node_combination",
    CATEGORY_NEW_COMBINATION: "new_combination",
    CATEGORY_NEW_RELATION: "new_relation",
    POOLED_CATEGORY: "pooled_new_knowledge",
}

COUNTRY_COLUMNS = [
    "pmid",
    COUNTRY_CODE_COLUMN,
    COUNTRY_NAME_COLUMN,
]
SOURCE_USE_COLUMNS = [
    SUBJECT_CUI_COLUMN,
    OBJECT_CUI_COLUMN,
    PREDICATE_COLUMN,
    ANNOTATION_COLUMN,
    TRANSITION_COLUMN,
    COUNTRY_CODE_COLUMN,
    COUNTRY_NAME_COLUMN,
]
FUTURE_USE_COLUMNS = [
    PREDICATION_ID_COLUMN,
    PMID_COLUMN,
    SUBJECT_CUI_COLUMN,
    OBJECT_CUI_COLUMN,
    PREDICATE_COLUMN,
]
NETWORK_COLUMNS = [
    "focal_year",
    "edge_annotation",
    "adopter_country_code",
    "adopter_country",
    "source_country_code",
    "source_country",
    "weight_predication_rows",
    "weight_unique_future_pmids",
]


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


def normalize_pmid(value: object) -> str:
    text = normalize_value(value)
    if not text:
        return ""

    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]

    match = re.search(r"(\d+)(?:/)?$", text)
    if match:
        return match.group(1)

    return text


def source_file_for_year(focal_year: int) -> Path:
    return SOURCE_TRANSITION_COUNTRY_DIR / f"{SOURCE_FILE_PREFIX}_{focal_year}.csv.gz"


def split_file_for_year(year: int) -> Path:
    return SPLIT_PREDICATION_DIR / f"{SPLIT_FILE_PREFIX}_{year}.csv.gz"


def output_file_for_category(focal_year: int, category: str) -> Path:
    slug = OUTPUT_CATEGORY_SLUGS[category]
    return OUTPUT_DIR / slug / f"{OUTPUT_FILE_PREFIX}_{slug}_{focal_year}.csv.gz"


def summary_file_for_year(focal_year: int) -> Path:
    return SUMMARY_DIR / f"{SUMMARY_FILE_PREFIX}_{focal_year}.csv"


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


def future_years_for_focal_year(focal_year: int) -> range:
    return range(focal_year + 1, focal_year + FUTURE_WINDOW_YEARS + 1)


def future_file_window_stats(focal_year: int) -> dict[str, object]:
    found_years = []
    missing_years = []
    for future_year in future_years_for_focal_year(focal_year):
        if split_file_for_year(future_year).exists():
            found_years.append(future_year)
        else:
            missing_years.append(future_year)

    return {
        "future_years_found": ";".join(str(year) for year in found_years),
        "future_years_missing": ";".join(str(year) for year in missing_years),
        "n_future_predication_rows_scanned": 0,
        "matched_future_predication_rows_by_category": {},
    }


def make_pair_key(subject_cui: object, object_cui: object) -> str:
    subject = normalize_value(subject_cui)
    obj = normalize_value(object_cui)
    if not subject or not obj or subject == obj:
        return ""
    node_a, node_b = sorted((subject, obj))
    return f"{node_a}\t{node_b}"


def make_relation_key(subject_cui: object, predicate: object, object_cui: object) -> str:
    subject = normalize_value(subject_cui)
    pred = normalize_value(predicate)
    obj = normalize_value(object_cui)
    if not subject or not pred or not obj or subject == obj:
        return ""
    return f"{subject}\t{pred}\t{obj}"


def add_match_keys(chunk: pd.DataFrame) -> pd.DataFrame:
    output = chunk.copy()
    output["undirected_cui_cui_combination_key"] = [
        make_pair_key(subject_cui, object_cui)
        for subject_cui, object_cui in zip(
            output[SUBJECT_CUI_COLUMN], output[OBJECT_CUI_COLUMN]
        )
    ]
    output["directed_semantic_relation_key"] = [
        make_relation_key(subject_cui, predicate, object_cui)
        for subject_cui, predicate, object_cui in zip(
            output[SUBJECT_CUI_COLUMN],
            output[PREDICATE_COLUMN],
            output[OBJECT_CUI_COLUMN],
        )
    ]
    return output


def load_pmid_country_table(country_file: Path) -> pd.DataFrame:
    country = pd.read_csv(
        country_file,
        compression="gzip",
        usecols=COUNTRY_COLUMNS,
        dtype="string",
    )
    country = country.rename(
        columns={
            "pmid": PMID_NORMALIZED_COLUMN,
            COUNTRY_CODE_COLUMN: "adopter_country_code",
            COUNTRY_NAME_COLUMN: "adopter_country",
        }
    )
    country[PMID_NORMALIZED_COLUMN] = country[PMID_NORMALIZED_COLUMN].map(
        normalize_pmid
    )
    country["adopter_country_code"] = country["adopter_country_code"].map(
        normalize_value
    )
    country["adopter_country"] = country["adopter_country"].map(normalize_value)
    country = country[
        (country[PMID_NORMALIZED_COLUMN] != "")
        & (country["adopter_country_code"] != "")
    ].copy()
    country = country.drop_duplicates(
        subset=[PMID_NORMALIZED_COLUMN, "adopter_country_code"]
    )
    print(f"Loaded future PMID-country rows: {len(country):,}.")
    return country


def build_source_country_table(source_file: Path) -> pd.DataFrame:
    source_chunks = []
    total_rows = 0
    adopted_rows = 0

    reader = pd.read_csv(
        source_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        usecols=SOURCE_USE_COLUMNS,
        dtype="string",
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        total_rows += len(chunk)
        chunk = chunk.copy()
        chunk[ANNOTATION_COLUMN] = chunk[ANNOTATION_COLUMN].map(normalize_value)
        chunk[TRANSITION_COLUMN] = chunk[TRANSITION_COLUMN].map(normalize_value)
        chunk[COUNTRY_CODE_COLUMN] = chunk[COUNTRY_CODE_COLUMN].map(normalize_value)
        chunk[COUNTRY_NAME_COLUMN] = chunk[COUNTRY_NAME_COLUMN].map(normalize_value)

        chunk = chunk[
            chunk[ANNOTATION_COLUMN].isin(ADOPTION_CATEGORIES)
            & (chunk[TRANSITION_COLUMN] == TRANSITION_ADOPTED)
            & (chunk[COUNTRY_CODE_COLUMN] != "")
        ].copy()
        adopted_rows += len(chunk)
        if chunk.empty:
            print(
                f"Source chunk {chunk_number:,}: no adopted source-country rows."
            )
            continue

        chunk = add_match_keys(chunk)
        chunk[MATCH_KEY_COLUMN] = ""
        combination_mask = chunk[ANNOTATION_COLUMN].isin(COMBINATION_CATEGORIES)
        relation_mask = chunk[ANNOTATION_COLUMN] == CATEGORY_NEW_RELATION
        chunk.loc[combination_mask, MATCH_KEY_COLUMN] = chunk.loc[
            combination_mask, "undirected_cui_cui_combination_key"
        ]
        chunk.loc[relation_mask, MATCH_KEY_COLUMN] = chunk.loc[
            relation_mask, "directed_semantic_relation_key"
        ]
        chunk = chunk[chunk[MATCH_KEY_COLUMN] != ""].copy()
        if chunk.empty:
            continue

        chunk = chunk.rename(
            columns={
                ANNOTATION_COLUMN: "edge_annotation",
                COUNTRY_CODE_COLUMN: "source_country_code",
                COUNTRY_NAME_COLUMN: "source_country",
            }
        )
        source_chunks.append(
            chunk[
                [
                    "edge_annotation",
                    MATCH_KEY_COLUMN,
                    "source_country_code",
                    "source_country",
                ]
            ].drop_duplicates()
        )

        print(
            f"Source chunk {chunk_number:,}: read {total_rows:,} rows total; "
            f"kept {adopted_rows:,} adopted source-country rows so far."
        )

    if not source_chunks:
        return pd.DataFrame(
            columns=[
                "edge_annotation",
                MATCH_KEY_COLUMN,
                "source_country_code",
                "source_country",
            ]
        )

    source = pd.concat(source_chunks, ignore_index=True).drop_duplicates()
    source = (
        source.sort_values(
            ["edge_annotation", MATCH_KEY_COLUMN, "source_country_code", "source_country"]
        )
        .drop_duplicates(
            subset=["edge_annotation", MATCH_KEY_COLUMN, "source_country_code"],
            keep="first",
        )
        .reset_index(drop=True)
    )

    print(f"Total source transition-country rows read: {total_rows:,}.")
    print(f"Adopted source transition-country rows kept: {adopted_rows:,}.")
    print(f"Unique source knowledge-country rows: {len(source):,}.")
    print(
        "Unique source match keys by annotation: "
        f"{source.groupby('edge_annotation')[MATCH_KEY_COLUMN].nunique().to_dict()}"
    )
    return source


def build_source_key_sets(
    source: pd.DataFrame,
) -> dict[str, set[str]]:
    return {
        category: set(
            source.loc[source["edge_annotation"] == category, MATCH_KEY_COLUMN]
        )
        for category in ADOPTION_CATEGORIES
    }


def build_future_matches(
    future_chunk: pd.DataFrame,
    source_key_sets: dict[str, set[str]],
) -> pd.DataFrame:
    chunk = add_match_keys(future_chunk)
    chunk[PMID_NORMALIZED_COLUMN] = chunk[PMID_COLUMN].map(normalize_pmid)
    chunk[PREDICATION_ID_COLUMN] = chunk[PREDICATION_ID_COLUMN].map(normalize_value)

    match_frames = []
    base_columns = [
        PREDICATION_ID_COLUMN,
        PMID_COLUMN,
        PMID_NORMALIZED_COLUMN,
        MATCH_KEY_COLUMN,
        "edge_annotation",
    ]

    for category in [CATEGORY_NEW_NODE, CATEGORY_NEW_COMBINATION]:
        keys = source_key_sets.get(category, set())
        if not keys:
            continue
        matches = chunk[
            chunk["undirected_cui_cui_combination_key"].isin(keys)
        ].copy()
        if matches.empty:
            continue
        matches[MATCH_KEY_COLUMN] = matches["undirected_cui_cui_combination_key"]
        matches["edge_annotation"] = category
        match_frames.append(matches[base_columns])

    relation_keys = source_key_sets.get(CATEGORY_NEW_RELATION, set())
    if relation_keys:
        matches = chunk[
            chunk["directed_semantic_relation_key"].isin(relation_keys)
        ].copy()
        if not matches.empty:
            matches[MATCH_KEY_COLUMN] = matches["directed_semantic_relation_key"]
            matches["edge_annotation"] = CATEGORY_NEW_RELATION
            match_frames.append(matches[base_columns])

    if not match_frames:
        return pd.DataFrame(columns=base_columns)

    return pd.concat(match_frames, ignore_index=True)


def update_network_counts(
    edges: pd.DataFrame,
    predication_row_counts: Counter[tuple[str, str, str]],
    future_pmid_sets: defaultdict[tuple[str, str, str], set[str]],
) -> None:
    if edges.empty:
        return

    group_columns = [
        "edge_annotation",
        "adopter_country_code",
        "adopter_country",
        "source_country_code",
        "source_country",
    ]
    for group_key, group in edges.groupby(group_columns, dropna=False):
        edge_annotation = normalize_value(group_key[0])
        adopter_country_code = normalize_value(group_key[1])
        adopter_country = normalize_value(group_key[2])
        source_country_code = normalize_value(group_key[3])
        source_country = normalize_value(group_key[4])
        key = (
            edge_annotation,
            adopter_country_code,
            adopter_country,
            source_country_code,
            source_country,
        )
        predication_row_counts[key] += len(group)
        future_pmid_sets[key].update(
            pmid for pmid in group[PMID_NORMALIZED_COLUMN].dropna().astype(str) if pmid
        )


def process_future_years(
    focal_year: int,
    source: pd.DataFrame,
    country: pd.DataFrame,
) -> tuple[
    Counter[tuple[str, str, str, str, str]],
    defaultdict[tuple[str, str, str, str, str], set[str]],
    dict[str, object],
]:
    source_key_sets = build_source_key_sets(source)
    source_match_columns = [
        "edge_annotation",
        MATCH_KEY_COLUMN,
        "source_country_code",
        "source_country",
    ]
    predication_row_counts: Counter[tuple[str, str, str, str, str]] = Counter()
    future_pmid_sets: defaultdict[tuple[str, str, str, str, str], set[str]] = defaultdict(
        set
    )
    matched_predication_rows_by_category: Counter[str] = Counter()
    future_rows_scanned = 0
    future_files_found = []
    future_files_missing = []

    for future_year in future_years_for_focal_year(focal_year):
        future_file = split_file_for_year(future_year)
        if not future_file.exists():
            future_files_missing.append(future_year)
            continue
        future_files_found.append(future_year)

        try:
            reader = pd.read_csv(
                future_file,
                compression="gzip",
                chunksize=CHUNK_SIZE,
                usecols=FUTURE_USE_COLUMNS,
                dtype="string",
            )
        except EmptyDataError:
            continue

        for chunk_number, chunk in enumerate(reader, start=1):
            future_rows_scanned += len(chunk)
            future_matches = build_future_matches(chunk, source_key_sets)
            if future_matches.empty:
                continue

            for category, count in future_matches["edge_annotation"].value_counts().items():
                matched_predication_rows_by_category[str(category)] += int(count)

            future_matches = future_matches.merge(
                country,
                how="inner",
                on=PMID_NORMALIZED_COLUMN,
                validate="many_to_many",
            )
            if future_matches.empty:
                continue

            edges = future_matches.merge(
                source[source_match_columns],
                how="inner",
                on=["edge_annotation", MATCH_KEY_COLUMN],
                validate="many_to_many",
            )
            if edges.empty:
                continue

            update_network_counts(edges, predication_row_counts, future_pmid_sets)
            print(
                f"Future year {future_year}, chunk {chunk_number:,}: "
                f"matched edges {len(edges):,}."
            )

    if not future_files_found:
        raise FileNotFoundError(
            f"No future-year files found for focal year {focal_year} in "
            f"{SPLIT_PREDICATION_DIR}."
        )

    stats = {
        "future_years_found": ";".join(str(year) for year in future_files_found),
        "future_years_missing": ";".join(str(year) for year in future_files_missing),
        "n_future_predication_rows_scanned": future_rows_scanned,
        "matched_future_predication_rows_by_category": dict(
            matched_predication_rows_by_category
        ),
    }
    return predication_row_counts, future_pmid_sets, stats


def counts_to_dataframe(
    focal_year: int,
    predication_row_counts: Counter[tuple[str, str, str, str, str]],
    future_pmid_sets: defaultdict[tuple[str, str, str, str, str], set[str]],
) -> pd.DataFrame:
    rows = []
    for key in sorted(predication_row_counts):
        (
            edge_annotation,
            adopter_country_code,
            adopter_country,
            source_country_code,
            source_country,
        ) = key
        rows.append(
            {
                "focal_year": focal_year,
                "edge_annotation": edge_annotation,
                "adopter_country_code": adopter_country_code,
                "adopter_country": adopter_country,
                "source_country_code": source_country_code,
                "source_country": source_country,
                "weight_predication_rows": predication_row_counts[key],
                "weight_unique_future_pmids": len(future_pmid_sets[key]),
            }
        )
    return pd.DataFrame(rows, columns=NETWORK_COLUMNS)


def build_pooled_network_from_counts(
    focal_year: int,
    predication_row_counts: Counter[tuple[str, str, str, str, str]],
    future_pmid_sets: defaultdict[tuple[str, str, str, str, str], set[str]],
) -> pd.DataFrame:
    pooled_predication_row_counts: Counter[tuple[str, str, str, str]] = Counter()
    pooled_future_pmid_sets: defaultdict[tuple[str, str, str, str], set[str]] = (
        defaultdict(set)
    )

    for key, count in predication_row_counts.items():
        (
            _edge_annotation,
            adopter_country_code,
            adopter_country,
            source_country_code,
            source_country,
        ) = key
        pooled_key = (
            adopter_country_code,
            adopter_country,
            source_country_code,
            source_country,
        )
        pooled_predication_row_counts[pooled_key] += count
        pooled_future_pmid_sets[pooled_key].update(future_pmid_sets[key])

    rows = []
    for key in sorted(pooled_predication_row_counts):
        (
            adopter_country_code,
            adopter_country,
            source_country_code,
            source_country,
        ) = key
        rows.append(
            {
                "focal_year": focal_year,
                "edge_annotation": POOLED_CATEGORY,
                "adopter_country_code": adopter_country_code,
                "adopter_country": adopter_country,
                "source_country_code": source_country_code,
                "source_country": source_country,
                "weight_predication_rows": pooled_predication_row_counts[key],
                "weight_unique_future_pmids": len(pooled_future_pmid_sets[key]),
            }
        )

    return pd.DataFrame(rows, columns=NETWORK_COLUMNS)


def write_network_outputs(
    focal_year: int,
    network: pd.DataFrame,
    pooled_network: pd.DataFrame,
) -> dict[str, int]:
    output_rows = {}
    outputs = {
        category: output_file_for_category(focal_year, category)
        for category in ADOPTION_CATEGORIES
    }
    pooled_output = output_file_for_category(focal_year, POOLED_CATEGORY)
    outputs[POOLED_CATEGORY] = pooled_output

    for output_file in outputs.values():
        check_output(output_file)

    for category in ADOPTION_CATEGORIES:
        category_network = network[network["edge_annotation"] == category].copy()
        output_file = outputs[category]
        category_network.to_csv(output_file, index=False, compression="gzip")
        output_rows[category] = len(category_network)
        print(f"Saved {category} network to {output_file}")

    pooled_network.to_csv(pooled_output, index=False, compression="gzip")
    output_rows[POOLED_CATEGORY] = len(pooled_network)
    print(f"Saved pooled new-knowledge network to {pooled_output}")
    return output_rows


def write_summary(
    focal_year: int,
    source_file: Path,
    summary_file: Path,
    source: pd.DataFrame,
    network: pd.DataFrame,
    pooled_network: pd.DataFrame,
    output_rows: dict[str, int],
    future_stats: dict[str, object],
) -> None:
    check_output(summary_file)
    rows = []
    for category in [*ADOPTION_CATEGORIES, POOLED_CATEGORY]:
        if category == POOLED_CATEGORY:
            category_network = pooled_network
            n_source_match_keys = source[MATCH_KEY_COLUMN].nunique()
            n_source_country_rows = len(source)
        else:
            category_source = source[source["edge_annotation"] == category]
            category_network = network[network["edge_annotation"] == category]
            n_source_match_keys = category_source[MATCH_KEY_COLUMN].nunique()
            n_source_country_rows = len(category_source)

        rows.append(
            {
                "focal_year": focal_year,
                "network_category": category,
                "source_transition_country_file": str(source_file),
                "n_source_match_keys": n_source_match_keys,
                "n_source_knowledge_country_rows": n_source_country_rows,
                "n_country_edges": len(category_network),
                "total_weight_predication_rows": (
                    int(category_network["weight_predication_rows"].sum())
                    if not category_network.empty
                    else 0
                ),
                "total_weight_unique_future_pmids": (
                    int(category_network["weight_unique_future_pmids"].sum())
                    if not category_network.empty
                    else 0
                ),
                "n_output_rows": output_rows.get(category, 0),
                **future_stats,
            }
        )

    summary = pd.DataFrame(rows)
    summary.to_csv(summary_file, index=False)
    print(f"Saved country adoption network summary to {summary_file}")


def build_first_layer_country_adoption_network(focal_year: int) -> None:
    source_file = source_file_for_year(focal_year)
    summary_file = summary_file_for_year(focal_year)

    check_input(source_file)
    check_input(COUNTRY_FILE)

    source = build_source_country_table(source_file)
    if source.empty:
        print(
            f"No adopted source country rows found for focal year {focal_year}; "
            "writing empty country adoption networks."
        )
        network = counts_to_dataframe(focal_year, Counter(), defaultdict(set))
        pooled_network = build_pooled_network_from_counts(
            focal_year, Counter(), defaultdict(set)
        )
        output_rows = write_network_outputs(focal_year, network, pooled_network)
        write_summary(
            focal_year=focal_year,
            source_file=source_file,
            summary_file=summary_file,
            source=source,
            network=network,
            pooled_network=pooled_network,
            output_rows=output_rows,
            future_stats=future_file_window_stats(focal_year),
        )
        return

    country = load_pmid_country_table(COUNTRY_FILE)
    predication_row_counts, future_pmid_sets, future_stats = process_future_years(
        focal_year=focal_year,
        source=source,
        country=country,
    )
    network = counts_to_dataframe(
        focal_year=focal_year,
        predication_row_counts=predication_row_counts,
        future_pmid_sets=future_pmid_sets,
    )
    pooled_network = build_pooled_network_from_counts(
        focal_year=focal_year,
        predication_row_counts=predication_row_counts,
        future_pmid_sets=future_pmid_sets,
    )
    output_rows = write_network_outputs(focal_year, network, pooled_network)
    write_summary(
        focal_year=focal_year,
        source_file=source_file,
        summary_file=summary_file,
        source=source,
        network=network,
        pooled_network=pooled_network,
        output_rows=output_rows,
        future_stats=future_stats,
    )


def main() -> None:
    focal_year = get_focal_year()
    build_first_layer_country_adoption_network(focal_year)


if __name__ == "__main__":
    main()
