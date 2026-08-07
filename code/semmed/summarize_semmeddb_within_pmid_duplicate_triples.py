"""Summarize repeated SemMedDB triples within the same PMID.

The script reads the filtered yearly SemMedDB files one year at a time. It
compares exact triples built from the original CUI fields with exact triples
built from the project's primary-CUI fields. This separates genuine repeated
predication records from repetitions created when pipe-delimited identifiers
are reduced to their first value.

No predication data are changed. The outputs are small diagnostic tables for
deciding whether a later preparation step should retain only one occurrence of
each exact triple within a PMID.
"""

from __future__ import annotations

import gc
import re
from pathlib import Path

import pandas as pd


INPUT_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/semmedVER43_R/"
    "split_predications_with_pyear_filtered_by_pyear"
)
OUTPUT_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/results/semmed/"
    "within_pmid_duplicate_triples"
)

INPUT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
SUMMARY_OUTPUT_FILE = OUTPUT_DIR / "semmeddb_within_pmid_duplicate_summary.csv"
GROUP_SIZE_OUTPUT_FILE = (
    OUTPUT_DIR / "semmeddb_within_pmid_duplicate_group_size_distribution.csv"
)
COLLISION_EXAMPLE_OUTPUT_FILE = (
    OUTPUT_DIR / "semmeddb_primary_cui_collision_examples.csv"
)

OVERWRITE = False
MAX_COLLISION_GROUPS_PER_YEAR_SCOPE = 100

PMID_COLUMN = "PMID"
SENTENCE_ID_COLUMN = "SENTENCE_ID"
PREDICATE_COLUMN = "PREDICATE"
RAW_SUBJECT_CUI_COLUMN = "SUBJECT_CUI"
RAW_OBJECT_CUI_COLUMN = "OBJECT_CUI"
PRIMARY_SUBJECT_CUI_COLUMN = "subject_cui_primary"
PRIMARY_OBJECT_CUI_COLUMN = "object_cui_primary"

USE_COLUMNS = [
    PMID_COLUMN,
    SENTENCE_ID_COLUMN,
    PREDICATE_COLUMN,
    RAW_SUBJECT_CUI_COLUMN,
    RAW_OBJECT_CUI_COLUMN,
    PRIMARY_SUBJECT_CUI_COLUMN,
    PRIMARY_OBJECT_CUI_COLUMN,
]

RAW_PMID_TRIPLE_COLUMNS = [
    PMID_COLUMN,
    RAW_SUBJECT_CUI_COLUMN,
    PREDICATE_COLUMN,
    RAW_OBJECT_CUI_COLUMN,
]
PRIMARY_PMID_TRIPLE_COLUMNS = [
    PMID_COLUMN,
    PRIMARY_SUBJECT_CUI_COLUMN,
    PREDICATE_COLUMN,
    PRIMARY_OBJECT_CUI_COLUMN,
]
PRIMARY_SENTENCE_TRIPLE_COLUMNS = [
    PMID_COLUMN,
    SENTENCE_ID_COLUMN,
    PRIMARY_SUBJECT_CUI_COLUMN,
    PREDICATE_COLUMN,
    PRIMARY_OBJECT_CUI_COLUMN,
]

COLLISION_EXAMPLE_COLUMNS = [
    "pyear",
    "scope",
    PMID_COLUMN,
    PRIMARY_SUBJECT_CUI_COLUMN,
    PREDICATE_COLUMN,
    PRIMARY_OBJECT_CUI_COLUMN,
    "n_distinct_raw_triples",
    RAW_SUBJECT_CUI_COLUMN,
    RAW_OBJECT_CUI_COLUMN,
    "n_predication_rows_for_raw_variant",
]


def safe_divide(numerator: int, denominator: int) -> float | pd.NA:
    if denominator == 0:
        return pd.NA
    return numerator / denominator


def clean_text_columns(data: pd.DataFrame) -> pd.DataFrame:
    for column in USE_COLUMNS:
        data[column] = data[column].astype("string").str.strip()
        data[column] = data[column].replace({"": pd.NA, r"\N": pd.NA})
    return data


def expected_primary_cui(raw_cui: pd.Series) -> pd.Series:
    return (
        raw_cui.astype("string")
        .str.split("|", regex=False)
        .str[0]
        .str.strip()
    )


def discover_yearly_files(input_dir: Path) -> list[tuple[int, Path]]:
    pattern = re.compile(
        rf"^{re.escape(INPUT_FILE_PREFIX)}_(\d{{4}})\.csv\.gz$"
    )
    yearly_files = []
    for path in input_dir.glob(f"{INPUT_FILE_PREFIX}_*.csv.gz"):
        match = pattern.match(path.name)
        if match:
            yearly_files.append((int(match.group(1)), path))

    yearly_files.sort()
    if not yearly_files:
        raise FileNotFoundError(
            f"No yearly filtered SemMedDB files found in {input_dir}."
        )
    return yearly_files


def check_outputs(output_files: list[Path]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    existing = [path for path in output_files if path.exists()]
    if existing and not OVERWRITE:
        examples = "\n".join(str(path) for path in existing)
        raise FileExistsError(
            "Diagnostic output file(s) already exist. Set OVERWRITE = True to "
            f"replace them:\n{examples}"
        )
    if OVERWRITE:
        for path in existing:
            path.unlink()


def group_metrics(group_sizes: pd.Series, n_scope_rows: int) -> dict[str, object]:
    duplicate_sizes = group_sizes[group_sizes > 1]
    n_rows_in_duplicate_groups = int(duplicate_sizes.sum())
    return {
        "n_unique_groups": int(len(group_sizes)),
        "n_duplicate_groups": int(len(duplicate_sizes)),
        "n_rows_in_duplicate_groups": n_rows_in_duplicate_groups,
        "n_excess_duplicate_rows": int((duplicate_sizes - 1).sum()),
        "duplicate_row_share": safe_divide(
            n_rows_in_duplicate_groups,
            n_scope_rows,
        ),
        "max_rows_per_group": int(group_sizes.max()) if len(group_sizes) else 0,
    }


def add_prefixed_metrics(
    output: dict[str, object],
    prefix: str,
    metrics: dict[str, object],
) -> None:
    for name, value in metrics.items():
        output[f"{prefix}_{name}"] = value


def build_group_size_rows(
    pyear: int,
    scope_name: str,
    key_type: str,
    group_sizes: pd.Series,
) -> list[dict[str, object]]:
    rows = []
    distribution = group_sizes.value_counts().sort_index()
    for group_size, n_groups in distribution.items():
        rows.append(
            {
                "pyear": str(pyear),
                "scope": scope_name,
                "key_type": key_type,
                "group_size": int(group_size),
                "n_groups": int(n_groups),
                "n_predication_rows": int(group_size * n_groups),
            }
        )
    return rows


def build_collision_metrics_and_examples(
    data: pd.DataFrame,
    primary_group_sizes: pd.Series,
    pyear: int,
    scope_name: str,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    variant_columns = PRIMARY_PMID_TRIPLE_COLUMNS + [
        RAW_SUBJECT_CUI_COLUMN,
        RAW_OBJECT_CUI_COLUMN,
    ]
    raw_variant_sizes = (
        data.groupby(variant_columns, sort=False, observed=True)
        .size()
        .rename("n_predication_rows_for_raw_variant")
        .reset_index()
    )
    variant_counts = raw_variant_sizes.groupby(
        PRIMARY_PMID_TRIPLE_COLUMNS,
        sort=False,
        observed=True,
    ).size()
    collision_counts = variant_counts[variant_counts > 1]

    if collision_counts.empty:
        return (
            {
                "n_primary_groups_with_multiple_raw_triples": 0,
                "n_rows_in_primary_collision_groups": 0,
                "n_raw_triples_collapsed_by_primary": 0,
                "primary_collision_row_share": 0.0 if len(data) else pd.NA,
            },
            [],
        )

    collision_index = collision_counts.index
    n_rows_in_collision_groups = int(
        primary_group_sizes.reindex(collision_index).fillna(0).sum()
    )
    n_raw_triples_collapsed = int((collision_counts - 1).sum())
    metrics = {
        "n_primary_groups_with_multiple_raw_triples": int(len(collision_counts)),
        "n_rows_in_primary_collision_groups": n_rows_in_collision_groups,
        "n_raw_triples_collapsed_by_primary": n_raw_triples_collapsed,
        "primary_collision_row_share": safe_divide(
            n_rows_in_collision_groups,
            len(data),
        ),
    }

    top_collision_keys = (
        collision_counts.sort_values(ascending=False)
        .head(MAX_COLLISION_GROUPS_PER_YEAR_SCOPE)
        .rename("n_distinct_raw_triples")
        .reset_index()
    )
    examples = raw_variant_sizes.merge(
        top_collision_keys,
        on=PRIMARY_PMID_TRIPLE_COLUMNS,
        how="inner",
        validate="many_to_one",
    )
    examples.insert(0, "scope", scope_name)
    examples.insert(0, "pyear", str(pyear))
    examples = examples[COLLISION_EXAMPLE_COLUMNS]
    return metrics, examples.to_dict("records")


def summarize_scope(
    data: pd.DataFrame,
    pyear: int,
    scope_name: str,
) -> tuple[dict[str, object], list[dict[str, object]], list[dict[str, object]]]:
    n_scope_rows = len(data)
    summary: dict[str, object] = {
        "pyear": str(pyear),
        "scope": scope_name,
        "n_scope_predication_rows": n_scope_rows,
        "n_unique_pmids": int(data[PMID_COLUMN].nunique()),
        "n_unique_sentences": int(
            data[[PMID_COLUMN, SENTENCE_ID_COLUMN]].dropna().drop_duplicates().shape[0]
        ),
        "n_rows_missing_sentence_id": int(data[SENTENCE_ID_COLUMN].isna().sum()),
        "n_compound_subject_cui_rows": int(
            data[RAW_SUBJECT_CUI_COLUMN].str.contains("|", regex=False, na=False).sum()
        ),
        "n_compound_object_cui_rows": int(
            data[RAW_OBJECT_CUI_COLUMN].str.contains("|", regex=False, na=False).sum()
        ),
        "n_subject_primary_mismatch": int(
            (
                expected_primary_cui(data[RAW_SUBJECT_CUI_COLUMN])
                != data[PRIMARY_SUBJECT_CUI_COLUMN]
            ).sum()
        ),
        "n_object_primary_mismatch": int(
            (
                expected_primary_cui(data[RAW_OBJECT_CUI_COLUMN])
                != data[PRIMARY_OBJECT_CUI_COLUMN]
            ).sum()
        ),
        "n_invalid_subject_primary_cui_format": int(
            (~data[PRIMARY_SUBJECT_CUI_COLUMN].str.fullmatch(r"C\d{7,}", na=False)).sum()
        ),
        "n_invalid_object_primary_cui_format": int(
            (~data[PRIMARY_OBJECT_CUI_COLUMN].str.fullmatch(r"C\d{7,}", na=False)).sum()
        ),
    }

    group_size_rows = []

    raw_group_sizes = data.groupby(
        RAW_PMID_TRIPLE_COLUMNS,
        sort=False,
        observed=True,
    ).size()
    add_prefixed_metrics(
        summary,
        "raw_pmid_triple",
        group_metrics(raw_group_sizes, n_scope_rows),
    )
    group_size_rows.extend(
        build_group_size_rows(
            pyear,
            scope_name,
            "raw_cui_exact_triple_within_pmid",
            raw_group_sizes,
        )
    )
    del raw_group_sizes
    gc.collect()

    primary_group_sizes = data.groupby(
        PRIMARY_PMID_TRIPLE_COLUMNS,
        sort=False,
        observed=True,
    ).size()
    add_prefixed_metrics(
        summary,
        "primary_pmid_triple",
        group_metrics(primary_group_sizes, n_scope_rows),
    )
    group_size_rows.extend(
        build_group_size_rows(
            pyear,
            scope_name,
            "primary_cui_exact_triple_within_pmid",
            primary_group_sizes,
        )
    )

    sentence_data = data.dropna(subset=[SENTENCE_ID_COLUMN])
    primary_sentence_group_sizes = sentence_data.groupby(
        PRIMARY_SENTENCE_TRIPLE_COLUMNS,
        sort=False,
        observed=True,
    ).size()
    add_prefixed_metrics(
        summary,
        "primary_sentence_triple",
        group_metrics(primary_sentence_group_sizes, len(sentence_data)),
    )
    group_size_rows.extend(
        build_group_size_rows(
            pyear,
            scope_name,
            "primary_cui_exact_triple_within_sentence",
            primary_sentence_group_sizes,
        )
    )
    del sentence_data, primary_sentence_group_sizes
    gc.collect()

    collision_metrics, collision_examples = build_collision_metrics_and_examples(
        data,
        primary_group_sizes,
        pyear,
        scope_name,
    )
    summary.update(collision_metrics)
    del primary_group_sizes
    gc.collect()
    return summary, group_size_rows, collision_examples


def add_overall_summary(summary: pd.DataFrame) -> pd.DataFrame:
    rows = [summary]
    share_columns = [column for column in summary if column.endswith("_share")]
    max_columns = [column for column in summary if column.startswith("max_")]
    max_columns.extend(
        column for column in summary if column.endswith("_max_rows_per_group")
    )
    max_columns = sorted(set(max_columns))

    for scope_name, scope_data in summary.groupby("scope", sort=False):
        overall: dict[str, object] = {"pyear": "ALL", "scope": scope_name}
        for column in summary.columns:
            if column in {"pyear", "scope"} or column in share_columns:
                continue
            if column in max_columns:
                overall[column] = scope_data[column].max()
            else:
                overall[column] = scope_data[column].sum()

        denominator = int(overall["n_scope_predication_rows"])
        for prefix in [
            "raw_pmid_triple",
            "primary_pmid_triple",
        ]:
            overall[f"{prefix}_duplicate_row_share"] = safe_divide(
                int(overall[f"{prefix}_n_rows_in_duplicate_groups"]),
                denominator,
            )

        sentence_denominator = denominator - int(overall["n_rows_missing_sentence_id"])
        overall["primary_sentence_triple_duplicate_row_share"] = safe_divide(
            int(overall["primary_sentence_triple_n_rows_in_duplicate_groups"]),
            sentence_denominator,
        )
        overall["primary_collision_row_share"] = safe_divide(
            int(overall["n_rows_in_primary_collision_groups"]),
            denominator,
        )
        rows.append(pd.DataFrame([overall], columns=summary.columns))

    return pd.concat(rows, ignore_index=True)


def add_overall_group_size_distribution(distribution: pd.DataFrame) -> pd.DataFrame:
    overall = (
        distribution.groupby(
            ["scope", "key_type", "group_size"],
            as_index=False,
            sort=True,
        )[["n_groups", "n_predication_rows"]]
        .sum()
    )
    overall.insert(0, "pyear", "ALL")
    return pd.concat([distribution, overall], ignore_index=True)


def process_yearly_file(
    pyear: int,
    input_file: Path,
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    print(f"Reading {input_file}")
    data = pd.read_csv(
        input_file,
        compression="gzip",
        usecols=USE_COLUMNS,
        dtype={column: "string" for column in USE_COLUMNS},
    )
    data = clean_text_columns(data)
    n_input_rows = len(data)

    required = [
        PMID_COLUMN,
        PREDICATE_COLUMN,
        RAW_SUBJECT_CUI_COLUMN,
        RAW_OBJECT_CUI_COLUMN,
        PRIMARY_SUBJECT_CUI_COLUMN,
        PRIMARY_OBJECT_CUI_COLUMN,
    ]
    valid = data.dropna(subset=required).copy()
    invalid_rows = n_input_rows - len(valid)
    del data
    gc.collect()
    print(
        f"Year {pyear}: read {n_input_rows:,} rows; "
        f"valid for triple diagnostics {len(valid):,}; invalid {invalid_rows:,}."
    )

    summary_rows = []
    group_size_rows = []
    collision_example_rows = []
    scope_names = ["all_valid_predications", "non_self_loop_predications"]
    for scope_name in scope_names:
        if scope_name == "all_valid_predications":
            scope_data = valid
        else:
            scope_data = valid[
                valid[PRIMARY_SUBJECT_CUI_COLUMN]
                != valid[PRIMARY_OBJECT_CUI_COLUMN]
            ].copy()

        summary, distribution, examples = summarize_scope(
            scope_data,
            pyear,
            scope_name,
        )
        summary["n_input_predication_rows"] = n_input_rows
        summary["n_invalid_triple_rows"] = invalid_rows
        summary["n_self_loop_rows_excluded_from_scope"] = (
            len(valid) - len(scope_data)
        )
        summary_rows.append(summary)
        group_size_rows.extend(distribution)
        collision_example_rows.extend(examples)

        if scope_name != "all_valid_predications":
            del scope_data
        gc.collect()

    del valid
    gc.collect()
    return summary_rows, group_size_rows, collision_example_rows


def main() -> None:
    output_files = [
        SUMMARY_OUTPUT_FILE,
        GROUP_SIZE_OUTPUT_FILE,
        COLLISION_EXAMPLE_OUTPUT_FILE,
    ]
    yearly_files = discover_yearly_files(INPUT_DIR)
    check_outputs(output_files)

    print(f"Found {len(yearly_files):,} yearly SemMedDB files.")
    all_summary_rows = []
    all_group_size_rows = []
    all_collision_example_rows = []

    for pyear, input_file in yearly_files:
        summary_rows, group_size_rows, collision_examples = process_yearly_file(
            pyear,
            input_file,
        )
        all_summary_rows.extend(summary_rows)
        all_group_size_rows.extend(group_size_rows)
        all_collision_example_rows.extend(collision_examples)

    summary = pd.DataFrame(all_summary_rows)
    summary = add_overall_summary(summary)
    summary.to_csv(SUMMARY_OUTPUT_FILE, index=False)

    group_size_distribution = pd.DataFrame(all_group_size_rows)
    group_size_distribution = add_overall_group_size_distribution(
        group_size_distribution
    )
    group_size_distribution.to_csv(GROUP_SIZE_OUTPUT_FILE, index=False)

    collision_examples = pd.DataFrame(
        all_collision_example_rows,
        columns=COLLISION_EXAMPLE_COLUMNS,
    )
    collision_examples.to_csv(COLLISION_EXAMPLE_OUTPUT_FILE, index=False)

    print(f"Saved summary to {SUMMARY_OUTPUT_FILE}")
    print(f"Saved group-size distribution to {GROUP_SIZE_OUTPUT_FILE}")
    print(f"Saved primary-CUI collision examples to {COLLISION_EXAMPLE_OUTPUT_FILE}")


if __name__ == "__main__":
    main()
