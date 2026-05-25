"""Build descriptive summary tables for raw and filtered SemMedDB predications."""

from __future__ import annotations

import gc
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd


INPUT_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/semmedVER43_R"
)
OUTPUT_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/results/semmed/descriptive_summary"
)

RAW_INPUT_FILE = INPUT_DIR / "semmedVER43_2024_R_predications_with_pyear.csv.gz"
FILTERED_INPUT_FILE = (
    INPUT_DIR / "semmedVER43_2024_R_predications_with_pyear_filtered.csv.gz"
)

CHUNK_SIZE = 250_000
OVERWRITE = False

BASE_COLUMNS = [
    "PREDICATION_ID",
    "PMID",
    "PREDICATE",
    "SUBJECT_CUI",
    "OBJECT_CUI",
    "PYEAR",
]
FILTERED_PRIMARY_COLUMNS = ["subject_cui_primary", "object_cui_primary"]


@dataclass
class OverallAgg:
    n_rows: int = 0
    pyears: set[str] = field(default_factory=set)
    pmids: set[str] = field(default_factory=set)
    predication_ids: set[str] = field(default_factory=set)
    predicates: set[str] = field(default_factory=set)
    subject_cuis: set[str] = field(default_factory=set)
    object_cuis: set[str] = field(default_factory=set)
    any_cuis: set[str] = field(default_factory=set)
    n_missing_pyear: int = 0
    n_missing_pmid: int = 0
    n_missing_predication_id: int = 0
    n_missing_predicate: int = 0
    n_missing_subject_cui_primary: int = 0
    n_missing_object_cui_primary: int = 0
    n_self_loop_rows: int = 0


@dataclass
class YearAgg:
    n_rows: int = 0
    pmids: set[str] = field(default_factory=set)
    predication_ids: set[str] = field(default_factory=set)
    predicates: set[str] = field(default_factory=set)
    subject_cuis: set[str] = field(default_factory=set)
    object_cuis: set[str] = field(default_factory=set)
    any_cuis: set[str] = field(default_factory=set)
    directed_triples: set[tuple[str, str, str]] = field(default_factory=set)
    directed_pairs: set[tuple[str, str]] = field(default_factory=set)
    undirected_pairs: set[tuple[str, str]] = field(default_factory=set)
    n_self_loop_rows: int = 0
    self_loop_pairs: set[tuple[str, str]] = field(default_factory=set)


@dataclass
class PredicateAgg:
    n_rows: int = 0
    pmids: set[str] = field(default_factory=set)
    subject_cuis: set[str] = field(default_factory=set)
    object_cuis: set[str] = field(default_factory=set)
    any_cuis: set[str] = field(default_factory=set)
    directed_triples: set[tuple[str, str, str]] = field(default_factory=set)
    directed_pairs: set[tuple[str, str]] = field(default_factory=set)
    undirected_pairs: set[tuple[str, str]] = field(default_factory=set)


def check_inputs() -> None:
    missing_files = [
        str(path)
        for path in [RAW_INPUT_FILE, FILTERED_INPUT_FILE]
        if not path.exists()
    ]
    if missing_files:
        missing = "\n".join(missing_files)
        raise FileNotFoundError(f"Missing required input file(s):\n{missing}")


def check_outputs() -> None:
    output_files = [
        OUTPUT_DIR / "semmeddb_overall_summary.csv",
        OUTPUT_DIR / "semmeddb_yearly_summary.csv",
        OUTPUT_DIR / "semmeddb_predicate_year_frequency.csv",
        OUTPUT_DIR / "semmeddb_filter_retention_by_year.csv",
        OUTPUT_DIR / "semmeddb_filter_retention_by_predicate.csv",
        OUTPUT_DIR / "semmeddb_cui_overlap_by_year_filtered.csv",
    ]
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    existing_files = [str(path) for path in output_files if path.exists()]
    if existing_files and not OVERWRITE:
        existing = "\n".join(existing_files)
        raise FileExistsError(
            "Output file(s) already exist. Set OVERWRITE = True to replace them:\n"
            f"{existing}"
        )

    if OVERWRITE:
        for path in output_files:
            if path.exists():
                path.unlink()


def normalize_text(value: object) -> str:
    return "" if pd.isna(value) else str(value).strip()


def clean_year(value: object) -> str:
    year_text = normalize_text(value)
    if year_text.endswith(".0"):
        year_text = year_text[:-2]
    return year_text


def parse_primary_cui(value: object) -> str:
    cui_text = normalize_text(value)
    if not cui_text:
        return ""
    return cui_text.split("|")[0].strip()


def normalize_undirected_pair(node_a: str, node_b: str) -> tuple[str, str]:
    return tuple(sorted((node_a, node_b)))


def safe_divide(numerator: int, denominator: int) -> float | pd.NA:
    if denominator == 0:
        return pd.NA
    return numerator / denominator


def sort_year_key(year: str) -> tuple[int, str]:
    try:
        return int(year), year
    except ValueError:
        return 999999, year


def get_usecols(input_file: Path, dataset_name: str) -> list[str]:
    columns = BASE_COLUMNS.copy()
    if dataset_name == "filtered":
        header = pd.read_csv(input_file, compression="gzip", nrows=0).columns
        for column in FILTERED_PRIMARY_COLUMNS:
            if column in header:
                columns.append(column)
    return columns


def standardize_chunk(chunk: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    standardized = chunk.copy()
    standardized["pyear_clean"] = standardized["PYEAR"].map(clean_year)
    standardized["predicate_clean"] = standardized["PREDICATE"].map(normalize_text)
    standardized["pmid_clean"] = standardized["PMID"].map(normalize_text)
    standardized["predication_id_clean"] = standardized["PREDICATION_ID"].map(
        normalize_text
    )

    if dataset_name == "filtered" and set(FILTERED_PRIMARY_COLUMNS).issubset(
        standardized.columns
    ):
        standardized["subject_cui_primary_for_stats"] = standardized[
            "subject_cui_primary"
        ].map(normalize_text)
        standardized["object_cui_primary_for_stats"] = standardized[
            "object_cui_primary"
        ].map(normalize_text)
    else:
        standardized["subject_cui_primary_for_stats"] = standardized[
            "SUBJECT_CUI"
        ].map(parse_primary_cui)
        standardized["object_cui_primary_for_stats"] = standardized[
            "OBJECT_CUI"
        ].map(parse_primary_cui)

    return standardized


def update_overall(overall: OverallAgg, chunk: pd.DataFrame) -> None:
    overall.n_rows += len(chunk)

    overall.n_missing_pyear += int((chunk["pyear_clean"] == "").sum())
    overall.n_missing_pmid += int((chunk["pmid_clean"] == "").sum())
    overall.n_missing_predication_id += int(
        (chunk["predication_id_clean"] == "").sum()
    )
    overall.n_missing_predicate += int((chunk["predicate_clean"] == "").sum())
    overall.n_missing_subject_cui_primary += int(
        (chunk["subject_cui_primary_for_stats"] == "").sum()
    )
    overall.n_missing_object_cui_primary += int(
        (chunk["object_cui_primary_for_stats"] == "").sum()
    )

    overall.pyears.update(chunk.loc[chunk["pyear_clean"] != "", "pyear_clean"])
    overall.pmids.update(chunk.loc[chunk["pmid_clean"] != "", "pmid_clean"])
    overall.predication_ids.update(
        chunk.loc[chunk["predication_id_clean"] != "", "predication_id_clean"]
    )
    overall.predicates.update(
        chunk.loc[chunk["predicate_clean"] != "", "predicate_clean"]
    )
    overall.subject_cuis.update(
        chunk.loc[chunk["subject_cui_primary_for_stats"] != "", "subject_cui_primary_for_stats"]
    )
    overall.object_cuis.update(
        chunk.loc[chunk["object_cui_primary_for_stats"] != "", "object_cui_primary_for_stats"]
    )
    overall.any_cuis.update(
        chunk.loc[chunk["subject_cui_primary_for_stats"] != "", "subject_cui_primary_for_stats"]
    )
    overall.any_cuis.update(
        chunk.loc[chunk["object_cui_primary_for_stats"] != "", "object_cui_primary_for_stats"]
    )

    self_loop_mask = (
        (chunk["subject_cui_primary_for_stats"] != "")
        & (chunk["object_cui_primary_for_stats"] != "")
        & (
            chunk["subject_cui_primary_for_stats"]
            == chunk["object_cui_primary_for_stats"]
        )
    )
    overall.n_self_loop_rows += int(self_loop_mask.sum())


def update_year_agg(agg: YearAgg, row: tuple[str, str, str, str, str, str]) -> None:
    pmid, predication_id, predicate, subject_cui, object_cui, _pyear = row
    agg.n_rows += 1

    if pmid:
        agg.pmids.add(pmid)
    if predication_id:
        agg.predication_ids.add(predication_id)
    if predicate:
        agg.predicates.add(predicate)
    if subject_cui:
        agg.subject_cuis.add(subject_cui)
        agg.any_cuis.add(subject_cui)
    if object_cui:
        agg.object_cuis.add(object_cui)
        agg.any_cuis.add(object_cui)

    if subject_cui and object_cui:
        directed_pair = (subject_cui, object_cui)
        undirected_pair = normalize_undirected_pair(subject_cui, object_cui)
        agg.directed_pairs.add(directed_pair)
        agg.undirected_pairs.add(undirected_pair)

        if subject_cui == object_cui:
            agg.n_self_loop_rows += 1
            agg.self_loop_pairs.add(directed_pair)

        if predicate:
            agg.directed_triples.add((subject_cui, predicate, object_cui))


def update_predicate_agg(
    agg: PredicateAgg,
    row: tuple[str, str, str, str, str, str],
) -> None:
    pmid, _predication_id, predicate, subject_cui, object_cui, _pyear = row
    agg.n_rows += 1

    if pmid:
        agg.pmids.add(pmid)
    if subject_cui:
        agg.subject_cuis.add(subject_cui)
        agg.any_cuis.add(subject_cui)
    if object_cui:
        agg.object_cuis.add(object_cui)
        agg.any_cuis.add(object_cui)

    if subject_cui and object_cui:
        directed_pair = (subject_cui, object_cui)
        agg.directed_pairs.add(directed_pair)
        agg.undirected_pairs.add(normalize_undirected_pair(subject_cui, object_cui))

        if predicate:
            agg.directed_triples.add((subject_cui, predicate, object_cui))


def process_dataset(
    dataset_name: str,
    input_file: Path,
) -> tuple[OverallAgg, dict[str, YearAgg], dict[tuple[str, str], PredicateAgg], dict[str, PredicateAgg]]:
    print(f"Processing {dataset_name} dataset from {input_file}")
    overall = OverallAgg()
    yearly: dict[str, YearAgg] = defaultdict(YearAgg)
    predicate_year: dict[tuple[str, str], PredicateAgg] = defaultdict(PredicateAgg)
    predicate_overall: dict[str, PredicateAgg] = defaultdict(PredicateAgg)

    usecols = get_usecols(input_file, dataset_name)
    reader = pd.read_csv(
        input_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        usecols=usecols,
        dtype={column: "string" for column in usecols},
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        chunk = standardize_chunk(chunk, dataset_name)
        update_overall(overall, chunk)

        row_iterator = zip(
            chunk["pmid_clean"],
            chunk["predication_id_clean"],
            chunk["predicate_clean"],
            chunk["subject_cui_primary_for_stats"],
            chunk["object_cui_primary_for_stats"],
            chunk["pyear_clean"],
        )

        for row in row_iterator:
            pmid, predication_id, predicate, subject_cui, object_cui, pyear = row
            if pyear:
                update_year_agg(yearly[pyear], row)

            if pyear and predicate:
                update_predicate_agg(predicate_year[(pyear, predicate)], row)
            if predicate:
                update_predicate_agg(predicate_overall[predicate], row)

        print(
            f"{dataset_name}: processed chunk {chunk_number:,}; "
            f"total rows so far {overall.n_rows:,}."
        )

        del chunk
        gc.collect()

    print(f"Finished {dataset_name}: {overall.n_rows:,} rows.")
    return overall, yearly, predicate_year, predicate_overall


def build_overall_row(dataset: str, overall: OverallAgg) -> dict[str, object]:
    numeric_years = [int(year) for year in overall.pyears if year.isdigit()]
    return {
        "dataset": dataset,
        "n_rows": overall.n_rows,
        "n_unique_pyear": len(overall.pyears),
        "min_pyear": min(numeric_years) if numeric_years else pd.NA,
        "max_pyear": max(numeric_years) if numeric_years else pd.NA,
        "n_unique_pmid": len(overall.pmids),
        "n_unique_predication_id": len(overall.predication_ids),
        "n_unique_predicate": len(overall.predicates),
        "n_unique_subject_cui_primary": len(overall.subject_cuis),
        "n_unique_object_cui_primary": len(overall.object_cuis),
        "n_unique_cui_primary": len(overall.any_cuis),
        "n_missing_pyear": overall.n_missing_pyear,
        "n_missing_pmid": overall.n_missing_pmid,
        "n_missing_predication_id": overall.n_missing_predication_id,
        "n_missing_predicate": overall.n_missing_predicate,
        "n_missing_subject_cui_primary": overall.n_missing_subject_cui_primary,
        "n_missing_object_cui_primary": overall.n_missing_object_cui_primary,
        "n_self_loop_rows": overall.n_self_loop_rows,
    }


def build_yearly_rows(dataset: str, yearly: dict[str, YearAgg]) -> list[dict[str, object]]:
    rows = []
    for pyear in sorted(yearly, key=sort_year_key):
        agg = yearly[pyear]
        rows.append(
            {
                "dataset": dataset,
                "pyear": pyear,
                "n_rows": agg.n_rows,
                "n_unique_pmid": len(agg.pmids),
                "n_unique_predication_id": len(agg.predication_ids),
                "n_unique_predicate": len(agg.predicates),
                "n_unique_subject_cui_primary": len(agg.subject_cuis),
                "n_unique_object_cui_primary": len(agg.object_cuis),
                "n_unique_cui_primary": len(agg.any_cuis),
                "n_unique_directed_triple": len(agg.directed_triples),
                "n_unique_directed_pair": len(agg.directed_pairs),
                "n_unique_undirected_pair": len(agg.undirected_pairs),
                "n_self_loop_rows": agg.n_self_loop_rows,
                "n_self_loop_pairs": len(agg.self_loop_pairs),
            }
        )
    return rows


def build_predicate_rows(
    dataset: str,
    predicate_year: dict[tuple[str, str], PredicateAgg],
    yearly: dict[str, YearAgg],
) -> list[dict[str, object]]:
    rows = []
    for (pyear, predicate), agg in sorted(
        predicate_year.items(),
        key=lambda item: (sort_year_key(item[0][0]), item[0][1]),
    ):
        year_rows = yearly[pyear].n_rows
        rows.append(
            {
                "dataset": dataset,
                "pyear": pyear,
                "predicate": predicate,
                "n_rows": agg.n_rows,
                "n_unique_pmid": len(agg.pmids),
                "n_unique_subject_cui_primary": len(agg.subject_cuis),
                "n_unique_object_cui_primary": len(agg.object_cuis),
                "n_unique_cui_primary": len(agg.any_cuis),
                "n_unique_directed_triple": len(agg.directed_triples),
                "n_unique_directed_pair": len(agg.directed_pairs),
                "n_unique_undirected_pair": len(agg.undirected_pairs),
                "share_of_year_rows": safe_divide(agg.n_rows, year_rows),
            }
        )
    return rows


def build_retention_by_year(yearly_summary: pd.DataFrame) -> pd.DataFrame:
    raw = yearly_summary[yearly_summary["dataset"] == "raw"].copy()
    filtered = yearly_summary[yearly_summary["dataset"] == "filtered"].copy()

    keep_columns = [
        "pyear",
        "n_rows",
        "n_unique_pmid",
        "n_unique_predicate",
        "n_unique_cui_primary",
        "n_unique_directed_triple",
        "n_unique_undirected_pair",
    ]
    merged = raw[keep_columns].merge(
        filtered[keep_columns],
        on="pyear",
        how="outer",
        suffixes=("_raw", "_filtered"),
    )

    rows = []
    for row in merged.itertuples(index=False):
        rows.append(
            {
                "pyear": row.pyear,
                "raw_n_rows": row.n_rows_raw,
                "filtered_n_rows": row.n_rows_filtered,
                "row_retention_rate": safe_divide(row.n_rows_filtered, row.n_rows_raw),
                "raw_n_unique_pmid": row.n_unique_pmid_raw,
                "filtered_n_unique_pmid": row.n_unique_pmid_filtered,
                "pmid_retention_rate": safe_divide(
                    row.n_unique_pmid_filtered, row.n_unique_pmid_raw
                ),
                "raw_n_unique_predicate": row.n_unique_predicate_raw,
                "filtered_n_unique_predicate": row.n_unique_predicate_filtered,
                "predicate_retention_rate": safe_divide(
                    row.n_unique_predicate_filtered,
                    row.n_unique_predicate_raw,
                ),
                "raw_n_unique_cui_primary": row.n_unique_cui_primary_raw,
                "filtered_n_unique_cui_primary": row.n_unique_cui_primary_filtered,
                "cui_retention_rate": safe_divide(
                    row.n_unique_cui_primary_filtered,
                    row.n_unique_cui_primary_raw,
                ),
                "raw_n_unique_directed_triple": row.n_unique_directed_triple_raw,
                "filtered_n_unique_directed_triple": (
                    row.n_unique_directed_triple_filtered
                ),
                "directed_triple_retention_rate": safe_divide(
                    row.n_unique_directed_triple_filtered,
                    row.n_unique_directed_triple_raw,
                ),
                "raw_n_unique_undirected_pair": row.n_unique_undirected_pair_raw,
                "filtered_n_unique_undirected_pair": (
                    row.n_unique_undirected_pair_filtered
                ),
                "undirected_pair_retention_rate": safe_divide(
                    row.n_unique_undirected_pair_filtered,
                    row.n_unique_undirected_pair_raw,
                ),
            }
        )
    return pd.DataFrame(rows).sort_values("pyear")


def predicate_overall_rows(
    dataset: str,
    predicate_overall: dict[str, PredicateAgg],
) -> pd.DataFrame:
    rows = []
    for predicate, agg in sorted(predicate_overall.items()):
        rows.append(
            {
                "predicate": predicate,
                f"{dataset}_n_rows": agg.n_rows,
                f"{dataset}_n_unique_pmid": len(agg.pmids),
                f"{dataset}_n_unique_cui_primary": len(agg.any_cuis),
                f"{dataset}_n_unique_directed_triple": len(agg.directed_triples),
                f"{dataset}_n_unique_undirected_pair": len(agg.undirected_pairs),
            }
        )
    return pd.DataFrame(rows)


def build_retention_by_predicate(
    raw_predicate_overall: dict[str, PredicateAgg],
    filtered_predicate_overall: dict[str, PredicateAgg],
) -> pd.DataFrame:
    raw = predicate_overall_rows("raw", raw_predicate_overall)
    filtered = predicate_overall_rows("filtered", filtered_predicate_overall)
    merged = raw.merge(filtered, on="predicate", how="outer").fillna(0)

    for column in merged.columns:
        if column != "predicate":
            merged[column] = merged[column].astype(int)

    merged["row_retention_rate"] = [
        safe_divide(filtered_value, raw_value)
        for filtered_value, raw_value in zip(
            merged["filtered_n_rows"], merged["raw_n_rows"]
        )
    ]
    merged["pmid_retention_rate"] = [
        safe_divide(filtered_value, raw_value)
        for filtered_value, raw_value in zip(
            merged["filtered_n_unique_pmid"], merged["raw_n_unique_pmid"]
        )
    ]
    merged["cui_retention_rate"] = [
        safe_divide(filtered_value, raw_value)
        for filtered_value, raw_value in zip(
            merged["filtered_n_unique_cui_primary"],
            merged["raw_n_unique_cui_primary"],
        )
    ]
    merged["directed_triple_retention_rate"] = [
        safe_divide(filtered_value, raw_value)
        for filtered_value, raw_value in zip(
            merged["filtered_n_unique_directed_triple"],
            merged["raw_n_unique_directed_triple"],
        )
    ]
    merged["undirected_pair_retention_rate"] = [
        safe_divide(filtered_value, raw_value)
        for filtered_value, raw_value in zip(
            merged["filtered_n_unique_undirected_pair"],
            merged["raw_n_unique_undirected_pair"],
        )
    ]
    return merged.sort_values(["raw_n_rows", "predicate"], ascending=[False, True])


def build_cui_overlap_rows(filtered_yearly: dict[str, YearAgg]) -> pd.DataFrame:
    years = sorted(filtered_yearly, key=sort_year_key)
    rows = []

    for pyear in years:
        focal_cuis = filtered_yearly[pyear].any_cuis
        pyear_int = int(pyear) if pyear.isdigit() else None

        row = {"pyear": pyear, "n_focal_year_cui": len(focal_cuis)}

        for window in [1, 3, 5]:
            if pyear_int is None:
                prior_years = []
            else:
                prior_years = [
                    str(year)
                    for year in range(pyear_int - window, pyear_int)
                    if str(year) in filtered_yearly
                ]

            prior_cuis: set[str] = set()
            for prior_year in prior_years:
                prior_cuis.update(filtered_yearly[prior_year].any_cuis)

            overlap = focal_cuis & prior_cuis
            union = focal_cuis | prior_cuis

            row[f"n_prior_{window}y_cui"] = len(prior_cuis)
            row[f"n_overlap_prior_{window}y"] = len(overlap)
            row[f"share_focal_seen_prior_{window}y"] = safe_divide(
                len(overlap), len(focal_cuis)
            )
            row[f"jaccard_prior_{window}y"] = safe_divide(len(overlap), len(union))
            row[f"n_new_cui_vs_prior_{window}y"] = len(focal_cuis - prior_cuis)

        rows.append(row)

    return pd.DataFrame(rows)


def main() -> None:
    check_inputs()
    check_outputs()

    raw_overall, raw_yearly, raw_predicate_year, raw_predicate_overall = (
        process_dataset("raw", RAW_INPUT_FILE)
    )
    (
        filtered_overall,
        filtered_yearly,
        filtered_predicate_year,
        filtered_predicate_overall,
    ) = process_dataset("filtered", FILTERED_INPUT_FILE)

    overall_summary = pd.DataFrame(
        [
            build_overall_row("raw", raw_overall),
            build_overall_row("filtered", filtered_overall),
        ]
    )
    yearly_summary = pd.DataFrame(
        build_yearly_rows("raw", raw_yearly)
        + build_yearly_rows("filtered", filtered_yearly)
    )
    predicate_year_frequency = pd.DataFrame(
        build_predicate_rows("raw", raw_predicate_year, raw_yearly)
        + build_predicate_rows("filtered", filtered_predicate_year, filtered_yearly)
    )
    retention_by_year = build_retention_by_year(yearly_summary)
    retention_by_predicate = build_retention_by_predicate(
        raw_predicate_overall,
        filtered_predicate_overall,
    )
    cui_overlap = build_cui_overlap_rows(filtered_yearly)

    outputs = {
        "semmeddb_overall_summary.csv": overall_summary,
        "semmeddb_yearly_summary.csv": yearly_summary,
        "semmeddb_predicate_year_frequency.csv": predicate_year_frequency,
        "semmeddb_filter_retention_by_year.csv": retention_by_year,
        "semmeddb_filter_retention_by_predicate.csv": retention_by_predicate,
        "semmeddb_cui_overlap_by_year_filtered.csv": cui_overlap,
    }

    for filename, df in outputs.items():
        output_file = OUTPUT_DIR / filename
        df.to_csv(output_file, index=False)
        print(f"Saved {len(df):,} rows to {output_file}")


if __name__ == "__main__":
    main()
