"""Prepare the SemMedDB analytic predication table.

The redesigned preparation workflow reads raw SemMedDB PREDICATION and
CITATIONS files, attaches publication year, applies the project novelty filter,
extracts primary CUI/name values, deduplicates exact directed typed triples
within PMID, and writes one analytic Parquet file plus small CSV summary files.
"""

from __future__ import annotations

import csv
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import duckdb
except ImportError as exc:  # pragma: no cover - exercised on HPC if missing.
    raise SystemExit(
        "Missing required Python package: duckdb. Install it in the HPC "
        "environment before running this script."
    ) from exc


SEMMED_RELEASE = "semmedVER43_2024_R"

DEFAULT_SEMMED_SOURCE_DIR = Path(
    "/home/u23/jinyugao/research/data/source/semmedVER43_R"
)
DEFAULT_PROJECT_ROOT = Path(
    "/xdisk/sebratt/jinyugao/research/projects/innovation_capacity"
)

PREDICATION_BASENAME = "semmedVER43_2024_R_PREDICATION"
CITATIONS_BASENAME = "semmedVER43_2024_R_CITATIONS"

OUTPUT_SUBDIR = Path("data/processed/semmedVER43_R")
LOGICAL_OUTPUT_FILES = [
    "semmedVER43_2024_R_predications_with_pyear_filtered_pmid_unique_triples.parquet",
    "semmedVER43_2024_R_prepare_diagnostic_summary.csv",
    "semmedVER43_2024_R_yearly_descriptive_statistics.csv",
    "semmedVER43_2024_R_predicate_year_descriptive_statistics.csv",
    "semmedVER43_2024_R_cui_descriptive_statistics.csv",
]

CHUNK_SIZE = 500_000
OVERWRITE = False
PRIOR_WINDOWS = [1, 3, 5]

CITATION_COLUMNS = ["PMID", "ISSN", "DP", "EDAT", "PYEAR"]
PREDICATION_COLUMNS = [
    "PREDICATION_ID",
    "SENTENCE_ID",
    "PMID",
    "PREDICATE",
    "SUBJECT_CUI",
    "SUBJECT_NAME",
    "SUBJECT_SEMTYPE",
    "SUBJECT_NOVELTY",
    "OBJECT_CUI",
    "OBJECT_NAME",
    "OBJECT_SEMTYPE",
    "OBJECT_NOVELTY",
    "M",
    "N",
    "O",
]

MAIN_OUTPUT_COLUMNS = [
    "PREDICATION_ID",
    "SENTENCE_ID",
    "PMID",
    "PYEAR",
    "PREDICATE",
    "SUBJECT_CUI",
    "SUBJECT_NAME",
    "SUBJECT_SEMTYPE",
    "SUBJECT_NOVELTY",
    "subject_cui_primary",
    "subject_name_primary",
    "OBJECT_CUI",
    "OBJECT_NAME",
    "OBJECT_SEMTYPE",
    "OBJECT_NOVELTY",
    "object_cui_primary",
    "object_name_primary",
]

DIAGNOSTIC_COLUMNS = [
    "run_timestamp",
    "semmed_release",
    "source_predication_file",
    "source_citations_file",
    "output_parquet_file",
    "n_citation_rows",
    "n_unique_citation_pmids",
    "n_duplicate_citation_rows",
    "n_duplicate_citation_pmids",
    "n_conflicting_citation_pyear_pmids",
    "n_raw_predication_rows",
    "n_unique_raw_predication_ids",
    "n_unique_raw_pmids",
    "n_rows_missing_pmid",
    "n_rows_missing_predication_id",
    "n_rows_with_pyear",
    "n_rows_missing_pyear",
    "min_pyear_with_pyear",
    "max_pyear_with_pyear",
    "n_rows_after_novelty_filter",
    "n_rows_removed_by_novelty_filter",
    "n_rows_missing_subject_novelty",
    "n_rows_missing_object_novelty",
    "n_rows_zero_subject_novelty",
    "n_rows_zero_object_novelty",
    "n_rows_missing_required_analytic_key",
    "n_rows_valid_for_analytic_key",
    "n_subject_cui_pipe_delimited_rows",
    "n_object_cui_pipe_delimited_rows",
    "n_subject_name_pipe_delimited_rows",
    "n_object_name_pipe_delimited_rows",
    "n_subject_cui_name_component_mismatch_rows",
    "n_object_cui_name_component_mismatch_rows",
    "n_missing_subject_cui_primary",
    "n_missing_object_cui_primary",
    "n_missing_subject_name_primary",
    "n_missing_object_name_primary",
    "n_pmid_exact_triple_groups_before_dedup",
    "n_duplicate_pmid_exact_triple_groups",
    "n_rows_in_duplicate_pmid_exact_triple_groups",
    "n_rows_removed_by_pmid_exact_triple_dedup",
    "n_final_pmid_unique_triples",
    "n_final_unique_pmids",
    "n_final_unique_predicates",
    "n_final_unique_subject_cui_primary",
    "n_final_unique_object_cui_primary",
    "n_final_unique_cui_primary",
    "min_pyear_final",
    "max_pyear_final",
    "n_self_loop_triples_final",
    "share_rows_missing_pyear",
    "share_rows_after_novelty_filter",
    "share_rows_missing_required_analytic_key",
    "share_rows_removed_by_pmid_exact_triple_dedup",
    "share_self_loop_triples_final",
]

YEARLY_COLUMNS = [
    "pyear",
    "n_pmid_unique_triples",
    "share_pmid_unique_triples",
    "n_unique_pmid",
    "share_unique_pmid",
    "n_unique_predicate",
    "n_unique_subject_cui_primary",
    "n_unique_object_cui_primary",
    "n_unique_cui_primary",
    "n_unique_subject_name_primary",
    "n_unique_object_name_primary",
    "n_unique_name_primary",
    "n_unique_exact_triple",
    "n_unique_directed_pair",
    "n_unique_pair_connection",
    "n_self_loop_triples",
    "share_self_loop_triples",
    "n_prior_1y_cui_primary",
    "n_overlap_prior_1y_cui_primary",
    "share_focal_cui_seen_prior_1y",
    "n_new_cui_vs_prior_1y",
    "has_full_prior_1y_window",
    "n_prior_3y_cui_primary",
    "n_overlap_prior_3y_cui_primary",
    "share_focal_cui_seen_prior_3y",
    "n_new_cui_vs_prior_3y",
    "has_full_prior_3y_window",
    "n_prior_5y_cui_primary",
    "n_overlap_prior_5y_cui_primary",
    "share_focal_cui_seen_prior_5y",
    "n_new_cui_vs_prior_5y",
    "has_full_prior_5y_window",
    "n_prior_5y_pair_connection",
    "n_overlap_prior_5y_pair_connection",
    "share_focal_pair_connection_seen_prior_5y",
    "n_prior_5y_exact_triple",
    "n_overlap_prior_5y_exact_triple",
    "share_focal_exact_triple_seen_prior_5y",
]


def get_env_path(name: str, default: Path) -> Path:
    value = os.environ.get(name)
    return Path(value).expanduser() if value else default


def sql_literal(value: Path | str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def discover_source_file(source_dir: Path, basename: str) -> Path:
    candidates = [
        source_dir / f"{basename}.csv.gz",
        source_dir / f"{basename}.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    missing = "\n".join(str(path) for path in candidates)
    raise FileNotFoundError(f"Missing required source file. Tried:\n{missing}")


def compression_for_path(path: Path) -> str:
    return "gzip" if path.name.endswith(".gz") else "none"


def check_outputs(output_files: list[Path], overwrite: bool) -> None:
    existing_files = [path for path in output_files if path.exists()]
    if existing_files and not overwrite:
        existing = "\n".join(str(path) for path in existing_files)
        raise FileExistsError(
            "Output file(s) already exist. Set OVERWRITE = True to replace them:\n"
            f"{existing}"
        )
    if overwrite:
        for path in existing_files:
            path.unlink()


def source_scan_sql(path: Path, columns: list[str]) -> str:
    column_spec = ", ".join(f"'{column}': 'VARCHAR'" for column in columns)
    # SemMedDB source rows can contain backslash-escaped quotes, e.g. \"U\".
    # Literal \N values are normalized after reading because DuckDB does not
    # allow the escape character to appear inside the CSV null marker.
    return (
        "read_csv("
        f"{sql_literal(path)}, "
        "header=false, "
        f"columns={{{column_spec}}}, "
        "delim=',', "
        "quote='\"', "
        "escape=chr(92), "
        "sample_size=-1, "
        f"compression='{compression_for_path(path)}'"
        ")"
    )


def normalize_source_select_sql(alias: str, columns: list[str]) -> str:
    null_marker = "chr(92) || 'N'"
    return ", ".join(
        f"NULLIF({alias}.{column}, {null_marker}) AS {column}"
        for column in columns
    )


def safe_divide(numerator: int | float | None, denominator: int | float | None) -> float | None:
    if denominator in (None, 0):
        return None
    if numerator is None:
        return None
    return float(numerator) / float(denominator)


def scalar(con: duckdb.DuckDBPyConnection, query: str, params: list[Any] | None = None) -> Any:
    result = con.execute(query, params or []).fetchone()
    return result[0] if result else None


def int_scalar(
    con: duckdb.DuckDBPyConnection,
    query: str,
    params: list[Any] | None = None,
) -> int:
    value = scalar(con, query, params)
    return 0 if value is None else int(value)


def make_component_count_sql(column: str) -> str:
    return (
        f"CASE WHEN {column} IS NULL THEN NULL "
        f"ELSE length({column}) - length(replace({column}, '|', '')) + 1 END"
    )


def is_missing_sql(column: str) -> str:
    return f"({column} IS NULL OR trim({column}) = '')"


def valid_key_where_clause() -> str:
    return (
        "passes_novelty_filter "
        "AND NOT missing_required_analytic_key"
    )


def configure_duckdb(con: duckdb.DuckDBPyConnection, temp_dir: Path) -> None:
    threads = os.environ.get("IC_DUCKDB_THREADS")
    memory_limit = os.environ.get("IC_DUCKDB_MEMORY_LIMIT")

    con.execute(f"SET temp_directory={sql_literal(temp_dir)}")
    if threads:
        con.execute(f"PRAGMA threads={int(threads)}")
    if memory_limit:
        con.execute(f"PRAGMA memory_limit={sql_literal(memory_limit)}")


def create_input_tables(
    con: duckdb.DuckDBPyConnection,
    predication_file: Path,
    citations_file: Path,
) -> None:
    print(f"Reading citations from {citations_file}")
    con.execute(
        f"""
        CREATE TEMP TABLE citations_raw AS
        SELECT {normalize_source_select_sql("C", CITATION_COLUMNS)}
        FROM {source_scan_sql(citations_file, CITATION_COLUMNS)} AS C
        """
    )

    conflicting_pmids = int_scalar(
        con,
        """
        SELECT COUNT(*)
        FROM (
            SELECT PMID
            FROM citations_raw
            WHERE PMID IS NOT NULL
            GROUP BY PMID
            HAVING COUNT(DISTINCT PYEAR) > 1
        )
        """,
    )
    if conflicting_pmids:
        examples = con.execute(
            """
            SELECT PMID
            FROM citations_raw
            WHERE PMID IS NOT NULL
            GROUP BY PMID
            HAVING COUNT(DISTINCT PYEAR) > 1
            ORDER BY PMID
            LIMIT 10
            """
        ).fetchall()
        example_text = ", ".join(str(row[0]) for row in examples)
        raise ValueError(
            "CITATIONS contains PMID values with conflicting PYEAR values. "
            f"Example PMID(s): {example_text}"
        )

    con.execute(
        """
        CREATE TEMP TABLE citation_map AS
        SELECT PMID, MIN(PYEAR) AS PYEAR
        FROM citations_raw
        WHERE PMID IS NOT NULL
        GROUP BY PMID
        """
    )

    print(f"Reading predications from {predication_file}")
    subject_cui_count = make_component_count_sql("P.SUBJECT_CUI")
    subject_name_count = make_component_count_sql("P.SUBJECT_NAME")
    object_cui_count = make_component_count_sql("P.OBJECT_CUI")
    object_name_count = make_component_count_sql("P.OBJECT_NAME")

    con.execute(
        f"""
        CREATE TEMP TABLE predications_prepared AS
        SELECT
            row_number() OVER () AS source_order,
            P.PREDICATION_ID,
            P.SENTENCE_ID,
            P.PMID,
            C.PYEAR,
            P.PREDICATE,
            P.SUBJECT_CUI,
            P.SUBJECT_NAME,
            P.SUBJECT_SEMTYPE,
            P.SUBJECT_NOVELTY,
            NULLIF(trim(split_part(P.SUBJECT_CUI, '|', 1)), '') AS subject_cui_primary,
            NULLIF(trim(split_part(P.SUBJECT_NAME, '|', 1)), '') AS subject_name_primary,
            P.OBJECT_CUI,
            P.OBJECT_NAME,
            P.OBJECT_SEMTYPE,
            P.OBJECT_NOVELTY,
            NULLIF(trim(split_part(P.OBJECT_CUI, '|', 1)), '') AS object_cui_primary,
            NULLIF(trim(split_part(P.OBJECT_NAME, '|', 1)), '') AS object_name_primary,
            TRY_CAST(P.SUBJECT_NOVELTY AS DOUBLE) AS subject_novelty_numeric,
            TRY_CAST(P.OBJECT_NOVELTY AS DOUBLE) AS object_novelty_numeric,
            {subject_cui_count} AS subject_cui_component_count,
            {subject_name_count} AS subject_name_component_count,
            {object_cui_count} AS object_cui_component_count,
            {object_name_count} AS object_name_component_count,
            (
                TRY_CAST(P.SUBJECT_NOVELTY AS DOUBLE) IS NOT NULL
                AND TRY_CAST(P.OBJECT_NOVELTY AS DOUBLE) IS NOT NULL
                AND TRY_CAST(P.SUBJECT_NOVELTY AS DOUBLE) != 0
                AND TRY_CAST(P.OBJECT_NOVELTY AS DOUBLE) != 0
            ) AS passes_novelty_filter,
            (
                {is_missing_sql('P.PMID')}
                OR C.PYEAR IS NULL
                OR {is_missing_sql('P.PREDICATE')}
                OR NULLIF(trim(split_part(P.SUBJECT_CUI, '|', 1)), '') IS NULL
                OR NULLIF(trim(split_part(P.OBJECT_CUI, '|', 1)), '') IS NULL
            ) AS missing_required_analytic_key
        FROM (
            SELECT {normalize_source_select_sql("R", PREDICATION_COLUMNS)}
            FROM {source_scan_sql(predication_file, PREDICATION_COLUMNS)} AS R
        ) AS P
        LEFT JOIN citation_map AS C
            ON P.PMID = C.PMID
        """
    )


def create_final_table(con: duckdb.DuckDBPyConnection) -> None:
    print("Building deduplicated PMID-level analytic triples")
    con.execute(
        f"""
        CREATE TEMP TABLE valid_group_sizes AS
        SELECT
            PMID,
            subject_cui_primary,
            PREDICATE,
            object_cui_primary,
            COUNT(*) AS n_rows
        FROM predications_prepared
        WHERE {valid_key_where_clause()}
        GROUP BY PMID, subject_cui_primary, PREDICATE, object_cui_primary
        """
    )

    con.execute(
        f"""
        CREATE TEMP TABLE final_analytic AS
        SELECT
            source_order,
            {", ".join(MAIN_OUTPUT_COLUMNS)}
        FROM (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY
                        PMID,
                        subject_cui_primary,
                        PREDICATE,
                        object_cui_primary
                    ORDER BY source_order
                ) AS dedup_rank
            FROM predications_prepared
            WHERE {valid_key_where_clause()}
        )
        WHERE dedup_rank = 1
        """
    )


def write_main_parquet(con: duckdb.DuckDBPyConnection, output_file: Path) -> None:
    print(f"Writing main analytic parquet to {output_file}")
    con.execute(
        f"""
        COPY (
            SELECT {", ".join(MAIN_OUTPUT_COLUMNS)}
            FROM final_analytic
            ORDER BY source_order
        )
        TO {sql_literal(output_file)}
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )


def collect_diagnostics(
    con: duckdb.DuckDBPyConnection,
    predication_file: Path,
    citations_file: Path,
    parquet_file: Path,
) -> dict[str, Any]:
    diagnostic: dict[str, Any] = {
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "semmed_release": SEMMED_RELEASE,
        "source_predication_file": str(predication_file),
        "source_citations_file": str(citations_file),
        "output_parquet_file": str(parquet_file),
    }

    citation_rows = int_scalar(con, "SELECT COUNT(*) FROM citations_raw")
    unique_citation_pmids = int_scalar(
        con,
        "SELECT COUNT(DISTINCT PMID) FROM citations_raw WHERE PMID IS NOT NULL",
    )
    duplicate_citation_pmids = int_scalar(
        con,
        """
        SELECT COUNT(*)
        FROM (
            SELECT PMID
            FROM citations_raw
            WHERE PMID IS NOT NULL
            GROUP BY PMID
            HAVING COUNT(*) > 1
        )
        """,
    )
    conflicting_citation_pmids = int_scalar(
        con,
        """
        SELECT COUNT(*)
        FROM (
            SELECT PMID
            FROM citations_raw
            WHERE PMID IS NOT NULL
            GROUP BY PMID
            HAVING COUNT(DISTINCT PYEAR) > 1
        )
        """,
    )
    diagnostic.update(
        {
            "n_citation_rows": citation_rows,
            "n_unique_citation_pmids": unique_citation_pmids,
            "n_duplicate_citation_rows": citation_rows - unique_citation_pmids,
            "n_duplicate_citation_pmids": duplicate_citation_pmids,
            "n_conflicting_citation_pyear_pmids": conflicting_citation_pmids,
        }
    )

    raw_rows = int_scalar(con, "SELECT COUNT(*) FROM predications_prepared")
    rows_missing_pyear = int_scalar(
        con,
        "SELECT COUNT(*) FROM predications_prepared WHERE PYEAR IS NULL",
    )
    rows_after_novelty = int_scalar(
        con,
        "SELECT COUNT(*) FROM predications_prepared WHERE passes_novelty_filter",
    )
    rows_missing_key = int_scalar(
        con,
        """
        SELECT COUNT(*)
        FROM predications_prepared
        WHERE passes_novelty_filter AND missing_required_analytic_key
        """,
    )
    rows_valid_key = int_scalar(
        con,
        f"SELECT COUNT(*) FROM predications_prepared WHERE {valid_key_where_clause()}",
    )
    rows_removed_dedup = int_scalar(
        con,
        "SELECT COALESCE(SUM(n_rows - 1), 0) FROM valid_group_sizes WHERE n_rows > 1",
    )
    final_rows = int_scalar(con, "SELECT COUNT(*) FROM final_analytic")
    self_loop_final = int_scalar(
        con,
        """
        SELECT COUNT(*)
        FROM final_analytic
        WHERE subject_cui_primary = object_cui_primary
        """,
    )

    min_max_with_pyear = con.execute(
        """
        SELECT
            MIN(TRY_CAST(PYEAR AS INTEGER)),
            MAX(TRY_CAST(PYEAR AS INTEGER))
        FROM predications_prepared
        WHERE PYEAR IS NOT NULL
        """
    ).fetchone()
    min_max_final = con.execute(
        """
        SELECT
            MIN(TRY_CAST(PYEAR AS INTEGER)),
            MAX(TRY_CAST(PYEAR AS INTEGER))
        FROM final_analytic
        WHERE PYEAR IS NOT NULL
        """
    ).fetchone()

    diagnostic.update(
        {
            "n_raw_predication_rows": raw_rows,
            "n_unique_raw_predication_ids": int_scalar(
                con,
                """
                SELECT COUNT(DISTINCT PREDICATION_ID)
                FROM predications_prepared
                WHERE PREDICATION_ID IS NOT NULL
                """,
            ),
            "n_unique_raw_pmids": int_scalar(
                con,
                "SELECT COUNT(DISTINCT PMID) FROM predications_prepared WHERE PMID IS NOT NULL",
            ),
            "n_rows_missing_pmid": int_scalar(
                con,
                f"SELECT COUNT(*) FROM predications_prepared WHERE {is_missing_sql('PMID')}",
            ),
            "n_rows_missing_predication_id": int_scalar(
                con,
                f"SELECT COUNT(*) FROM predications_prepared WHERE {is_missing_sql('PREDICATION_ID')}",
            ),
            "n_rows_with_pyear": raw_rows - rows_missing_pyear,
            "n_rows_missing_pyear": rows_missing_pyear,
            "min_pyear_with_pyear": min_max_with_pyear[0],
            "max_pyear_with_pyear": min_max_with_pyear[1],
            "n_rows_after_novelty_filter": rows_after_novelty,
            "n_rows_removed_by_novelty_filter": raw_rows - rows_after_novelty,
            "n_rows_missing_subject_novelty": int_scalar(
                con,
                "SELECT COUNT(*) FROM predications_prepared WHERE subject_novelty_numeric IS NULL",
            ),
            "n_rows_missing_object_novelty": int_scalar(
                con,
                "SELECT COUNT(*) FROM predications_prepared WHERE object_novelty_numeric IS NULL",
            ),
            "n_rows_zero_subject_novelty": int_scalar(
                con,
                "SELECT COUNT(*) FROM predications_prepared WHERE subject_novelty_numeric = 0",
            ),
            "n_rows_zero_object_novelty": int_scalar(
                con,
                "SELECT COUNT(*) FROM predications_prepared WHERE object_novelty_numeric = 0",
            ),
            "n_rows_missing_required_analytic_key": rows_missing_key,
            "n_rows_valid_for_analytic_key": rows_valid_key,
            "n_subject_cui_pipe_delimited_rows": int_scalar(
                con,
                """
                SELECT COUNT(*)
                FROM predications_prepared
                WHERE passes_novelty_filter AND strpos(COALESCE(SUBJECT_CUI, ''), '|') > 0
                """,
            ),
            "n_object_cui_pipe_delimited_rows": int_scalar(
                con,
                """
                SELECT COUNT(*)
                FROM predications_prepared
                WHERE passes_novelty_filter AND strpos(COALESCE(OBJECT_CUI, ''), '|') > 0
                """,
            ),
            "n_subject_name_pipe_delimited_rows": int_scalar(
                con,
                """
                SELECT COUNT(*)
                FROM predications_prepared
                WHERE passes_novelty_filter AND strpos(COALESCE(SUBJECT_NAME, ''), '|') > 0
                """,
            ),
            "n_object_name_pipe_delimited_rows": int_scalar(
                con,
                """
                SELECT COUNT(*)
                FROM predications_prepared
                WHERE passes_novelty_filter AND strpos(COALESCE(OBJECT_NAME, ''), '|') > 0
                """,
            ),
            "n_subject_cui_name_component_mismatch_rows": int_scalar(
                con,
                """
                SELECT COUNT(*)
                FROM predications_prepared
                WHERE passes_novelty_filter
                    AND SUBJECT_CUI IS NOT NULL
                    AND SUBJECT_NAME IS NOT NULL
                    AND subject_cui_component_count != subject_name_component_count
                """,
            ),
            "n_object_cui_name_component_mismatch_rows": int_scalar(
                con,
                """
                SELECT COUNT(*)
                FROM predications_prepared
                WHERE passes_novelty_filter
                    AND OBJECT_CUI IS NOT NULL
                    AND OBJECT_NAME IS NOT NULL
                    AND object_cui_component_count != object_name_component_count
                """,
            ),
            "n_missing_subject_cui_primary": int_scalar(
                con,
                """
                SELECT COUNT(*)
                FROM predications_prepared
                WHERE passes_novelty_filter AND subject_cui_primary IS NULL
                """,
            ),
            "n_missing_object_cui_primary": int_scalar(
                con,
                """
                SELECT COUNT(*)
                FROM predications_prepared
                WHERE passes_novelty_filter AND object_cui_primary IS NULL
                """,
            ),
            "n_missing_subject_name_primary": int_scalar(
                con,
                """
                SELECT COUNT(*)
                FROM predications_prepared
                WHERE passes_novelty_filter AND subject_name_primary IS NULL
                """,
            ),
            "n_missing_object_name_primary": int_scalar(
                con,
                """
                SELECT COUNT(*)
                FROM predications_prepared
                WHERE passes_novelty_filter AND object_name_primary IS NULL
                """,
            ),
            "n_pmid_exact_triple_groups_before_dedup": int_scalar(
                con,
                "SELECT COUNT(*) FROM valid_group_sizes",
            ),
            "n_duplicate_pmid_exact_triple_groups": int_scalar(
                con,
                "SELECT COUNT(*) FROM valid_group_sizes WHERE n_rows > 1",
            ),
            "n_rows_in_duplicate_pmid_exact_triple_groups": int_scalar(
                con,
                "SELECT COALESCE(SUM(n_rows), 0) FROM valid_group_sizes WHERE n_rows > 1",
            ),
            "n_rows_removed_by_pmid_exact_triple_dedup": rows_removed_dedup,
            "n_final_pmid_unique_triples": final_rows,
            "n_final_unique_pmids": int_scalar(
                con,
                "SELECT COUNT(DISTINCT PMID) FROM final_analytic",
            ),
            "n_final_unique_predicates": int_scalar(
                con,
                "SELECT COUNT(DISTINCT PREDICATE) FROM final_analytic",
            ),
            "n_final_unique_subject_cui_primary": int_scalar(
                con,
                "SELECT COUNT(DISTINCT subject_cui_primary) FROM final_analytic",
            ),
            "n_final_unique_object_cui_primary": int_scalar(
                con,
                "SELECT COUNT(DISTINCT object_cui_primary) FROM final_analytic",
            ),
            "n_final_unique_cui_primary": int_scalar(
                con,
                """
                SELECT COUNT(DISTINCT primary_cui)
                FROM (
                    SELECT subject_cui_primary AS primary_cui FROM final_analytic
                    UNION ALL
                    SELECT object_cui_primary AS primary_cui FROM final_analytic
                )
                """,
            ),
            "min_pyear_final": min_max_final[0],
            "max_pyear_final": min_max_final[1],
            "n_self_loop_triples_final": self_loop_final,
            "share_rows_missing_pyear": safe_divide(rows_missing_pyear, raw_rows),
            "share_rows_after_novelty_filter": safe_divide(rows_after_novelty, raw_rows),
            "share_rows_missing_required_analytic_key": safe_divide(
                rows_missing_key,
                rows_after_novelty,
            ),
            "share_rows_removed_by_pmid_exact_triple_dedup": safe_divide(
                rows_removed_dedup,
                rows_valid_key,
            ),
            "share_self_loop_triples_final": safe_divide(self_loop_final, final_rows),
        }
    )
    return diagnostic


def write_single_row_csv(path: Path, fieldnames: list[str], row: dict[str, Any]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({field: row.get(field) for field in fieldnames})
    print(f"Saved {path}")


def create_final_views(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TEMP VIEW final_long_cui AS
        SELECT
            source_order,
            PMID,
            PYEAR,
            TRY_CAST(PYEAR AS INTEGER) AS pyear_int,
            PREDICATE,
            subject_cui_primary AS primary_cui,
            subject_name_primary AS primary_name,
            SUBJECT_SEMTYPE AS primary_semtype,
            object_cui_primary AS neighbor_cui,
            'subject' AS role
        FROM final_analytic
        UNION ALL
        SELECT
            source_order,
            PMID,
            PYEAR,
            TRY_CAST(PYEAR AS INTEGER) AS pyear_int,
            PREDICATE,
            object_cui_primary AS primary_cui,
            object_name_primary AS primary_name,
            OBJECT_SEMTYPE AS primary_semtype,
            subject_cui_primary AS neighbor_cui,
            'object' AS role
        FROM final_analytic
        """
    )
    con.execute(
        """
        CREATE TEMP VIEW final_network_units AS
        SELECT
            source_order,
            PMID,
            PYEAR,
            TRY_CAST(PYEAR AS INTEGER) AS pyear_int,
            PREDICATE,
            subject_cui_primary,
            subject_name_primary,
            object_cui_primary,
            object_name_primary,
            subject_cui_primary || chr(31) || PREDICATE || chr(31) ||
                object_cui_primary AS exact_triple_key,
            subject_cui_primary || chr(31) || object_cui_primary AS directed_pair_key,
            LEAST(subject_cui_primary, object_cui_primary) || chr(31) ||
                GREATEST(subject_cui_primary, object_cui_primary) AS pair_connection_key
        FROM final_analytic
        """
    )


def yearly_base_rows(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        WITH year_base AS (
            SELECT
                PYEAR AS pyear,
                TRY_CAST(PYEAR AS INTEGER) AS pyear_int,
                COUNT(*) AS n_pmid_unique_triples,
                COUNT(DISTINCT PMID) AS n_unique_pmid,
                COUNT(DISTINCT PREDICATE) AS n_unique_predicate,
                COUNT(DISTINCT subject_cui_primary) AS n_unique_subject_cui_primary,
                COUNT(DISTINCT object_cui_primary) AS n_unique_object_cui_primary,
                COUNT(DISTINCT subject_name_primary) AS n_unique_subject_name_primary,
                COUNT(DISTINCT object_name_primary) AS n_unique_object_name_primary,
                COUNT(DISTINCT exact_triple_key) AS n_unique_exact_triple,
                COUNT(DISTINCT directed_pair_key) AS n_unique_directed_pair,
                COUNT(DISTINCT pair_connection_key) AS n_unique_pair_connection,
                SUM(
                    CASE
                        WHEN subject_cui_primary = object_cui_primary THEN 1
                        ELSE 0
                    END
                ) AS n_self_loop_triples
            FROM final_network_units
            GROUP BY PYEAR
        ),
        year_cui AS (
            SELECT
                PYEAR AS pyear,
                COUNT(DISTINCT primary_cui) AS n_unique_cui_primary,
                COUNT(DISTINCT primary_name) AS n_unique_name_primary
            FROM final_long_cui
            GROUP BY PYEAR
        ),
        totals AS (
            SELECT
                COUNT(*) AS total_triples,
                COUNT(DISTINCT PMID) AS total_pmids
            FROM final_analytic
        )
        SELECT
            year_base.*,
            year_cui.n_unique_cui_primary,
            year_cui.n_unique_name_primary,
            year_base.n_pmid_unique_triples::DOUBLE / totals.total_triples
                AS share_pmid_unique_triples,
            year_base.n_unique_pmid::DOUBLE / totals.total_pmids
                AS share_unique_pmid,
            year_base.n_self_loop_triples::DOUBLE /
                NULLIF(year_base.n_pmid_unique_triples, 0)
                AS share_self_loop_triples
        FROM year_base
        LEFT JOIN year_cui USING (pyear)
        CROSS JOIN totals
        ORDER BY pyear_int NULLS LAST, pyear
        """
    ).fetchall()
    columns = [column[0] for column in con.description]
    return [dict(zip(columns, row)) for row in rows]


def prior_count(
    con: duckdb.DuckDBPyConnection,
    table: str,
    key_column: str,
    pyear_int: int,
    window: int,
) -> int:
    return int_scalar(
        con,
        f"""
        SELECT COUNT(DISTINCT {key_column})
        FROM {table}
        WHERE pyear_int BETWEEN ? AND ?
        """,
        [pyear_int - window, pyear_int - 1],
    )


def prior_overlap_count(
    con: duckdb.DuckDBPyConnection,
    table: str,
    key_column: str,
    focal_year: str,
    pyear_int: int,
    window: int,
) -> int:
    return int_scalar(
        con,
        f"""
        SELECT COUNT(DISTINCT focal.{key_column})
        FROM (
            SELECT DISTINCT {key_column}
            FROM {table}
            WHERE PYEAR = ?
        ) AS focal
        INNER JOIN (
            SELECT DISTINCT {key_column}
            FROM {table}
            WHERE pyear_int BETWEEN ? AND ?
        ) AS prior
            ON focal.{key_column} = prior.{key_column}
        """,
        [focal_year, pyear_int - window, pyear_int - 1],
    )


def write_yearly_descriptive_statistics(
    con: duckdb.DuckDBPyConnection,
    output_file: Path,
) -> None:
    print(f"Writing yearly descriptive statistics to {output_file}")
    con.execute(
        """
        CREATE TEMP TABLE year_cui_distinct AS
        SELECT DISTINCT PYEAR, pyear_int, primary_cui
        FROM final_long_cui
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE year_pair_distinct AS
        SELECT DISTINCT PYEAR, pyear_int, pair_connection_key
        FROM final_network_units
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE year_exact_triple_distinct AS
        SELECT DISTINCT PYEAR, pyear_int, exact_triple_key
        FROM final_network_units
        """
    )

    base_rows = yearly_base_rows(con)
    available_years = {
        int(row["pyear_int"])
        for row in base_rows
        if row.get("pyear_int") is not None
    }

    output_rows = []
    for row in base_rows:
        pyear = str(row["pyear"])
        pyear_int = row.get("pyear_int")
        output = {
            "pyear": pyear,
            "n_pmid_unique_triples": row["n_pmid_unique_triples"],
            "share_pmid_unique_triples": row["share_pmid_unique_triples"],
            "n_unique_pmid": row["n_unique_pmid"],
            "share_unique_pmid": row["share_unique_pmid"],
            "n_unique_predicate": row["n_unique_predicate"],
            "n_unique_subject_cui_primary": row["n_unique_subject_cui_primary"],
            "n_unique_object_cui_primary": row["n_unique_object_cui_primary"],
            "n_unique_cui_primary": row["n_unique_cui_primary"],
            "n_unique_subject_name_primary": row["n_unique_subject_name_primary"],
            "n_unique_object_name_primary": row["n_unique_object_name_primary"],
            "n_unique_name_primary": row["n_unique_name_primary"],
            "n_unique_exact_triple": row["n_unique_exact_triple"],
            "n_unique_directed_pair": row["n_unique_directed_pair"],
            "n_unique_pair_connection": row["n_unique_pair_connection"],
            "n_self_loop_triples": row["n_self_loop_triples"],
            "share_self_loop_triples": row["share_self_loop_triples"],
        }

        if pyear_int is None:
            for window in PRIOR_WINDOWS:
                output[f"n_prior_{window}y_cui_primary"] = None
                output[f"n_overlap_prior_{window}y_cui_primary"] = None
                output[f"share_focal_cui_seen_prior_{window}y"] = None
                output[f"n_new_cui_vs_prior_{window}y"] = None
                output[f"has_full_prior_{window}y_window"] = False
            output["n_prior_5y_pair_connection"] = None
            output["n_overlap_prior_5y_pair_connection"] = None
            output["share_focal_pair_connection_seen_prior_5y"] = None
            output["n_prior_5y_exact_triple"] = None
            output["n_overlap_prior_5y_exact_triple"] = None
            output["share_focal_exact_triple_seen_prior_5y"] = None
            output_rows.append(output)
            continue

        pyear_int = int(pyear_int)
        for window in PRIOR_WINDOWS:
            prior = prior_count(
                con,
                "year_cui_distinct",
                "primary_cui",
                pyear_int,
                window,
            )
            overlap = prior_overlap_count(
                con,
                "year_cui_distinct",
                "primary_cui",
                pyear,
                pyear_int,
                window,
            )
            focal = int(row["n_unique_cui_primary"])
            output[f"n_prior_{window}y_cui_primary"] = prior
            output[f"n_overlap_prior_{window}y_cui_primary"] = overlap
            output[f"share_focal_cui_seen_prior_{window}y"] = safe_divide(
                overlap,
                focal,
            )
            output[f"n_new_cui_vs_prior_{window}y"] = focal - overlap
            output[f"has_full_prior_{window}y_window"] = all(
                year in available_years
                for year in range(pyear_int - window, pyear_int)
            )

        pair_prior = prior_count(
            con,
            "year_pair_distinct",
            "pair_connection_key",
            pyear_int,
            5,
        )
        pair_overlap = prior_overlap_count(
            con,
            "year_pair_distinct",
            "pair_connection_key",
            pyear,
            pyear_int,
            5,
        )
        output["n_prior_5y_pair_connection"] = pair_prior
        output["n_overlap_prior_5y_pair_connection"] = pair_overlap
        output["share_focal_pair_connection_seen_prior_5y"] = safe_divide(
            pair_overlap,
            int(row["n_unique_pair_connection"]),
        )

        triple_prior = prior_count(
            con,
            "year_exact_triple_distinct",
            "exact_triple_key",
            pyear_int,
            5,
        )
        triple_overlap = prior_overlap_count(
            con,
            "year_exact_triple_distinct",
            "exact_triple_key",
            pyear,
            pyear_int,
            5,
        )
        output["n_prior_5y_exact_triple"] = triple_prior
        output["n_overlap_prior_5y_exact_triple"] = triple_overlap
        output["share_focal_exact_triple_seen_prior_5y"] = safe_divide(
            triple_overlap,
            int(row["n_unique_exact_triple"]),
        )
        output_rows.append(output)

    with output_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=YEARLY_COLUMNS)
        writer.writeheader()
        writer.writerows(output_rows)
    print(f"Saved {len(output_rows):,} rows to {output_file}")


def write_predicate_year_descriptive_statistics(
    con: duckdb.DuckDBPyConnection,
    output_file: Path,
) -> None:
    print(f"Writing predicate-year descriptive statistics to {output_file}")
    con.execute(
        f"""
        COPY (
            WITH base AS (
                SELECT
                    PYEAR AS pyear,
                    TRY_CAST(PYEAR AS INTEGER) AS pyear_int,
                    PREDICATE AS predicate,
                    COUNT(*) AS n_pmid_unique_triples,
                    COUNT(DISTINCT PMID) AS n_unique_pmid,
                    COUNT(DISTINCT subject_cui_primary)
                        AS n_unique_subject_cui_primary,
                    COUNT(DISTINCT object_cui_primary)
                        AS n_unique_object_cui_primary,
                    COUNT(DISTINCT subject_name_primary)
                        AS n_unique_subject_name_primary,
                    COUNT(DISTINCT object_name_primary)
                        AS n_unique_object_name_primary,
                    COUNT(DISTINCT subject_cui_primary || chr(31) || PREDICATE
                        || chr(31) || object_cui_primary)
                        AS n_unique_exact_triple,
                    COUNT(DISTINCT subject_cui_primary || chr(31) ||
                        object_cui_primary)
                        AS n_unique_directed_pair,
                    COUNT(DISTINCT LEAST(subject_cui_primary, object_cui_primary)
                        || chr(31) ||
                        GREATEST(subject_cui_primary, object_cui_primary))
                        AS n_unique_pair_connection
                FROM final_analytic
                GROUP BY PYEAR, PREDICATE
            ),
            long_counts AS (
                SELECT
                    PYEAR AS pyear,
                    PREDICATE AS predicate,
                    COUNT(DISTINCT primary_cui) AS n_unique_cui_primary,
                    COUNT(DISTINCT primary_name) AS n_unique_name_primary
                FROM final_long_cui
                GROUP BY PYEAR, PREDICATE
            ),
            year_totals AS (
                SELECT PYEAR AS pyear, COUNT(*) AS n_year_triples
                FROM final_analytic
                GROUP BY PYEAR
            ),
            all_total AS (
                SELECT COUNT(*) AS n_all_triples FROM final_analytic
            ),
            predicate_totals AS (
                SELECT
                    PREDICATE AS predicate,
                    COUNT(*) AS n_predicate_triples
                FROM final_analytic
                GROUP BY PREDICATE
            ),
            predicate_ranks AS (
                SELECT
                    predicate,
                    DENSE_RANK() OVER (
                        ORDER BY n_predicate_triples DESC
                    ) AS predicate_rank_overall_by_triples
                FROM predicate_totals
            )
            SELECT
                base.pyear,
                base.predicate,
                base.n_pmid_unique_triples,
                base.n_unique_pmid,
                base.n_unique_subject_cui_primary,
                base.n_unique_object_cui_primary,
                long_counts.n_unique_cui_primary,
                base.n_unique_subject_name_primary,
                base.n_unique_object_name_primary,
                long_counts.n_unique_name_primary,
                base.n_unique_exact_triple,
                base.n_unique_directed_pair,
                base.n_unique_pair_connection,
                base.n_pmid_unique_triples::DOUBLE /
                    NULLIF(year_totals.n_year_triples, 0)
                    AS share_of_year_pmid_unique_triples,
                base.n_pmid_unique_triples::DOUBLE /
                    NULLIF(all_total.n_all_triples, 0)
                    AS share_of_all_pmid_unique_triples,
                DENSE_RANK() OVER (
                    PARTITION BY base.pyear
                    ORDER BY base.n_pmid_unique_triples DESC
                ) AS predicate_rank_within_year_by_triples,
                predicate_ranks.predicate_rank_overall_by_triples
            FROM base
            LEFT JOIN long_counts
                ON base.pyear = long_counts.pyear
                AND base.predicate = long_counts.predicate
            LEFT JOIN year_totals
                ON base.pyear = year_totals.pyear
            CROSS JOIN all_total
            LEFT JOIN predicate_ranks
                ON base.predicate = predicate_ranks.predicate
            ORDER BY base.pyear_int NULLS LAST, base.pyear, base.predicate
        )
        TO {sql_literal(output_file)}
        (HEADER, DELIMITER ',')
        """
    )


def write_cui_descriptive_statistics(
    con: duckdb.DuckDBPyConnection,
    output_file: Path,
) -> None:
    print(f"Writing CUI descriptive statistics to {output_file}")
    con.execute(
        f"""
        COPY (
            WITH names AS (
                SELECT primary_cui, primary_name, COUNT(*) AS n_name_rows
                FROM final_long_cui
                WHERE primary_name IS NOT NULL
                GROUP BY primary_cui, primary_name
            ),
            primary_names AS (
                SELECT primary_cui, primary_name
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY primary_cui
                            ORDER BY n_name_rows DESC, primary_name ASC
                        ) AS name_rank
                    FROM names
                )
                WHERE name_rank = 1
            ),
            name_lists AS (
                SELECT
                    primary_cui,
                    COUNT(*) AS n_distinct_primary_name,
                    string_agg(primary_name, '|' ORDER BY primary_name)
                        AS all_primary_names
                FROM (
                    SELECT DISTINCT primary_cui, primary_name
                    FROM final_long_cui
                    WHERE primary_name IS NOT NULL
                )
                GROUP BY primary_cui
            ),
            semtypes AS (
                SELECT primary_cui, primary_semtype, COUNT(*) AS n_semtype_rows
                FROM final_long_cui
                WHERE primary_semtype IS NOT NULL
                GROUP BY primary_cui, primary_semtype
            ),
            primary_semtypes AS (
                SELECT primary_cui, primary_semtype
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY primary_cui
                            ORDER BY n_semtype_rows DESC, primary_semtype ASC
                        ) AS semtype_rank
                    FROM semtypes
                )
                WHERE semtype_rank = 1
            ),
            semtype_lists AS (
                SELECT
                    primary_cui,
                    COUNT(*) AS n_distinct_primary_semtype,
                    string_agg(primary_semtype, '|' ORDER BY primary_semtype)
                        AS all_primary_semtypes
                FROM (
                    SELECT DISTINCT primary_cui, primary_semtype
                    FROM final_long_cui
                    WHERE primary_semtype IS NOT NULL
                )
                GROUP BY primary_cui
            ),
            overall AS (
                SELECT
                    primary_cui,
                    COUNT(DISTINCT source_order) AS n_pmid_unique_triples,
                    COUNT(DISTINCT PMID) AS n_unique_pmid,
                    COUNT(DISTINCT PYEAR) AS n_unique_pyear,
                    MIN(pyear_int) AS first_seen_pyear,
                    MAX(pyear_int) AS last_seen_pyear,
                    COUNT(DISTINCT pyear_int) AS n_active_pyear,
                    COUNT(DISTINCT PREDICATE) AS n_unique_predicate
                FROM final_long_cui
                GROUP BY primary_cui
            ),
            role_counts AS (
                SELECT
                    primary_cui,
                    COUNT(DISTINCT source_order) FILTER (WHERE role = 'subject')
                        AS n_subject_triples,
                    COUNT(DISTINCT source_order) FILTER (WHERE role = 'object')
                        AS n_object_triples,
                    COUNT(DISTINCT PMID) FILTER (WHERE role = 'subject')
                        AS n_unique_pmid_as_subject,
                    COUNT(DISTINCT PMID) FILTER (WHERE role = 'object')
                        AS n_unique_pmid_as_object,
                    COUNT(DISTINCT PREDICATE) FILTER (WHERE role = 'subject')
                        AS n_unique_predicate_as_subject,
                    COUNT(DISTINCT PREDICATE) FILTER (WHERE role = 'object')
                        AS n_unique_predicate_as_object
                FROM final_long_cui
                GROUP BY primary_cui
            ),
            neighbors AS (
                SELECT
                    primary_cui,
                    COUNT(DISTINCT neighbor_cui) FILTER (
                        WHERE neighbor_cui IS NOT NULL
                            AND neighbor_cui != primary_cui
                    ) AS n_unique_neighbor_cui,
                    COUNT(DISTINCT neighbor_cui) FILTER (
                        WHERE role = 'subject'
                            AND neighbor_cui IS NOT NULL
                            AND neighbor_cui != primary_cui
                    ) AS n_unique_out_neighbor_cui,
                    COUNT(DISTINCT neighbor_cui) FILTER (
                        WHERE role = 'object'
                            AND neighbor_cui IS NOT NULL
                            AND neighbor_cui != primary_cui
                    ) AS n_unique_in_neighbor_cui
                FROM final_long_cui
                GROUP BY primary_cui
            )
            SELECT
                overall.primary_cui,
                primary_names.primary_name,
                primary_semtypes.primary_semtype,
                name_lists.n_distinct_primary_name,
                name_lists.all_primary_names,
                semtype_lists.n_distinct_primary_semtype,
                semtype_lists.all_primary_semtypes,
                overall.n_pmid_unique_triples,
                overall.n_unique_pmid,
                overall.n_unique_pyear,
                overall.first_seen_pyear,
                overall.last_seen_pyear,
                (
                    overall.last_seen_pyear - overall.first_seen_pyear + 1
                ) AS active_year_span,
                overall.n_active_pyear,
                role_counts.n_subject_triples,
                role_counts.n_object_triples,
                role_counts.n_subject_triples::DOUBLE /
                    NULLIF(
                        role_counts.n_subject_triples
                        + role_counts.n_object_triples,
                        0
                    ) AS share_as_subject,
                role_counts.n_object_triples::DOUBLE /
                    NULLIF(
                        role_counts.n_subject_triples
                        + role_counts.n_object_triples,
                        0
                    ) AS share_as_object,
                role_counts.n_unique_pmid_as_subject,
                role_counts.n_unique_pmid_as_object,
                overall.n_unique_predicate,
                role_counts.n_unique_predicate_as_subject,
                role_counts.n_unique_predicate_as_object,
                neighbors.n_unique_neighbor_cui,
                neighbors.n_unique_out_neighbor_cui,
                neighbors.n_unique_in_neighbor_cui
            FROM overall
            LEFT JOIN primary_names USING (primary_cui)
            LEFT JOIN primary_semtypes USING (primary_cui)
            LEFT JOIN name_lists USING (primary_cui)
            LEFT JOIN semtype_lists USING (primary_cui)
            LEFT JOIN role_counts USING (primary_cui)
            LEFT JOIN neighbors USING (primary_cui)
            ORDER BY overall.n_pmid_unique_triples DESC, overall.primary_cui
        )
        TO {sql_literal(output_file)}
        (HEADER, DELIMITER ',')
        """
    )


def run_preparation(
    source_dir: Path,
    output_dir: Path,
    overwrite: bool,
) -> None:
    predication_file = discover_source_file(source_dir, PREDICATION_BASENAME)
    citations_file = discover_source_file(source_dir, CITATIONS_BASENAME)

    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {name: output_dir / name for name in LOGICAL_OUTPUT_FILES}
    check_outputs(list(outputs.values()), overwrite)

    temp_dir = Path(tempfile.mkdtemp(prefix="duckdb_prepare_", dir=output_dir))
    print(f"Using DuckDB temp directory: {temp_dir}")
    try:
        con = duckdb.connect(database=":memory:")
        configure_duckdb(con, temp_dir)
        create_input_tables(con, predication_file, citations_file)
        create_final_table(con)
        write_main_parquet(con, outputs[LOGICAL_OUTPUT_FILES[0]])
        diagnostic = collect_diagnostics(
            con,
            predication_file,
            citations_file,
            outputs[LOGICAL_OUTPUT_FILES[0]],
        )
        write_single_row_csv(
            outputs[LOGICAL_OUTPUT_FILES[1]],
            DIAGNOSTIC_COLUMNS,
            diagnostic,
        )
        con.execute("DROP TABLE predications_prepared")
        create_final_views(con)
        write_yearly_descriptive_statistics(con, outputs[LOGICAL_OUTPUT_FILES[2]])
        write_predicate_year_descriptive_statistics(
            con,
            outputs[LOGICAL_OUTPUT_FILES[3]],
        )
        write_cui_descriptive_statistics(con, outputs[LOGICAL_OUTPUT_FILES[4]])
        con.close()
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    print("SemMedDB preparation complete.")
    for output in outputs.values():
        print(f"Output: {output}")


def main() -> None:
    source_dir = get_env_path("IC_SEMMED_SOURCE_DIR", DEFAULT_SEMMED_SOURCE_DIR)
    project_root = get_env_path("IC_PROJECT_ROOT", DEFAULT_PROJECT_ROOT)
    output_dir = project_root / OUTPUT_SUBDIR

    print(f"SemMedDB source directory: {source_dir}")
    print(f"Project root: {project_root}")
    print(f"Output directory: {output_dir}")

    run_preparation(source_dir, output_dir, OVERWRITE)


if __name__ == "__main__":
    main()
