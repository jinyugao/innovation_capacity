"""Build the CUI text table for semantic proximity.

The table provides one representative text string per primary CUI/identifier.
It is the stable lookup used before building BiomedBERT CUI embeddings.
"""

from __future__ import annotations

import csv
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

try:
    import duckdb
except ImportError as exc:  # pragma: no cover - exercised on HPC if missing.
    raise SystemExit(
        "Missing required Python package: duckdb. Install it in the HPC "
        "environment before running this script."
    ) from exc


DEFAULT_PROJECT_ROOT = Path(
    "/xdisk/sebratt/jinyugao/research/projects/innovation_capacity"
)

INPUT_SUBDIR = Path("data/processed/semmedVER43_R")
INPUT_FILE_NAME = "semmedVER43_2024_R_cui_descriptive_statistics.csv"

OUTPUT_SUBDIR = Path("data/processed/knowledge_combination/semantic_proximity/cui_text")
OUTPUT_FILE_NAME = "cui_text_table.parquet"
SUMMARY_FILE_NAME = "cui_text_table_summary.csv"

OUTPUT_COLUMNS = [
    "primary_cui",
    "cui_text",
    "cui_text_source",
    "primary_name",
    "primary_semtype",
    "n_distinct_primary_name",
    "all_primary_names",
    "n_distinct_primary_semtype",
    "all_primary_semtypes",
    "n_pmid_unique_triples",
    "n_unique_pmid",
    "first_seen_pyear",
    "last_seen_pyear",
]


def get_env_path(name: str, default: Path) -> Path:
    value = os.environ.get(name)
    return Path(value).expanduser() if value else default


def get_env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


def sql_literal(value: Path | str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def configure_duckdb(con: duckdb.DuckDBPyConnection, temp_dir: Path) -> None:
    threads = os.environ.get("IC_DUCKDB_THREADS")
    memory_limit = os.environ.get("IC_DUCKDB_MEMORY_LIMIT")

    con.execute(f"SET temp_directory={sql_literal(temp_dir)}")
    if threads:
        con.execute(f"PRAGMA threads={int(threads)}")
    if memory_limit:
        con.execute(f"PRAGMA memory_limit={sql_literal(memory_limit)}")


def scalar(
    con: duckdb.DuckDBPyConnection,
    query: str,
    params: list[Any] | None = None,
) -> Any:
    return con.execute(query, params or []).fetchone()[0]


def int_scalar(
    con: duckdb.DuckDBPyConnection,
    query: str,
    params: list[Any] | None = None,
) -> int:
    value = scalar(con, query, params)
    return 0 if value is None else int(value)


def check_paths(input_file: Path, output_files: list[Path], overwrite: bool) -> None:
    if not input_file.exists():
        raise FileNotFoundError(f"Missing input CSV: {input_file}")

    for output_file in output_files:
        output_file.parent.mkdir(parents=True, exist_ok=True)

    existing = [path for path in output_files if path.exists()]
    if existing and not overwrite:
        existing_text = "\n".join(str(path) for path in existing)
        raise FileExistsError(
            "Output file(s) already exist. Move them, delete them, or rerun "
            f"with IC_CUI_TEXT_TABLE_OVERWRITE=1:\n{existing_text}"
        )
    if overwrite:
        for path in existing:
            path.unlink()


def write_single_row_csv(path: Path, row: dict[str, Any]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)


def build_cui_text_table(
    input_file: Path,
    output_file: Path,
    summary_file: Path,
    temp_dir: Path,
) -> None:
    con = duckdb.connect(database=":memory:")
    try:
        configure_duckdb(con, temp_dir)

        source = (
            "read_csv("
            f"{sql_literal(input_file)}, "
            "header=true, "
            "all_varchar=true, "
            "sample_size=-1"
            ")"
        )
        con.execute(
            f"""
            CREATE TEMP TABLE cui_text AS
            SELECT
                primary_cui,
                CASE
                    WHEN primary_name IS NOT NULL AND trim(primary_name) != ''
                        THEN primary_name
                    ELSE primary_cui
                END AS cui_text,
                CASE
                    WHEN primary_name IS NOT NULL AND trim(primary_name) != ''
                        THEN 'primary_name'
                    ELSE 'primary_cui'
                END AS cui_text_source,
                primary_name,
                primary_semtype,
                TRY_CAST(n_distinct_primary_name AS BIGINT)
                    AS n_distinct_primary_name,
                all_primary_names,
                TRY_CAST(n_distinct_primary_semtype AS BIGINT)
                    AS n_distinct_primary_semtype,
                all_primary_semtypes,
                TRY_CAST(n_pmid_unique_triples AS BIGINT)
                    AS n_pmid_unique_triples,
                TRY_CAST(n_unique_pmid AS BIGINT) AS n_unique_pmid,
                TRY_CAST(first_seen_pyear AS BIGINT) AS first_seen_pyear,
                TRY_CAST(last_seen_pyear AS BIGINT) AS last_seen_pyear
            FROM {source}
            WHERE primary_cui IS NOT NULL
                AND trim(primary_cui) != ''
            """
        )

        n_rows = int_scalar(con, "SELECT COUNT(*) FROM cui_text")
        n_unique_cui = int_scalar(con, "SELECT COUNT(DISTINCT primary_cui) FROM cui_text")
        n_duplicate_cui_rows = int_scalar(
            con,
            """
            SELECT COALESCE(SUM(n_rows - 1), 0)
            FROM (
                SELECT primary_cui, COUNT(*) AS n_rows
                FROM cui_text
                GROUP BY primary_cui
                HAVING COUNT(*) > 1
            )
            """,
        )
        if n_duplicate_cui_rows:
            raise ValueError(
                "CUI text table input is not unique by primary_cui. "
                f"Duplicate excess rows: {n_duplicate_cui_rows:,}"
            )

        n_missing_primary_name = int_scalar(
            con,
            """
            SELECT COUNT(*)
            FROM cui_text
            WHERE primary_name IS NULL OR trim(primary_name) = ''
            """,
        )
        n_using_primary_cui_fallback = int_scalar(
            con,
            "SELECT COUNT(*) FROM cui_text WHERE cui_text_source = 'primary_cui'",
        )
        n_multiple_names = int_scalar(
            con,
            """
            SELECT COUNT(*)
            FROM cui_text
            WHERE n_distinct_primary_name > 1
            """,
        )

        print(f"CUI text rows: {n_rows:,}")
        print(f"Unique primary_cui values: {n_unique_cui:,}")
        print(f"Rows missing primary_name: {n_missing_primary_name:,}")
        print(f"Rows using primary_cui fallback: {n_using_primary_cui_fallback:,}")
        print(f"Rows with multiple observed names: {n_multiple_names:,}")
        print(f"Writing CUI text table to {output_file}")

        con.execute(
            f"""
            COPY (
                SELECT {", ".join(OUTPUT_COLUMNS)}
                FROM cui_text
                ORDER BY n_pmid_unique_triples DESC NULLS LAST, primary_cui
            )
            TO {sql_literal(output_file)}
            (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )

        write_single_row_csv(
            summary_file,
            {
                "n_rows": n_rows,
                "n_unique_cui": n_unique_cui,
                "n_duplicate_cui_rows": n_duplicate_cui_rows,
                "n_missing_primary_name": n_missing_primary_name,
                "n_using_primary_cui_fallback": n_using_primary_cui_fallback,
                "n_multiple_observed_name_cui": n_multiple_names,
            },
        )
    finally:
        con.close()


def main() -> None:
    project_root = get_env_path("IC_PROJECT_ROOT", DEFAULT_PROJECT_ROOT)
    overwrite = get_env_bool("IC_CUI_TEXT_TABLE_OVERWRITE", False)
    input_file = project_root / INPUT_SUBDIR / INPUT_FILE_NAME
    output_dir = project_root / OUTPUT_SUBDIR
    output_file = output_dir / OUTPUT_FILE_NAME
    summary_file = output_dir / SUMMARY_FILE_NAME

    print(f"Project root: {project_root}")
    print(f"Input CSV: {input_file}")
    print(f"Output parquet: {output_file}")
    print(f"Summary CSV: {summary_file}")
    print(f"Overwrite outputs: {overwrite}")

    check_paths(input_file, [output_file, summary_file], overwrite)
    temp_dir = Path(tempfile.mkdtemp(prefix="duckdb_cui_text_", dir=output_dir))
    print(f"Using DuckDB temp directory: {temp_dir}")
    try:
        build_cui_text_table(input_file, output_file, summary_file, temp_dir)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    print("CUI text table complete.")


if __name__ == "__main__":
    main()
