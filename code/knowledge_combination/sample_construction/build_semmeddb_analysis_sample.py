"""Build the SemMedDB analysis-sample parquet.

This script filters the prepared PMID-unique SemMedDB triple parquet into the
analysis sample used by downstream knowledge-combination code. It preserves the
prepared parquet as the reproducible source and writes a separate filtered
analysis-sample parquet.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

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

SEMMED_PROCESSED_SUBDIR = Path("data/processed/semmedVER43_R")
INPUT_FILE_NAME = (
    "semmedVER43_2024_R_predications_with_pyear_filtered_pmid_unique_triples.parquet"
)
OUTPUT_FILE_NAME = "semmeddb_analysis_sample.parquet"

EXCLUDED_PYEAR_VALUES = ["5664"]
EXCLUDED_PREDICATES = ["1078", "1532", "241", "NOM", "VERB", "PREP"]


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


def sql_string_list(values: list[str]) -> str:
    return ", ".join(sql_literal(value) for value in values)


def configure_duckdb(con: duckdb.DuckDBPyConnection, temp_dir: Path) -> None:
    threads = os.environ.get("IC_DUCKDB_THREADS")
    memory_limit = os.environ.get("IC_DUCKDB_MEMORY_LIMIT")

    con.execute(f"SET temp_directory={sql_literal(temp_dir)}")
    if threads:
        con.execute(f"PRAGMA threads={int(threads)}")
    if memory_limit:
        con.execute(f"PRAGMA memory_limit={sql_literal(memory_limit)}")


def scalar(con: duckdb.DuckDBPyConnection, query: str) -> int:
    value = con.execute(query).fetchone()[0]
    return 0 if value is None else int(value)


def check_paths(input_file: Path, output_file: Path, overwrite: bool) -> None:
    if not input_file.exists():
        raise FileNotFoundError(f"Missing input parquet: {input_file}")
    if output_file.exists() and not overwrite:
        raise FileExistsError(
            "Output file already exists. Move it, delete it, or rerun with "
            f"IC_ANALYSIS_SAMPLE_OVERWRITE=1:\n{output_file}"
        )
    if output_file.exists() and overwrite:
        output_file.unlink()


def build_analysis_sample(
    input_file: Path,
    output_file: Path,
    temp_dir: Path,
) -> None:
    con = duckdb.connect(database=":memory:")
    try:
        configure_duckdb(con, temp_dir)

        source = f"read_parquet({sql_literal(input_file)})"
        excluded_years = sql_string_list(EXCLUDED_PYEAR_VALUES)
        excluded_predicates = sql_string_list(EXCLUDED_PREDICATES)
        keep_where = (
            f"CAST(PYEAR AS VARCHAR) NOT IN ({excluded_years}) "
            f"AND PREDICATE NOT IN ({excluded_predicates})"
        )

        n_input = scalar(con, f"SELECT COUNT(*) FROM {source}")
        n_excluded_pyear = scalar(
            con,
            f"""
            SELECT COUNT(*)
            FROM {source}
            WHERE CAST(PYEAR AS VARCHAR) IN ({excluded_years})
            """,
        )
        n_excluded_predicate = scalar(
            con,
            f"""
            SELECT COUNT(*)
            FROM {source}
            WHERE PREDICATE IN ({excluded_predicates})
            """,
        )
        n_excluded_either = scalar(
            con,
            f"""
            SELECT COUNT(*)
            FROM {source}
            WHERE
                CAST(PYEAR AS VARCHAR) IN ({excluded_years})
                OR PREDICATE IN ({excluded_predicates})
            """,
        )
        n_output = n_input - n_excluded_either

        print(f"Input rows: {n_input:,}")
        print(f"Rows with excluded PYEAR values: {n_excluded_pyear:,}")
        print(f"Rows with excluded predicates: {n_excluded_predicate:,}")
        print(f"Rows excluded by either rule: {n_excluded_either:,}")
        print(f"Expected output rows: {n_output:,}")
        print(f"Excluded PYEAR values: {', '.join(EXCLUDED_PYEAR_VALUES)}")
        print(f"Excluded predicates: {', '.join(EXCLUDED_PREDICATES)}")
        print(f"Writing analysis sample parquet to {output_file}")

        con.execute(
            f"""
            COPY (
                SELECT *
                FROM {source}
                WHERE {keep_where}
            )
            TO {sql_literal(output_file)}
            (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )

        n_written = scalar(
            con,
            f"SELECT COUNT(*) FROM read_parquet({sql_literal(output_file)})",
        )
        if n_written != n_output:
            raise RuntimeError(
                f"Output row count mismatch: expected {n_output:,}, "
                f"wrote {n_written:,}"
            )
        print(f"Rows written: {n_written:,}")
    finally:
        con.close()


def main() -> None:
    project_root = get_env_path("IC_PROJECT_ROOT", DEFAULT_PROJECT_ROOT)
    overwrite = get_env_bool("IC_ANALYSIS_SAMPLE_OVERWRITE", False)
    output_dir = project_root / SEMMED_PROCESSED_SUBDIR
    input_file = output_dir / INPUT_FILE_NAME
    output_file = output_dir / OUTPUT_FILE_NAME

    print(f"Project root: {project_root}")
    print(f"Input parquet: {input_file}")
    print(f"Output parquet: {output_file}")
    print(f"Overwrite output: {overwrite}")

    check_paths(input_file, output_file, overwrite)
    temp_dir = Path(tempfile.mkdtemp(prefix="duckdb_analysis_sample_", dir=output_dir))
    print(f"Using DuckDB temp directory: {temp_dir}")
    try:
        build_analysis_sample(input_file, output_file, temp_dir)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    print("SemMedDB analysis sample complete.")


if __name__ == "__main__":
    main()
