"""Split the SemMedDB analysis-sample parquet into yearly parquet files."""

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
INPUT_FILE_NAME = "semmeddb_analysis_sample.parquet"
OUTPUT_DIR_NAME = "semmeddb_analysis_sample"
OUTPUT_FILE_STEM = "semmeddb_analysis_sample"


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


def scalar(con: duckdb.DuckDBPyConnection, query: str) -> int:
    value = con.execute(query).fetchone()[0]
    return 0 if value is None else int(value)


def check_paths(input_file: Path, output_dir: Path, overwrite: bool) -> None:
    if not input_file.exists():
        raise FileNotFoundError(f"Missing input parquet: {input_file}")
    if output_dir.exists() and not overwrite:
        existing_files = list(output_dir.glob("*.parquet"))
        if existing_files:
            raise FileExistsError(
                "Output directory already contains parquet files. Move them, "
                "delete them, or rerun with IC_SPLIT_ANALYSIS_SAMPLE_OVERWRITE=1:\n"
                f"{output_dir}"
            )
    if output_dir.exists() and overwrite:
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)


def split_analysis_sample(
    input_file: Path,
    output_dir: Path,
    temp_dir: Path,
) -> None:
    partition_dir = temp_dir / "partitioned_by_pyear"
    con = duckdb.connect(database=":memory:")
    try:
        configure_duckdb(con, temp_dir)

        source = f"read_parquet({sql_literal(input_file)})"
        n_input = scalar(con, f"SELECT COUNT(*) FROM {source}")
        n_years = scalar(
            con,
            f"SELECT COUNT(DISTINCT PYEAR) FROM {source} WHERE PYEAR IS NOT NULL",
        )

        print(f"Input rows: {n_input:,}")
        print(f"Distinct PYEAR values: {n_years:,}")
        print(f"Writing partitioned parquet to temporary directory: {partition_dir}")

        con.execute(
            f"""
            COPY (
                SELECT *
                FROM {source}
            )
            TO {sql_literal(partition_dir)}
            (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                PARTITION_BY (PYEAR),
                WRITE_PARTITION_COLUMNS true,
                PER_THREAD_OUTPUT false,
                OVERWRITE_OR_IGNORE true
            )
            """
        )
    finally:
        con.close()

    written_files = []
    for partition_path in sorted(partition_dir.glob("PYEAR=*")):
        if not partition_path.is_dir():
            continue
        pyear = partition_path.name.split("=", 1)[1]
        parquet_files = sorted(partition_path.glob("*.parquet"))
        if len(parquet_files) != 1:
            raise RuntimeError(
                f"Expected one parquet file for {partition_path}, "
                f"found {len(parquet_files)}"
            )
        output_file = output_dir / f"{OUTPUT_FILE_STEM}_{pyear}.parquet"
        shutil.move(str(parquet_files[0]), output_file)
        written_files.append(output_file)

    if len(written_files) != n_years:
        raise RuntimeError(
            f"Expected {n_years:,} yearly files, wrote {len(written_files):,}"
        )

    con = duckdb.connect(database=":memory:")
    try:
        n_written = scalar(
            con,
            f"SELECT COUNT(*) FROM read_parquet({sql_literal(str(output_dir / '*.parquet'))})",
        )
    finally:
        con.close()
    if n_written != n_input:
        raise RuntimeError(
            f"Yearly row count mismatch: input {n_input:,}, yearly files {n_written:,}"
        )

    print(f"Yearly files written: {len(written_files):,}")
    print(f"Rows across yearly files: {n_written:,}")
    print(f"Output directory: {output_dir}")


def main() -> None:
    project_root = get_env_path("IC_PROJECT_ROOT", DEFAULT_PROJECT_ROOT)
    overwrite = get_env_bool("IC_SPLIT_ANALYSIS_SAMPLE_OVERWRITE", False)
    processed_dir = project_root / SEMMED_PROCESSED_SUBDIR
    input_file = processed_dir / INPUT_FILE_NAME
    output_dir = processed_dir / OUTPUT_DIR_NAME

    print(f"Project root: {project_root}")
    print(f"Input parquet: {input_file}")
    print(f"Output directory: {output_dir}")
    print(f"Overwrite output directory: {overwrite}")

    check_paths(input_file, output_dir, overwrite)
    temp_dir = Path(tempfile.mkdtemp(prefix="duckdb_split_analysis_sample_", dir=output_dir))
    print(f"Using DuckDB temp directory: {temp_dir}")
    try:
        split_analysis_sample(input_file, output_dir, temp_dir)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    print("SemMedDB analysis sample split complete.")


if __name__ == "__main__":
    main()
