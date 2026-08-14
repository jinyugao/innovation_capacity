"""Build future-development measures for focal-year New_Combination rows."""

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

BASE_YEAR = 1980
N_YEARS = 39
FUTURE_WINDOW_YEARS = 5

EDGE_ANNOTATION_SUBDIR = Path("data/processed/knowledge_combination/edge_annotation")
OUTPUT_SUBDIR = Path(
    "data/processed/knowledge_combination/future_development/new_combination"
)
SUMMARY_SUBDIR = OUTPUT_SUBDIR / "summary"

EDGE_ANNOTATION_FILE_STEM = "edge_annotation"
OUTPUT_FILE_STEM = "future_development_new_combination"
SUMMARY_FILE_STEM = "future_development_new_combination_summary"

SUBJECT_CUI_COLUMN = "subject_cui_primary"
PREDICATE_COLUMN = "PREDICATE"
OBJECT_CUI_COLUMN = "object_cui_primary"
EDGE_ANNOTATION_COLUMN = "edge_annotation"
NODE_A_COLUMN = "node_a"
NODE_B_COLUMN = "node_b"

CATEGORY_NEW_COMBINATION = "New_Combination"
CATEGORY_NEW_RELATION = "New_Relation"


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


def safe_share(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def get_focal_year() -> int:
    task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if task_id is None:
        focal_year = os.environ.get("IC_FOCAL_YEAR")
        if focal_year is None:
            raise RuntimeError("Set SLURM_ARRAY_TASK_ID or IC_FOCAL_YEAR.")
        return int(focal_year)

    task_index = int(task_id)
    if task_index < 0 or task_index >= N_YEARS:
        raise ValueError(
            f"SLURM_ARRAY_TASK_ID={task_index} is out of range. "
            f"Expected 0-{N_YEARS - 1}."
        )
    return BASE_YEAR + task_index


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


def edge_annotation_file(project_root: Path, year: int) -> Path:
    return (
        project_root
        / EDGE_ANNOTATION_SUBDIR
        / f"{EDGE_ANNOTATION_FILE_STEM}_{year}.parquet"
    )


def output_file(project_root: Path, year: int) -> Path:
    return project_root / OUTPUT_SUBDIR / f"{OUTPUT_FILE_STEM}_{year}.parquet"


def summary_file(project_root: Path, year: int) -> Path:
    return project_root / SUMMARY_SUBDIR / f"{SUMMARY_FILE_STEM}_{year}.csv"


def future_years_for_focal_year(focal_year: int) -> list[int]:
    return list(range(focal_year + 1, focal_year + FUTURE_WINDOW_YEARS + 1))


def parquet_relation(paths: list[Path] | Path) -> str:
    if isinstance(paths, Path):
        return f"read_parquet({sql_literal(paths)})"
    path_list = ", ".join(sql_literal(path) for path in paths)
    return f"read_parquet([{path_list}])"


def check_inputs(project_root: Path, focal_year: int) -> list[Path]:
    focal_file = edge_annotation_file(project_root, focal_year)
    future_files = [
        edge_annotation_file(project_root, year)
        for year in future_years_for_focal_year(focal_year)
    ]
    input_files = [focal_file, *future_files]
    missing = [path for path in input_files if not path.exists()]
    if missing:
        missing_text = "\n".join(str(path) for path in missing)
        raise FileNotFoundError(f"Missing required input file(s):\n{missing_text}")
    return future_files


def check_outputs(output_files: list[Path], overwrite: bool) -> None:
    for path in output_files:
        path.parent.mkdir(parents=True, exist_ok=True)

    existing = [path for path in output_files if path.exists()]
    if existing and not overwrite:
        existing_text = "\n".join(str(path) for path in existing)
        raise FileExistsError(
            "Output file(s) already exist. Move them, delete them, or rerun "
            "with IC_NEW_COMBINATION_FUTURE_DEVELOPMENT_OVERWRITE=1:\n"
            f"{existing_text}"
        )
    if overwrite:
        for path in existing:
            path.unlink()


def create_input_tables(
    con: duckdb.DuckDBPyConnection,
    annotation_file: Path,
    future_annotation_files: list[Path],
) -> None:
    con.execute(
        f"""
        CREATE TEMP TABLE focal_new_combination AS
        SELECT *
        FROM {parquet_relation(annotation_file)}
        WHERE {EDGE_ANNOTATION_COLUMN} = {sql_literal(CATEGORY_NEW_COMBINATION)}
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE future_new_relation AS
        SELECT
            {NODE_A_COLUMN},
            {NODE_B_COLUMN},
            {SUBJECT_CUI_COLUMN},
            {PREDICATE_COLUMN},
            {OBJECT_CUI_COLUMN}
        FROM {parquet_relation(future_annotation_files)}
        WHERE {EDGE_ANNOTATION_COLUMN} = {sql_literal(CATEGORY_NEW_RELATION)}
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE future_pair_new_relation_counts AS
        SELECT
            {NODE_A_COLUMN},
            {NODE_B_COLUMN},
            COUNT(*) AS n_future_focal_pair_new_relation_rows,
            COUNT(DISTINCT concat_ws(
                '\t',
                {SUBJECT_CUI_COLUMN},
                {PREDICATE_COLUMN},
                {OBJECT_CUI_COLUMN}
            )) AS n_future_focal_pair_new_relation_triples
        FROM future_new_relation
        GROUP BY {NODE_A_COLUMN}, {NODE_B_COLUMN}
        """
    )


def create_development_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        CREATE TEMP TABLE future_development AS
        SELECT
            F.*,
            COALESCE(C.n_future_focal_pair_new_relation_rows, 0)
                > 0 AS future_focal_pair_new_relation_seen,
            COALESCE(C.n_future_focal_pair_new_relation_rows, 0)
                AS n_future_focal_pair_new_relation_rows,
            COALESCE(C.n_future_focal_pair_new_relation_triples, 0)
                AS n_future_focal_pair_new_relation_triples
        FROM focal_new_combination AS F
        LEFT JOIN future_pair_new_relation_counts AS C
            ON F.{NODE_A_COLUMN} = C.{NODE_A_COLUMN}
            AND F.{NODE_B_COLUMN} = C.{NODE_B_COLUMN}
        """
    )


def write_parquet(con: duckdb.DuckDBPyConnection, output_path: Path) -> None:
    con.execute(
        f"""
        COPY (
            SELECT *
            FROM future_development
        )
        TO {sql_literal(output_path)}
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )


def write_single_row_csv(path: Path, row: dict[str, Any]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)


def collect_summary(
    con: duckdb.DuckDBPyConnection,
    focal_year: int,
    future_years: list[int],
) -> dict[str, Any]:
    n_rows = int_scalar(con, "SELECT COUNT(*) FROM future_development")
    n_developed_rows = int_scalar(
        con,
        """
        SELECT COUNT(*)
        FROM future_development
        WHERE future_focal_pair_new_relation_seen
        """,
    )
    return {
        "pyear": focal_year,
        "future_years_found": "|".join(str(year) for year in future_years),
        "n_future_years_found": len(future_years),
        "n_new_combination_rows": n_rows,
        "n_new_combination_rows_with_future_new_relation": n_developed_rows,
        "share_new_combination_rows_with_future_new_relation": safe_share(
            n_developed_rows,
            n_rows,
        ),
        "total_future_focal_pair_new_relation_rows": int_scalar(
            con,
            """
            SELECT COALESCE(SUM(n_future_focal_pair_new_relation_rows), 0)
            FROM future_development
            """,
        ),
        "total_future_focal_pair_new_relation_triples": int_scalar(
            con,
            """
            SELECT COALESCE(SUM(n_future_focal_pair_new_relation_triples), 0)
            FROM future_development
            """,
        ),
        "max_future_focal_pair_new_relation_rows": int_scalar(
            con,
            """
            SELECT COALESCE(MAX(n_future_focal_pair_new_relation_rows), 0)
            FROM future_development
            """,
        ),
        "max_future_focal_pair_new_relation_triples": int_scalar(
            con,
            """
            SELECT COALESCE(MAX(n_future_focal_pair_new_relation_triples), 0)
            FROM future_development
            """,
        ),
    }


def build_future_development(
    project_root: Path,
    focal_year: int,
    overwrite: bool,
) -> None:
    future_years = future_years_for_focal_year(focal_year)
    annotation_file = edge_annotation_file(project_root, focal_year)
    future_annotation_files = check_inputs(project_root, focal_year)
    development_output_file = output_file(project_root, focal_year)
    development_summary_file = summary_file(project_root, focal_year)
    check_outputs([development_output_file, development_summary_file], overwrite)

    temp_parent = development_output_file.parent
    temp_parent.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(
        tempfile.mkdtemp(prefix="duckdb_new_combination_future_", dir=temp_parent)
    )
    print(f"Using DuckDB temp directory: {temp_dir}")

    con = duckdb.connect(database=":memory:")
    try:
        configure_duckdb(con, temp_dir)
        print(f"Focal year: {focal_year}")
        print(f"Future years: {future_years}")
        print(f"Focal edge annotation input: {annotation_file}")
        print(
            "Future edge-annotation inputs:\n"
            + "\n".join(str(path) for path in future_annotation_files)
        )
        create_input_tables(con, annotation_file, future_annotation_files)
        print(
            "Focal New_Combination rows: "
            f"{int_scalar(con, 'SELECT COUNT(*) FROM focal_new_combination'):,}"
        )
        print(
            "Future New_Relation rows: "
            f"{int_scalar(con, 'SELECT COUNT(*) FROM future_new_relation'):,}"
        )
        create_development_table(con)
        print(
            "New_Combination rows with future New_Relation on focal pair: "
            f"{int_scalar(con, 'SELECT COUNT(*) FROM future_development WHERE future_focal_pair_new_relation_seen'):,}"
        )
        write_parquet(con, development_output_file)
        summary = collect_summary(con, focal_year, future_years)
        write_single_row_csv(development_summary_file, summary)
    finally:
        con.close()
        shutil.rmtree(temp_dir, ignore_errors=True)

    print(f"Saved New_Combination future development to {development_output_file}")
    print(f"Saved summary to {development_summary_file}")


def main() -> None:
    project_root = get_env_path("IC_PROJECT_ROOT", DEFAULT_PROJECT_ROOT)
    focal_year = get_focal_year()
    overwrite = get_env_bool(
        "IC_NEW_COMBINATION_FUTURE_DEVELOPMENT_OVERWRITE",
        False,
    )

    print(f"Project root: {project_root}")
    print(f"Focal year: {focal_year}")
    print(f"Overwrite outputs: {overwrite}")
    build_future_development(project_root, focal_year, overwrite)
    print("New_Combination future development construction complete.")


if __name__ == "__main__":
    main()
