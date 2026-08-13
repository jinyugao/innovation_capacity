"""Build future five-year edge transitions for SemMedDB edge annotations.

For each focal-year annotated row, the script checks whether the exact directed
typed triple appears in the following five-year edge-annotation files. The
output preserves all focal edge-annotation columns and appends transition
diagnostics.
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

BASE_YEAR = 1980
N_YEARS = 39
FUTURE_WINDOW_YEARS = 5

EDGE_ANNOTATION_SUBDIR = Path("data/processed/knowledge_combination/edge_annotation")
EDGE_TRANSITION_SUBDIR = Path("data/processed/knowledge_combination/edge_transition")
SUMMARY_SUBDIR = EDGE_TRANSITION_SUBDIR / "summary"

EDGE_ANNOTATION_FILE_STEM = "edge_annotation"
OUTPUT_FILE_STEM = "edge_transition"
SUMMARY_FILE_STEM = "edge_transition_summary"

SUBJECT_CUI_COLUMN = "subject_cui_primary"
PREDICATE_COLUMN = "PREDICATE"
OBJECT_CUI_COLUMN = "object_cui_primary"
EDGE_ANNOTATION_COLUMN = "edge_annotation"

CATEGORY_NEW_NODE = "New_Node"
CATEGORY_NEW_COMBINATION = "New_Combination"
CATEGORY_NEW_RELATION = "New_Relation"
CATEGORY_REPEATED_TRIPLE = "Repeated_Triple"
CATEGORIES = [
    CATEGORY_NEW_NODE,
    CATEGORY_NEW_COMBINATION,
    CATEGORY_NEW_RELATION,
    CATEGORY_REPEATED_TRIPLE,
]
NEW_KNOWLEDGE_CATEGORIES = {
    CATEGORY_NEW_NODE,
    CATEGORY_NEW_COMBINATION,
    CATEGORY_NEW_RELATION,
}

TRANSITION_ADOPTED = "Adopted"
TRANSITION_CONTINUED = "Continued"
TRANSITION_DISAPPEARED = "Disappeared"
TRANSITION_NOT_ANALYZED = "Not_Analyzed"
TRANSITIONS = [
    TRANSITION_ADOPTED,
    TRANSITION_CONTINUED,
    TRANSITION_DISAPPEARED,
    TRANSITION_NOT_ANALYZED,
]

KEY_COLUMNS = [
    SUBJECT_CUI_COLUMN,
    PREDICATE_COLUMN,
    OBJECT_CUI_COLUMN,
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
    return project_root / EDGE_TRANSITION_SUBDIR / f"{OUTPUT_FILE_STEM}_{year}.parquet"


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
            f"with IC_EDGE_TRANSITION_OVERWRITE=1:\n{existing_text}"
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
        CREATE TEMP TABLE focal_annotation AS
        SELECT *
        FROM {parquet_relation(annotation_file)}
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE future_exact_triples AS
        SELECT DISTINCT
            {SUBJECT_CUI_COLUMN},
            {PREDICATE_COLUMN},
            {OBJECT_CUI_COLUMN}
        FROM {parquet_relation(future_annotation_files)}
        WHERE
            {SUBJECT_CUI_COLUMN} IS NOT NULL
            AND {PREDICATE_COLUMN} IS NOT NULL
            AND {OBJECT_CUI_COLUMN} IS NOT NULL
            AND trim({SUBJECT_CUI_COLUMN}) != ''
            AND trim({PREDICATE_COLUMN}) != ''
            AND trim({OBJECT_CUI_COLUMN}) != ''
            AND {SUBJECT_CUI_COLUMN} != {OBJECT_CUI_COLUMN}
        """
    )


def create_transition_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        CREATE TEMP TABLE edge_transition AS
        SELECT
            FA.*,
            FE.{SUBJECT_CUI_COLUMN} IS NOT NULL AS future_exact_triple_seen,
            CASE
                WHEN FA.{EDGE_ANNOTATION_COLUMN} = {sql_literal(CATEGORY_REPEATED_TRIPLE)}
                    AND FE.{SUBJECT_CUI_COLUMN} IS NOT NULL
                    THEN {sql_literal(TRANSITION_CONTINUED)}
                WHEN FA.{EDGE_ANNOTATION_COLUMN} = {sql_literal(CATEGORY_REPEATED_TRIPLE)}
                    THEN {sql_literal(TRANSITION_DISAPPEARED)}
                WHEN FA.{EDGE_ANNOTATION_COLUMN} IN (
                    {sql_literal(CATEGORY_NEW_NODE)},
                    {sql_literal(CATEGORY_NEW_COMBINATION)},
                    {sql_literal(CATEGORY_NEW_RELATION)}
                )
                    AND FE.{SUBJECT_CUI_COLUMN} IS NOT NULL
                    THEN {sql_literal(TRANSITION_ADOPTED)}
                WHEN FA.{EDGE_ANNOTATION_COLUMN} IN (
                    {sql_literal(CATEGORY_NEW_NODE)},
                    {sql_literal(CATEGORY_NEW_COMBINATION)},
                    {sql_literal(CATEGORY_NEW_RELATION)}
                )
                    THEN {sql_literal(TRANSITION_DISAPPEARED)}
                ELSE {sql_literal(TRANSITION_NOT_ANALYZED)}
            END AS future_five_year_transition,
            concat(
                FA.{EDGE_ANNOTATION_COLUMN},
                ' -> ',
                CASE
                    WHEN FA.{EDGE_ANNOTATION_COLUMN} = {sql_literal(CATEGORY_REPEATED_TRIPLE)}
                        AND FE.{SUBJECT_CUI_COLUMN} IS NOT NULL
                        THEN {sql_literal(TRANSITION_CONTINUED)}
                    WHEN FA.{EDGE_ANNOTATION_COLUMN} = {sql_literal(CATEGORY_REPEATED_TRIPLE)}
                        THEN {sql_literal(TRANSITION_DISAPPEARED)}
                    WHEN FA.{EDGE_ANNOTATION_COLUMN} IN (
                        {sql_literal(CATEGORY_NEW_NODE)},
                        {sql_literal(CATEGORY_NEW_COMBINATION)},
                        {sql_literal(CATEGORY_NEW_RELATION)}
                    )
                        AND FE.{SUBJECT_CUI_COLUMN} IS NOT NULL
                        THEN {sql_literal(TRANSITION_ADOPTED)}
                    WHEN FA.{EDGE_ANNOTATION_COLUMN} IN (
                        {sql_literal(CATEGORY_NEW_NODE)},
                        {sql_literal(CATEGORY_NEW_COMBINATION)},
                        {sql_literal(CATEGORY_NEW_RELATION)}
                    )
                        THEN {sql_literal(TRANSITION_DISAPPEARED)}
                    ELSE {sql_literal(TRANSITION_NOT_ANALYZED)}
                END
            ) AS edge_annotation_to_future_transition
        FROM focal_annotation AS FA
        LEFT JOIN future_exact_triples AS FE
            ON FA.{SUBJECT_CUI_COLUMN} = FE.{SUBJECT_CUI_COLUMN}
            AND FA.{PREDICATE_COLUMN} = FE.{PREDICATE_COLUMN}
            AND FA.{OBJECT_CUI_COLUMN} = FE.{OBJECT_CUI_COLUMN}
        """
    )


def write_transition_parquet(
    con: duckdb.DuckDBPyConnection,
    output_path: Path,
) -> None:
    con.execute(
        f"""
        COPY (
            SELECT *
            FROM edge_transition
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
    n_transition_rows = int_scalar(con, "SELECT COUNT(*) FROM edge_transition")
    n_future_exact_triples = int_scalar(
        con,
        "SELECT COUNT(*) FROM future_exact_triples",
    )
    n_future_exact_triple_seen = int_scalar(
        con,
        "SELECT COUNT(*) FROM edge_transition WHERE future_exact_triple_seen",
    )
    row: dict[str, Any] = {
        "pyear": focal_year,
        "future_years_found": "|".join(str(year) for year in future_years),
        "n_future_years_found": len(future_years),
        "n_edge_annotation_rows": n_transition_rows,
        "n_transition_rows": n_transition_rows,
        "n_future_exact_triples": n_future_exact_triples,
        "n_future_exact_triple_seen": n_future_exact_triple_seen,
        "share_future_exact_triple_seen": safe_share(
            n_future_exact_triple_seen,
            n_transition_rows,
        ),
    }

    for transition in TRANSITIONS:
        row[f"n_{transition}"] = int_scalar(
            con,
            """
            SELECT COUNT(*)
            FROM edge_transition
            WHERE future_five_year_transition = ?
            """,
            [transition],
        )
    for transition in TRANSITIONS:
        row[f"share_{transition}"] = safe_share(
            row[f"n_{transition}"],
            n_transition_rows,
        )

    for category in CATEGORIES:
        n_category = int_scalar(
            con,
            f"""
            SELECT COUNT(*)
            FROM edge_transition
            WHERE {EDGE_ANNOTATION_COLUMN} = ?
            """,
            [category],
        )
        row[f"n_{category}"] = n_category
        if category in NEW_KNOWLEDGE_CATEGORIES:
            row[f"n_{category}_Adopted"] = int_scalar(
                con,
                f"""
                SELECT COUNT(*)
                FROM edge_transition
                WHERE {EDGE_ANNOTATION_COLUMN} = ?
                    AND future_five_year_transition = ?
                """,
                [category, TRANSITION_ADOPTED],
            )
            row[f"n_{category}_Disappeared"] = int_scalar(
                con,
                f"""
                SELECT COUNT(*)
                FROM edge_transition
                WHERE {EDGE_ANNOTATION_COLUMN} = ?
                    AND future_five_year_transition = ?
                """,
                [category, TRANSITION_DISAPPEARED],
            )
            row[f"share_{category}_Adopted"] = safe_share(
                row[f"n_{category}_Adopted"],
                n_category,
            )
        elif category == CATEGORY_REPEATED_TRIPLE:
            row[f"n_{category}_Continued"] = int_scalar(
                con,
                f"""
                SELECT COUNT(*)
                FROM edge_transition
                WHERE {EDGE_ANNOTATION_COLUMN} = ?
                    AND future_five_year_transition = ?
                """,
                [category, TRANSITION_CONTINUED],
            )
            row[f"n_{category}_Disappeared"] = int_scalar(
                con,
                f"""
                SELECT COUNT(*)
                FROM edge_transition
                WHERE {EDGE_ANNOTATION_COLUMN} = ?
                    AND future_five_year_transition = ?
                """,
                [category, TRANSITION_DISAPPEARED],
            )
            row[f"share_{category}_Continued"] = safe_share(
                row[f"n_{category}_Continued"],
                n_category,
            )

    return row


def build_edge_transition(
    project_root: Path,
    focal_year: int,
    overwrite: bool,
) -> None:
    future_years = future_years_for_focal_year(focal_year)
    annotation_file = edge_annotation_file(project_root, focal_year)
    future_annotation_files = check_inputs(project_root, focal_year)
    transition_output_file = output_file(project_root, focal_year)
    transition_summary_file = summary_file(project_root, focal_year)
    check_outputs([transition_output_file, transition_summary_file], overwrite)

    temp_parent = transition_output_file.parent
    temp_parent.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(tempfile.mkdtemp(prefix="duckdb_edge_transition_", dir=temp_parent))
    print(f"Using DuckDB temp directory: {temp_dir}")

    con = duckdb.connect(database=":memory:")
    try:
        configure_duckdb(con, temp_dir)
        print(f"Focal year: {focal_year}")
        print(f"Future years: {future_years}")
        print(f"Edge annotation input: {annotation_file}")
        print(
            "Future edge-annotation inputs:\n"
            + "\n".join(str(path) for path in future_annotation_files)
        )

        create_input_tables(con, annotation_file, future_annotation_files)
        print(
            "Focal edge-annotation rows: "
            f"{int_scalar(con, 'SELECT COUNT(*) FROM focal_annotation'):,}"
        )
        print(
            "Future exact directed typed triples: "
            f"{int_scalar(con, 'SELECT COUNT(*) FROM future_exact_triples'):,}"
        )
        create_transition_table(con)
        print(
            "Rows with future exact-triple match: "
            f"{int_scalar(con, 'SELECT COUNT(*) FROM edge_transition WHERE future_exact_triple_seen'):,}"
        )
        write_transition_parquet(con, transition_output_file)
        summary = collect_summary(con, focal_year, future_years)
        write_single_row_csv(transition_summary_file, summary)
    finally:
        con.close()
        shutil.rmtree(temp_dir, ignore_errors=True)

    print(f"Saved edge transition to {transition_output_file}")
    print(f"Saved edge transition summary to {transition_summary_file}")


def main() -> None:
    project_root = get_env_path("IC_PROJECT_ROOT", DEFAULT_PROJECT_ROOT)
    focal_year = get_focal_year()
    overwrite = get_env_bool("IC_EDGE_TRANSITION_OVERWRITE", False)

    print(f"Project root: {project_root}")
    print(f"Focal year: {focal_year}")
    print(f"Overwrite outputs: {overwrite}")
    build_edge_transition(project_root, focal_year, overwrite)
    print("Edge transition construction complete.")


if __name__ == "__main__":
    main()
