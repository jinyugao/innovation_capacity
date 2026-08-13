"""Build yearly edge annotations for the SemMedDB analysis sample.

For each focal year, the script compares focal-year non-self-loop edges with
the prior five-year directed typed predication network and assigns one label:

1. New_Node
2. New_Combination
3. New_Relation
4. Repeated_Triple
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
N_YEARS = 40
PRIOR_WINDOW_YEARS = 5

SEMMED_YEARLY_SUBDIR = Path("data/processed/semmedVER43_R/semmeddb_analysis_sample")
EDGE_ANNOTATION_SUBDIR = Path("data/processed/knowledge_combination/edge_annotation")
SUMMARY_SUBDIR = EDGE_ANNOTATION_SUBDIR / "summary"
SELF_LOOP_SUMMARY_SUBDIR = EDGE_ANNOTATION_SUBDIR / "self_loop_summary"

INPUT_FILE_STEM = "semmeddb_analysis_sample"
OUTPUT_FILE_STEM = "edge_annotation"
SUMMARY_FILE_STEM = "edge_annotation_summary"
SELF_LOOP_SUMMARY_FILE_STEM = "edge_annotation_self_loop_summary"

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

KEY_COLUMNS = [
    "PMID",
    "subject_cui_primary",
    "PREDICATE",
    "object_cui_primary",
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
    value = con.execute(query, params or []).fetchone()[0]
    return value


def int_scalar(
    con: duckdb.DuckDBPyConnection,
    query: str,
    params: list[Any] | None = None,
) -> int:
    value = scalar(con, query, params)
    return 0 if value is None else int(value)


def yearly_input_file(project_root: Path, year: int) -> Path:
    return (
        project_root
        / SEMMED_YEARLY_SUBDIR
        / f"{INPUT_FILE_STEM}_{year}.parquet"
    )


def edge_annotation_file(project_root: Path, year: int) -> Path:
    return (
        project_root
        / EDGE_ANNOTATION_SUBDIR
        / f"{OUTPUT_FILE_STEM}_{year}.parquet"
    )


def summary_file(project_root: Path, year: int) -> Path:
    return (
        project_root
        / SUMMARY_SUBDIR
        / f"{SUMMARY_FILE_STEM}_{year}.csv"
    )


def self_loop_summary_file(project_root: Path, year: int) -> Path:
    return (
        project_root
        / SELF_LOOP_SUMMARY_SUBDIR
        / f"{SELF_LOOP_SUMMARY_FILE_STEM}_{year}.csv"
    )


def prior_years_for_focal_year(focal_year: int) -> list[int]:
    return list(range(focal_year - PRIOR_WINDOW_YEARS, focal_year))


def check_inputs(project_root: Path, focal_year: int) -> list[Path]:
    focal_file = yearly_input_file(project_root, focal_year)
    missing = [focal_file] if not focal_file.exists() else []
    prior_files = []
    for year in prior_years_for_focal_year(focal_year):
        prior_file = yearly_input_file(project_root, year)
        prior_files.append(prior_file)
        if not prior_file.exists():
            missing.append(prior_file)
    if missing:
        missing_text = "\n".join(str(path) for path in missing)
        raise FileNotFoundError(f"Missing required input file(s):\n{missing_text}")
    return prior_files


def check_outputs(output_files: list[Path], overwrite: bool) -> None:
    for output_file in output_files:
        output_file.parent.mkdir(parents=True, exist_ok=True)

    existing = [path for path in output_files if path.exists()]
    if existing and not overwrite:
        existing_text = "\n".join(str(path) for path in existing)
        raise FileExistsError(
            "Output file(s) already exist. Move them, delete them, or rerun "
            f"with IC_EDGE_ANNOTATION_OVERWRITE=1:\n{existing_text}"
        )
    if overwrite:
        for path in existing:
            path.unlink()


def parquet_relation(paths: list[Path] | Path) -> str:
    if isinstance(paths, Path):
        return f"read_parquet({sql_literal(paths)})"
    path_list = ", ".join(sql_literal(path) for path in paths)
    return f"read_parquet([{path_list}])"


def create_input_views(
    con: duckdb.DuckDBPyConnection,
    focal_file: Path,
    prior_files: list[Path],
) -> None:
    con.execute(
        f"""
        CREATE TEMP VIEW focal AS
        SELECT
            *,
            row_number() OVER () AS focal_row_id,
            LEAST(subject_cui_primary, object_cui_primary) AS node_a,
            GREATEST(subject_cui_primary, object_cui_primary) AS node_b,
            (
                subject_cui_primary IS NULL
                OR object_cui_primary IS NULL
                OR trim(subject_cui_primary) = ''
                OR trim(object_cui_primary) = ''
            ) AS invalid_endpoint,
            subject_cui_primary = object_cui_primary AS is_self_loop
        FROM {parquet_relation(focal_file)}
        """
    )

    con.execute(
        f"""
        CREATE TEMP VIEW prior_non_self_loop AS
        SELECT
            subject_cui_primary,
            PREDICATE,
            object_cui_primary,
            LEAST(subject_cui_primary, object_cui_primary) AS node_a,
            GREATEST(subject_cui_primary, object_cui_primary) AS node_b
        FROM {parquet_relation(prior_files)}
        WHERE
            subject_cui_primary IS NOT NULL
            AND object_cui_primary IS NOT NULL
            AND trim(subject_cui_primary) != ''
            AND trim(object_cui_primary) != ''
            AND subject_cui_primary != object_cui_primary
        """
    )


def check_focal_uniqueness(con: duckdb.DuckDBPyConnection) -> int:
    duplicate_rows = int_scalar(
        con,
        f"""
        SELECT COALESCE(SUM(n_rows - 1), 0)
        FROM (
            SELECT {", ".join(KEY_COLUMNS)}, COUNT(*) AS n_rows
            FROM focal
            GROUP BY {", ".join(KEY_COLUMNS)}
            HAVING COUNT(*) > 1
        )
        """,
    )
    if duplicate_rows:
        raise ValueError(
            "Focal-year input is not PMID-level unique by the expected key. "
            f"Duplicate excess rows: {duplicate_rows:,}"
        )
    return duplicate_rows


def create_prior_tables(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    con.execute(
        """
        CREATE TEMP TABLE prior_nodes AS
        SELECT subject_cui_primary AS primary_cui
        FROM prior_non_self_loop
        UNION
        SELECT object_cui_primary AS primary_cui
        FROM prior_non_self_loop
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE prior_pair_connections AS
        SELECT DISTINCT node_a, node_b
        FROM prior_non_self_loop
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE prior_exact_edges AS
        SELECT DISTINCT subject_cui_primary, PREDICATE, object_cui_primary
        FROM prior_non_self_loop
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE prior_directed_pairs AS
        SELECT DISTINCT subject_cui_primary, object_cui_primary
        FROM prior_non_self_loop
        """
    )

    return {
        "n_prior_nodes": int_scalar(con, "SELECT COUNT(*) FROM prior_nodes"),
        "n_prior_pair_connections": int_scalar(
            con,
            "SELECT COUNT(*) FROM prior_pair_connections",
        ),
        "n_prior_exact_edges": int_scalar(
            con,
            "SELECT COUNT(*) FROM prior_exact_edges",
        ),
    }


def create_annotated_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        CREATE TEMP TABLE annotated AS
        SELECT
            F.* EXCLUDE (focal_row_id, node_a, node_b, invalid_endpoint, is_self_loop),
            F.node_a,
            F.node_b,
            PN_SUB.primary_cui IS NOT NULL AS subject_seen_in_prior_window,
            PN_OBJ.primary_cui IS NOT NULL AS object_seen_in_prior_window,
            (
                PN_SUB.primary_cui IS NULL
                OR PN_OBJ.primary_cui IS NULL
            ) AS has_new_node,
            PPC.node_a IS NOT NULL AS pair_connected_in_prior_window,
            PEE.subject_cui_primary IS NOT NULL AS exact_edge_seen_in_prior_window,
            RDP.subject_cui_primary IS NOT NULL
                AS reversed_edge_seen_in_prior_window,
            RPE.subject_cui_primary IS NOT NULL
                AS same_predicate_reversed_edge_seen_in_prior_window,
            EXISTS (
                SELECT 1
                FROM prior_exact_edges AS SDP
                WHERE F.subject_cui_primary = SDP.subject_cui_primary
                    AND F.object_cui_primary = SDP.object_cui_primary
                    AND F.PREDICATE != SDP.PREDICATE
            ) AS same_direction_different_predicate_seen_in_prior_window,
            CASE
                WHEN (
                    PN_SUB.primary_cui IS NULL
                    OR PN_OBJ.primary_cui IS NULL
                ) THEN {sql_literal(CATEGORY_NEW_NODE)}
                WHEN PPC.node_a IS NULL
                    THEN {sql_literal(CATEGORY_NEW_COMBINATION)}
                WHEN PEE.subject_cui_primary IS NULL
                    THEN {sql_literal(CATEGORY_NEW_RELATION)}
                ELSE {sql_literal(CATEGORY_REPEATED_TRIPLE)}
            END AS edge_annotation
        FROM focal AS F
        LEFT JOIN prior_nodes AS PN_SUB
            ON F.subject_cui_primary = PN_SUB.primary_cui
        LEFT JOIN prior_nodes AS PN_OBJ
            ON F.object_cui_primary = PN_OBJ.primary_cui
        LEFT JOIN prior_pair_connections AS PPC
            ON F.node_a = PPC.node_a
            AND F.node_b = PPC.node_b
        LEFT JOIN prior_exact_edges AS PEE
            ON F.subject_cui_primary = PEE.subject_cui_primary
            AND F.PREDICATE = PEE.PREDICATE
            AND F.object_cui_primary = PEE.object_cui_primary
        LEFT JOIN prior_directed_pairs AS RDP
            ON F.object_cui_primary = RDP.subject_cui_primary
            AND F.subject_cui_primary = RDP.object_cui_primary
        LEFT JOIN prior_exact_edges AS RPE
            ON F.object_cui_primary = RPE.subject_cui_primary
            AND F.PREDICATE = RPE.PREDICATE
            AND F.subject_cui_primary = RPE.object_cui_primary
        WHERE NOT F.invalid_endpoint
            AND NOT F.is_self_loop
        ORDER BY F.focal_row_id
        """
    )


def write_parquet(con: duckdb.DuckDBPyConnection, output_file: Path) -> None:
    con.execute(
        f"""
        COPY (
            SELECT *
            FROM annotated
        )
        TO {sql_literal(output_file)}
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )


def write_single_row_csv(path: Path, row: dict[str, Any]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)


def collect_self_loop_summary(
    con: duckdb.DuckDBPyConnection,
    focal_year: int,
) -> dict[str, Any]:
    n_focal_rows = int_scalar(con, "SELECT COUNT(*) FROM focal")
    n_invalid_endpoint_rows = int_scalar(
        con,
        "SELECT COUNT(*) FROM focal WHERE invalid_endpoint",
    )
    n_self_loop_rows = int_scalar(
        con,
        "SELECT COUNT(*) FROM focal WHERE NOT invalid_endpoint AND is_self_loop",
    )
    n_non_self_loop_rows = n_focal_rows - n_invalid_endpoint_rows - n_self_loop_rows
    return {
        "pyear": focal_year,
        "n_focal_rows": n_focal_rows,
        "n_invalid_endpoint_rows": n_invalid_endpoint_rows,
        "n_self_loop_rows": n_self_loop_rows,
        "n_non_self_loop_rows": n_non_self_loop_rows,
        "share_invalid_endpoint_rows": safe_share(n_invalid_endpoint_rows, n_focal_rows),
        "share_self_loop_rows": safe_share(n_self_loop_rows, n_focal_rows),
    }


def collect_annotation_summary(
    con: duckdb.DuckDBPyConnection,
    focal_year: int,
    prior_years: list[int],
    prior_counts: dict[str, int],
    self_loop_summary: dict[str, Any],
) -> dict[str, Any]:
    category_counts = {
        category: int_scalar(
            con,
            "SELECT COUNT(*) FROM annotated WHERE edge_annotation = ?",
            [category],
        )
        for category in CATEGORIES
    }
    n_annotated_rows = int_scalar(con, "SELECT COUNT(*) FROM annotated")
    row: dict[str, Any] = {
        "pyear": focal_year,
        "n_focal_rows": self_loop_summary["n_focal_rows"],
        "n_invalid_endpoint_rows": self_loop_summary["n_invalid_endpoint_rows"],
        "n_self_loop_rows": self_loop_summary["n_self_loop_rows"],
        "n_annotated_rows": n_annotated_rows,
        "share_invalid_endpoint_rows": self_loop_summary[
            "share_invalid_endpoint_rows"
        ],
        "share_self_loop_rows": self_loop_summary["share_self_loop_rows"],
        "n_prior_years_found": len(prior_years),
        "prior_years_found": "|".join(str(year) for year in prior_years),
        **prior_counts,
    }

    for category in CATEGORIES:
        row[f"n_{category}"] = category_counts[category]
    for category in CATEGORIES:
        row[f"share_{category}"] = safe_share(
            category_counts[category],
            n_annotated_rows,
        )
    return row


def build_edge_annotation(project_root: Path, focal_year: int, overwrite: bool) -> None:
    prior_years = prior_years_for_focal_year(focal_year)
    prior_files = check_inputs(project_root, focal_year)
    focal_file = yearly_input_file(project_root, focal_year)
    output_file = edge_annotation_file(project_root, focal_year)
    year_summary_file = summary_file(project_root, focal_year)
    loop_summary_file = self_loop_summary_file(project_root, focal_year)
    check_outputs([output_file, year_summary_file, loop_summary_file], overwrite)

    temp_parent = output_file.parent
    temp_parent.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(tempfile.mkdtemp(prefix="duckdb_edge_annotation_", dir=temp_parent))
    print(f"Using DuckDB temp directory: {temp_dir}")

    con = duckdb.connect(database=":memory:")
    try:
        configure_duckdb(con, temp_dir)

        print(f"Focal year: {focal_year}")
        print(f"Prior years: {prior_years}")
        print(f"Focal input: {focal_file}")
        create_input_views(con, focal_file, prior_files)
        duplicate_rows = check_focal_uniqueness(con)
        print(f"Duplicate PMID-exact-triple excess rows: {duplicate_rows:,}")

        prior_counts = create_prior_tables(con)
        print(f"Prior nodes: {prior_counts['n_prior_nodes']:,}")
        print(
            "Prior pair connections: "
            f"{prior_counts['n_prior_pair_connections']:,}"
        )
        print(f"Prior exact edges: {prior_counts['n_prior_exact_edges']:,}")

        self_loop_summary = collect_self_loop_summary(con, focal_year)
        print(
            "Self-loop rows: "
            f"{self_loop_summary['n_self_loop_rows']:,} / "
            f"{self_loop_summary['n_focal_rows']:,}"
        )
        create_annotated_table(con)
        write_parquet(con, output_file)
        annotation_summary = collect_annotation_summary(
            con,
            focal_year,
            prior_years,
            prior_counts,
            self_loop_summary,
        )
        write_single_row_csv(loop_summary_file, self_loop_summary)
        write_single_row_csv(year_summary_file, annotation_summary)
    finally:
        con.close()
        shutil.rmtree(temp_dir, ignore_errors=True)

    print(f"Saved edge annotation to {output_file}")
    print(f"Saved annotation summary to {year_summary_file}")
    print(f"Saved self-loop summary to {loop_summary_file}")


def main() -> None:
    project_root = get_env_path("IC_PROJECT_ROOT", DEFAULT_PROJECT_ROOT)
    focal_year = get_focal_year()
    overwrite = get_env_bool("IC_EDGE_ANNOTATION_OVERWRITE", False)

    print(f"Project root: {project_root}")
    print(f"Focal year: {focal_year}")
    print(f"Overwrite outputs: {overwrite}")
    build_edge_annotation(project_root, focal_year, overwrite)


if __name__ == "__main__":
    main()
