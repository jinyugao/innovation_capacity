"""Summarize same-year multi-PMID exact triples for new knowledge edges."""

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

BASE_YEAR = 1980
N_YEARS = 44
TOP_EXAMPLES_PER_YEAR_CATEGORY = 20
EXAMPLE_PMID_LIMIT = 20

EDGE_ANNOTATION_SUBDIR = Path("data/processed/knowledge_combination/edge_annotation")
OUTPUT_SUBDIR = Path("data/processed/knowledge_combination/parallel_discovery")

EDGE_ANNOTATION_FILE_STEM = "edge_annotation"
SUMMARY_BY_YEAR_CATEGORY_FILE_NAME = (
    "parallel_discovery_summary_by_year_category.csv"
)
SUMMARY_BY_YEAR_FILE_NAME = "parallel_discovery_summary_by_year.csv"
TOP_EXACT_TRIPLES_FILE_NAME = "parallel_discovery_top_exact_triples.csv"

PMID_COLUMN = "PMID"
PYEAR_COLUMN = "PYEAR"
SUBJECT_CUI_COLUMN = "subject_cui_primary"
SUBJECT_NAME_COLUMN = "subject_name_primary"
PREDICATE_COLUMN = "PREDICATE"
OBJECT_CUI_COLUMN = "object_cui_primary"
OBJECT_NAME_COLUMN = "object_name_primary"
EDGE_ANNOTATION_COLUMN = "edge_annotation"

CATEGORY_NEW_NODE = "New_Node"
CATEGORY_NEW_COMBINATION = "New_Combination"
CATEGORY_NEW_RELATION = "New_Relation"
CATEGORY_POOLED_NEW_KNOWLEDGE = "Pooled_New_Knowledge"

NEW_KNOWLEDGE_CATEGORIES = [
    CATEGORY_NEW_NODE,
    CATEGORY_NEW_COMBINATION,
    CATEGORY_NEW_RELATION,
]


def get_env_path(name: str, default: Path) -> Path:
    value = os.environ.get(name)
    return Path(value).expanduser() if value else default


def get_env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


def get_env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    return default if value is None else int(value)


def sql_literal(value: Path | str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def edge_annotation_file(project_root: Path, year: int) -> Path:
    return (
        project_root
        / EDGE_ANNOTATION_SUBDIR
        / f"{EDGE_ANNOTATION_FILE_STEM}_{year}.parquet"
    )


def expected_years() -> list[int]:
    return list(range(BASE_YEAR, BASE_YEAR + N_YEARS))


def input_files(project_root: Path) -> list[Path]:
    return [edge_annotation_file(project_root, year) for year in expected_years()]


def output_files(project_root: Path) -> list[Path]:
    return [
        project_root / OUTPUT_SUBDIR / SUMMARY_BY_YEAR_CATEGORY_FILE_NAME,
        project_root / OUTPUT_SUBDIR / SUMMARY_BY_YEAR_FILE_NAME,
        project_root / OUTPUT_SUBDIR / TOP_EXACT_TRIPLES_FILE_NAME,
    ]


def parquet_relation(paths: list[Path]) -> str:
    path_list = ", ".join(sql_literal(path) for path in paths)
    return f"read_parquet([{path_list}])"


def check_inputs(paths: list[Path]) -> None:
    missing = [path for path in paths if not path.exists()]
    if missing:
        missing_text = "\n".join(str(path) for path in missing)
        raise FileNotFoundError(f"Missing required input file(s):\n{missing_text}")


def check_outputs(paths: list[Path], overwrite: bool) -> None:
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)

    existing = [path for path in paths if path.exists()]
    if existing and not overwrite:
        existing_text = "\n".join(str(path) for path in existing)
        raise FileExistsError(
            "Output file(s) already exist. Move them, delete them, or rerun "
            f"with IC_PARALLEL_DISCOVERY_OVERWRITE=1:\n{existing_text}"
        )
    if overwrite:
        for path in existing:
            path.unlink()


def configure_duckdb(con: duckdb.DuckDBPyConnection, temp_dir: Path) -> None:
    threads = os.environ.get("IC_DUCKDB_THREADS")
    memory_limit = os.environ.get("IC_DUCKDB_MEMORY_LIMIT")

    con.execute(f"SET temp_directory={sql_literal(temp_dir)}")
    if threads:
        con.execute(f"PRAGMA threads={int(threads)}")
    if memory_limit:
        con.execute(f"PRAGMA memory_limit={sql_literal(memory_limit)}")


def int_scalar(con: duckdb.DuckDBPyConnection, query: str) -> int:
    value = con.execute(query).fetchone()[0]
    return 0 if value is None else int(value)


def create_base_tables(con: duckdb.DuckDBPyConnection, annotation_files: list[Path]) -> None:
    category_literals = ", ".join(sql_literal(category) for category in NEW_KNOWLEDGE_CATEGORIES)
    con.execute(
        f"""
        CREATE TEMP TABLE base_edges AS
        SELECT
            TRY_CAST({PYEAR_COLUMN} AS INTEGER) AS pyear,
            CAST({PMID_COLUMN} AS VARCHAR) AS pmid,
            {EDGE_ANNOTATION_COLUMN},
            {SUBJECT_CUI_COLUMN},
            {SUBJECT_NAME_COLUMN},
            {PREDICATE_COLUMN},
            {OBJECT_CUI_COLUMN},
            {OBJECT_NAME_COLUMN},
            concat_ws(
                '\t',
                {SUBJECT_CUI_COLUMN},
                {PREDICATE_COLUMN},
                {OBJECT_CUI_COLUMN}
            ) AS exact_triple_key
        FROM {parquet_relation(annotation_files)}
        WHERE
            TRY_CAST({PYEAR_COLUMN} AS INTEGER)
                BETWEEN {BASE_YEAR} AND {BASE_YEAR + N_YEARS - 1}
            AND {EDGE_ANNOTATION_COLUMN} IN ({category_literals})
            AND {PMID_COLUMN} IS NOT NULL
            AND trim(CAST({PMID_COLUMN} AS VARCHAR)) != ''
            AND {SUBJECT_CUI_COLUMN} IS NOT NULL
            AND {PREDICATE_COLUMN} IS NOT NULL
            AND {OBJECT_CUI_COLUMN} IS NOT NULL
            AND trim({SUBJECT_CUI_COLUMN}) != ''
            AND trim({PREDICATE_COLUMN}) != ''
            AND trim({OBJECT_CUI_COLUMN}) != ''
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE base_edges_with_pooled AS
        SELECT *
        FROM base_edges

        UNION ALL

        SELECT
            pyear,
            pmid,
            {sql_literal(CATEGORY_POOLED_NEW_KNOWLEDGE)}
                AS {EDGE_ANNOTATION_COLUMN},
            {SUBJECT_CUI_COLUMN},
            {SUBJECT_NAME_COLUMN},
            {PREDICATE_COLUMN},
            {OBJECT_CUI_COLUMN},
            {OBJECT_NAME_COLUMN},
            exact_triple_key
        FROM base_edges
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE triple_counts AS
        SELECT
            pyear,
            edge_annotation,
            exact_triple_key,
            MIN(subject_cui_primary) AS subject_cui_primary,
            MIN(subject_name_primary) AS subject_name_primary,
            MIN(PREDICATE) AS PREDICATE,
            MIN(object_cui_primary) AS object_cui_primary,
            MIN(object_name_primary) AS object_name_primary,
            COUNT(*) AS n_rows,
            COUNT(DISTINCT pmid) AS n_pmids
        FROM base_edges_with_pooled
        GROUP BY pyear, edge_annotation, exact_triple_key
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE year_category_denominators AS
        SELECT
            pyear,
            edge_annotation,
            COUNT(*) AS n_rows,
            COUNT(DISTINCT pmid) AS n_unique_pmids
        FROM base_edges_with_pooled
        GROUP BY pyear, edge_annotation
        """
    )


def write_summary_by_year_category(
    con: duckdb.DuckDBPyConnection,
    output_file: Path,
) -> None:
    con.execute(
        f"""
        COPY (
            WITH triple_summary AS (
                SELECT
                    pyear,
                    edge_annotation,
                    COUNT(*) AS n_unique_exact_triples,
                    SUM(CASE WHEN n_pmids = 1 THEN 1 ELSE 0 END)
                        AS n_single_pmid_exact_triples,
                    SUM(CASE WHEN n_pmids > 1 THEN 1 ELSE 0 END)
                        AS n_multi_pmid_exact_triples,
                    SUM(CASE WHEN n_pmids = 1 THEN n_rows ELSE 0 END)
                        AS n_rows_in_single_pmid_exact_triples,
                    SUM(CASE WHEN n_pmids > 1 THEN n_rows ELSE 0 END)
                        AS n_rows_in_multi_pmid_exact_triples
                FROM triple_counts
                WHERE edge_annotation != {sql_literal(CATEGORY_POOLED_NEW_KNOWLEDGE)}
                GROUP BY pyear, edge_annotation
            )
            SELECT
                D.pyear,
                D.edge_annotation,
                D.n_rows,
                D.n_unique_pmids,
                T.n_unique_exact_triples,
                T.n_single_pmid_exact_triples,
                T.n_multi_pmid_exact_triples,
                T.n_multi_pmid_exact_triples::DOUBLE
                    / NULLIF(T.n_unique_exact_triples, 0)
                    AS share_multi_pmid_exact_triples,
                T.n_rows_in_single_pmid_exact_triples,
                T.n_rows_in_multi_pmid_exact_triples,
                T.n_rows_in_multi_pmid_exact_triples::DOUBLE
                    / NULLIF(D.n_rows, 0)
                    AS share_rows_in_multi_pmid_exact_triples
            FROM year_category_denominators AS D
            INNER JOIN triple_summary AS T
                USING (pyear, edge_annotation)
            WHERE D.edge_annotation != {sql_literal(CATEGORY_POOLED_NEW_KNOWLEDGE)}
            ORDER BY pyear, edge_annotation
        )
        TO {sql_literal(output_file)}
        (FORMAT CSV, HEADER TRUE)
        """
    )


def write_summary_by_year(
    con: duckdb.DuckDBPyConnection,
    output_file: Path,
) -> None:
    con.execute(
        f"""
        COPY (
            WITH triple_summary AS (
                SELECT
                    pyear,
                    COUNT(*) AS n_unique_exact_triples,
                    SUM(CASE WHEN n_pmids = 1 THEN 1 ELSE 0 END)
                        AS n_single_pmid_exact_triples,
                    SUM(CASE WHEN n_pmids > 1 THEN 1 ELSE 0 END)
                        AS n_multi_pmid_exact_triples,
                    SUM(CASE WHEN n_pmids = 1 THEN n_rows ELSE 0 END)
                        AS n_rows_in_single_pmid_exact_triples,
                    SUM(CASE WHEN n_pmids > 1 THEN n_rows ELSE 0 END)
                        AS n_rows_in_multi_pmid_exact_triples
                FROM triple_counts
                WHERE edge_annotation = {sql_literal(CATEGORY_POOLED_NEW_KNOWLEDGE)}
                GROUP BY pyear
            ),
            denominator AS (
                SELECT
                    pyear,
                    n_rows,
                    n_unique_pmids
                FROM year_category_denominators
                WHERE edge_annotation = {sql_literal(CATEGORY_POOLED_NEW_KNOWLEDGE)}
            )
            SELECT
                D.pyear,
                D.n_rows,
                D.n_unique_pmids,
                T.n_unique_exact_triples,
                T.n_single_pmid_exact_triples,
                T.n_multi_pmid_exact_triples,
                T.n_multi_pmid_exact_triples::DOUBLE
                    / NULLIF(T.n_unique_exact_triples, 0)
                    AS share_multi_pmid_exact_triples,
                T.n_rows_in_single_pmid_exact_triples,
                T.n_rows_in_multi_pmid_exact_triples,
                T.n_rows_in_multi_pmid_exact_triples::DOUBLE
                    / NULLIF(D.n_rows, 0)
                    AS share_rows_in_multi_pmid_exact_triples
            FROM denominator AS D
            INNER JOIN triple_summary AS T
                USING (pyear)
            ORDER BY D.pyear
        )
        TO {sql_literal(output_file)}
        (FORMAT CSV, HEADER TRUE)
        """
    )


def write_top_exact_triples(
    con: duckdb.DuckDBPyConnection,
    output_file: Path,
    top_n: int,
    example_pmid_limit: int,
) -> None:
    con.execute(
        f"""
        COPY (
            WITH ranked_triples AS (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY pyear, edge_annotation
                        ORDER BY
                            n_pmids DESC,
                            n_rows DESC,
                            exact_triple_key
                    ) AS top_rank
                FROM triple_counts
                WHERE n_pmids > 1
            ),
            top_triples AS (
                SELECT *
                FROM ranked_triples
                WHERE top_rank <= {top_n}
            ),
            ranked_pmids AS (
                SELECT DISTINCT
                    B.pyear,
                    B.edge_annotation,
                    B.exact_triple_key,
                    B.pmid,
                    row_number() OVER (
                        PARTITION BY
                            B.pyear,
                            B.edge_annotation,
                            B.exact_triple_key
                        ORDER BY TRY_CAST(B.pmid AS BIGINT) NULLS LAST, B.pmid
                    ) AS pmid_rank
                FROM base_edges_with_pooled AS B
                INNER JOIN top_triples AS T
                    USING (pyear, edge_annotation, exact_triple_key)
            ),
            example_pmids AS (
                SELECT
                    pyear,
                    edge_annotation,
                    exact_triple_key,
                    string_agg(pmid, '|' ORDER BY pmid_rank) AS example_pmids
                FROM ranked_pmids
                WHERE pmid_rank <= {example_pmid_limit}
                GROUP BY pyear, edge_annotation, exact_triple_key
            )
            SELECT
                T.pyear,
                T.edge_annotation,
                T.top_rank,
                T.subject_cui_primary,
                T.subject_name_primary,
                T.PREDICATE,
                T.object_cui_primary,
                T.object_name_primary,
                T.n_rows,
                T.n_pmids,
                T.n_pmids::DOUBLE / NULLIF(D.n_unique_pmids, 0)
                    AS share_pmids_in_year_category,
                E.example_pmids
            FROM top_triples AS T
            INNER JOIN year_category_denominators AS D
                USING (pyear, edge_annotation)
            LEFT JOIN example_pmids AS E
                USING (pyear, edge_annotation, exact_triple_key)
            ORDER BY T.pyear, T.edge_annotation, T.top_rank
        )
        TO {sql_literal(output_file)}
        (FORMAT CSV, HEADER TRUE)
        """
    )


def build_parallel_discovery_summary(
    project_root: Path,
    overwrite: bool,
    top_n: int,
    example_pmid_limit: int,
) -> None:
    annotations = input_files(project_root)
    outputs = output_files(project_root)
    check_inputs(annotations)
    check_outputs(outputs, overwrite)

    temp_parent = project_root / OUTPUT_SUBDIR
    temp_parent.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(
        tempfile.mkdtemp(prefix="duckdb_parallel_discovery_", dir=temp_parent)
    )
    print(f"Using DuckDB temp directory: {temp_dir}")

    con = duckdb.connect(database=":memory:")
    try:
        configure_duckdb(con, temp_dir)
        print(f"Project root: {project_root}")
        print(f"Years: {BASE_YEAR}-{BASE_YEAR + N_YEARS - 1}")
        print(f"Input edge-annotation files: {len(annotations):,}")
        print(f"Overwrite outputs: {overwrite}")
        print(f"Top examples per year-category: {top_n}")
        print(f"Example PMID limit per top triple: {example_pmid_limit}")

        create_base_tables(con, annotations)
        print(f"Base new-knowledge rows: {int_scalar(con, 'SELECT COUNT(*) FROM base_edges'):,}")
        print(
            "Base rows including pooled category: "
            f"{int_scalar(con, 'SELECT COUNT(*) FROM base_edges_with_pooled'):,}"
        )
        print(f"Triple-count rows: {int_scalar(con, 'SELECT COUNT(*) FROM triple_counts'):,}")

        write_summary_by_year_category(con, outputs[0])
        print(f"Saved year-category summary to {outputs[0]}")
        write_summary_by_year(con, outputs[1])
        print(f"Saved yearly summary to {outputs[1]}")
        write_top_exact_triples(con, outputs[2], top_n, example_pmid_limit)
        print(f"Saved top exact triples to {outputs[2]}")
    finally:
        con.close()
        shutil.rmtree(temp_dir, ignore_errors=True)


def main() -> None:
    project_root = get_env_path("IC_PROJECT_ROOT", DEFAULT_PROJECT_ROOT)
    overwrite = get_env_bool("IC_PARALLEL_DISCOVERY_OVERWRITE", False)
    top_n = get_env_int(
        "IC_PARALLEL_DISCOVERY_TOP_N",
        TOP_EXAMPLES_PER_YEAR_CATEGORY,
    )
    example_pmid_limit = get_env_int(
        "IC_PARALLEL_DISCOVERY_EXAMPLE_PMID_LIMIT",
        EXAMPLE_PMID_LIMIT,
    )
    build_parallel_discovery_summary(
        project_root,
        overwrite,
        top_n,
        example_pmid_limit,
    )
    print("Parallel discovery summary construction complete.")


if __name__ == "__main__":
    main()
